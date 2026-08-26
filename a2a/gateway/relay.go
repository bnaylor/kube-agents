package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// discordChunk leaves headroom under Discord's 2000-char message cap.
const discordChunk = 1900

// relayState is the in-memory render state for one task's rolling line. It
// is cache: a gateway restart loses it, and the terminal path falls back to
// a stream replay to recover the result — the stream is the record.
type relayState struct {
	state    lib.TaskState
	progress string
	result   []lib.Part
}

// relayEvent relays one status or artifact update into the conversation.
// The gateway never parses harness output — executors already mapped it onto
// A2A events; this renders those events and nothing else.
func (g *Gateway) relayEvent(ctx context.Context, env *lib.Envelope) {
	if env.Kind != lib.KindStatusUpdate && env.Kind != lib.KindArtifactUpdate {
		return
	}
	sessionKey := g.sessionForTask(ctx, env.TaskID)
	if sessionKey == "" {
		// Not a task this gateway submitted (another requester's traffic on
		// the shared events wildcard); not ours to render.
		return
	}

	l := g.lockSession(sessionKey)
	l.Lock()
	defer l.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	rec, err := g.reg.Get(ctx, sessionKey)
	if err != nil || rec == nil {
		g.log.Error("relay: session record missing", "session", sessionKey, "err", err)
		return
	}

	g.mu.Lock()
	rs, ok := g.relays[env.TaskID]
	if !ok {
		rs = &relayState{}
		g.relays[env.TaskID] = rs
	}
	g.mu.Unlock()

	switch env.Kind {
	case lib.KindStatusUpdate:
		var s lib.StatusUpdate
		if err := json.Unmarshal(env.Payload, &s); err != nil {
			g.log.Error("relay: malformed status-update", "taskId", env.TaskID, "err", err)
			return
		}
		g.relayStatus(ctx, rec, rs, env.TaskID, s)
	case lib.KindArtifactUpdate:
		var a lib.ArtifactUpdate
		if err := json.Unmarshal(env.Payload, &a); err != nil {
			g.log.Error("relay: malformed artifact-update", "taskId", env.TaskID, "err", err)
			return
		}
		g.relayArtifact(ctx, rec, rs, env.TaskID, a)
	}

	if err := g.reg.Put(ctx, rec); err != nil {
		g.log.Error("relay: session record write failed", "session", rec.Key, "err", err)
	}
}

func (g *Gateway) relayStatus(ctx context.Context, rec *SessionRecord, rs *relayState, taskID string, s lib.StatusUpdate) {
	rs.state = s.Status.State
	switch {
	case s.Final:
		g.relayTerminal(ctx, rec, rs, taskID, s)
	case s.Status.State == lib.StateInputRequired:
		ask := ""
		if s.Status.Message != nil {
			ask = joinTextParts(s.Status.Message.Parts)
		}
		if ask == "" {
			ask = "the task needs input to continue"
		}
		g.post(rec.Key, "❓ "+ask)
		g.updateRollingLine(rec, taskID, rs)
	default:
		// A non-final status message (eg W7's honest "Hermes cannot absorb
		// mid-run input" answer to a steer) is worth the room seeing.
		if s.Status.Message != nil {
			if note := joinTextParts(s.Status.Message.Parts); note != "" {
				g.post(rec.Key, "ℹ️ "+note)
			}
		}
		g.updateRollingLine(rec, taskID, rs)
	}
}

func (g *Gateway) relayArtifact(ctx context.Context, rec *SessionRecord, rs *relayState, taskID string, a lib.ArtifactUpdate) {
	switch a.Artifact.Name {
	case lib.ArtifactProgress:
		// The rolling progress line: one edited chat message as progress
		// artifacts arrive — no model calls, zero marginal cost.
		if text := lastTextPart(a.Artifact.Parts); text != "" {
			rs.progress = text
		}
		g.updateRollingLine(rec, taskID, rs)
	case lib.ArtifactResult:
		if a.Append {
			rs.result = append(rs.result, a.Artifact.Parts...)
		} else {
			rs.result = append([]lib.Part(nil), a.Artifact.Parts...)
		}
	case lib.ArtifactThinking, lib.ArtifactActivity:
		// Debug/audit views only; never rendered to chat.
	}
}

// relayTerminal posts the deliverable (or the failure) and releases the
// session's serialization.
func (g *Gateway) relayTerminal(ctx context.Context, rec *SessionRecord, rs *relayState, taskID string, s lib.StatusUpdate) {
	result := joinTextParts(rs.result)
	if result == "" && s.Status.State == lib.StateCompleted {
		// Render state is cache; if a restart lost it, the stream still has
		// everything.
		if task, err := g.client.TasksGet(ctx, rec.Addressee, taskID); err == nil {
			if art := task.Artifact(lib.ArtifactResult); art != nil {
				result = joinTextParts(art.Parts)
			}
		} else {
			g.log.Error("relay: terminal replay fallback failed", "taskId", taskID, "err", err)
		}
	}

	switch s.Status.State {
	case lib.StateCompleted:
		if result == "" {
			result = "(completed with a non-text result; see the stream)"
		}
		for _, chunk := range chatChunks(result, discordChunk) {
			g.post(rec.Key, chunk)
		}
	case lib.StateFailed:
		reason := ""
		if s.Status.Message != nil {
			reason = joinTextParts(s.Status.Message.Parts)
		}
		if reason != "" {
			g.post(rec.Key, "❌ failed: "+reason)
		} else {
			g.post(rec.Key, "❌ the task failed")
		}
	case lib.StateCanceled:
		g.post(rec.Key, "🛑 canceled")
	case lib.StateRejected:
		g.post(rec.Key, "🚫 the executor rejected the task")
	}

	if active := rec.ActiveTask; active != nil && active.TaskID == taskID {
		if active.StatusMsgID != "" {
			_ = g.adapter.Edit(rec.Key, active.StatusMsgID, terminalLine(s.Status.State, rs.progress))
		}
		rec.ActiveTask = nil
	}
	g.mu.Lock()
	delete(g.relays, taskID)
	g.mu.Unlock()
}

// updateRollingLine edits the task's single status message in place.
func (g *Gateway) updateRollingLine(rec *SessionRecord, taskID string, rs *relayState) {
	active := rec.ActiveTask
	if active == nil || active.TaskID != taskID || active.StatusMsgID == "" {
		return
	}
	line := statusLine(rs.state, rs.progress)
	if err := g.adapter.Edit(rec.Key, active.StatusMsgID, line); err != nil {
		g.log.Warn("rolling line edit failed", "taskId", taskID, "err", err)
	}
}

func statusLine(state lib.TaskState, progress string) string {
	icon := map[lib.TaskState]string{
		lib.StateSubmitted:     "⏳",
		lib.StateWorking:       "⚙️",
		lib.StateInputRequired: "❓",
	}[state]
	if icon == "" {
		icon = "⏳"
	}
	label := string(state)
	if label == "" {
		label = "submitted"
	}
	line := fmt.Sprintf("%s **%s**", icon, label)
	if progress != "" {
		line += " — " + progress
	}
	return line
}

func terminalLine(state lib.TaskState, progress string) string {
	icon := map[lib.TaskState]string{
		lib.StateCompleted: "✅",
		lib.StateFailed:    "❌",
		lib.StateCanceled:  "🛑",
		lib.StateRejected:  "🚫",
	}[state]
	line := fmt.Sprintf("%s **%s**", icon, state)
	if progress != "" {
		line += " — " + progress
	}
	return line
}

// post writes to the conversation, logging rather than failing the relay —
// chat delivery is best-effort; the stream is the record.
func (g *Gateway) post(conversation, text string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	if _, err := g.adapter.Post(conversation, text); err != nil {
		g.log.Error("post failed", "conversation", conversation, "err", err)
	}
}

// sessionForTask resolves a task to its conversation: the in-memory cache
// first, the KV task index after a restart.
func (g *Gateway) sessionForTask(ctx context.Context, taskID string) string {
	g.mu.Lock()
	key := g.taskSessions[taskID]
	g.mu.Unlock()
	if key != "" {
		return key
	}
	key, err := g.reg.SessionForTask(ctx, taskID)
	if err != nil {
		g.log.Error("task index lookup failed", "taskId", taskID, "err", err)
		return ""
	}
	if key != "" {
		g.mu.Lock()
		g.taskSessions[taskID] = key
		g.mu.Unlock()
	}
	return key
}
