package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

const (
	// reapInterval paces the idle scan; reapPassTimeout bounds one pass so
	// a hung registry or API call cannot make passes pile up. Same clock
	// and reasoning as the orphan sweep's pair in spawn.go.
	reapInterval    = time.Minute
	reapPassTimeout = time.Minute
	// primerTaskResultCap bounds one task's result text in the rehydration
	// primer, so one giant artifact cannot crowd every other task out of a
	// fresh pod's first input.
	primerTaskResultCap = 2000
)

// noFirstEventNotice is what the reap scan posts, once per task, when a
// task has produced nothing on its event stream past FirstEventGrace and
// nobody has spoken since: the task id, the grace, and what the next
// message will do. Nothing is released here (noticeNoFirstEvent says why),
// so the line hedges on a start that is merely late.
const noFirstEventNotice = "⚠️ task `%s` has produced nothing on its event stream in %s; unless it starts first, your next message here starts a new task instead of going to it"

// reapLoop enforces the idle TTL — a session silent past the TTL loses its
// pod — and the ask bound (boundAskCopy), which runs on every record the
// scan visits, pod or no pod. It also enforces SessionTTL, deleting session
// records that have been idle past the retention horizon.
func (g *Gateway) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.reapOnce(ctx)
		}
	}
}

func (g *Gateway) reapOnce(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, reapPassTimeout)
	defer cancel()

	g.mu.Lock()
	cursor := g.reapCursor
	g.mu.Unlock()

	nextCursor, done, err := g.reg.ScanSessions(ctx, cursor, func(rec *SessionRecord) (bool, error) {
		g.reapSession(ctx, rec)
		if g.reapScanHook != nil {
			return g.reapScanHook(rec), nil
		}
		return true, nil
	})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		g.log.Error("reap: session scan failed", "err", err, "cursor", cursor)
	}

	g.mu.Lock()
	if done {
		g.reapCursor = ""
	} else if nextCursor != "" {
		g.reapCursor = nextCursor
	}
	g.mu.Unlock()
}

func (g *Gateway) reapSession(ctx context.Context, rec *SessionRecord) {
	g.boundAskCopy(ctx, rec)
	g.noticeNoFirstEvent(ctx, rec)

	// Check if the session record itself has outlived the retention horizon.
	// Prune records older than SessionTTL whose pod has been reaped (or never
	// incarnated). If an ActiveTask is present, only prune if it has also
	// outlived its execution deadline (stale/abandoned executor).
	if g.cfg.SessionTTL > 0 && rec.PodName == "" &&
		!rec.LastActivity.IsZero() &&
		time.Since(rec.LastActivity) >= g.cfg.SessionTTL {
		if rec.ActiveTask != nil && !rec.ActiveTask.SubmittedAt.IsZero() &&
			time.Since(rec.ActiveTask.SubmittedAt) < g.cfg.TaskDeadline {
			return
		}
		l := g.lockSession(rec.Key)
		l.Lock()
		fresh, err := g.reg.Get(ctx, rec.Key)
		if err == nil && fresh != nil && fresh.PodName == "" &&
			!fresh.LastActivity.IsZero() &&
			time.Since(fresh.LastActivity) >= g.cfg.SessionTTL {
			if fresh.ActiveTask != nil && !fresh.ActiveTask.SubmittedAt.IsZero() &&
				time.Since(fresh.ActiveTask.SubmittedAt) < g.cfg.TaskDeadline {
				l.Unlock()
				return
			}
			if err := g.reg.DeleteSession(ctx, fresh.Key); err != nil {
				g.log.Error("reap: session record delete failed", "session", fresh.Key, "err", err)
			} else {
				g.log.Info("reaped expired session record", "session", fresh.Key, "lastActivity", fresh.LastActivity)
				if fresh.ActiveTask != nil {
					_ = g.reg.DropTask(ctx, fresh.ActiveTask.TaskID)
					g.mu.Lock()
					delete(g.relays, fresh.ActiveTask.TaskID)
					delete(g.taskSessions, fresh.ActiveTask.TaskID)
					g.mu.Unlock()
				}
			}
			l.Unlock()
			return
		}
		l.Unlock()
	}

	if rec.PodName == "" {
		return // nothing incarnated (the Hermes-first world, or already reaped)
	}
	if rec.ActiveTask != nil && !rec.ActiveTask.Detached {
		return // never delete a pod out from under a running task
	}
	if time.Since(rec.LastActivity) < g.cfg.IdleTTL {
		return
	}
	l := g.lockSession(rec.Key)
	l.Lock()
	// Re-run every predicate on the fresh record under the lock: a
	// message that arrived between scan and lock may have started a task
	// or reset the idle clock, and reap must never delete a pod out from
	// under either.
	fresh, err := g.reg.Get(ctx, rec.Key)
	if err != nil || fresh == nil || fresh.PodName == "" ||
		(fresh.ActiveTask != nil && !fresh.ActiveTask.Detached) ||
		time.Since(fresh.LastActivity) < g.cfg.IdleTTL {
		l.Unlock()
		return
	}
	// A detached task does not exempt the session, so reap may delete a
	// pod whose harness is still working — the supervisor rule is what
	// keeps that from being a silent stop: its terminal `canceled` goes
	// on the stream before the pod goes. A publish failure keeps the
	// pod (and the reap retries next cycle) rather than stranding the
	// task non-terminal for the retention window.
	if !g.closeDetachedBeforeDelete(ctx, fresh) {
		l.Unlock()
		return
	}
	if g.spawner != nil {
		if err := g.spawner.Delete(ctx, fresh.PodName); err != nil {
			g.log.Error("reap: pod delete failed", "pod", fresh.PodName, "err", err)
			l.Unlock()
			return
		}
	}
	g.log.Info("reaped idle session", "session", fresh.Key, "pod", fresh.PodName)
	// The pod was an incarnation, not the identity: contextId persists
	// until SessionTTL expires.
	fresh.PodName = ""
	if err := g.reg.Put(ctx, fresh); err != nil {
		g.log.Error("reap: record write failed", "session", fresh.Key, "err", err)
	}
	l.Unlock()
}

// boundAskCopy is the independent bound the content posture owes the `ask`
// copy in session-state. The copy's justification — same text on the
// W-bounded stream, deleted with the active-task record at the terminal
// event — holds only where a terminal is guaranteed, and the spec names the
// cases where it is not (a wedged adapter until every pod carries its
// deadline; fixed-route executors with no janitor until stage 3). So an ask
// older than AskTTL is cleared here, in the same scan that reaps — content
// only: the task record itself, its serialization, and its detach state are
// untouched, because this bound is about the copy's horizon, not the
// task's lifecycle.
func (g *Gateway) boundAskCopy(ctx context.Context, rec *SessionRecord) {
	active := rec.ActiveTask
	if active == nil || active.Ask == "" || active.SubmittedAt.IsZero() ||
		time.Since(active.SubmittedAt) < g.cfg.AskTTL {
		return
	}
	l := g.lockSession(rec.Key)
	l.Lock()
	defer l.Unlock()
	// Same discipline as the reap: re-check on the fresh record under the
	// lock, and clear only the copy the scan saw expire.
	fresh, err := g.reg.Get(ctx, rec.Key)
	if err != nil || fresh == nil || fresh.ActiveTask == nil ||
		fresh.ActiveTask.TaskID != active.TaskID || fresh.ActiveTask.Ask == "" ||
		fresh.ActiveTask.SubmittedAt.IsZero() ||
		time.Since(fresh.ActiveTask.SubmittedAt) < g.cfg.AskTTL {
		return
	}
	fresh.ActiveTask.Ask = ""
	if err := g.reg.Put(ctx, fresh); err != nil {
		g.log.Error("ask bound: record write failed", "session", fresh.Key, "err", err)
		return
	}
	g.log.Info("ask bound: cleared an ask copy past its TTL", "session", fresh.Key, "taskId", fresh.ActiveTask.TaskID)
}

// buildRehydrationPrimer folds the context's tasks from JetStream into a
// transcript primer for a fresh pod — the next incarnation's first input.
// Task-stream retention bounds how far back this reaches, deliberately: a
// three-day-silent thread restarting with fresh context beats a bot that
// suddenly remembers June. Session files are cache; the stream is the
// record.
func (g *Gateway) buildRehydrationPrimer(ctx context.Context, rec *SessionRecord) string {
	var b strings.Builder
	b.WriteString("Transcript primer, replayed from the task stream for this conversation:\n")
	found := 0
	for _, ref := range rec.Tasks {
		task, err := g.client.TasksGet(ctx, ref.Addressee, ref.ID)
		if err != nil {
			continue // aged out of retention, or never produced events
		}
		found++
		fmt.Fprintf(&b, "\n--- task %s (%s)\n", task.ID, task.State)
		if art := task.Artifact(lib.ArtifactResult); art != nil {
			// truncateRunes, not a byte cut: the primer is annotated onto
			// the next pod and marshalled to JSON on the way, where invalid
			// UTF-8 becomes U+FFFD rather than an error. spawn.go's outer
			// truncateRunes only guards the primer's tail; a byte cut here
			// lands mid-transcript and survives it.
			text := truncateRunes(joinTextParts(art.Parts), primerTaskResultCap)
			b.WriteString(text)
			b.WriteString("\n")
		}
	}
	if found == 0 {
		return ""
	}
	return b.String()
}

// noFirstEventPastGrace is the one test for "this task has produced nothing
// past the first-event grace": the stream answered TaskNotFound for the
// task's subjects (both of them; the fold reads them together) and the task
// is older than grace. A pure function of its arguments, so the heal, the
// reap scan's notice, and any later caller that has to tell a task nobody
// took from one in flight (a count of queued tasks, say) all draw the line
// in the same place. Only TaskNotFound qualifies: a transport failure cannot
// rule out events. A task with no SubmittedAt has no age to judge and never
// qualifies. Detach is the caller's business: a detached task no longer
// holds the conversation, so neither the heal nor the notice looks at one.
func noFirstEventPastGrace(active *ActiveTask, streamErr error, grace time.Duration, now time.Time) bool {
	return active != nil && isTaskNotFound(streamErr) &&
		!active.SubmittedAt.IsZero() && now.Sub(active.SubmittedAt) > grace
}

// firstEventOverdue is noFirstEventPastGrace for a caller holding only the
// record: it reads the active task's stream, when the task is old enough
// for the answer to matter, and applies the test. A read and nothing else:
// no lock, no post, no write. A task inside the grace is answered without
// touching the stream, which is what keeps a scan over every record cheap.
func (g *Gateway) firstEventOverdue(ctx context.Context, rec *SessionRecord) bool {
	active := rec.ActiveTask
	if active == nil || active.SubmittedAt.IsZero() ||
		time.Since(active.SubmittedAt) <= g.cfg.FirstEventGrace {
		return false
	}
	_, _, err := g.client.TasksGetAttributed(ctx, rec.AddresseeFor(active.TaskID), active.TaskID)
	return noFirstEventPastGrace(active, err, g.cfg.FirstEventGrace, time.Now())
}

// noticeNoFirstEvent tells a conversation, without waiting for it to speak,
// that its task has produced nothing past FirstEventGrace. The heal says the
// same thing, but only inside the next turn; a human who waits for the
// placeholder to move would otherwise hear nothing at all.
//
// It runs in the reap scan rather than on a timer per task: the scan
// already visits every record every reapInterval, survives a restart
// because the records are in KV, and is where the ask bound, the same
// kind of age bound on the same field, already lives. A per-task timer
// would be lost on a restart and need this scan to re-arm it anyway. The
// cost is latency: the line lands up to one reapInterval after the grace.
//
// It posts and does nothing else. No terminal and no release, for the
// heal's reasons (handleInbound): age alone is not evidence, and a first
// event that is merely late could still arrive and render. The release
// stays the next turn's, where the heal re-reads the stream first.
//
// Once per task, across restarts: the record carries the marker
// (ActiveTask.NoFirstEventNoticeAt), and it is written before the post, so
// a write that fails posts nothing and the next pass tries again, while a
// post that fails after the write is not repeated. At most once, because a
// line that repeats every minute is worse than one that is lost.
func (g *Gateway) noticeNoFirstEvent(ctx context.Context, rec *SessionRecord) {
	active := rec.ActiveTask
	if active == nil || active.Detached || !active.NoFirstEventNoticeAt.IsZero() {
		return
	}
	// The stream read happens before the lock, so a slow read never holds
	// up a turn; everything it decided is re-checked on the fresh record.
	if !g.firstEventOverdue(ctx, rec) {
		return
	}
	l := g.lockSession(rec.Key)
	l.Lock()
	defer l.Unlock()
	fresh, err := g.reg.Get(ctx, rec.Key)
	if err != nil || fresh == nil || fresh.ActiveTask == nil ||
		fresh.ActiveTask.TaskID != active.TaskID || fresh.ActiveTask.Detached ||
		!fresh.ActiveTask.NoFirstEventNoticeAt.IsZero() {
		return
	}
	fresh.ActiveTask.NoFirstEventNoticeAt = time.Now().UTC()
	if err := g.reg.Put(ctx, fresh); err != nil {
		g.log.Error("no-first-event notice: record write failed", "conversation", fresh.Key, "err", err)
		return
	}
	g.log.Info("no first event inside the grace; told the conversation",
		"taskId", active.TaskID, "conversation", fresh.Key, "addressee", fresh.AddresseeFor(active.TaskID),
		"age", time.Since(active.SubmittedAt).Round(time.Second), "grace", g.cfg.FirstEventGrace)
	g.post(fresh.Key, fmt.Sprintf(noFirstEventNotice, active.TaskID, g.cfg.FirstEventGrace))
}
