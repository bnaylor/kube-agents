package gateway

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// reapLoop enforces the idle TTL: a session silent past the TTL loses its
// pod. Nothing is saved first, because the stream already has everything —
// that's the whole point of the transcript of record. The KV entry stays,
// holding the contextId.
func (g *Gateway) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
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
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	recs, err := g.reg.Sessions(ctx)
	if err != nil {
		g.log.Error("reap: session scan failed", "err", err)
		return
	}
	for _, rec := range recs {
		if rec.PodName == "" {
			continue // nothing incarnated (the Hermes-first world, or already reaped)
		}
		if rec.ActiveTask != nil && !rec.ActiveTask.Detached {
			continue // never delete a pod out from under a running task
		}
		if time.Since(rec.LastActivity) < g.cfg.IdleTTL {
			continue
		}
		l := g.lockSession(rec.Key)
		l.Lock()
		fresh, err := g.reg.Get(ctx, rec.Key)
		if err != nil || fresh == nil || fresh.PodName == "" {
			l.Unlock()
			continue
		}
		if g.spawner != nil {
			if err := g.spawner.Delete(ctx, fresh.PodName); err != nil {
				g.log.Error("reap: pod delete failed", "pod", fresh.PodName, "err", err)
				l.Unlock()
				continue
			}
		}
		g.log.Info("reaped idle session", "session", fresh.Key, "pod", fresh.PodName)
		// The pod was an incarnation, not the identity: contextId persists.
		fresh.PodName = ""
		if err := g.reg.Put(ctx, fresh); err != nil {
			g.log.Error("reap: record write failed", "session", fresh.Key, "err", err)
		}
		l.Unlock()
	}
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
	for _, taskID := range rec.TaskIDs {
		task, err := g.client.TasksGet(ctx, rec.Addressee, taskID)
		if err != nil {
			continue // aged out of retention, or never produced events
		}
		found++
		fmt.Fprintf(&b, "\n--- task %s (%s)\n", task.ID, task.State)
		if art := task.Artifact(lib.ArtifactResult); art != nil {
			text := joinTextParts(art.Parts)
			if len(text) > 2000 {
				text = text[:2000] + "…"
			}
			b.WriteString(text)
			b.WriteString("\n")
		}
	}
	if found == 0 {
		return ""
	}
	return b.String()
}
