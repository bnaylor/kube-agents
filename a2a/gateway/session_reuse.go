package gateway

import (
	"context"
	"slices"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

const (
	// noticeSessionPaused is posted in a conversation whose idle pod the
	// cap evicted to make room for another. Nothing it said is lost: the
	// stream has every turn, and the pod its next message starts reads them
	// in the primer.
	noticeSessionPaused = "⏸️ this session was paused to make room for another conversation; your next message here starts it fresh with the conversation so far"
	// noteTaskOverdue is the supervisor's terminal for a task its reused
	// pod did not finish inside the task deadline plus the pod grace.
	noteTaskOverdue = "the session worker did not finish this task within its deadline; declared failed by its supervisor, and the worker was retired"
)

// reuseLivePod reports whether the conversation's next task can go to the pod
// it already has, and when it can, points the record back at that pod's
// session. That is the whole of the reuse: the bus session name stays, so the
// capability is minted for the same delegate, the author set carries on
// (currentSessionAuthors resets only when BusSession moves), and
// ensureSessionPod finds PodName set and spawns nothing. The live pod's
// adapter takes the task off its own subjects (workeradapter.runSessionLoop).
//
// It takes the pod only when all of these hold, and otherwise the caller
// spawns as before, cold, with the primer:
//   - reuse is on (Config.SessionReuse), so switching it off stops reuse at
//     once, for pods already running too;
//   - the pod was spawned to serve more than one task (PodReuse, and the
//     pod's own annotation in Reusable), so a pod from an older gateway or
//     with reuse switched off is never handed a second task;
//   - nothing holds the conversation: an ActiveTask here is a stopped task
//     still finishing (a running one would have been a steer), and its pod
//     is retired as before rather than handed a task behind it;
//   - the API says the pod is running, its worker has not exited, it is not
//     being deleted, and it has room left for a whole task. A failed read is
//     a no: spawning fresh is always safe, and routing to a pod nothing
//     vouched for is the one way to lose a turn.
func (g *Gateway) reuseLivePod(ctx context.Context, rec *SessionRecord) bool {
	if g.spawner == nil || !g.cfg.SessionReuse || !rec.PodReuse || rec.PodName == "" || rec.BusSession == "" ||
		rec.PodName != rec.BusSession || rec.ActiveTask != nil {
		return false
	}
	ok, err := g.spawner.Reusable(ctx, rec.PodName)
	if err != nil {
		g.log.Warn("session pod liveness read failed; starting a fresh pod", "conversation", rec.Key, "pod", rec.PodName, "err", err)
		return false
	}
	if !ok {
		return false
	}
	rec.Addressee = rec.BusSession
	g.log.Info("routing the turn to the live session pod", "conversation", rec.Key, "pod", rec.PodName)
	return true
}

// nextIncarnation readies the record for a new task on the session route: the
// live pod when it can take it (reuseLivePod), a fresh incarnation otherwise
// (freshIncarnation, which holds the cap and retires the old pod). False means
// the turn was refused and a post has said so.
func (g *Gateway) nextIncarnation(ctx context.Context, rec *SessionRecord) bool {
	if g.reuseLivePod(ctx, rec) {
		return true
	}
	return g.freshIncarnation(ctx, rec)
}

// evictIdleSession frees a slot at the session cap by retiring the session
// pod that has been idle longest: a live pod whose conversation has no task
// at all, oldest last user message first. It reports whether a pod went.
//
// Idle is read off each conversation's own record, under that
// conversation's lock, and re-checked there, because the lock is what orders
// a reap or an eviction against a turn: a pod whose conversation is running
// anything (a task, a stopped task finishing, a delegated child its wake
// will need) is never a candidate. The lock is only tried, never waited on:
// the caller holds its own conversation's lock, so waiting could deadlock
// against a conversation evicting in the other direction, and a conversation
// whose lock is held is busy anyway.
//
// The evicted conversation is told in one line. Its next message finds no
// pod and spawns cold with the primer, as after a reap.
func (g *Gateway) evictIdleSession(ctx context.Context, requester *SessionRecord) bool {
	pods, err := g.spawner.SessionPods(ctx)
	if err != nil {
		g.log.Error("session cap: pod list for eviction failed", "err", err)
		return false
	}
	type candidate struct {
		pod       sessionPod
		idleSince time.Time
	}
	var candidates []candidate
	for _, p := range pods {
		if p.SessionKey == "" || p.SessionKey == requester.Key {
			continue
		}
		rec, err := g.reg.Get(ctx, p.SessionKey)
		switch {
		case err != nil:
			continue
		case rec == nil || rec.PodName != p.PodName:
			// Untracked: its conversation no longer names it (a retirement
			// whose delete failed), so nothing will route to it or reap it,
			// and a reused pod idles until its lifetime ends. It goes first.
			candidates = append(candidates, candidate{p, time.Time{}})
		case rec.ActiveTask == nil:
			candidates = append(candidates, candidate{p, rec.idleSince()})
		}
	}
	slices.SortFunc(candidates, func(a, b candidate) int { return a.idleSince.Compare(b.idleSince) })
	for _, c := range candidates {
		if g.evictSession(ctx, c.pod) {
			return true
		}
	}
	return false
}

// evictSession retires one idle conversation's pod under that conversation's
// lock, if the lock is free and the conversation is still idle on its fresh
// record.
func (g *Gateway) evictSession(ctx context.Context, p sessionPod) bool {
	l := g.lockSession(p.SessionKey)
	if !l.TryLock() {
		return false
	}
	defer l.Unlock()
	rec, err := g.reg.Get(ctx, p.SessionKey)
	if err != nil {
		return false
	}
	// Re-read under the lock, which every spawn and every record write for
	// that conversation holds: a pod its record does not name here is
	// untracked, not one whose spawn is still being written down.
	if rec == nil || rec.PodName != p.PodName {
		if err := g.spawner.Delete(ctx, p.PodName); err != nil {
			g.log.Error("session cap: untracked pod delete failed", "pod", p.PodName, "err", err)
			return false
		}
		g.log.Info("session cap: deleted an untracked session pod", "conversation", p.SessionKey, "pod", p.PodName)
		return true
	}
	if rec.ActiveTask != nil {
		return false
	}
	if err := g.spawner.Delete(ctx, p.PodName); err != nil {
		g.log.Error("session cap: eviction delete failed", "conversation", rec.Key, "pod", p.PodName, "err", err)
		return false
	}
	rec.PodName = ""
	if err := g.reg.Put(ctx, rec); err != nil {
		// The pod is gone either way; a record still naming it is what
		// reuseLivePod's liveness read exists for.
		g.log.Error("session cap: eviction record write failed", "conversation", rec.Key, "err", err)
	}
	g.log.Info("session cap: evicted the longest-idle session pod", "conversation", rec.Key, "pod", p.PodName,
		"idleSince", rec.idleSince())
	g.post(rec.Key, noticeSessionPaused)
	return true
}

// taskOverdue reports a running task on a reused pod past the bound a
// one-task pod's activeDeadlineSeconds used to give it: the task deadline
// plus the pod grace, from its submission. The adapter's own deadline fires
// well before this, so reaching it means the adapter is wedged, or its
// terminal never reached the gateway; a reused pod's own deadline is hours
// away and would not save the conversation.
func (g *Gateway) taskOverdue(rec *SessionRecord, now time.Time) bool {
	active := rec.ActiveTask
	return rec.PodReuse && rec.PodName != "" && rec.PodName == rec.BusSession &&
		active != nil && !active.Detached && !active.SubmittedAt.IsZero() &&
		rec.AddresseeFor(active.TaskID) == rec.BusSession &&
		now.Sub(active.SubmittedAt) >= g.cfg.TaskDeadline+podDeadlineGrace
}

// retireOverdue closes an overdue task as its supervisor and retires the pod
// that held it, under the conversation's lock and on the fresh record. The
// order is the deletion rule's: the terminal goes on the task's `…supervisor`
// subject first, and a terminal that cannot be ruled out (the stream not
// answering) or published keeps the pod for the next pass. A task already
// final on the stream is the relay's to deliver, and is left to it.
func (g *Gateway) retireOverdue(ctx context.Context, key string) {
	l := g.lockSession(key)
	l.Lock()
	defer l.Unlock()
	rec, err := g.reg.Get(ctx, key)
	if err != nil || rec == nil || !g.taskOverdue(rec, time.Now()) {
		return
	}
	active := rec.ActiveTask
	task, err := g.client.TasksGet(ctx, rec.BusSession, active.TaskID)
	if err == nil && task.Final {
		return
	}
	if err != nil && !isTaskNotFound(err) {
		g.log.Error("overdue check: replay failed; retrying next pass", "task", active.TaskID, "err", err)
		return
	}
	if err := g.publishSupervisorTerminal(ctx, rec.BusSession, active.TaskID, rec.ContextID, active.CorrelationID,
		lib.StateFailed, noteTaskOverdue); err != nil {
		g.log.Error("overdue check: supervisor terminal failed; keeping the pod", "task", active.TaskID, "err", err)
		return
	}
	if err := g.spawner.Delete(ctx, rec.PodName); err != nil {
		// The terminal is out, so the relay releases the task, and the pod,
		// still on the record, goes at the idle TTL like any idle pod.
		g.log.Error("overdue check: pod delete failed; the reap retires it once idle", "pod", rec.PodName, "err", err)
		return
	}
	g.log.Warn("overdue check: closed a wedged task and retired its pod", "task", active.TaskID, "pod", rec.PodName,
		"age", time.Since(active.SubmittedAt).Round(time.Second))
	rec.PodName = ""
	if err := g.reg.Put(ctx, rec); err != nil {
		g.log.Error("overdue check: record write failed", "conversation", rec.Key, "err", err)
	}
}
