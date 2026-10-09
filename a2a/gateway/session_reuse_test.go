package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

func reuseOn(c *Config) { c.SessionReuse = true }

// startReuseRig is the post-flip gateway (every conversation a session) with
// pod reuse on.
func startReuseRig(t *testing.T) (*rig, *fakeSpawner) {
	t.Helper()
	return startRigWithSpawnerCap(t, RouteSession, 0, reuseOn)
}

func say(r *rig, conv, id, text string) {
	r.adapter.inbox <- InboundMessage{Conversation: conv, Kind: "group", AuthorID: "1001", MessageID: id, Text: text}
}

func waitIdle(t *testing.T, r *rig, conv string) {
	t.Helper()
	waitFor(t, "the turn's terminal relayed", func() bool {
		rec, err := r.g.reg.Get(context.Background(), conv)
		return err == nil && rec != nil && rec.ActiveTask == nil
	})
}

// The second turn of a conversation goes to the pod the first one started:
// no spawn, no delete, the same bus session, and a capability minted for that
// same session as its delegate. Then a stop reaches the task that is running
// in it, not the one before.
func TestASecondTurnGoesToTheLivePod(t *testing.T) {
	r, spawn := startReuseRig(t)
	ctx := context.Background()
	conv := "discord:g1/t-reuse-1"
	say(r, conv, "m1", "first question")
	waitFor(t, "first spawn", func() bool { return len(spawn.calls()) == 1 })
	session := spawn.calls()[0].Session
	spawn.setReusable(session)
	first := r.awaitTask(t, session)
	completeTask(t, r.execFor(t, first, session), "first answer")
	waitIdle(t, r, conv)

	rec, _ := r.g.reg.Get(ctx, conv)
	if !rec.PodReuse || rec.PodName != session {
		t.Fatalf("after the first turn: PodReuse=%v PodName=%q, want a reusable pod %q", rec.PodReuse, rec.PodName, session)
	}

	say(r, conv, "m2", "second question")
	second := awaitSubmission(t, r, session, 1)
	if got := envText(t, second); !strings.Contains(got, "second question") {
		t.Fatalf("the second submission on %s reads %q", session, got)
	}
	if n := len(spawn.calls()); n != 1 {
		t.Fatalf("%d spawns, want 1: the live pod should have taken the second turn", n)
	}
	if d := spawn.deleted(); len(d) != 0 {
		t.Fatalf("pods deleted %v; the live pod must stay", d)
	}
	rec, _ = r.g.reg.Get(ctx, conv)
	if rec.BusSession != session || rec.Addressee != session || rec.PodName != session {
		t.Fatalf("record busSession=%s addressee=%s pod=%s, want all %s", rec.BusSession, rec.Addressee, rec.PodName, session)
	}
	var auth Authority
	if err := json.Unmarshal(second.Authority, &auth); err != nil {
		t.Fatal(err)
	}
	assertRootCapability(t, r, auth, second.TaskID, session)

	say(r, conv, "m3", "stop")
	waitFor(t, "cancel on the running task", func() bool {
		for _, e := range inSubjectEnvelopes(t, r.url, session) {
			if e.Kind == lib.KindCancel {
				return e.TaskID == second.TaskID
			}
		}
		return false
	})
	for _, e := range inSubjectEnvelopes(t, r.url, session) {
		if e.Kind == lib.KindCancel && e.TaskID != second.TaskID {
			t.Fatalf("a cancel reached task %s, not the running %s", e.TaskID, second.TaskID)
		}
	}
}

// Every way the live pod cannot take the turn falls back to today's path:
// retire it, mint a new incarnation, spawn cold.
func TestATurnStartsAFreshPodWhenTheLiveOneCannotTakeIt(t *testing.T) {
	const pod = "chat-otter-r9"
	for _, tc := range []struct {
		name        string
		reuseOff    bool
		podReuse    bool
		reusable    bool
		reusableErr error
		detached    bool
	}{
		{name: "the pod is not live", podReuse: true},
		{name: "the liveness read fails", podReuse: true, reusable: true, reusableErr: errors.New("api down")},
		{name: "an older gateway spawned it", podReuse: false, reusable: true},
		{name: "a stopped task is still finishing on it", podReuse: true, reusable: true, detached: true},
		{name: "reuse is switched off", reuseOff: true, podReuse: true, reusable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tweak := reuseOn
			if tc.reuseOff {
				tweak = func(*Config) {}
			}
			r, spawn := startRigWithSpawnerCap(t, RouteSession, 0, tweak)
			conv := "discord:g1/t-fresh"
			rec := &SessionRecord{
				Key: conv, ContextID: "ctx-fresh", Kind: "group",
				Addressee: pod, BusSession: pod, PodName: pod, PodReuse: tc.podReuse,
				SessionRouted: true, Profile: "chat", LastActivity: time.Now().UTC(),
			}
			if tc.detached {
				rec.ActiveTask = &ActiveTask{TaskID: "task-old", CorrelationID: "corr-old", Detached: true}
				rec.Tasks = []TaskRef{{ID: "task-old", Addressee: pod, Canceled: true}}
			}
			if err := r.g.reg.Put(context.Background(), rec); err != nil {
				t.Fatal(err)
			}
			if tc.reusable {
				spawn.setReusable(pod)
			}
			spawn.reusableErr = tc.reusableErr
			say(r, conv, "m1", "next question")
			waitFor(t, "a fresh spawn", func() bool { return len(spawn.calls()) == 1 })
			if s := spawn.calls()[0].Session; s == pod {
				t.Fatalf("the fresh spawn reused the name %s", s)
			}
			if d := spawn.deleted(); !slices.Contains(d, pod) {
				t.Fatalf("deleted %v, want the old pod retired", d)
			}
			if tc.detached {
				if s := terminalFor(t, r.url, pod, "task-old"); s == nil || s.Status.State != lib.StateCanceled {
					t.Fatalf("the stopped task's terminal = %+v, want canceled before the retirement", s)
				}
			}
		})
	}
}

// On a conversation marked with a bare /session before the flip, a
// "/session <text>" turn is an ordinary turn and goes to the live pod the same
// way.
func TestASessionCommandTurnGoesToTheLivePod(t *testing.T) {
	r, spawn := startRigWithSpawnerCap(t, "platform", 0, reuseOn)
	ctx := context.Background()
	conv := "discord:g1/t-reuse-cmd"
	_, first, session := sessionTurnVia(t, r, spawn, conv, "", "first question")
	spawn.setReusable(session)
	completeTask(t, r.execFor(t, first, session), "first answer")
	waitIdle(t, r, conv)

	say(r, conv, "m2", "/session second question")
	second := awaitSubmission(t, r, session, 1)
	if got := envText(t, second); !strings.Contains(got, "second question") {
		t.Fatalf("the second submission on %s reads %q", session, got)
	}
	if n := len(spawn.calls()); n != 1 {
		t.Fatalf("%d spawns, want 1: the live pod should have taken the /session turn", n)
	}
	if rec, _ := r.g.reg.Get(ctx, conv); rec.BusSession != session {
		t.Fatalf("busSession = %s, want %s", rec.BusSession, session)
	}
}

// A wake into a live pod goes to that pod: the same incarnation, so its
// author set is the delegating turn's already and nothing is re-seeded, and
// the wake still carries via, which is what turns its delegate tool off.
func TestAWakeGoesToTheLivePod(t *testing.T) {
	r, spawn := startRigWithSpawnerCap(t, "platform", 0, reuseOn)
	ctx := context.Background()
	conv := "discord:g1/t-wake-reuse"
	_, session, child := delegated(t, r, spawn, conv, "")
	spawn.setReusable(session)
	before, _ := r.g.reg.Get(ctx, conv)
	authorsBefore := append([]TaskRequester(nil), before.SessionAuthors...)

	completeTask(t, r.execFor(t, child, targetPlatform), "fleet is green")
	wake := awaitSubmission(t, r, session, 1)
	if n := len(spawn.calls()); n != 1 {
		t.Fatalf("%d spawns, want 1: the wake belongs in the live pod", n)
	}
	var auth Authority
	if err := json.Unmarshal(wake.Authority, &auth); err != nil {
		t.Fatal(err)
	}
	if auth.Via == nil || auth.Via.Session != session || auth.Via.TaskID != child.TaskID {
		t.Fatalf("wake via = %+v, want task %s on %s", auth.Via, child.TaskID, session)
	}
	assertRootCapability(t, r, auth, wake.TaskID, session)
	waitFor(t, "the wake on the record", func() bool {
		rec, _ := r.g.reg.Get(ctx, conv)
		return rec.ActiveTask != nil && rec.ActiveTask.TaskID == wake.TaskID
	})
	rec, _ := r.g.reg.Get(ctx, conv)
	if rec.BusSession != session || rec.Addressee != session || rec.SessionAuthorsFor != session {
		t.Fatalf("record busSession=%s addressee=%s authorsFor=%s, want all %s", rec.BusSession, rec.Addressee, rec.SessionAuthorsFor, session)
	}
	for _, a := range authorsBefore {
		if !slices.Contains(rec.SessionAuthors, a) {
			t.Fatalf("the wake dropped author %+v from the live incarnation's set %+v", a, rec.SessionAuthors)
		}
	}
	if d := spawn.deleted(); len(d) != 0 {
		t.Fatalf("pods deleted %v; the live pod must stay", d)
	}
}

// The idle clock is the last user message. An answer that landed a minute ago
// does not keep a pod whose conversation nobody has written in past the TTL,
// and a message inside the TTL keeps it whatever LastActivity says.
func TestTheReapCountsFromTheLastUserMessage(t *testing.T) {
	r, spawn := startReuseRig(t)
	ctx := context.Background()
	now := time.Now().UTC()
	quiet := &SessionRecord{
		Key: "discord:g1/t-reap-quiet", ContextID: "ctx-q", Kind: "group",
		Addressee: "chat-vole-q", BusSession: "chat-vole-q", PodName: "chat-vole-q", PodReuse: true,
		SessionRouted: true, LastUserMessage: now.Add(-40 * time.Minute), LastActivity: now.Add(-time.Minute),
	}
	spoken := &SessionRecord{
		Key: "discord:g1/t-reap-spoken", ContextID: "ctx-s", Kind: "group",
		Addressee: "chat-vole-s", BusSession: "chat-vole-s", PodName: "chat-vole-s", PodReuse: true,
		SessionRouted: true, LastUserMessage: now.Add(-5 * time.Minute), LastActivity: now.Add(-40 * time.Minute),
	}
	for _, rec := range []*SessionRecord{quiet, spoken} {
		if err := r.g.reg.Put(ctx, rec); err != nil {
			t.Fatal(err)
		}
		r.g.reapSession(ctx, rec)
	}
	if d := spawn.deleted(); len(d) != 1 || d[0] != "chat-vole-q" {
		t.Fatalf("reap deleted %v, want only the pod nobody has written to in 40m", d)
	}
}

// A verified turn moves the idle clock; nothing else does.
func TestATurnStampsTheLastUserMessage(t *testing.T) {
	r, spawn := startReuseRig(t)
	conv := "discord:g1/t-stamp"
	say(r, conv, "m1", "hello")
	waitFor(t, "spawn", func() bool { return len(spawn.calls()) == 1 })
	rec, _ := r.g.reg.Get(context.Background(), conv)
	if rec.LastUserMessage.IsZero() || time.Since(rec.LastUserMessage) > time.Minute {
		t.Fatalf("LastUserMessage = %v after a turn", rec.LastUserMessage)
	}
}

// A reused pod's running task past the task deadline plus the pod grace is
// closed by its supervisor and its pod retired; a younger task, a one-task
// pod's (which has its own deadline), and a task already final on the stream
// are left alone.
func TestAnOverdueTaskOnAReusedPodIsClosedAndThePodRetired(t *testing.T) {
	for _, tc := range []struct {
		name     string
		age      time.Duration
		podReuse bool
		final    bool
		retire   bool
	}{
		{name: "overdue", age: defaultTaskDeadline + podDeadlineGrace + time.Minute, podReuse: true, retire: true},
		{name: "inside its bound", age: defaultTaskDeadline, podReuse: true},
		{name: "a one-task pod", age: defaultTaskDeadline + podDeadlineGrace + time.Minute},
		{name: "already final", age: defaultTaskDeadline + podDeadlineGrace + time.Minute, podReuse: true, final: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, spawn := startReuseRig(t)
			ctx := context.Background()
			pod := "chat-lynx-o1"
			rec := &SessionRecord{
				Key: "discord:g1/t-overdue", ContextID: "ctx-o", Kind: "group",
				Addressee: pod, BusSession: pod, PodName: pod, PodReuse: tc.podReuse, SessionRouted: true,
				LastUserMessage: time.Now().UTC(),
				ActiveTask:      &ActiveTask{TaskID: "task-o1", CorrelationID: "corr-o1", SubmittedAt: time.Now().Add(-tc.age)},
				Tasks:           []TaskRef{{ID: "task-o1", Addressee: pod}},
			}
			if err := r.g.reg.Put(ctx, rec); err != nil {
				t.Fatal(err)
			}
			if tc.final {
				origin := &lib.Envelope{Kind: lib.KindMessage, TaskID: "task-o1", ContextID: "ctx-o", CorrelationID: "corr-o1"}
				exec, err := r.bus.NewTaskExecution(origin, lib.Party{Session: pod, AgentType: "test-executor"}, pod)
				if err != nil {
					t.Fatal(err)
				}
				if err := exec.PublishStatus(ctx, lib.StateCompleted, true); err != nil {
					t.Fatal(err)
				}
			}
			r.g.reapSession(ctx, rec)
			sup := supervisorTerminal(t, r, pod, "task-o1")
			fresh, _ := r.g.reg.Get(ctx, rec.Key)
			if tc.retire {
				if sup == nil || sup.Status.State != lib.StateFailed {
					t.Fatalf("supervisor terminal = %+v, want failed", sup)
				}
				if d := spawn.deleted(); len(d) != 1 || d[0] != pod {
					t.Fatalf("deleted %v, want %s", d, pod)
				}
				if fresh.PodName != "" {
					t.Fatalf("PodName = %q after the retirement", fresh.PodName)
				}
				return
			}
			if sup != nil {
				t.Fatalf("a supervisor terminal was published: %+v", sup)
			}
			if d := spawn.deleted(); len(d) != 0 {
				t.Fatalf("deleted %v, want nothing", d)
			}
		})
	}
}

// supervisorTerminal is the final the gateway wrote for a task as its
// supervisor, or nil.
func supervisorTerminal(t *testing.T, r *rig, addressee, taskID string) *lib.StatusUpdate {
	t.Helper()
	envs, err := eventsEnvelopes(r.url, addressee)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range envs {
		var s lib.StatusUpdate
		if e.TaskID == taskID && e.Kind == lib.KindStatusUpdate && e.From.Session == gatewayParty.Session &&
			json.Unmarshal(e.Payload, &s) == nil && s.Final {
			return &s
		}
	}
	return nil
}

// postTextsFor is every post the gateway made into one conversation.
func (a *fakeAdapter) postTextsFor(conversation string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for _, p := range a.posts {
		if p.Conversation == conversation {
			out = append(out, p.Text)
		}
	}
	return out
}

// A reused pod's annotation names its first task only. When it dies on a
// later turn, the sweep has to close that turn, which the record holds as
// active, and not report a clean exit because the first task is final.
func TestTheSweepClosesTheTaskAReusedPodDiedOn(t *testing.T) {
	r, spawn := startReuseRig(t)
	ctx := context.Background()
	pod := "chat-heron-sw"
	rec := &SessionRecord{
		Key: "discord:g1/t-sweep-reuse", ContextID: "ctx-sw", Kind: "group",
		Addressee: pod, BusSession: pod, PodName: pod, PodReuse: true, SessionRouted: true,
		ActiveTask: &ActiveTask{TaskID: "task-sw-3", CorrelationID: "corr-sw-3", SubmittedAt: time.Now()},
		Tasks: []TaskRef{
			{ID: "task-sw-1", Addressee: pod},
			{ID: "task-sw-2", Addressee: pod},
			{ID: "task-sw-3", Addressee: pod},
		},
	}
	if err := r.g.reg.Put(ctx, rec); err != nil {
		t.Fatal(err)
	}
	origin := &lib.Envelope{Kind: lib.KindMessage, TaskID: "task-sw-1", ContextID: "ctx-sw", CorrelationID: "corr-sw-1"}
	exec, err := r.bus.NewTaskExecution(origin, lib.Party{Session: pod, AgentType: "test-executor"}, pod)
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.PublishStatus(ctx, lib.StateCompleted, true); err != nil {
		t.Fatal(err)
	}
	spawn.setOrphans([]orphanPod{{PodName: pod, SessionKey: rec.Key, Addressee: pod,
		TaskID: "task-sw-1", ContextID: "ctx-sw", CorrelationID: "corr-sw-1"}})

	r.g.sweepOnce(ctx)

	s := terminalFor(t, r.url, pod, "task-sw-3")
	if s == nil || s.Status.State != lib.StateFailed {
		t.Fatalf("the turn the pod died on has terminal %+v, want failed by its supervisor", s)
	}
	if sup := supervisorTerminal(t, r, pod, "task-sw-1"); sup != nil {
		t.Fatalf("the sweep wrote a second final for the first task: %+v", sup)
	}
	if d := spawn.deleted(); len(d) != 1 || d[0] != pod {
		t.Fatalf("deleted %v, want %s", d, pod)
	}

	// A pod whose conversation has moved on to another pod closes nothing
	// of the other pod's: the active task there is not this pod's to end.
	moved := &SessionRecord{
		Key: "discord:g1/t-sweep-moved", ContextID: "ctx-mv", Kind: "group",
		Addressee: "chat-heron-new", BusSession: "chat-heron-new", PodName: "chat-heron-new", PodReuse: true,
		ActiveTask: &ActiveTask{TaskID: "task-mv-2", CorrelationID: "corr-mv-2", SubmittedAt: time.Now()},
		Tasks:      []TaskRef{{ID: "task-mv-1", Addressee: "chat-heron-old"}, {ID: "task-mv-2", Addressee: "chat-heron-new"}},
	}
	if err := r.g.reg.Put(ctx, moved); err != nil {
		t.Fatal(err)
	}
	old := &lib.Envelope{Kind: lib.KindMessage, TaskID: "task-mv-1", ContextID: "ctx-mv", CorrelationID: "corr-mv-1"}
	oexec, err := r.bus.NewTaskExecution(old, lib.Party{Session: "chat-heron-old", AgentType: "test-executor"}, "chat-heron-old")
	if err != nil {
		t.Fatal(err)
	}
	if err := oexec.PublishStatus(ctx, lib.StateCompleted, true); err != nil {
		t.Fatal(err)
	}
	spawn.setOrphans([]orphanPod{{PodName: "chat-heron-old", SessionKey: moved.Key, Addressee: "chat-heron-old",
		TaskID: "task-mv-1", ContextID: "ctx-mv", CorrelationID: "corr-mv-1"}})
	r.g.sweepOnce(ctx)
	for _, addr := range []string{"chat-heron-old", "chat-heron-new"} {
		if s := supervisorTerminal(t, r, addr, "task-mv-2"); s != nil {
			t.Fatalf("the old pod's sweep closed the new pod's task on %s: %+v", addr, s)
		}
	}
}

// At the cap, a new conversation's pod is made room for by evicting the pod
// that has been idle longest, and that conversation is told. A pod whose
// conversation is running anything is never evicted, and with only those at
// the cap the turn is refused as before.
func TestTheCapEvictsTheLongestIdlePod(t *testing.T) {
	now := time.Now().UTC()
	idle := func(key, pod string, quiet time.Duration) *SessionRecord {
		return &SessionRecord{Key: key, ContextID: "ctx-" + pod, Kind: "group", Addressee: pod, BusSession: pod,
			PodName: pod, PodReuse: true, SessionRouted: true, LastUserMessage: now.Add(-quiet)}
	}
	t.Run("an idle pod makes room", func(t *testing.T) {
		r, spawn := startRigWithSpawnerCap(t, RouteSession, 2, reuseOn)
		ctx := context.Background()
		older := idle("discord:g1/t-old", "chat-vole-old", 20*time.Minute)
		newer := idle("discord:g1/t-new", "chat-vole-new", 5*time.Minute)
		for _, rec := range []*SessionRecord{older, newer} {
			if err := r.g.reg.Put(ctx, rec); err != nil {
				t.Fatal(err)
			}
		}
		spawn.live = 2
		spawn.setSessionPods(sessionPod{PodName: "chat-vole-new", SessionKey: newer.Key, Reuse: true}, sessionPod{PodName: "chat-vole-old", SessionKey: older.Key, Reuse: true})
		say(r, "discord:g1/t-third", "m1", "a third conversation")
		waitFor(t, "the third conversation's spawn", func() bool { return len(spawn.calls()) == 1 })
		if d := spawn.deleted(); len(d) != 1 || d[0] != "chat-vole-old" {
			t.Fatalf("evicted %v, want only the longest-idle pod", d)
		}
		rec, _ := r.g.reg.Get(ctx, older.Key)
		if rec.PodName != "" {
			t.Fatalf("the evicted conversation still names its pod %q", rec.PodName)
		}
		if !slices.Contains(r.adapter.postTextsFor(older.Key), noticeSessionPaused) {
			t.Fatalf("the evicted conversation was not told; its posts: %v", r.adapter.postTextsFor(older.Key))
		}
	})
	t.Run("an untracked pod goes first, without a notice", func(t *testing.T) {
		r, spawn := startRigWithSpawnerCap(t, RouteSession, 2, reuseOn)
		ctx := context.Background()
		older := idle("discord:g1/t-old2", "chat-vole-old2", time.Hour)
		moved := idle("discord:g1/t-moved", "chat-vole-now", time.Minute)
		for _, rec := range []*SessionRecord{older, moved} {
			if err := r.g.reg.Put(ctx, rec); err != nil {
				t.Fatal(err)
			}
		}
		spawn.live = 2
		// The moved conversation's record names a later pod; its old one
		// lingers after a failed delete.
		spawn.setSessionPods(sessionPod{PodName: "chat-vole-old2", SessionKey: older.Key, Reuse: true}, sessionPod{PodName: "chat-vole-gone", SessionKey: moved.Key, Reuse: true})
		say(r, "discord:g1/t-third2", "m1", "a third conversation")
		waitFor(t, "the third conversation's spawn", func() bool { return len(spawn.calls()) == 1 })
		if d := spawn.deleted(); len(d) != 1 || d[0] != "chat-vole-gone" {
			t.Fatalf("evicted %v, want the untracked pod", d)
		}
		if posts := r.adapter.postTextsFor(moved.Key); slices.Contains(posts, noticeSessionPaused) {
			t.Fatalf("a conversation whose pod was untracked was told it was paused: %v", posts)
		}
	})
	t.Run("a conversation whose lock is held is busy", func(t *testing.T) {
		r, spawn := startRigWithSpawnerCap(t, RouteSession, 1, reuseOn)
		ctx := context.Background()
		held := idle("discord:g1/t-held", "chat-vole-held", time.Hour)
		if err := r.g.reg.Put(ctx, held); err != nil {
			t.Fatal(err)
		}
		// A turn in flight there holds this lock while it decides; an
		// eviction must not wait on it, or decide around it.
		l := r.g.lockSession(held.Key)
		l.Lock()
		defer l.Unlock()
		spawn.live = 1
		spawn.setSessionPods(sessionPod{PodName: "chat-vole-held", SessionKey: held.Key, Reuse: true})
		say(r, "discord:g1/t-blocked", "m1", "anyone home?")
		waitFor(t, "the cap refusal", postedContaining(r, "not started"))
		if d := spawn.deleted(); len(d) != 0 {
			t.Fatalf("evicted %v from a conversation whose lock was held", d)
		}
	})
	t.Run("a busy pod is never evicted", func(t *testing.T) {
		r, spawn := startRigWithSpawnerCap(t, RouteSession, 1, reuseOn)
		ctx := context.Background()
		busy := idle("discord:g1/t-busy", "chat-vole-busy", time.Hour)
		busy.ActiveTask = &ActiveTask{TaskID: "task-busy", CorrelationID: "corr-busy", SubmittedAt: now}
		if err := r.g.reg.Put(ctx, busy); err != nil {
			t.Fatal(err)
		}
		spawn.live = 1
		spawn.setSessionPods(sessionPod{PodName: "chat-vole-busy", SessionKey: busy.Key, Reuse: true})
		say(r, "discord:g1/t-refused", "m1", "anyone home?")
		waitFor(t, "the cap refusal", postedContaining(r, "not started"))
		if d := spawn.deleted(); len(d) != 0 {
			t.Fatalf("evicted %v; a pod with a running task must stay", d)
		}
		if n := len(spawn.calls()); n != 0 {
			t.Fatalf("%d spawns past a full cap of busy pods", n)
		}
	})
}

// A reused pod carries the flag its adapter reads and the annotation the
// gateway checks before routing to it, the session's maximum lifetime as its
// deadline, and the delegate tool on whatever its first task is: the adapter
// turns it off per task, for a wake.
func TestAReusedPodIsSpawnedWithTheFlagTheLifetimeAndThePerTaskTool(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reuse      bool
		fixedRoute bool
		lifetime   time.Duration
		deadline   time.Duration
		tool       string
	}{
		{name: "reuse on", reuse: true, lifetime: 4 * time.Hour, deadline: 4 * time.Hour, tool: "on"},
		{name: "reuse on, lifetime unset", reuse: true, deadline: defaultSessionMaxLifetime, tool: "on"},
		{name: "reuse off", deadline: 15*time.Minute + podDeadlineGrace, tool: "off"},
		{name: "a one-shot Delegate off the session route", reuse: true, fixedRoute: true, deadline: 15*time.Minute + podDeadlineGrace, tool: "off"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs := k8sfake.NewSimpleClientset()
			cfg := &Config{Namespace: "test-ns", WorkerImage: "img", SessionServiceAccount: "agent-a2a-session",
				TaskDeadline: 15 * time.Minute, SessionReuse: tc.reuse, SessionMaxLifetime: tc.lifetime}
			s := &podSpawner{cfg: cfg, client: cs, log: slog.Default()}
			rec := &SessionRecord{Key: "discord:g1/t", ContextID: "ctx-1", BusSession: "chat-otter-wake", Addressee: "chat-otter-wake",
				SessionRouted: !tc.fixedRoute, Tasks: []TaskRef{{ID: "task-w", Role: taskRoleWake}}}
			if _, err := s.Spawn(context.Background(), rec, "task-w", "", 1); err != nil {
				t.Fatal(err)
			}
			pod, err := cs.CoreV1().Pods("test-ns").Get(context.Background(), "chat-otter-wake", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if got := time.Duration(*pod.Spec.ActiveDeadlineSeconds) * time.Second; got != tc.deadline {
				t.Errorf("activeDeadlineSeconds = %v, want %v", got, tc.deadline)
			}
			env := map[string]string{}
			for _, e := range pod.Spec.Containers[0].Env {
				env[e.Name] = e.Value
			}
			if got := env[lib.EnvDelegateTool]; got != tc.tool {
				t.Errorf("%s = %q, want %q", lib.EnvDelegateTool, got, tc.tool)
			}
			want := tc.reuse && !tc.fixedRoute
			flag, anno := env[lib.EnvSessionReuse] == "true", pod.Annotations[annoReuse] == "true"
			if flag != want || anno != want {
				t.Errorf("reuse env=%v annotation=%v, want both %v", flag, anno, want)
			}
		})
	}
}

// Reusable vouches only for a pod that can run a whole task: spawned for
// reuse, running with its worker up, not being deleted, and with the task's
// bound left in its lifetime.
func TestPodReusable(t *testing.T) {
	now := time.Now()
	const deadline = 30 * time.Minute
	base := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Annotations: map[string]string{annoReuse: reuseValue}},
			Spec:       corev1.PodSpec{ActiveDeadlineSeconds: ptr.To(int64((4 * time.Hour) / time.Second))},
			Status: corev1.PodStatus{
				Phase:     corev1.PodRunning,
				StartTime: &metav1.Time{Time: now.Add(-time.Hour)},
				ContainerStatuses: []corev1.ContainerStatus{{Name: workerContainer,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
			},
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*corev1.Pod)
		want   bool
	}{
		{"a live reused pod", func(*corev1.Pod) {}, true},
		{"no reuse annotation", func(p *corev1.Pod) { delete(p.Annotations, annoReuse) }, false},
		{"pending", func(p *corev1.Pod) { p.Status.Phase = corev1.PodPending }, false},
		{"succeeded", func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded }, false},
		{"being deleted", func(p *corev1.Pod) { p.DeletionTimestamp = &metav1.Time{Time: now} }, false},
		{"worker exited", func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}
		}, false},
		{"no time left for a task", func(p *corev1.Pod) {
			p.Status.StartTime = &metav1.Time{Time: now.Add(-4*time.Hour + deadline + podDeadlineGrace)}
		}, false},
		{"just enough time left", func(p *corev1.Pod) {
			p.Status.StartTime = &metav1.Time{Time: now.Add(-4*time.Hour + deadline + 2*podDeadlineGrace + time.Minute)}
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := base()
			tc.mutate(p)
			if got := podReusable(p, deadline, now); got != tc.want {
				t.Errorf("podReusable = %v, want %v", got, tc.want)
			}
		})
	}
}

// SessionPods lists live session pods with their conversation, skipping the
// terminal and the ones already going.
func TestSessionPodsListsTheLiveOnes(t *testing.T) {
	mk := func(name string, phase corev1.PodPhase, deleting bool) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test-ns",
				Labels:      map[string]string{labelPartOf: partOfValue, labelRole: sessionRole},
				Annotations: map[string]string{annoConvo: "conv-" + name}},
			Status: corev1.PodStatus{Phase: phase},
		}
		if deleting {
			p.DeletionTimestamp = &metav1.Time{Time: time.Now()}
			p.Finalizers = []string{"test"}
		}
		return p
	}
	cs := k8sfake.NewSimpleClientset(
		mk("live", corev1.PodRunning, false),
		mk("pending", corev1.PodPending, false),
		mk("done", corev1.PodSucceeded, false),
		mk("going", corev1.PodRunning, true),
	)
	s := &podSpawner{cfg: &Config{Namespace: "test-ns"}, client: cs, log: slog.Default()}
	pods, err := s.SessionPods(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, p := range pods {
		got = append(got, fmt.Sprintf("%s=%s", p.PodName, p.SessionKey))
	}
	slices.Sort(got)
	if want := []string{"live=conv-live", "pending=conv-pending"}; !slices.Equal(got, want) {
		t.Fatalf("SessionPods = %v, want %v", got, want)
	}
}

// A reused pod that its conversation no longer names (a retirement whose
// delete failed) never exits on its own; the sweep deletes it. A pod its
// record still names, and a one-task pod, are left alone.
func TestTheSweepDeletesAnUntrackedReusedPod(t *testing.T) {
	r, spawn := startReuseRig(t)
	ctx := context.Background()
	tracked := &SessionRecord{Key: "discord:g1/t-tracked", ContextID: "ctx-t", Kind: "group",
		Addressee: "chat-otter-now", BusSession: "chat-otter-now", PodName: "chat-otter-now", PodReuse: true, SessionRouted: true}
	if err := r.g.reg.Put(ctx, tracked); err != nil {
		t.Fatal(err)
	}
	spawn.setSessionPods(
		sessionPod{PodName: "chat-otter-now", SessionKey: tracked.Key, Reuse: true},
		sessionPod{PodName: "chat-otter-was", SessionKey: tracked.Key, Reuse: true},
		sessionPod{PodName: "chat-otter-once", SessionKey: tracked.Key},
	)
	r.g.sweepOnce(ctx)
	if d := spawn.deleted(); len(d) != 1 || d[0] != "chat-otter-was" {
		t.Fatalf("sweep deleted %v, want only the untracked reused pod", d)
	}
}

// A one-shot Delegate from a fixed-route conversation spawns a pod that serves
// that task only: the next plain ask goes back to the fixed addressee, and a
// pod waiting for a turn that never comes would idle.
func TestADelegateOffTheSessionRouteIsNotReused(t *testing.T) {
	r, spawn := startRigWithSpawnerCap(t, "platform", 0, reuseOn)
	conv := "discord:g1/t-delegate-once"
	say(r, conv, "m1", "Delegate: check the fleet")
	waitFor(t, "the delegate spawn", func() bool { return len(spawn.calls()) == 1 })
	waitFor(t, "the pod on the record", func() bool {
		rec, _ := r.g.reg.Get(context.Background(), conv)
		return rec != nil && rec.PodName != ""
	})
	if rec, _ := r.g.reg.Get(context.Background(), conv); rec.PodReuse {
		t.Fatal("a fixed-route conversation's Delegate pod was marked for reuse")
	}
}

// A pod that never started its task is not handed the next one: the heal
// unmarks it, and the next turn retires it and spawns fresh.
func TestANeverStartedTaskUnmarksItsPod(t *testing.T) {
	r, spawn := startRigWithSpawnerCap(t, RouteSession, 0, func(c *Config) {
		c.SessionReuse = true
		c.FirstEventGrace = time.Second
	})
	ctx := context.Background()
	pod := "chat-puffin-ns"
	rec := &SessionRecord{Key: "discord:g1/t-never", ContextID: "ctx-ns", Kind: "group",
		Addressee: pod, BusSession: pod, PodName: pod, PodReuse: true, SessionRouted: true, Profile: "chat",
		ActiveTask: &ActiveTask{TaskID: "task-ns", CorrelationID: "corr-ns", SubmittedAt: time.Now().Add(-time.Minute)},
		Tasks:      []TaskRef{{ID: "task-ns", Addressee: pod}}}
	if err := r.g.reg.Put(ctx, rec); err != nil {
		t.Fatal(err)
	}
	spawn.setReusable(pod)
	say(r, rec.Key, "m1", "hello?")
	waitFor(t, "a fresh spawn", func() bool { return len(spawn.calls()) == 1 })
	if s := spawn.calls()[0].Session; s == pod {
		t.Fatalf("the next turn went to the pod that never started its task")
	}
	if d := spawn.deleted(); !slices.Contains(d, pod) {
		t.Fatalf("deleted %v, want the never-started pod retired", d)
	}
}
