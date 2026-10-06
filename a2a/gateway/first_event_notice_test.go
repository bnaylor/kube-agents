package gateway

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// noticePosts returns the posts that are the reap scan's no-first-event
// notice for the given task.
func noticePosts(a *fakeAdapter, taskID string) []string {
	var out []string
	for _, p := range a.postTexts() {
		if strings.Contains(p, taskID) && strings.Contains(p, "your next message here starts a new task instead of going to it") {
			out = append(out, p)
		}
	}
	return out
}

// seedTasklessFixed is seedTasklessDelegate on the fixed route: the task went
// to the standing executor (platform), which never picked it up.
func seedTasklessFixed(t *testing.T, r *rig, conv string, age time.Duration) *SessionRecord {
	t.Helper()
	rec := seedTasklessDelegate(t, r, conv, age)
	rec.BusSession, rec.Addressee = "", "platform"
	rec.Tasks = []TaskRef{{ID: rec.ActiveTask.TaskID, Addressee: "platform"}}
	if err := r.g.reg.Put(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

// TestNoFirstEventNoticePostsOnceWithoutATurn (#2405): a task with nothing on
// its event stream past FirstEventGrace is announced by the reap scan, with no
// inbound message, exactly once however many passes run. The notice releases
// nothing and publishes nothing: the record still serializes on the task, the
// stream is still empty, and the conversation's next message is what the heal
// releases it on, as the notice says.
func TestNoFirstEventNoticePostsOnceWithoutATurn(t *testing.T) {
	r := startRig(t)
	ctx := context.Background()
	conv := "discord:g1/thread-notice-once"
	seedTasklessFixed(t, r, conv, defaultFirstEventGrace+time.Minute)

	r.g.reapOnce(ctx)
	if got := noticePosts(r.adapter, "task-never"); len(got) != 1 {
		t.Fatalf("after one reap pass, %d notices, want 1; posts: %q", len(got), r.adapter.postTexts())
	}
	r.g.reapOnce(ctx)
	r.g.reapOnce(ctx)
	if got := noticePosts(r.adapter, "task-never"); len(got) != 1 {
		t.Fatalf("after three reap passes, %d notices, want 1; posts: %q", len(got), r.adapter.postTexts())
	}

	// Not released early: the record still holds the task, with the marker,
	// and nothing was published for the task on either event subject.
	rec, err := r.g.reg.Get(ctx, conv)
	if err != nil || rec == nil || rec.ActiveTask == nil || rec.ActiveTask.TaskID != "task-never" {
		t.Fatalf("the notice released the record: %+v (err=%v)", rec, err)
	}
	if rec.ActiveTask.NoFirstEventNoticeAt.IsZero() {
		t.Fatal("the notice was posted but its marker is not on the record")
	}
	if _, err := r.client.TasksGet(ctx, "platform", "task-never"); !isTaskNotFound(err) {
		t.Fatalf("the notice put something on the task's stream: TasksGet err = %v", err)
	}
	for _, p := range r.adapter.postTexts() {
		if strings.Contains(p, "this conversation is released") {
			t.Fatalf("the reap scan posted the heal's release line: %q", p)
		}
	}

	// The notice's promise holds: the next message starts a new task.
	r.adapter.inbox <- InboundMessage{Conversation: conv, Kind: "group",
		AuthorID: "1001", MessageID: "n-1", Text: "check the fleet again"}
	waitFor(t, "a new task after the notice", func() bool {
		for _, e := range inSubjectEnvelopes(t, r.url, "platform") {
			if e.Kind == lib.KindMessage && e.TaskID != "task-never" && e.ContextID == rec.ContextID {
				return true
			}
		}
		return false
	})
}

// TestNoFirstEventNoticeNotRepeatedAfterRestart: the once-per-task rule holds
// across a gateway restart. A second gateway on the same bus has none of the
// first one's memory; the marker on the record is what stops it posting the
// same notice again.
func TestNoFirstEventNoticeNotRepeatedAfterRestart(t *testing.T) {
	r := startRig(t)
	ctx := context.Background()
	conv := "discord:g1/thread-notice-restart"
	seedTasklessFixed(t, r, conv, defaultFirstEventGrace+time.Minute)

	r.g.reapOnce(ctx)
	if got := noticePosts(r.adapter, "task-never"); len(got) != 1 {
		t.Fatalf("first gateway: %d notices, want 1; posts: %q", len(got), r.adapter.postTexts())
	}

	restarted := newFakeAdapter()
	g2, err := New(Options{Client: r.client, Adapter: restarted, Config: r.g.cfg, Backend: "discord"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	g2.reapOnce(ctx)
	g2.reapOnce(ctx)
	if got := restarted.postTexts(); len(got) != 0 {
		t.Fatalf("the restarted gateway posted again for a task already noticed: %q", got)
	}
}

// TestNoFirstEventNoticeSkipsTasksThatStarted: a task whose first event
// arrived, however old it is now, and a task with nothing yet but still
// inside the grace, get no notice.
func TestNoFirstEventNoticeSkipsTasksThatStarted(t *testing.T) {
	r := startRig(t)
	ctx := context.Background()

	started := "discord:g1/thread-notice-started"
	r.adapter.inbox <- InboundMessage{Conversation: started, Kind: "group",
		AuthorID: "1001", MessageID: "s-1", Text: "check the fleet"}
	origin := r.awaitTask(t, "platform")
	if err := r.execFor(t, origin, "platform").PublishStatus(ctx, lib.StateSubmitted, false); err != nil {
		t.Fatal(err)
	}
	// Age the started task past the grace, under the session lock so the
	// relay's own write of the record cannot interleave with this one.
	l := r.g.lockSession(started)
	l.Lock()
	rec, err := r.g.reg.Get(ctx, started)
	if err != nil || rec == nil || rec.ActiveTask == nil || rec.ActiveTask.TaskID != origin.TaskID {
		l.Unlock()
		t.Fatalf("started task not on the record: %+v (err=%v)", rec, err)
	}
	rec.ActiveTask.SubmittedAt = time.Now().Add(-(defaultFirstEventGrace + time.Hour))
	if err := r.g.reg.Put(ctx, rec); err != nil {
		l.Unlock()
		t.Fatal(err)
	}
	l.Unlock()

	young := "discord:g1/thread-notice-young"
	seedTasklessFixed(t, r, young, time.Minute)

	r.g.reapOnce(ctx)
	r.g.reapOnce(ctx)
	if got := noticePosts(r.adapter, origin.TaskID); len(got) != 0 {
		t.Fatalf("a task with a first event was noticed: %q", got)
	}
	if got := noticePosts(r.adapter, "task-never"); len(got) != 0 {
		t.Fatalf("a task inside the grace was noticed: %q", got)
	}
}

// TestSteerIntoTaskWithNoFirstEventPromisesNoReply (#2405 b): a steer into a
// task with nothing on its stream is still sent, but the acknowledgement
// promises no reply on either route, and says when the conversation frees up.
func TestSteerIntoTaskWithNoFirstEventPromisesNoReply(t *testing.T) {
	for name, seed := range map[string]func(*testing.T, *rig, string, time.Duration) *SessionRecord{
		"fixed":   seedTasklessFixed,
		"session": seedTasklessDelegate,
	} {
		t.Run(name, func(t *testing.T) {
			r := startRig(t)
			conv := "discord:g1/thread-steer-silent-" + name
			rec := seed(t, r, conv, time.Minute)
			r.adapter.inbox <- InboundMessage{Conversation: conv, Kind: "group",
				AuthorID: "1001", MessageID: "ss-" + name, Text: "make it about otters"}
			var ack string
			waitFor(t, "steer ack", func() bool {
				for _, p := range r.adapter.postTexts() {
					if strings.Contains(p, "steering sent") {
						ack = p
						return true
					}
				}
				return false
			})
			for _, promise := range []string{"its reply will say so", "picks it up at its next turn boundary"} {
				if strings.Contains(ack, promise) {
					t.Fatalf("steer into a task with no first event promised a reply: %q", ack)
				}
			}
			if !strings.Contains(ack, "task-never") || !strings.Contains(ack, "nothing may answer this") ||
				!strings.Contains(ack, defaultFirstEventGrace.String()) {
				t.Fatalf("ack does not say the task is silent and when the conversation frees up: %q", ack)
			}
			// Still sent: the steer is on the task's in subject.
			var sent bool
			for _, e := range inSubjectEnvelopes(t, r.url, rec.Addressee) {
				if e.Kind == lib.KindMessage && e.TaskID == "task-never" {
					sent = true
				}
			}
			if !sent {
				t.Fatal("the steer was not published")
			}
		})
	}
}

// TestNoFirstEventPastGrace pins the shared test the heal and the notice
// both draw the line with: TaskNotFound only, strictly past the grace, and
// never on a task with no age.
func TestNoFirstEventPastGrace(t *testing.T) {
	now := time.Now()
	grace := defaultFirstEventGrace
	notFound := &lib.A2AError{Code: lib.CodeTaskNotFound}
	other := &lib.A2AError{Code: lib.CodeInvalidParams}
	aged := func(age time.Duration) *ActiveTask { return &ActiveTask{TaskID: "t", SubmittedAt: now.Add(-age)} }
	for name, tc := range map[string]struct {
		active *ActiveTask
		err    error
		want   bool
	}{
		"not found past the grace":   {aged(grace + time.Second), notFound, true},
		"not found exactly at grace": {aged(grace), notFound, false},
		"not found inside the grace": {aged(time.Minute), notFound, false},
		"events past the grace":      {aged(grace + time.Hour), nil, false},
		"transport error past grace": {aged(grace + time.Hour), other, false},
		"no submittedAt":             {&ActiveTask{TaskID: "t"}, notFound, false},
		"no active task":             {nil, notFound, false},
	} {
		if got := noFirstEventPastGrace(tc.active, tc.err, grace, now); got != tc.want {
			t.Errorf("%s: got %v, want %v", name, got, tc.want)
		}
	}
}
