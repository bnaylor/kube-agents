package dispatch

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// A task goes from submission to a worker and back: offered once, accepted,
// renewed, finished; the submission is acked and the record deleted, and the
// only terminal is the worker's.
func TestATaskRunsToCompletion(t *testing.T) {
	b := newBus(t, "tse-0")
	b.ensure("tse", 2, 0, "read")
	w := b.worker("tse", "read", "tse-0", answerAccept, 2500*time.Millisecond)
	v := &fakeVerifier{}
	b.runDispatcher(testConfig(), NewStaticSource(oneProfile("tse", 2, "tse-0")), v)

	b.submitWith("tse", "read", lib.SubmitKindTop, "task-1", "cap-ok",
		map[string]string{"project": "p1", "location": "us-central1", "cluster": "c1"})
	waitFor(t, 10*time.Second, "the offer", func() bool { return w.offerCount() == 1 })
	waitFor(t, 5*time.Second, "the lease record", func() bool { return len(b.leases()) == 1 })
	for key, rec := range b.leases() {
		if key != lib.ProfileLeaseKey("tse", "read", "tse-0", "task-1") || rec.Pod != "tse-0" || rec.AcceptedAt.IsZero() {
			t.Errorf("lease %s = %+v", key, rec)
		}
	}
	ts := b.waitTerminal("task-1", 10*time.Second)
	if ts.state != lib.StateCompleted || ts.from != "tse-0" {
		t.Errorf("terminal %+v, want the worker's completed", ts)
	}
	waitFor(t, 5*time.Second, "the submission acked and the record deleted", func() bool {
		ack, num := b.pending(lib.ProfileDurableName("tse", "read", lib.SubmitKindTop))
		return ack == 0 && num == 0 && len(b.leases()) == 0
	})
	time.Sleep(2 * time.Second)
	if n := w.offerCount(); n != 1 {
		t.Errorf("offered %d times, want 1", n)
	}
	if ts := b.terminalsOf("task-1"); len(ts) != 1 {
		t.Errorf("terminals %+v, want only the worker's", ts)
	}
	if calls := v.callList(); len(calls) != 1 || calls[0] != "cap-ok|tse|read|project/p1/location/us-central1/cluster/c1" {
		t.Errorf("verifier calls %v", calls)
	}
	// The offer carried what the worker needs.
	w.mu.Lock()
	offer := w.offers[0]
	w.mu.Unlock()
	var env lib.Envelope
	if err := json.Unmarshal(offer.Envelope, &env); err != nil || env.TaskID != "task-1" {
		t.Errorf("offer envelope %s (%v)", offer.Envelope, err)
	}
	if offer.LeaseSeconds != 3 || offer.RenewSeconds != 1 || offer.Kind != lib.SubmitKindTop || offer.Deadline.IsZero() {
		t.Errorf("offer %+v", offer)
	}
}

// No accept within the timeout means the task never started: its record is
// removed, it goes back to the queue unfailed, and it runs once a worker
// that answers appears. A decline is the same.
func TestNoAcceptPutsTheTaskBackOnTheQueue(t *testing.T) {
	b := newBus(t, "tse-0", "tse-1", "tse-2")
	b.ensure("tse", 2, 0, "read")
	silent := b.worker("tse", "read", "tse-0", answerSilent, 0)
	decline := b.worker("tse", "read", "tse-1", answerDecline, 0)
	src := NewStaticSource(oneProfile("tse", 2, "tse-0", "tse-1"))
	b.runDispatcher(testConfig(), src, &fakeVerifier{})

	b.submit("tse", "read", lib.SubmitKindTop, "task-1")
	waitFor(t, 10*time.Second, "offers to both pods, twice", func() bool {
		return silent.offerCount() >= 2 && decline.offerCount() >= 2
	})
	if ts := b.terminalsOf("task-1"); len(ts) != 0 {
		t.Fatalf("a task no worker accepted got a terminal: %+v", ts)
	}
	time.Sleep(300 * time.Millisecond)
	for key := range b.leases() {
		// Only a record for an offer in flight may exist.
		if !strings.HasSuffix(key, ".task-1") {
			t.Errorf("stray lease %s", key)
		}
	}
	good := b.worker("tse", "read", "tse-2", answerAccept, 500*time.Millisecond)
	src.Set(oneProfile("tse", 2, "tse-0", "tse-1", "tse-2"))
	if ts := b.waitTerminal("task-1", 15*time.Second); ts.state != lib.StateCompleted || ts.from != "tse-2" {
		t.Errorf("terminal %+v, want tse-2's completed", ts)
	}
	if good.offerCount() != 1 {
		t.Errorf("the answering pod was offered %d times", good.offerCount())
	}
}

// The subject's pod token is the accept's identity: an accept another pod
// publishes on its own subject for a task offered elsewhere is not taken
// (and is answered with an abandon), and no lease is written for it.
func TestAnAcceptFromAnotherPodIsNotTaken(t *testing.T) {
	b := newBus(t, "tse-0", "intruder")
	b.ensure("tse", 2, 0, "read")
	target := b.worker("tse", "read", "tse-0", answerSilent, 0)
	intruder := b.worker("tse", "read", "intruder", answerSilent, 0)
	b.runDispatcher(testConfig(), NewStaticSource(oneProfile("tse", 2, "tse-0")), &fakeVerifier{})

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
				intruder.publish(lib.DispatchAcceptSubject("tse", "read", "intruder"), lib.DispatchAccept{TaskID: "task-1", Accepted: true})
			}
		}
	}()
	b.submit("tse", "read", lib.SubmitKindTop, "task-1")
	waitFor(t, 10*time.Second, "three offers to the real pod", func() bool { return target.offerCount() >= 3 })
	for key := range b.leases() {
		if strings.Contains(key, "intruder") {
			t.Errorf("a lease was written for the intruder: %s", key)
		}
	}
	if intruder.offerCount() != 0 {
		t.Errorf("the intruder was offered the task")
	}
	if intruder.abandonCount() == 0 {
		t.Errorf("the intruder's unasked accept was not answered with an abandon")
	}
	if ts := b.terminalsOf("task-1"); len(ts) != 0 {
		t.Errorf("terminals %+v: the intruder's accept started the task", ts)
	}
}

// A renewal is the subject's pod's: one from another pod for a task handed
// to tse-0 is ignored, so tse-0's silence still expires the lease.
func TestARenewalFromAnotherPodIsIgnored(t *testing.T) {
	b := newBus(t, "tse-0", "intruder")
	b.ensure("tse", 2, 0, "read")
	w := b.worker("tse", "read", "tse-0", answerAccept, time.Hour)
	w.noRenew = true
	intruder := b.worker("tse", "read", "intruder", answerSilent, 0)
	b.runDispatcher(testConfig(), NewStaticSource(oneProfile("tse", 2, "tse-0")), &fakeVerifier{})

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
				intruder.publish(lib.DispatchLeaseSubject("tse", "read", "intruder"), lib.LeaseRenewal{TaskID: "task-1"})
			}
		}
	}()
	b.submit("tse", "read", lib.SubmitKindTop, "task-1")
	ts := b.waitTerminal("task-1", 15*time.Second)
	if ts.state != lib.StateFailed || !hasReason(ts, ReasonLeaseExpired) {
		t.Errorf("terminal %+v, want failed lease-expired", ts)
	}
}

// An expired lease is a terminal failure, and the task is never run again:
// one offer ever, the submission acked, the record gone.
func TestAnExpiredLeaseIsTerminalAndNeverReRun(t *testing.T) {
	b := newBus(t, "tse-0", "tse-1")
	b.ensure("tse", 2, 0, "read")
	w0 := b.worker("tse", "read", "tse-0", answerAccept, time.Hour)
	w1 := b.worker("tse", "read", "tse-1", answerAccept, time.Hour)
	b.runDispatcher(testConfig(), NewStaticSource(oneProfile("tse", 2, "tse-0", "tse-1")), &fakeVerifier{})

	b.submit("tse", "read", lib.SubmitKindTop, "task-1")
	waitFor(t, 10*time.Second, "the offer", func() bool { return w0.offerCount()+w1.offerCount() == 1 })
	time.Sleep(1500 * time.Millisecond) // a renewal or so
	w0.kill()
	w1.kill()
	ts := b.waitTerminal("task-1", 10*time.Second)
	if ts.state != lib.StateFailed || !hasReason(ts, ReasonLeaseExpired) || ts.from != selfSession {
		t.Errorf("terminal %+v, want the dispatcher's failed lease-expired", ts)
	}
	time.Sleep(4 * time.Second) // more than a lease, many retry delays
	if n := w0.offerCount() + w1.offerCount(); n != 1 {
		t.Errorf("offered %d times, want once: a failed task was run again", n)
	}
	if ack, num := b.pending(lib.ProfileDurableName("tse", "read", lib.SubmitKindTop)); ack != 0 || num != 0 {
		t.Errorf("submission still held (%d) or queued (%d)", ack, num)
	}
	if l := b.leases(); len(l) != 0 {
		t.Errorf("leases left: %v", l)
	}
	if n := len(b.terminalsOf("task-1")); n != 1 {
		t.Errorf("%d terminals, want 1", n)
	}
}

// Past lifecycle.activeDeadlineSeconds a task is failed even while its
// worker renews, and the worker is told to abandon it.
func TestAPassedDeadlineIsTerminal(t *testing.T) {
	b := newBus(t, "tse-0")
	b.ensure("tse", 2, 0, "read")
	w := b.worker("tse", "read", "tse-0", answerAccept, time.Hour)
	snap := oneProfile("tse", 2, "tse-0")
	snap.Profiles[0].ActiveDeadlineSeconds = 2
	b.runDispatcher(testConfig(), NewStaticSource(snap), &fakeVerifier{})

	b.submit("tse", "read", lib.SubmitKindTop, "task-1")
	ts := b.waitTerminal("task-1", 10*time.Second)
	if ts.state != lib.StateFailed || !hasReason(ts, ReasonDeadlineExceeded) {
		t.Errorf("terminal %+v, want failed deadline-exceeded", ts)
	}
	waitFor(t, 5*time.Second, "the abandon", func() bool { return w.abandonCount() == 1 })
	time.Sleep(2 * time.Second)
	if n := w.offerCount(); n != 1 {
		t.Errorf("offered %d times, want 1", n)
	}
}

// A worker that publishes its terminal and dies before its final renewal
// gets a lease-expired terminal after its own; the fold keeps the worker's
// and counts the dispatcher's as dropped after final.
func TestAWorkersTerminalSurvivesALateLeaseExpiry(t *testing.T) {
	b := newBus(t, "tse-0")
	b.ensure("tse", 2, 0, "read")
	w := b.worker("tse", "read", "tse-0", answerAccept, 1500*time.Millisecond)
	w.skipFinal = true
	w.run = func(ctx context.Context, _ lib.DispatchMessage) {
		select {
		case <-time.After(1500 * time.Millisecond):
		case <-ctx.Done():
		}
	}
	b.runDispatcher(testConfig(), NewStaticSource(oneProfile("tse", 2, "tse-0")), &fakeVerifier{})
	b.submit("tse", "read", lib.SubmitKindTop, "task-1")
	waitFor(t, 10*time.Second, "the worker's terminal", func() bool { return w.completed.Load() == 1 })
	w.kill()
	waitFor(t, 10*time.Second, "the dispatcher's late terminal", func() bool { return len(b.terminalsOf("task-1")) == 2 })
	task, err := b.gateway.TasksGet(b.ctx, "tse", "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if task.State != lib.StateCompleted || task.PostFinalDropped != 1 {
		t.Errorf("folded state %s, dropped %d: want the worker's completed, one dropped", task.State, task.PostFinalDropped)
	}
}

// The dispatcher is the lease bucket's only writer. Workers run here with the
// contract's grants and none on the bucket (a put could carry a rollup and
// purge every lease); the whole lifecycle still works, because the
// dispatcher writes each renewal it hears into the record.
func TestOnlyTheDispatcherWritesLeases(t *testing.T) {
	b := newBus(t, "tse-0")
	b.ensure("tse", 2, 0, "read")
	w := b.worker("tse", "read", "tse-0", answerAccept, 5*time.Second)
	b.runDispatcher(testConfig(), NewStaticSource(oneProfile("tse", 2, "tse-0")), &fakeVerifier{})

	// The worker can't write the bucket at all.
	if err := w.nc.Publish(lib.ProfileLeaseSubject(lib.ProfileLeaseKey("tse", "read", "tse-0", "task-x")), []byte("{}")); err != nil {
		t.Fatal(err)
	}
	_ = w.nc.Flush()
	time.Sleep(200 * time.Millisecond)
	if err := w.nc.LastError(); err == nil {
		t.Error("the server did not refuse the worker's put")
	}
	if _, ok := b.leases()[lib.ProfileLeaseKey("tse", "read", "tse-0", "task-x")]; ok {
		t.Fatal("a worker wrote the lease bucket")
	}

	b.submit("tse", "read", lib.SubmitKindTop, "task-1")
	key := lib.ProfileLeaseKey("tse", "read", "tse-0", "task-1")
	waitFor(t, 10*time.Second, "the accepted record", func() bool { return !b.leases()[key].AcceptedAt.IsZero() })
	first := b.leases()[key].RenewedAt
	waitFor(t, 5*time.Second, "a renewal written by the dispatcher", func() bool {
		return b.leases()[key].RenewedAt.After(first.Add(500 * time.Millisecond))
	})
	if ts := b.waitTerminal("task-1", 10*time.Second); ts.state != lib.StateCompleted {
		t.Errorf("terminal %+v", ts)
	}
}
