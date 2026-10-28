package dispatch

import (
	"fmt"
	"testing"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

func bareDispatcher() *Dispatcher {
	return &Dispatcher{cfg: testConfig(), now: time.Now, active: map[string]*holding{}, waiting: map[string]waiting{},
		lastAdmit: map[string]time.Time{}, released: map[string]bool{}}
}

func capsSnapshot(ceiling int) lib.DispatchConfig {
	return testDispatchConfig(ceiling, testProfile("chat", 100, "read", "unattended"), testProfile("tse", 2))
}

// fill reserves n tasks of one shape, failing if any is refused.
func fill(t *testing.T, d *Dispatcher, snap lib.DispatchConfig, profile, tier, kind string, n int) {
	t.Helper()
	p, _ := profileOf(snap, profile)
	for i := 0; i < n; i++ {
		if h, why := d.reserve(snap, p, tier, kind, fmt.Sprintf("%s-%s-%s-%d-%d", profile, tier, kind, i, len(d.active))); h == nil {
			t.Fatalf("reserve %s/%s/%s #%d refused: %s", profile, tier, kind, i, why)
		}
	}
}

func refused(d *Dispatcher, snap lib.DispatchConfig, profile, tier, kind string) string {
	p, _ := profileOf(snap, profile)
	h, why := d.reserve(snap, p, tier, kind, "probe")
	if h != nil {
		d.release(h)
		return ""
	}
	return why
}

// Unattended work leaves the interactive reserve free: with ceiling 9 the
// reserves are 3 each, so unattended top-level work stops at 3 (the
// top-level share of 6 less the reserve) and unattended work overall at 6,
// while interactive work still gets its share.
func TestTheInteractiveReserveHoldsUnderUnattendedLoad(t *testing.T) {
	d := bareDispatcher()
	snap := capsSnapshot(9)
	fill(t, d, snap, "chat", "unattended", lib.SubmitKindTop, 3)
	if why := refused(d, snap, "chat", "unattended", lib.SubmitKindTop); why != "interactive reserve" {
		t.Fatalf("a fourth unattended top-level task: refused for %q, want the interactive reserve", why)
	}
	fill(t, d, snap, "chat", "unattended", lib.SubmitKindChild, 3)
	if why := refused(d, snap, "chat", "unattended", lib.SubmitKindChild); why != "interactive reserve" {
		t.Fatalf("a seventh unattended task: refused for %q, want the interactive reserve", why)
	}
	fill(t, d, snap, "chat", "read", lib.SubmitKindTop, 3)
}

// Top-level work stops at the ceiling minus the child reserve, so children
// always have a slot.
func TestTheChildReserveIsLeftForChildren(t *testing.T) {
	d := bareDispatcher()
	snap := capsSnapshot(6)
	fill(t, d, snap, "chat", "read", lib.SubmitKindTop, 4)
	if why := refused(d, snap, "chat", "read", lib.SubmitKindTop); why != "child reserve" {
		t.Fatalf("a fifth top-level task: refused for %q, want the child reserve", why)
	}
	fill(t, d, snap, "chat", "read", lib.SubmitKindChild, 2)
	if why := refused(d, snap, "chat", "read", lib.SubmitKindChild); why != "install ceiling" {
		t.Errorf("a child past the ceiling: refused for %q, want the install ceiling", why)
	}
}

// A profile's concurrency caps its top-level tasks and, separately, its
// children.
func TestProfileConcurrencyCapsEachKind(t *testing.T) {
	d := bareDispatcher()
	snap := capsSnapshot(20)
	fill(t, d, snap, "tse", "read", lib.SubmitKindTop, 2)
	if why := refused(d, snap, "tse", "read", lib.SubmitKindTop); why != "profile concurrency" {
		t.Errorf("tse's third top-level task: %q", why)
	}
	fill(t, d, snap, "tse", "read", lib.SubmitKindChild, 2)
	if why := refused(d, snap, "tse", "read", lib.SubmitKindChild); why != "profile concurrency" {
		t.Errorf("tse's third child: %q", why)
	}
}

// Caps follow the config per decision: raising the ceiling admits at once.
func TestCapsFollowTheConfig(t *testing.T) {
	d := bareDispatcher()
	snap := capsSnapshot(3)
	fill(t, d, snap, "chat", "read", lib.SubmitKindTop, 2)
	if refused(d, snap, "chat", "read", lib.SubmitKindTop) == "" {
		t.Fatal("ceiling 3 admitted a third top-level task")
	}
	if why := refused(d, capsSnapshot(30), "chat", "read", lib.SubmitKindTop); why != "" {
		t.Errorf("after the ceiling rose: refused for %q", why)
	}
}

// Over a cap the task waits: no terminal, not offered, still queued; it runs
// when a slot frees. Never failed for the cap.
func TestOverCapStaysQueuedAndRunsLater(t *testing.T) {
	b := newBus(t, "tse-0", "tse-1")
	b.ensure("tse", 4, 0, "read")
	w0 := b.worker("tse", "read", "tse-0", answerAccept, 3*time.Second)
	w1 := b.worker("tse", "read", "tse-1", answerAccept, 3*time.Second)
	d, _ := b.runDispatcher(testConfig(), oneProfile("tse", 1, "tse-0", "tse-1"), &fakeVerifier{})

	b.submit("tse", "read", lib.SubmitKindTop, "task-1")
	waitFor(t, 10*time.Second, "the first offer", func() bool { return w0.offerCount()+w1.offerCount() == 1 })
	b.submit("tse", "read", lib.SubmitKindTop, "task-2")
	time.Sleep(1500 * time.Millisecond)
	if n := w0.offerCount() + w1.offerCount(); n != 1 {
		t.Fatalf("%d offers with concurrency 1", n)
	}
	if ts := b.terminalsOf("task-2"); len(ts) != 0 {
		t.Fatalf("the over-cap task got a terminal: %+v", ts)
	}
	if h := d.Health(); h.Waiting[lib.ProfileDurableName("tse", "read", lib.SubmitKindTop)] != 1 {
		t.Errorf("health %+v, want task-2 waiting", h)
	}
	for _, id := range []string{"task-1", "task-2"} {
		if ts := b.waitTerminal(id, 15*time.Second); ts.state != lib.StateCompleted {
			t.Errorf("%s: %+v, want completed", id, ts)
		}
	}
}
