package dispatch

import (
	"sort"
	"time"
)

// waitingForgetAfter is how many retry delays a waiting entry outlives its
// last redelivery before the health line forgets it (the task was settled
// some other way, or its durable is gone).
const waitingForgetAfter = 3

// Health is the dispatcher's state at one moment, as the health line logs
// it.
type Health struct {
	// Running is every held task, by profile.
	Running map[string]int
	// Waiting is the submissions over a cap, by durable.
	Waiting map[string]int
	// OldestWait is the longest a waiting submission has been queued, by
	// durable.
	OldestWait map[string]time.Duration
	// Stalled are the durables with a submission waiting longer than
	// StallAfter and no admission from them in that time: what a
	// cross-chain admission cycle looks like until the deadlines end it.
	Stalled []string
}

func (d *Dispatcher) noteWaiting(taskID, durable string, queued time.Time, why string) {
	now := d.now()
	if queued.IsZero() {
		queued = now
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	w, ok := d.waiting[taskID]
	if !ok {
		w = waiting{durable: durable, queued: queued}
	}
	w.lastSeen, w.why = now, why
	d.waiting[taskID] = w
}

func (d *Dispatcher) clearWaiting(taskID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.waiting, taskID)
}

// Health reports the current state.
func (d *Dispatcher) Health() Health {
	now := d.now()
	d.mu.Lock()
	defer d.mu.Unlock()
	h := Health{Running: map[string]int{}, Waiting: map[string]int{}, OldestWait: map[string]time.Duration{}}
	for _, a := range d.active {
		h.Running[a.profile]++
	}
	forget := time.Duration(waitingForgetAfter) * d.cfg.RetryDelay
	for id, w := range d.waiting {
		if now.Sub(w.lastSeen) > forget {
			delete(d.waiting, id)
			continue
		}
		h.Waiting[w.durable]++
		if age := now.Sub(w.queued); age > h.OldestWait[w.durable] {
			h.OldestWait[w.durable] = age
		}
	}
	for durable, oldest := range h.OldestWait {
		if oldest > d.cfg.StallAfter && now.Sub(d.lastAdmit[durable]) > d.cfg.StallAfter {
			h.Stalled = append(h.Stalled, durable)
		}
	}
	sort.Strings(h.Stalled)
	return h
}

func (d *Dispatcher) logHealth() {
	h := d.Health()
	d.log.Info("dispatcher health", "running", h.Running, "waiting", h.Waiting, "oldestWait", h.OldestWait)
	if len(h.Stalled) > 0 {
		d.log.Warn("admission stalled: submissions waiting over a cap with no admission; a cross-chain cycle holds until the parents' deadlines",
			"durables", h.Stalled, "stallAfter", d.cfg.StallAfter)
	}
}
