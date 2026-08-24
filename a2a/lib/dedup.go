package lib

import "sync"

// dedupWindow bounds how many envelopeIds a subscription remembers. JetStream
// redelivery is near-term, so a bounded window is enough for the at-most-once
// property without unbounded memory.
const dedupWindow = 4096

// dedupSet is a bounded FIFO set of envelopeIds.
type dedupSet struct {
	mu    sync.Mutex
	seen  map[string]struct{}
	order []string
	cap   int
}

func newDedupSet(capacity int) *dedupSet {
	return &dedupSet{seen: make(map[string]struct{}, capacity), cap: capacity}
}

// add records id and reports whether it was new.
func (d *dedupSet) add(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, dup := d.seen[id]; dup {
		return false
	}
	d.seen[id] = struct{}{}
	d.order = append(d.order, id)
	if len(d.order) > d.cap {
		delete(d.seen, d.order[0])
		d.order = d.order[1:]
	}
	return true
}
