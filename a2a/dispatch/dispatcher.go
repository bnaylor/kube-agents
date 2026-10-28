package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

const (
	// The dispatcher's wildcard subscriptions on every worker's answers:
	// a2a.dispatch.<profile>.<tier>.<pod>.accept|lease.
	acceptWildcard = "a2a.dispatch.*.*.*.accept"
	leaseWildcard  = "a2a.dispatch.*.*.*.lease"
	// answerTokens is the token count of an accept or lease subject.
	answerTokens = 6

	// ticksPerRenewal is how often a hold checks its lease and deadline,
	// per renewal interval.
	ticksPerRenewal = 4
	// minTick floors that check interval.
	minTick = 10 * time.Millisecond
	// kvOpTimeout bounds one lease-bucket operation.
	kvOpTimeout = 5 * time.Second
	// renewalBuffer is how many renewals a hold may have unread.
	renewalBuffer = 16
	// settleRetry paces retries of a terminal publish or an ack that
	// failed: the task's outcome is decided, only its delivery is owed.
	settleRetry = 500 * time.Millisecond
	// shutdownWait bounds Run's wait for handlers to return on shutdown.
	shutdownWait = 10 * time.Second
	// abandonDeadline and abandonWithdrawn are the abandon reasons.
	abandonDeadline  = "deadline exceeded"
	abandonWithdrawn = "offer withdrawn: the accept came after the dispatcher stopped waiting"
)

// The kinds a profile's submissions come in; one durable per tier each.
var submitKinds = []string{lib.SubmitKindTop, lib.SubmitKindChild}

// holding is one task this dispatcher holds: offered to a pod, running on
// it, or re-adopted after a restart. It is counted against the caps from
// the moment it is reserved until it is settled.
type holding struct {
	profile, tier, kind, taskID string
	unattended                  bool
	// lease and renew are what the task was offered (or, re-adopted, what
	// its record shows): a config change never moves them.
	lease, renew time.Duration

	// Guarded by Dispatcher.mu.
	pod string

	renewals chan renewal
	done     chan struct{}
}

// renewal is a renewal from the pod the subject names.
type renewal struct {
	pod   string
	final bool
}

type offerKey struct{ profile, tier, pod, taskID string }

// bindBackoff is when a durable's bind is next tried, and the delay after.
type bindBackoff struct {
	next  time.Time
	delay time.Duration
}

// waiting is a submission over a cap, remembered for the health line.
type waiting struct {
	durable  string
	queued   time.Time
	lastSeen time.Time
	why      string
}

// Dispatcher is the profile dispatcher. New builds it; Run runs it.
type Dispatcher struct {
	cfg      Config
	bus      *lib.Client
	src      Source
	pool     Pool
	verifier Verifier
	log      *slog.Logger
	now      func() time.Time

	mu        sync.Mutex
	started   time.Time
	active    map[string]*holding
	offers    map[offerKey]chan lib.DispatchAccept
	waiting   map[string]waiting
	lastAdmit map[string]time.Time
	released  map[string]bool
	subs      map[string]lib.HeldSubscription
	binding   map[string]bool
	draining  []lib.HeldSubscription
	// granted is the durables, and grantedProfiles the profiles (their
	// `.events` publish), the current connection's grants cover: the callout
	// fixes a principal's grants per connection, from the config the
	// operator rendered when it connected. A config listing a durable or a
	// profile outside them needs a reconnect first.
	granted         map[string]bool
	grantedProfiles map[string]bool
	// bindRetry paces the retries of durables that failed to bind.
	bindRetry map[string]bindBackoff
	// regrantNotBefore holds off another round of reconnects after one
	// that left a durable unbindable.
	regrantNotBefore time.Time
	reconnects       atomic.Int64
}

// New builds a dispatcher on bus. src is the install config, pool the
// workers it may offer to, verifier the profile.run check (a nil one refuses
// every submission).
func New(cfg Config, bus *lib.Client, src Source, pool Pool, verifier Verifier, log *slog.Logger) (*Dispatcher, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if bus == nil || src == nil || pool == nil {
		return nil, errors.New("dispatch.New: bus, source and pool are required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Dispatcher{
		cfg: cfg, bus: bus, src: src, pool: pool, verifier: verifier, log: log, now: time.Now,
		active:    map[string]*holding{},
		offers:    map[offerKey]chan lib.DispatchAccept{},
		waiting:   map[string]waiting{},
		lastAdmit: map[string]time.Time{},
		released:  map[string]bool{},
		subs:      map[string]lib.HeldSubscription{},
		binding:   map[string]bool{},
		granted:   map[string]bool{},

		grantedProfiles: map[string]bool{},
		bindRetry:       map[string]bindBackoff{},
	}, nil
}

// Run binds the durables and the answer subjects, then serves until ctx
// ends. On the way out it stops every hold without settling it: each held
// submission goes back to the stream at once, its lease record kept, for
// the next instance to re-adopt.
func (d *Dispatcher) Run(ctx context.Context) error {
	d.mu.Lock()
	d.started = d.now()
	// The connection was made against the config as it stands now.
	for _, p := range d.src.Config().Profiles {
		d.grantedProfiles[p.Name] = true
		for _, dur := range p.Durables {
			d.granted[dur.Name] = true
		}
	}
	d.mu.Unlock()
	acceptSub, err := d.bus.SubscribeCore(acceptWildcard, d.onAccept)
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", acceptWildcard, err)
	}
	defer acceptSub.Stop()
	leaseSub, err := d.bus.SubscribeCore(leaseWildcard, d.onRenewal)
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", leaseWildcard, err)
	}
	defer leaseSub.Stop()

	d.syncBindings(ctx)
	refresh := time.NewTicker(d.cfg.RefreshInterval)
	defer refresh.Stop()
	health := time.NewTicker(d.cfg.HealthInterval)
	defer health.Stop()
	for {
		select {
		case <-ctx.Done():
			d.shutdown()
			return nil
		case <-refresh.C:
			d.syncBindings(ctx)
		case <-health.C:
			d.logHealth()
		}
	}
}

func (d *Dispatcher) shutdown() {
	d.mu.Lock()
	subs := append([]lib.HeldSubscription(nil), d.draining...)
	for _, s := range d.subs {
		subs = append(subs, s)
	}
	d.mu.Unlock()
	for _, s := range subs {
		s.Stop()
	}
	wctx, cancel := context.WithTimeout(context.Background(), shutdownWait)
	defer cancel()
	for _, s := range subs {
		if err := s.Wait(wctx); err != nil {
			d.log.Warn("dispatcher shutdown: a hold did not return in time", "err", err)
		}
	}
}

// syncBindings brings the bound durables in line with the config. A
// durable the config adds outside the current connection's grants first
// costs a reconnect (regrant), since grants are fixed per connection. Every
// granted durable not bound yet is bound on its own goroutine (a durable
// the operator hasn't created yet retries for the lib's bind window, and
// must not hold up the rest). A bound durable the config no longer lists
// has its intake stopped, with no reconnect: a profile absent from the
// config has no durables to read, and tasks already held from it run to
// their end.
func (d *Dispatcher) syncBindings(ctx context.Context) {
	cfg := d.src.Config()
	want := map[string]string{} // durable name -> profile
	durables := map[string]lib.DispatchDurable{}
	var added, addedProfiles []string
	d.mu.Lock()
	for _, p := range cfg.Profiles {
		if !d.grantedProfiles[p.Name] {
			addedProfiles = append(addedProfiles, p.Name)
		}
		for _, dur := range p.Durables {
			want[dur.Name], durables[dur.Name] = p.Name, dur
			if !d.granted[dur.Name] {
				added = append(added, dur.Name)
			}
		}
	}
	cooling := d.now().Before(d.regrantNotBefore)
	d.mu.Unlock()
	if (len(added) > 0 || len(addedProfiles) > 0) && !cooling {
		d.regrant(ctx, want, durables, added, addedProfiles)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for name := range d.grantedProfiles {
		if _, ok := profileOf(cfg, name); !ok {
			delete(d.grantedProfiles, name)
		}
	}
	for name := range d.granted {
		if want[name] == "" {
			delete(d.granted, name)
		}
	}
	now := d.now()
	for name, profile := range want {
		if !d.granted[name] || d.subs[name] != nil || d.binding[name] || now.Before(d.bindRetry[name].next) {
			continue
		}
		d.binding[name] = true
		go d.bind(ctx, profile, durables[name])
	}
	for name, sub := range d.subs {
		if want[name] == "" {
			sub.StopIntake()
			d.draining = append(d.draining, sub)
			delete(d.subs, name)
			d.log.Info("unbound a durable the config no longer lists", "durable", name)
		}
	}
}

// regrant reconnects so the bus connection carries grants for the durables
// and profiles the config added, then binds the durables. A bind still refused (the callout's
// watch lagging the config) gets another reconnect, with backoff, up to
// RegrantAttempts; then it is logged as an error and left for the next
// refresh. Held work is untouched throughout (lib.Client.Reconnect).
func (d *Dispatcher) regrant(ctx context.Context, want map[string]string, durables map[string]lib.DispatchDurable, added, addedProfiles []string) {
	backoff := d.cfg.RegrantBackoff
	pending := added
	for attempt := 1; attempt <= d.cfg.RegrantAttempts; attempt++ {
		if attempt > 1 && !sleepCtx(ctx, backoff) {
			return
		}
		if attempt > 1 {
			backoff *= 2
		}
		rctx, cancel := context.WithTimeout(ctx, d.cfg.ReconnectTimeout)
		err := d.bus.Reconnect(rctx)
		cancel()
		d.reconnects.Add(1)
		if err != nil {
			d.log.Warn("reconnect for new durables' grants", "attempt", attempt, "err", err)
			continue
		}
		// A profile's `.events` grant can't be probed without publishing;
		// the reconnect is what carries it.
		d.mu.Lock()
		for _, p := range addedProfiles {
			d.grantedProfiles[p] = true
		}
		d.mu.Unlock()
		var refused []string
		for _, name := range pending {
			err := d.bindNow(ctx, want[name], durables[name])
			switch {
			case err == nil:
			case errors.Is(err, jetstream.ErrConsumerNotFound):
				// The bus answered, so the grant is there: the durable
				// just isn't yet, and the bind retries pick it up.
				d.noteBindFailure(name, err)
			default:
				refused = append(refused, name)
				d.log.Warn("bind refused after a reconnect; the grants may lag the config", "durable", name, "attempt", attempt, "err", err)
			}
		}
		d.mu.Lock()
		for _, name := range pending {
			if !slices.Contains(refused, name) {
				d.granted[name] = true
			}
		}
		d.mu.Unlock()
		if len(refused) == 0 {
			d.log.Info("reconnected for new durables and profiles", "durables", added, "profiles", addedProfiles, "attempts", attempt)
			return
		}
		pending = refused
	}
	d.mu.Lock()
	d.regrantNotBefore = d.now().Add(d.cfg.RegrantCooldown)
	d.mu.Unlock()
	d.log.Error("durables still not bindable after reconnecting; trying again after the cooldown",
		"durables", pending, "attempts", d.cfg.RegrantAttempts, "cooldown", d.cfg.RegrantCooldown)
}

// noteBindFailure backs a durable's next bind off, doubling to the max.
// Callers don't hold d.mu.
func (d *Dispatcher) noteBindFailure(name string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	b := d.bindRetry[name]
	b.delay = min(max(2*b.delay, d.cfg.BindRetryBase), d.cfg.BindRetryMax)
	b.next = d.now().Add(b.delay)
	d.bindRetry[name] = b
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		d.log.Info("durable not created yet; retrying", "durable", name, "in", b.delay)
	} else {
		d.log.Warn("durable bind failed; retrying", "durable", name, "in", b.delay, "err", err)
	}
}

// bindNow binds one durable within BindAttemptTimeout.
func (d *Dispatcher) bindNow(ctx context.Context, profile string, dur lib.DispatchDurable) error {
	bctx, cancel := context.WithTimeout(ctx, d.cfg.BindAttemptTimeout)
	defer cancel()
	sub, err := d.subscribe(bctx, profile, dur)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if old := d.subs[dur.Name]; old != nil {
		old.StopIntake()
		d.draining = append(d.draining, old)
	}
	d.subs[dur.Name] = sub
	delete(d.bindRetry, dur.Name)
	return nil
}

func (d *Dispatcher) subscribe(ctx context.Context, profile string, dur lib.DispatchDurable) (lib.HeldSubscription, error) {
	return d.bus.SubscribeHeld(ctx, lib.SubscribeConfig{
		Stream:    d.cfg.Stream,
		Durable:   dur.Name,
		Subject:   lib.ProfileDurableFilter(profile, dur.Tier, dur.Kind),
		Session:   profile,
		Agreement: &lib.AgreementPolicy{Submitter: d.cfg.Submitter},
	}, d.handle)
}

// Reconnects counts the reconnects config changes have cost.
func (d *Dispatcher) Reconnects() int64 { return d.reconnects.Load() }

func (d *Dispatcher) bind(ctx context.Context, profile string, dur lib.DispatchDurable) {
	name := dur.Name
	bctx, cancel := context.WithTimeout(ctx, d.cfg.BindAttemptTimeout)
	sub, err := d.subscribe(bctx, profile, dur)
	cancel()
	if err != nil {
		d.noteBindFailure(name, err)
		d.mu.Lock()
		delete(d.binding, name)
		d.mu.Unlock()
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.binding, name)
	delete(d.bindRetry, name)
	if ctx.Err() != nil {
		sub.Stop()
		return
	}
	d.subs[name] = sub
	d.log.Info("bound a profile durable", "durable", name)
}

// handle is one held submission, from delivery to settlement.
func (d *Dispatcher) handle(ctx context.Context, del lib.Delivery) {
	profile, tier, kind, taskID, ok := lib.ParseSubmitSubject(del.Subject())
	if !ok {
		d.log.Error("a held delivery off a non-submission subject", "subject", del.Subject())
		_ = del.Term()
		return
	}
	log := d.log.With("profile", profile, "tier", tier, "kind", kind, "task", taskID)

	// A second delivery of a task this process still holds (the first
	// holder's heartbeats were lost for longer than AckWait): the first
	// holder decides the outcome, and this delivery is settled with it.
	d.mu.Lock()
	if h := d.active[taskID]; h != nil {
		d.mu.Unlock()
		log.Warn("redelivered while still held here; settling with the first holder")
		select {
		case <-h.done:
			d.ackRetry(ctx, del)
		case <-ctx.Done():
		}
		return
	}
	d.mu.Unlock()

	snap := d.src.Config()
	prof, ok := profileOf(snap, profile)
	if !ok {
		// A durable for a profile the config doesn't know yet (or any
		// more): the config lags the operator, so the task stays held.
		log.Warn("no config for this profile; the submission stays queued")
		_ = del.Nak(d.cfg.RetryDelay)
		return
	}

	// Started already? A lease record means the task was offered to a
	// worker, so it is never offered again: re-adopted or failed.
	rec, key, found, err := d.findLease(ctx, profile, tier, taskID)
	if err != nil {
		// Unknown is not "never started": offering it could run it twice.
		log.Warn("could not read the task's lease; the submission stays queued", "err", err)
		_ = del.Nak(d.cfg.RetryDelay)
		return
	}
	if found {
		d.adopt(ctx, log, del, snap, prof, tier, kind, rec, key)
		return
	}

	now := d.now()
	ref, err := d.admit(ctx, del, snap, prof, tier, now)
	if err != nil {
		return // stopping: the lib gives the delivery back
	}
	if ref != nil {
		log.Info("submission refused at admission", "reason", ref.reason, "detail", ref.detail)
		d.clearWaiting(taskID)
		d.reject(ctx, log, del, profile, ref)
		return
	}

	h, why := d.reserve(snap, prof, tier, kind, taskID)
	if h == nil {
		d.noteWaiting(taskID, lib.ProfileDurableName(profile, tier, kind), del.Received(), why)
		log.Debug("over a cap; the submission stays queued", "cap", why)
		_ = del.Nak(d.cfg.RetryDelay)
		return
	}
	d.clearWaiting(taskID)

	rec, key, deadline, err := d.handOff(ctx, log, del, snap, prof, h)
	if err != nil || key == "" {
		d.release(h)
		if err != nil {
			log.Warn("hand-off failed; the submission stays queued", "err", err)
		} else {
			d.noteWaiting(taskID, lib.ProfileDurableName(profile, tier, kind), del.Received(), ReasonNoWorker)
			log.Info("no worker took the task; the submission stays queued")
		}
		if ctx.Err() == nil {
			_ = del.Nak(d.cfg.RetryDelay)
		}
		return
	}
	d.mu.Lock()
	d.lastAdmit[lib.ProfileDurableName(profile, tier, kind)] = d.now()
	d.mu.Unlock()
	log.Info("task handed off", "pod", rec.Pod)
	d.hold(ctx, log, del, h, rec, key, deadline, time.Time{})
}

// reject publishes a refusal's terminal and terminates the submission, so
// it is never redelivered. A terminal that can't be published leaves the
// delivery to come back and be refused again (the terminal's envelope id is
// fixed, so a repeat is a duplicate the stream drops).
func (d *Dispatcher) reject(ctx context.Context, log *slog.Logger, del lib.Delivery, profile string, ref *refusal) {
	for {
		err := d.publishTerminal(ctx, profile, del.Envelope(), lib.StateRejected, ref.reason, ref.detail)
		if err == nil {
			break
		}
		log.Warn("publish the rejection terminal; retrying", "err", err)
		if !sleepCtx(ctx, settleRetry) {
			return
		}
	}
	for del.Term() != nil {
		if !sleepCtx(ctx, settleRetry) {
			return
		}
	}
}

// reserve takes a slot for the task if every cap allows it, or names the
// cap that doesn't. The counts are this process's live holdings, read under
// the lock, so caps follow the config from one decision to the next.
//
// The caps:
//   - the profile's concurrency, for its top-level tasks and, as its own
//     cap, for its children;
//   - the install ceiling C, every kind counted, split into three disjoint
//     shares: the child reserve, the interactive reserve, and the shared
//     rest (at least one slot whenever C is; the config's Validate holds
//     the reserves to C-1 together). A child fills the child reserve first,
//     then the shared slots; an interactive top-level task the interactive
//     reserve first, then the shared slots; unattended top-level work only
//     the shared slots. So parents never hold the slots their children
//     need, and unattended work never holds the slots people need.
//
// The reserves come resolved in the config. A task over a cap is never
// failed for it: it stays queued.
func (d *Dispatcher) reserve(snap lib.DispatchConfig, prof lib.DispatchProfile, tier, kind, taskID string) (*holding, string) {
	unattended := d.cfg.unattended(tier)
	shared := snap.Ceiling - snap.ChildReserve - snap.InteractiveReserve

	d.mu.Lock()
	defer d.mu.Unlock()
	var total, children, interactiveTop, unattendedTop, profKind int
	for _, h := range d.active {
		total++
		switch {
		case h.kind == lib.SubmitKindChild:
			children++
		case h.unattended:
			unattendedTop++
		default:
			interactiveTop++
		}
		if h.profile == prof.Name && h.kind == kind {
			profKind++
		}
	}
	sharedUsed := max(0, children-snap.ChildReserve) + max(0, interactiveTop-snap.InteractiveReserve) + unattendedTop
	var fits bool
	switch {
	case kind == lib.SubmitKindChild:
		fits = children < snap.ChildReserve || sharedUsed < shared
	case unattended:
		fits = sharedUsed < shared
	default:
		fits = interactiveTop < snap.InteractiveReserve || sharedUsed < shared
	}
	switch {
	case profKind >= prof.Concurrency:
		return nil, "profile concurrency"
	case total >= snap.Ceiling:
		return nil, "install ceiling"
	case !fits:
		return nil, "the free slots are reserved for other kinds"
	}
	h := &holding{profile: prof.Name, tier: tier, kind: kind, taskID: taskID, unattended: unattended,
		renewals: make(chan renewal, renewalBuffer), done: make(chan struct{})}
	d.active[taskID] = h
	return h, ""
}

// release frees a holding's slot.
func (d *Dispatcher) release(h *holding) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.active[h.taskID] == h {
		delete(d.active, h.taskID)
		delete(d.released, h.taskID)
		close(h.done)
	}
}

// handOff offers the task to each free pod of its pool in turn until one
// accepts. The lease record is written before each offer, so no worker is
// ever offered a task the bucket doesn't show as started: after a crash
// mid-offer the task is re-adopted (and failed if nobody renews it), never
// offered a second time. An empty key with a nil error means no pod took it.
func (d *Dispatcher) handOff(ctx context.Context, log *slog.Logger, del lib.Delivery, snap lib.DispatchConfig,
	prof lib.DispatchProfile, h *holding) (lib.LeaseRecord, string, time.Time, error) {
	envelope, err := json.Marshal(del.Envelope())
	if err != nil {
		return lib.LeaseRecord{}, "", time.Time{}, err
	}
	// The intervals this task is offered with, kept for its life.
	h.lease, h.renew = seconds(snap.LeaseSeconds), seconds(snap.RenewSeconds)
	acceptTimeout := seconds(snap.AcceptTimeoutSeconds)
	pods := d.pool.Pods(h.profile, h.tier)
	// A pod holding a lease in the bucket is busy even if this process
	// doesn't hold its task yet (a restart's redeliveries come up to an
	// AckWait later): each worker runs one task at a time.
	tried, err := d.leasedPods(ctx, h.profile, h.tier)
	if err != nil {
		return lib.LeaseRecord{}, "", time.Time{}, fmt.Errorf("read which pods hold leases: %w", err)
	}
	for {
		d.mu.Lock()
		pod := d.freePodLocked(pods, h.profile, h.tier, tried)
		var ch chan lib.DispatchAccept
		var ok offerKey
		if pod != "" {
			h.pod = pod
			ch = make(chan lib.DispatchAccept, 1)
			ok = offerKey{h.profile, h.tier, pod, h.taskID}
			d.offers[ok] = ch
		}
		d.mu.Unlock()
		if pod == "" {
			return lib.LeaseRecord{}, "", time.Time{}, nil
		}
		tried[pod] = true
		withdraw := func() {
			d.mu.Lock()
			delete(d.offers, ok)
			d.mu.Unlock()
		}

		now := d.now()
		key := lib.ProfileLeaseKey(h.profile, h.tier, pod, h.taskID)
		rec := lib.LeaseRecord{TaskID: h.taskID, Pod: pod, Kind: h.kind,
			RenewedAt: now, ExpiresAt: now.Add(acceptTimeout + h.lease)}
		if err := d.putLease(ctx, key, rec); err != nil {
			withdraw()
			return lib.LeaseRecord{}, "", time.Time{}, fmt.Errorf("write the lease before the offer: %w", err)
		}
		deadline := now.Add(seconds(prof.ActiveDeadlineSeconds))
		msg := lib.DispatchMessage{Type: lib.DispatchOffer, TaskID: h.taskID, Kind: h.kind, Envelope: envelope,
			Deadline: deadline, LeaseSeconds: snap.LeaseSeconds, RenewSeconds: snap.RenewSeconds}
		if err := d.send(lib.DispatchSubject(h.profile, h.tier, pod), msg); err != nil {
			withdraw()
			d.deleteLease(log, key)
			log.Warn("offer not sent", "pod", pod, "err", err)
			continue
		}
		timer := time.NewTimer(acceptTimeout)
		var acc lib.DispatchAccept
		var answered bool
		select {
		case acc = <-ch:
			answered = true
		case <-timer.C:
		case <-ctx.Done():
			// Stopping mid-offer: the record stays, so the next instance
			// treats the task as started.
			timer.Stop()
			withdraw()
			return lib.LeaseRecord{}, "", time.Time{}, ctx.Err()
		}
		timer.Stop()
		withdraw()
		if answered && acc.Accepted {
			now := d.now()
			rec.AcceptedAt, rec.RenewedAt, rec.ExpiresAt = now, now, now.Add(h.lease)
			if err := d.putLease(ctx, key, rec); err != nil {
				log.Warn("record the accept; the next renewal writes it", "err", err)
			}
			return rec, key, deadline, nil
		}
		if answered {
			log.Info("offer declined", "pod", pod, "reason", acc.Reason)
		} else {
			log.Info("offer not accepted in time", "pod", pod, "timeout", acceptTimeout)
		}
		d.deleteLease(log, key)
		d.mu.Lock()
		h.pod = ""
		d.mu.Unlock()
	}
}

// freePodLocked is a pod of the (profile, tier) pool that holds no task
// here and hasn't been tried for this one. Callers hold d.mu.
func (d *Dispatcher) freePodLocked(pods []string, profile, tier string, tried map[string]bool) string {
	busy := map[string]bool{}
	for _, h := range d.active {
		if h.profile == profile && h.tier == tier && h.pod != "" {
			busy[h.pod] = true
		}
	}
	for _, pod := range pods {
		if !busy[pod] && !tried[pod] {
			return pod
		}
	}
	return ""
}

// adopt takes back a task a lease record shows was started: after a
// restart, or after an earlier holder here gave it up without settling it.
// It is never offered again. It is failed lease-expired only once its
// record has expired and the restart grace, counted from this dispatcher's
// start, has passed without a renewal.
func (d *Dispatcher) adopt(ctx context.Context, log *slog.Logger, del lib.Delivery, snap lib.DispatchConfig, prof lib.DispatchProfile,
	tier, kind string, rec lib.LeaseRecord, key string) {
	pod := keyPod(key)
	h := &holding{profile: prof.Name, tier: tier, kind: kind, taskID: rec.TaskID, unattended: d.cfg.unattended(tier),
		lease: recordLease(rec, seconds(snap.LeaseSeconds)), renew: seconds(snap.RenewSeconds),
		pod: pod, renewals: make(chan renewal, renewalBuffer), done: make(chan struct{})}
	d.mu.Lock()
	d.active[rec.TaskID] = h
	released := d.released[rec.TaskID]
	graceUntil := d.started.Add(seconds(snap.RestartGraceSeconds))
	d.mu.Unlock()
	if released {
		log.Info("re-adopted a task its worker had already finished; settling it", "pod", pod)
		d.settle(ctx, log, del, h, key)
		return
	}
	started := rec.AcceptedAt
	if started.IsZero() {
		started = rec.RenewedAt
	}
	deadline := started.Add(seconds(prof.ActiveDeadlineSeconds))
	log.Info("re-adopted a started task", "pod", pod, "expiresAt", rec.ExpiresAt, "graceUntil", graceUntil)
	d.hold(ctx, log, del, h, rec, key, deadline, graceUntil)
}

// hold keeps a running task's submission held (the lib sends the in-progress
// acks) until the task ends: a final renewal settles it, an expired lease or
// a passed deadline fails it. Neither ever runs it again. graceUntil, set on
// re-adoption, keeps an expired record from failing the task before then.
func (d *Dispatcher) hold(ctx context.Context, log *slog.Logger, del lib.Delivery, h *holding, rec lib.LeaseRecord,
	key string, deadline, graceUntil time.Time) {
	tick := time.NewTicker(max(h.renew/ticksPerRenewal, minTick))
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			// Stopping: no settlement; the record stays for the next
			// instance and the lib gives the submission back.
			return
		case r := <-h.renewals:
			if r.pod != rec.Pod {
				continue
			}
			if r.final {
				log.Info("task finished", "pod", rec.Pod)
				d.settle(ctx, log, del, h, key)
				return
			}
			now := d.now()
			if rec.AcceptedAt.IsZero() {
				rec.AcceptedAt = now
			}
			rec.RenewedAt, rec.ExpiresAt = now, now.Add(h.lease)
			graceUntil = time.Time{}
			if err := d.putLease(ctx, key, rec); err != nil {
				log.Warn("record a renewal", "err", err)
			}
		case <-tick.C:
			now := d.now()
			switch {
			case now.After(deadline):
				log.Warn("task passed its deadline; failing it", "pod", rec.Pod, "deadline", deadline)
				_ = d.send(lib.DispatchSubject(h.profile, h.tier, rec.Pod),
					lib.DispatchMessage{Type: lib.DispatchAbandon, TaskID: h.taskID, Reason: abandonDeadline})
				d.fail(ctx, log, del, h, key, ReasonDeadlineExceeded,
					fmt.Sprintf("ran past the profile's %s limit", deadlineLimit(deadline, rec)))
				return
			case now.Before(graceUntil):
			case rec.Expired(now, 0):
				log.Warn("task's lease expired; failing it", "pod", rec.Pod, "renewedAt", rec.RenewedAt)
				d.fail(ctx, log, del, h, key, ReasonLeaseExpired,
					fmt.Sprintf("worker %s stopped renewing at %s", rec.Pod, rec.RenewedAt.UTC().Format(time.RFC3339)))
				return
			}
		}
	}
}

// recordLease is the lease a record was written with (its expiry less its
// last renewal), which is what the task was offered; fallback when the
// record can't say.
func recordLease(rec lib.LeaseRecord, fallback time.Duration) time.Duration {
	if span := rec.ExpiresAt.Sub(rec.RenewedAt); span > 0 {
		return span
	}
	return fallback
}

func deadlineLimit(deadline time.Time, rec lib.LeaseRecord) time.Duration {
	start := rec.AcceptedAt
	if start.IsZero() {
		start = rec.RenewedAt
	}
	return deadline.Sub(start).Round(time.Second)
}

// fail publishes a terminal failure and settles the task. If the terminal
// can't be published before shutdown the task isn't settled: it comes back
// to the next instance, whose record says started, and is failed there.
func (d *Dispatcher) fail(ctx context.Context, log *slog.Logger, del lib.Delivery, h *holding, key, reason, detail string) {
	for {
		err := d.publishTerminal(ctx, h.profile, del.Envelope(), lib.StateFailed, reason, detail)
		if err == nil {
			break
		}
		log.Warn("publish the failure terminal; retrying", "err", err)
		if !sleepCtx(ctx, settleRetry) {
			return
		}
	}
	d.settle(ctx, log, del, h, key)
}

// settle acks the submission, so it is never redelivered, then deletes the
// lease record and frees the slot. The ack comes first: a record without a
// submission is garbage, but a submission without a record would be offered
// again.
func (d *Dispatcher) settle(ctx context.Context, log *slog.Logger, del lib.Delivery, h *holding, key string) {
	if !d.ackRetry(ctx, del) {
		return
	}
	d.deleteLease(log, key)
	d.release(h)
}

func (d *Dispatcher) ackRetry(ctx context.Context, del lib.Delivery) bool {
	for del.Ack() != nil {
		if !sleepCtx(ctx, settleRetry) {
			return false
		}
	}
	return true
}

// onAccept routes a worker's accept to the offer waiting on it. The pod is
// the subject's token, never a payload field, so an accept on one pod's
// subject never answers another pod's offer. An accept no offer is waiting
// for is answered with an abandon, so a late accepter doesn't run a task the
// dispatcher has given up on.
func (d *Dispatcher) onAccept(msg *nats.Msg) {
	profile, tier, pod, ok := answerSubject(msg.Subject, "accept")
	if !ok {
		return
	}
	var acc lib.DispatchAccept
	if err := json.Unmarshal(msg.Data, &acc); err != nil || acc.Validate() != nil {
		d.log.Warn("ignored a malformed accept", "subject", msg.Subject)
		return
	}
	d.mu.Lock()
	ch := d.offers[offerKey{profile, tier, pod, acc.TaskID}]
	h := d.active[acc.TaskID]
	d.mu.Unlock()
	if ch != nil {
		select {
		case ch <- acc:
		default:
		}
		return
	}
	if h != nil && h.profile == profile && h.tier == tier && d.podOf(h) == pod {
		return // a repeat of an accept already taken
	}
	d.log.Warn("ignored an accept for a task not offered to this pod", "subject", msg.Subject, "task", acc.TaskID)
	if acc.Accepted {
		_ = d.send(lib.DispatchSubject(profile, tier, pod),
			lib.DispatchMessage{Type: lib.DispatchAbandon, TaskID: acc.TaskID, Reason: abandonWithdrawn})
	}
}

// onRenewal routes a renewal to the hold for that task, if this dispatcher
// handed the task to the pod the subject names. A renewal for a task held
// here on another pod is ignored and logged. One for a task not held here
// yet (a restart's redeliveries arrive up to an AckWait later) refreshes the
// record only if the bucket shows the task on that same pod.
func (d *Dispatcher) onRenewal(msg *nats.Msg) {
	profile, tier, pod, ok := answerSubject(msg.Subject, "lease")
	if !ok {
		return
	}
	var r lib.LeaseRenewal
	if err := json.Unmarshal(msg.Data, &r); err != nil || r.Validate() != nil {
		d.log.Warn("ignored a malformed renewal", "subject", msg.Subject)
		return
	}
	d.mu.Lock()
	h := d.active[r.TaskID]
	var owner string
	if h != nil {
		owner = h.pod
	}
	d.mu.Unlock()
	switch {
	case h != nil && h.profile == profile && h.tier == tier && owner == pod:
		rn := renewal{pod: pod, final: r.Final}
		select {
		case h.renewals <- rn:
		default:
			if rn.final {
				go func() {
					select {
					case h.renewals <- rn:
					case <-h.done:
					}
				}()
			}
		}
	case h != nil:
		d.log.Warn("ignored a renewal for a task not handed to this pod", "subject", msg.Subject, "task", r.TaskID)
	default:
		go d.renewOrphan(profile, tier, pod, r)
	}
}

// renewOrphan handles a renewal for a task not held here: refreshed (by
// compare-and-set, so a record deleted meanwhile stays deleted) when the
// bucket holds it under this pod's key, ignored and logged otherwise. A
// final one is remembered, so the task is settled, not failed, when its
// submission comes back.
func (d *Dispatcher) renewOrphan(profile, tier, pod string, r lib.LeaseRenewal) {
	ctx, cancel := context.WithTimeout(context.Background(), kvOpTimeout)
	defer cancel()
	key := lib.ProfileLeaseKey(profile, tier, pod, r.TaskID)
	kv, err := d.bus.KV(ctx, lib.ProfileLeaseBucket)
	if err != nil {
		d.log.Warn("renewal for a task not held here: lease bucket unavailable", "key", key, "err", err)
		return
	}
	entry, err := kv.Get(ctx, key)
	if err != nil {
		d.log.Warn("ignored a renewal for a task not handed to this pod", "key", key, "err", err)
		return
	}
	if r.Final {
		d.mu.Lock()
		d.released[r.TaskID] = true
		d.mu.Unlock()
		return
	}
	var rec lib.LeaseRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		d.log.Warn("renewal for a task not held here: unreadable record", "key", key, "err", err)
		return
	}
	now := d.now()
	if rec.AcceptedAt.IsZero() {
		rec.AcceptedAt = now
	}
	rec.RenewedAt, rec.ExpiresAt = now, now.Add(recordLease(rec, seconds(d.src.Config().LeaseSeconds)))
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	if _, err := kv.Update(ctx, key, data, entry.Revision()); err != nil {
		d.log.Info("renewal for a task not held here: record moved meanwhile", "key", key, "err", err)
	}
}

func (d *Dispatcher) podOf(h *holding) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return h.pod
}

// answerSubject splits a2a.dispatch.<profile>.<tier>.<pod>.<class>.
func answerSubject(subject, class string) (profile, tier, pod string, ok bool) {
	t := strings.Split(subject, ".")
	if len(t) != answerTokens || t[len(t)-1] != class {
		return "", "", "", false
	}
	return t[2], t[3], t[4], true
}

// keyPod is the pod token of a lease key, <profile>.<tier>.<pod>.<task>:
// the key, which only the dispatcher writes, is the pod's identity, not
// the record's Pod field.
func keyPod(key string) string {
	t := strings.Split(key, ".")
	if len(t) != strings.Count(lib.ProfileLeaseKey("p", "t", "x", "k"), ".")+1 {
		return ""
	}
	return t[2]
}

func (d *Dispatcher) send(subject string, msg lib.DispatchMessage) error {
	if err := msg.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	nc := d.bus.Conn()
	if nc == nil {
		return errors.New("not connected")
	}
	return nc.Publish(subject, data)
}

// findLease looks the task up by ProfileLeaseKeyFilter. With more than one
// record (an offer whose cleanup failed, then another), the latest renewed
// wins.
func (d *Dispatcher) findLease(ctx context.Context, profile, tier, taskID string) (lib.LeaseRecord, string, bool, error) {
	kctx, cancel := context.WithTimeout(ctx, kvOpTimeout)
	defer cancel()
	kv, err := d.bus.KV(kctx, lib.ProfileLeaseBucket)
	if err != nil {
		return lib.LeaseRecord{}, "", false, err
	}
	lister, err := kv.ListKeysFiltered(kctx, lib.ProfileLeaseKeyFilter(profile, tier, taskID))
	if err != nil {
		return lib.LeaseRecord{}, "", false, err
	}
	var keys []string
	for k := range lister.Keys() {
		keys = append(keys, k)
	}
	if kctx.Err() != nil {
		return lib.LeaseRecord{}, "", false, kctx.Err()
	}
	sort.Strings(keys)
	var best lib.LeaseRecord
	var bestKey string
	for _, k := range keys {
		entry, err := kv.Get(kctx, k)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return lib.LeaseRecord{}, "", false, err
		}
		var rec lib.LeaseRecord
		if err := json.Unmarshal(entry.Value(), &rec); err != nil {
			// A record nobody can read still says the task started.
			d.log.Warn("unreadable lease record; treating the task as started", "key", k, "err", err)
			rec = lib.LeaseRecord{TaskID: taskID, Pod: keyPod(k)}
		}
		if bestKey == "" || rec.RenewedAt.After(best.RenewedAt) {
			best, bestKey = rec, k
		}
	}
	if bestKey == "" {
		return lib.LeaseRecord{}, "", false, nil
	}
	best.TaskID = taskID
	best.Pod = keyPod(bestKey)
	return best, bestKey, true, nil
}

// leasedPods is the pods that hold a lease record in a (profile, tier).
func (d *Dispatcher) leasedPods(ctx context.Context, profile, tier string) (map[string]bool, error) {
	kctx, cancel := context.WithTimeout(ctx, kvOpTimeout)
	defer cancel()
	kv, err := d.bus.KV(kctx, lib.ProfileLeaseBucket)
	if err != nil {
		return nil, err
	}
	lister, err := kv.ListKeysFiltered(kctx, lib.ProfileLeaseKeyFilter(profile, tier, "*"))
	if err != nil {
		return nil, err
	}
	pods := map[string]bool{}
	for k := range lister.Keys() {
		if pod := keyPod(k); pod != "" {
			pods[pod] = true
		}
	}
	if kctx.Err() != nil {
		return nil, kctx.Err()
	}
	return pods, nil
}

func (d *Dispatcher) putLease(ctx context.Context, key string, rec lib.LeaseRecord) error {
	kctx, cancel := context.WithTimeout(ctx, kvOpTimeout)
	defer cancel()
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	kv, err := d.bus.KV(kctx, lib.ProfileLeaseBucket)
	if err != nil {
		return err
	}
	_, err = kv.Put(kctx, key, data)
	return err
}

func (d *Dispatcher) deleteLease(log *slog.Logger, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), kvOpTimeout)
	defer cancel()
	kv, err := d.bus.KV(ctx, lib.ProfileLeaseBucket)
	if err == nil {
		err = kv.Delete(ctx, key)
	}
	if err != nil {
		log.Warn("delete a lease record", "key", key, "err", err)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
