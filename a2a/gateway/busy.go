package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
	"github.com/nats-io/nats.go/jetstream"
)

// defaultBusyNoticeAt is what BusyNoticeAt means when unset: the rendered
// Hermes bridge's default BRIDGE_CONCURRENCY (a2aRenderedBridgeDefaultConcurrency
// in the k8s-operator module), so on a default install the notice goes out
// exactly when a new turn has to wait for a worker. The operator renders the
// env from the bridge's worker count; this default is for a gateway run
// outside the operator.
const defaultBusyNoticeAt = 10

// busyCountTimeout bounds one count. A count that takes longer sends no
// notice for that turn; the task is already submitted either way.
const busyCountTimeout = 5 * time.Second

// sessionRecordKeys is the KV filter that matches every session record and
// no task index entry (kvKey and taskKey in registry.go).
const sessionRecordKeys = "sessions.>"

// errSessionWatchClosed is a session-record watch that ended before it had
// delivered every current record.
var errSessionWatchClosed = errors.New("session-state watch closed before its initial values were delivered")

// The busy notice, singular and plural. Plain words, no task id: it is for
// the person who just asked, and what they need is that the request is in and
// roughly how long the line is. "As soon as there's room" rather than "as
// soon as one finishes", because the count includes the running tasks, and
// with more ahead than the executor runs at once more than one has to finish.
const (
	busyNoticeOne  = "🚦 The system is busy. 1 request is ahead of yours; I'll start on it as soon as there's room."
	busyNoticeMany = "🚦 The system is busy. %d requests are ahead of yours; I'll start on it as soon as there's room."
)

// busyNotice renders the notice for ahead tasks in front of a new turn.
func busyNotice(ahead int) string {
	if ahead == 1 {
		return busyNoticeOne
	}
	return fmt.Sprintf(busyNoticeMany, ahead)
}

// busyNoticeBackend reports whether a turn that arrived through backend gets
// the busy notice: the chat backends and the console do. The inject and A2A
// doors do not, because their callers are programs that read every
// unedited post of a task as its output (the eval harness's deliverable, an
// A2A client's task messages), so the notice would be graded or returned as
// part of the answer.
func busyNoticeBackend(backend string) bool {
	return backend != injectBackend && backend != a2aBackend
}

// busyNoticeAt is the configured threshold, or defaultBusyNoticeAt when the
// Config leaves it unset (a gateway built in a test or a playground, where
// FromEnv did not run).
func (g *Gateway) busyNoticeAt() int {
	if g.cfg.BusyNoticeAt > 0 {
		return g.cfg.BusyNoticeAt
	}
	return defaultBusyNoticeAt
}

// fixedRouteAhead decides whether a turn that has just started taskID on rec
// gets the busy notice, and with what count. It answers only for a task
// headed to the fixed addressee from a chat backend; everything else is
// (0, false) without a count. It runs after the submission, so the count
// costs the turn nothing before its task is on the bus, and leaves taskID
// itself out, so the number is the tasks ahead of it. A count that fails
// sends no notice: the notice is informational, and a guess is worse than
// silence here.
func (g *Gateway) fixedRouteAhead(ctx context.Context, rec *SessionRecord, backend, taskID string) (int, bool) {
	if g.cfg.DefaultAddressee == RouteSession || rec.Addressee != g.cfg.DefaultAddressee ||
		!busyNoticeBackend(backend) {
		return 0, false
	}
	n, err := g.fixedRouteBacklog(ctx, taskID)
	if err != nil {
		g.log.Warn("busy count failed; no busy notice for this turn",
			"conversation", rec.Key, "err", err)
		return 0, false
	}
	return n, n >= g.busyNoticeAt()
}

// fixedRouteBacklog counts the fixed addressee's outstanding work, leaving
// out the task exclude (the turn's own, "" for none): session records whose
// active task was published to DefaultAddressee, read from session-state, so
// the count is the same after a gateway restart as before it. Past
// FirstEventGrace a task is read from the stream and left out when nothing is
// on it (busyNeverStarted: no executor took it) or when its newest event is a
// terminal (the record's clear was lost, the case the heal releases on the
// conversation's next turn). Either would otherwise hold the number up until
// someone spoke in that conversation again. Inside the grace a task is
// counted without a read. A detached task is counted while it is still its
// record's active task; a new turn in that conversation replaces it.
//
// The count falls as terminals arrive: the relay's durable consumer delivers
// a terminal published while the gateway was down once it is back, and the
// terminal deletes the active task from the record.
func (g *Gateway) fixedRouteBacklog(ctx context.Context, exclude string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, busyCountTimeout)
	defer cancel()
	now := time.Now()
	n := 0
	tasks := &busyTasksStream{g: g}
	err := g.reg.eachSessionRecord(ctx, func(rec *SessionRecord) {
		active := rec.ActiveTask
		if active == nil || active.TaskID == exclude ||
			rec.AddresseeFor(active.TaskID) != g.cfg.DefaultAddressee {
			return
		}
		if g.busyTaskLeftOut(ctx, tasks, active, g.cfg.DefaultAddressee, now) {
			return
		}
		n++
	})
	return n, err
}

// eachSessionRecord calls fn with every session record in the bucket, read
// through one KV watch (an ordered consumer that streams the current value
// of every key) rather than ScanSessions' key listing and one Get per key.
// The count runs on a human's turn, and a Get per record is a round trip per
// record: a week of conversations at SessionTTL is thousands of them. The
// watch is the same consumer kind the key listing already opens, so it needs
// no grant the gateway does not hold. A record that does not parse is
// skipped, as the reap scan skips one.
func (r *Registry) eachSessionRecord(ctx context.Context, fn func(*SessionRecord)) error {
	kv, err := r.kv(ctx)
	if err != nil {
		return err
	}
	w, err := kv.WatchFiltered(ctx, []string{sessionRecordKeys}, jetstream.IgnoreDeletes())
	if err != nil {
		return err
	}
	defer func() { _ = w.Stop() }()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case entry, ok := <-w.Updates():
			if !ok {
				return errSessionWatchClosed
			}
			if entry == nil {
				// The marker after the initial values: every record
				// current when the watch opened has been delivered.
				return nil
			}
			var rec SessionRecord
			if err := json.Unmarshal(entry.Value(), &rec); err != nil {
				continue
			}
			fn(&rec)
		}
	}
}

// busyTaskLeftOut reports whether an active task is left out of the count:
// past the grace, with nothing on its stream (busyNeverStarted) or a terminal
// as its newest event. Inside the grace it answers false without reading the
// stream. A read that fails counts the task: a transport failure cannot rule
// out events, the rule the heal follows.
func (g *Gateway) busyTaskLeftOut(ctx context.Context, tasks *busyTasksStream, active *ActiveTask, addressee string, now time.Time) bool {
	if active.SubmittedAt.IsZero() || now.Sub(active.SubmittedAt) <= g.cfg.FirstEventGrace {
		return false
	}
	empty, final, err := tasks.readTask(ctx, addressee, active.TaskID)
	if err != nil {
		g.log.Warn("busy count: stream read failed; counting the task",
			"taskId", active.TaskID, "err", err)
		return false
	}
	return final || busyNeverStarted(active, empty, g.cfg.FirstEventGrace, now)
}

// busyNeverStarted is the never-started test the heal (healActiveTask)
// applies: nothing on the task's replay subjects, and the task older than the
// grace. A task with no SubmittedAt has no age and never qualifies. The heal
// spells the same test inline; this one is a pure function of its arguments
// so that one helper can serve both, by a rename, once the heal takes it.
func busyNeverStarted(active *ActiveTask, streamEmpty bool, grace time.Duration, now time.Time) bool {
	return active != nil && streamEmpty &&
		!active.SubmittedAt.IsZero() && now.Sub(active.SubmittedAt) > grace
}

// busyTasksStream is one count's handle on the TASKS stream, looked up on the
// first task that needs a read and reused for the rest, so a count over N
// stale tasks costs one STREAM.INFO and 2N direct gets rather than 3N round
// trips inside busyCountTimeout.
type busyTasksStream struct {
	g      *Gateway
	stream jetstream.Stream
}

func (b *busyTasksStream) handle(ctx context.Context) (jetstream.Stream, error) {
	if b.stream != nil {
		return b.stream, nil
	}
	stream, err := b.g.client.JetStream().Stream(ctx, lib.TasksStream)
	if err != nil {
		return nil, fmt.Errorf("stream %s: %w", lib.TasksStream, err)
	}
	b.stream = stream
	return stream, nil
}

// readTask reads the newest message on each of a task's replay subjects
// (lib.TaskReplaySubjects) with a direct get, and no consumer, so a count
// over many records opens no ephemeral consumer on TASKS. empty is nothing on
// either subject in the retention window, the question the replay answers
// with TaskNotFound; any message at all is a message here, as it is for the
// replay. final is a terminal status as the newest message on either: the
// executor writes nothing after its terminal, and the supervisor subject
// carries only terminals. An error is the read failing, not the subjects
// being empty.
func (b *busyTasksStream) readTask(ctx context.Context, addressee, taskID string) (empty, final bool, err error) {
	stream, err := b.handle(ctx)
	if err != nil {
		return false, false, err
	}
	empty = true
	for _, subject := range lib.TaskReplaySubjects(addressee, taskID) {
		msg, err := stream.GetLastMsgForSubject(ctx, subject)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return false, false, fmt.Errorf("newest message on %s: %w", subject, err)
		}
		empty = false
		final = final || isFinalStatus(msg.Data)
	}
	return empty, final, nil
}

// isFinalStatus reports whether data is a status-update envelope marked
// final. Anything that does not parse as one is not a terminal.
func isFinalStatus(data []byte) bool {
	env, err := lib.ParseEnvelope(data)
	if err != nil || env.Kind != lib.KindStatusUpdate {
		return false
	}
	var s lib.StatusUpdate
	if err := json.Unmarshal(env.Payload, &s); err != nil {
		return false
	}
	return s.Final
}
