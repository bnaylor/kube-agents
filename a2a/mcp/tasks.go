package mcp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

const (
	// taskReadTimeout bounds one task read end to end.
	taskReadTimeout = 10 * time.Second
	// taskConsumerIdle is how long the server keeps a read's consumer once
	// it goes idle; the read is over well before then.
	taskConsumerIdle = 5 * time.Second
	taskFetchBatch   = 256
	taskFetchWait    = 2 * time.Second
	// maxTaskEvents bounds what one read folds. A task's events are its
	// status updates and artifact chunks; past this the task is reported as
	// too large to read here rather than read without limit.
	maxTaskEvents = 4096
)

// ErrTaskNotFound is a task with no events on TASKS: never submitted, or aged
// out of the stream's retention.
var ErrTaskNotFound = errors.New("no such task on the bus")

// taskEventSubjects are the subjects tasks/get folds for taskID, whatever its
// addressee: its events and its supervisor's terminal (lib.TaskReplaySubjects
// with the addressee a wildcard, since a caller names a task by id alone).
func taskEventSubjects(taskID string) []string {
	return []string{
		fmt.Sprintf("a2a.tasks.*.%s.%s", taskID, lib.TaskClassEvents),
		fmt.Sprintf("a2a.tasks.*.%s.%s", taskID, lib.TaskClassSupervisor),
	}
}

// ReadTask folds a task from TASKS with what the console grant allows: one
// plain pull consumer filtered to the task's subjects, read until nothing is
// pending, folded by lib.FoldTask so the answer is the gateway's tasks/get
// answer. Not lib.TasksGet, which needs a direct message read for its
// horizon and an ordered consumer, a delete of which the console grant
// refuses.
func (b *NATSBus) ReadTask(ctx context.Context, taskID string) (*lib.Task, error) {
	if !lib.ValidSubjectToken(taskID) {
		return nil, fmt.Errorf("%q is not a task id", taskID)
	}
	ctx, cancel := context.WithTimeout(ctx, taskReadTimeout)
	defer cancel()
	js := b.client.JetStream()
	s, err := js.Stream(ctx, lib.TasksStream)
	if err != nil {
		return nil, err
	}
	cons, err := s.CreateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubjects:    taskEventSubjects(taskID),
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		AckPolicy:         jetstream.AckNonePolicy,
		MemoryStorage:     true,
		InactiveThreshold: taskConsumerIdle,
	})
	if err != nil {
		return nil, err
	}
	pending := cons.CachedInfo().NumPending
	if pending == 0 {
		return nil, ErrTaskNotFound
	}
	var events []*lib.Envelope
	seen := 0
	for {
		wait := taskFetchWait
		if dl, ok := ctx.Deadline(); ok {
			wait = min(wait, time.Until(dl))
		}
		if wait < minFetchWait {
			return nil, fmt.Errorf("read %d events of task %s before the deadline", seen, taskID)
		}
		// Sized to what is pending, so the fetch returns as soon as it has
		// it rather than waiting out FetchMaxWait for a batch that cannot fill.
		batch, err := cons.Fetch(int(min(pending, taskFetchBatch)), jetstream.FetchMaxWait(wait))
		if err != nil {
			return nil, err
		}
		n, done := 0, false
		for msg := range batch.Messages() {
			n++
			seen++
			if seen > maxTaskEvents {
				return nil, fmt.Errorf("task %s has more than %d events, too many to read here", taskID, maxTaskEvents)
			}
			if env := agreedEnvelope(msg.Subject(), msg.Data()); env != nil {
				events = append(events, env)
			}
			if md, err := msg.Metadata(); err == nil {
				pending = md.NumPending
				done = pending == 0
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
			return nil, err
		}
		if done || n == 0 {
			break
		}
	}
	return lib.FoldTask(taskID, events)
}

// agreedEnvelope parses one TASKS message, dropping what lib's own replay
// drops: an unparseable write, and an envelope that disagrees with the subject
// it arrived on (another task's id, a writer the subject does not imply).
func agreedEnvelope(subject string, data []byte) *lib.Envelope {
	env, err := lib.ParseEnvelope(data)
	if err != nil {
		return nil
	}
	if aerr := lib.CheckSubjectAgreement(subject, env, lib.AgreementPolicy{}); aerr != nil && !lib.IsAdvisoryDisagreement(aerr) {
		return nil
	}
	return env
}
