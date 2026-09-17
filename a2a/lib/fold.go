package lib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// TasksStream is the JetStream stream holding a2a.tasks.> (provisioned by
	// the deployment, W2).
	TasksStream = "TASKS"

	// EphemeralConsumerInactiveThreshold is how long the server keeps an
	// ephemeral consumer this module creates on TASKS after its last client
	// went away. TasksGet sets it on the replay's ordered consumer and the
	// worker adapter sets it on a session's named consumers, so the two
	// reap on the same clock.
	//
	// It is set explicitly because nats.go's ordered-consumer default is five
	// MINUTES (v1.53.1, jetstream/ordered.go:635; the caller's value replaces
	// it only when non-zero, :646), and stopping an ordered iterator never
	// deletes the consumer behind it (orderedSubscription.Stop, :364). With
	// the default in place every tasks/get left a consumer on TASKS for five
	// minutes, so the count tracked the call rate over that window rather
	// than the replays in flight, against a max_consumers sized for the
	// latter (#1739). Five seconds is the fallback when the explicit delete
	// below cannot land; it is not the primary cleanup.
	EphemeralConsumerInactiveThreshold = 5 * time.Second

	// replayConsumerDeleteTimeout bounds the explicit delete of a replay
	// consumer. A refused publish gets no reply -- nats.go routes a
	// permissions violation to subscriptions only (processTransientError),
	// so a request on a subject the principal lacks waits out its context --
	// and past the inactive threshold the server has reaped the consumer
	// anyway, so there is nothing to wait longer for.
	replayConsumerDeleteTimeout = EphemeralConsumerInactiveThreshold
)

// Task is the A2A Task materialized by folding a task's event stream —
// tasks/get with no live executor required.
type Task struct {
	ID            string
	ContextID     string
	CorrelationID string
	State         TaskState
	Final         bool
	StatusHistory []TaskState
	Artifacts     []Artifact
	// PostFinalDropped counts events that arrived after the final event and
	// were dropped from the fold (assertion 10): surfaced as a warning and a
	// metric by the caller, never allowed to disturb the terminal state or
	// kill the fold.
	PostFinalDropped int
}

// Artifact returns the merged artifact with the given name, or nil.
func (t *Task) Artifact(name string) *Artifact {
	for i := range t.Artifacts {
		if t.Artifacts[i].Name == name {
			return &t.Artifacts[i]
		}
	}
	return nil
}

// FoldTask folds a task's events (status-update and artifact-update
// envelopes, in stream order) into a Task. Events after the final one are
// dropped and counted in PostFinalDropped (assertion 10) — the caller
// surfaces them as a warning and a metric; the fold survives.
func FoldTask(taskID string, events []*Envelope) (*Task, error) {
	task := &Task{ID: taskID}
	for _, env := range events {
		if env.TaskID != taskID {
			return nil, &ProtocolError{Msg: fmt.Sprintf("event for task %q on task %q's stream", env.TaskID, taskID)}
		}
		if task.Final {
			// Assertion 10: nothing follows the final event. The violation is
			// surfaced (warning + metric, by the caller) and the event
			// dropped; the fold survives - a hostile post-final write must
			// not revoke tasks/get.
			task.PostFinalDropped++
			continue
		}
		if task.CorrelationID == "" {
			task.CorrelationID = env.CorrelationID
		}
		if task.ContextID == "" {
			task.ContextID = env.ContextID
		}
		switch env.Kind {
		case KindStatusUpdate:
			var s StatusUpdate
			if err := json.Unmarshal(env.Payload, &s); err != nil {
				return nil, &ProtocolError{Msg: fmt.Sprintf("malformed status-update %s: %v", env.EnvelopeID, err)}
			}
			if s.TaskID != "" && s.TaskID != taskID {
				return nil, &ProtocolError{Msg: fmt.Sprintf("status-update %s payload names task %q inside task %q", env.EnvelopeID, s.TaskID, taskID)}
			}
			task.State = s.Status.State
			task.Final = s.Final
			task.StatusHistory = append(task.StatusHistory, s.Status.State)
		case KindArtifactUpdate:
			var a ArtifactUpdate
			if err := json.Unmarshal(env.Payload, &a); err != nil {
				return nil, &ProtocolError{Msg: fmt.Sprintf("malformed artifact-update %s: %v", env.EnvelopeID, err)}
			}
			if a.TaskID != "" && a.TaskID != taskID {
				return nil, &ProtocolError{Msg: fmt.Sprintf("artifact-update %s payload names task %q inside task %q", env.EnvelopeID, a.TaskID, taskID)}
			}
			task.mergeArtifact(a)
		default:
			return nil, &ProtocolError{Msg: fmt.Sprintf("kind %q on an events subject", env.Kind)}
		}
	}
	return task, nil
}

// mergeArtifact applies one artifact-update: append chunks extend the
// artifact's parts per A2A chunking rules, otherwise the update replaces or
// introduces the artifact.
func (t *Task) mergeArtifact(u ArtifactUpdate) {
	key := u.Artifact.ArtifactID
	if key == "" {
		key = u.Artifact.Name
	}
	for i := range t.Artifacts {
		k := t.Artifacts[i].ArtifactID
		if k == "" {
			k = t.Artifacts[i].Name
		}
		if k == key {
			if u.Append {
				t.Artifacts[i].Parts = append(t.Artifacts[i].Parts, u.Artifact.Parts...)
			} else {
				t.Artifacts[i] = u.Artifact
			}
			return
		}
	}
	t.Artifacts = append(t.Artifacts, u.Artifact)
}

// TaskReplaySubjects is the pair tasks/get folds, in one stream order: the
// executor's events and the supervisor's terminal, if it wrote one.
func TaskReplaySubjects(addressee, taskID string) []string {
	return []string{TaskEventsSubject(addressee, taskID), TaskSupervisorSubject(addressee, taskID)}
}

// TasksGet replays the task's events and supervisor subjects from sequence 1
// on an ephemeral ordered consumer and folds the result — the durability
// payoff: no live executor required. The two subjects share the TASKS stream
// sequence, so the ordered consumer supplies their total order and the fold
// needs no merge: a supervisor terminal that raced an executor's own lands
// wherever the stream put it, and whichever came second is the post-final
// drop.
func (c *Client) TasksGet(ctx context.Context, addressee, taskID string) (*Task, error) {
	_, js := c.conn()
	subjects := TaskReplaySubjects(addressee, taskID)
	stream, err := js.Stream(ctx, TasksStream)
	if err != nil {
		return nil, fmt.Errorf("stream %s: %w", TasksStream, err)
	}
	// Snapshot the replay horizon first: fold what the stream holds now, and
	// terminate deterministically even while the task is still emitting.
	// GetLastMsgForSubject is single-subject, so the horizon is the later of
	// the two, and a task exists if either subject holds a message.
	var last uint64
	found := false
	for _, subject := range subjects {
		msg, err := stream.GetLastMsgForSubject(ctx, subject)
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				continue
			}
			return nil, fmt.Errorf("replay horizon for %s: %w", taskID, err)
		}
		found = true
		if msg.Sequence > last {
			last = msg.Sequence
		}
	}
	if !found {
		// No events in the retention window: the A2A answer is
		// TaskNotFound, not an empty Task indistinguishable from a broken
		// one.
		return nil, &A2AError{Code: CodeTaskNotFound, Message: fmt.Sprintf("task %q has no events in the retention window", taskID)}
	}
	cons, err := js.OrderedConsumer(ctx, TasksStream, jetstream.OrderedConsumerConfig{
		FilterSubjects:    subjects,
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		InactiveThreshold: EphemeralConsumerInactiveThreshold,
	})
	if err != nil {
		return nil, fmt.Errorf("ordered consumer for %s: %w", taskID, err)
	}
	// Registered before the iterator's Stop so that it runs after it (defers
	// unwind in reverse), and before Messages so a failed iterator still
	// releases the consumer it was created for.
	defer c.deleteReplayConsumer(js, cons, taskID)
	it, err := cons.Messages()
	if err != nil {
		return nil, fmt.Errorf("replay messages for %s: %w", taskID, err)
	}
	defer it.Stop()
	// it.Next does not observe ctx on its own; stopping the iterator is what
	// unblocks it, so a canceled context cannot hang the replay.
	stopWatch := context.AfterFunc(ctx, it.Stop)
	defer stopWatch()
	var events []*Envelope
	for {
		msg, err := it.Next()
		if err != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("replay for %s: %w", taskID, ctx.Err())
			}
			return nil, fmt.Errorf("replay next for %s: %w", taskID, err)
		}
		meta, err := msg.Metadata()
		if err != nil {
			return nil, fmt.Errorf("replay metadata for %s: %w", taskID, err)
		}
		subject := msg.Subject()
		env, err := ParseEnvelope(msg.Data())
		if err != nil {
			// A hostile or foreign write must not revoke tasks/get for the
			// task: the live path terms poison and keeps going, so replay
			// skips it the same way rather than failing the whole fold.
			c.log.Error("a2a replay skipping unparseable event", "subject", subject, "err", err)
		} else if aerr := CheckSubjectAgreement(subject, env, c.opts.agreement); aerr != nil && !IsAdvisoryDisagreement(aerr) {
			// A relocated envelope - the wrong kind for the class, another
			// task's id, a writer the subject does not imply - carries no
			// identity and does not fold. Replay's job is narrower than the
			// live path's: FoldTask would hard-error on a foreign kind or
			// taskId, and one foreign write must not revoke tasks/get for
			// the task, so the screen drops it and counts it instead.
			c.protocolViolations.Add(1)
			c.log.Error("a2a replay skipping envelope that disagrees with its subject", "subject", subject, "err", aerr)
		} else {
			if aerr != nil {
				// The advisory `…events` writer check: a pre-split
				// supervisor terminal, or a forged one. Counted, folded,
				// and attributed to the subject's principal - never to
				// its `from`.
				c.protocolViolations.Add(1)
				c.log.Warn("a2a replay folding envelope whose writer disagrees with its subject (advisory)", "subject", subject, "err", aerr)
			}
			events = append(events, env)
		}
		// Two exits: the snapshotted horizon, or nothing left pending — the
		// horizon message itself may have aged out between snapshot and
		// replay, and waiting for it then would block forever.
		if meta.Sequence.Stream >= last || meta.NumPending == 0 {
			break
		}
	}
	task, err := FoldTask(taskID, events)
	if err != nil {
		return nil, err
	}
	if task.PostFinalDropped > 0 {
		c.protocolViolations.Add(int64(task.PostFinalDropped))
		c.log.Warn("a2a events after final dropped from fold",
			"task", taskID, "dropped", task.PostFinalDropped)
	}
	return task, nil
}

// deleteReplayConsumer removes the ordered consumer TasksGet created, under
// the name it carries when TasksGet returns. Stopping the iterator does not
// do this (nats.go v1.53.1, orderedSubscription.Stop), and the name is read
// at return time rather than at creation because an ordered consumer that
// reset mid-replay -- a reconnect, a sequence gap -- is a new consumer under
// a new name, and the old one is the reset's to delete, not this function's.
//
// Best effort, off the caller's path. It runs in its own goroutine with its
// own deadline because the principal may not hold
// $JS.API.CONSUMER.DELETE.TASKS.* -- the rendered worker grant does not, and
// the gateway's narrowing in #1672 withholds it -- and a request on a subject
// the principal lacks is refused without a reply, so a synchronous delete
// would hold every tasks/get for the whole timeout under that grant. The
// refusal itself reaches the connection's async error handler, which logs it
// at Error naming the subject; this logs the outcome at Debug because the
// inactive threshold on the consumer reaps it within
// EphemeralConsumerInactiveThreshold either way, and a not-found answer is
// the threshold having got there first.
func (c *Client) deleteReplayConsumer(js jetstream.JetStream, cons jetstream.Consumer, taskID string) {
	info := cons.CachedInfo()
	if info == nil {
		return
	}
	name := info.Name
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), replayConsumerDeleteTimeout)
		defer cancel()
		err := js.DeleteConsumer(ctx, TasksStream, name)
		if err == nil || errors.Is(err, jetstream.ErrConsumerNotFound) {
			return
		}
		c.log.Debug("a2a replay consumer not deleted; the inactive threshold reaps it",
			"task", taskID, "consumer", name, "threshold", EphemeralConsumerInactiveThreshold, "err", err)
	}()
}

// ValidateArtifacts enforces assertion 18: a completed task carries at least
// one result artifact, and reserved names carry only their defined content —
// result is the deliverable, thinking and progress are text, activity is the
// structured tool-call trace.
func (t *Task) ValidateArtifacts() error {
	if t.State == StateCompleted && t.Artifact(ArtifactResult) == nil {
		return &ProtocolError{Msg: fmt.Sprintf("task %q completed without a result artifact", t.ID)}
	}
	for _, a := range t.Artifacts {
		switch a.Name {
		case ArtifactThinking, ArtifactProgress:
			for _, p := range a.Parts {
				if p.Kind != "text" {
					return &ProtocolError{Msg: fmt.Sprintf("artifact %q carries a %q part; reserved name is text-only", a.Name, p.Kind)}
				}
			}
		case ArtifactActivity:
			for _, p := range a.Parts {
				if p.Kind != "data" {
					return &ProtocolError{Msg: fmt.Sprintf("artifact %q carries a %q part; the tool-call trace is data parts", a.Name, p.Kind)}
				}
			}
		}
	}
	return nil
}
