package hermesbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// The in-flight registry: one KV entry per accepted task, written before
// submitted is published and deleted after the terminal event. The sweep
// reads it on startup, so a bridge death mid-task costs an honest terminal
// failed rather than a task that hangs open forever.

func (b *Bridge) kvKey(taskID string) string {
	return "bridge." + b.cfg.Profile + "." + taskID
}

func (b *Bridge) markInFlight(ctx context.Context, taskID string) error {
	_, err := b.kv.Put(ctx, b.kvKey(taskID), []byte(b.from.Session))
	return err
}

func (b *Bridge) clearInFlight(ctx context.Context, taskID string) {
	if err := b.kv.Delete(ctx, b.kvKey(taskID)); err != nil {
		b.cfg.Logger.Warn("in-flight registry delete failed", "task", taskID, "err", err)
	}
}

// sweep finalizes tasks a prior incarnation accepted and never finished.
// Runs before the durable consumer starts, so nothing it reads is a task
// this incarnation is executing.
func (b *Bridge) sweep(ctx context.Context) error {
	lister, err := b.kv.ListKeys(ctx)
	if err != nil {
		return fmt.Errorf("list in-flight keys: %w", err)
	}
	prefix := "bridge." + b.cfg.Profile + "."
	var keys []string
	for key := range lister.Keys() {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		taskID := strings.TrimPrefix(key, prefix)
		if err := b.sweepTask(ctx, taskID); err != nil {
			return fmt.Errorf("sweep task %s: %w", taskID, err)
		}
		if err := b.kv.Delete(ctx, key); err != nil {
			b.cfg.Logger.Warn("sweep key delete failed", "task", taskID, "err", err)
		}
	}
	return nil
}

func (b *Bridge) sweepTask(ctx context.Context, taskID string) error {
	task, err := b.c.TasksGet(ctx, b.cfg.Profile, taskID)
	switch {
	case isTaskNotFound(err):
		// Died between the KV put and the submitted publish: the unacked
		// submission redelivers, so there is nothing to finalize.
		return nil
	case err != nil:
		return err
	case task.Final:
		// Died between the terminal publish and the KV delete.
		return nil
	}
	b.cfg.Logger.Warn("sweeping orphaned task to terminal failed", "task", taskID, "state", task.State)
	return b.synthesizeTerminal(ctx, taskID, task,
		lib.StateFailed, "reason: bridge-died-without-terminal-event")
}

// synthesizeTerminal writes a terminal event on behalf of a task with no
// live executor. Compare-and-swap per the profiles spec, not read-then-write:
// the publish pins the expected last subject sequence, so a dying process's
// flush racing this write wins cleanly and the loser re-reads instead of
// double-finalizing.
func (b *Bridge) synthesizeTerminal(ctx context.Context, taskID string, task *lib.Task, state lib.TaskState, reason string) error {
	subject := lib.TaskEventsSubject(b.cfg.Profile, taskID)
	stream, err := b.js.Stream(ctx, lib.TasksStream)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 3; attempt++ {
		last, err := stream.GetLastMsgForSubject(ctx, subject)
		if err != nil {
			return fmt.Errorf("last event for %s: %w", taskID, err)
		}
		payload, err := json.Marshal(lib.StatusUpdate{
			TaskID:    taskID,
			ContextID: task.ContextID,
			Status: lib.TaskStatus{
				State: state,
				Message: &lib.Message{
					Role:      "agent",
					MessageID: "msg-" + nuid.Next(),
					Parts:     []lib.Part{{Kind: "text", Text: reason}},
					TaskID:    taskID,
					ContextID: task.ContextID,
				},
			},
			Final: true,
		})
		if err != nil {
			return err
		}
		env, err := lib.NewStatusUpdateEnvelope(b.from, taskID, task.ContextID, task.CorrelationID, payload)
		if err != nil {
			return err
		}
		if err := env.ValidateEmit(); err != nil {
			return err
		}
		data, err := json.Marshal(env)
		if err != nil {
			return err
		}
		_, err = b.js.Publish(ctx, subject, data,
			jetstream.WithMsgID(env.EnvelopeID),
			jetstream.WithExpectLastSequencePerSubject(last.Sequence))
		if err == nil {
			return nil
		}
		var apiErr *jetstream.APIError
		if !errors.As(err, &apiErr) || apiErr.ErrorCode != jetstream.JSErrCodeStreamWrongLastSequence {
			return fmt.Errorf("synthesize terminal for %s: %w", taskID, err)
		}
		// Lost the CAS: something else wrote. Re-read; if it was the terminal
		// event, done.
		task, err = b.c.TasksGet(ctx, b.cfg.Profile, taskID)
		if err != nil {
			return err
		}
		if task.Final {
			return nil
		}
	}
	return fmt.Errorf("synthesize terminal for %s: lost the CAS three times to non-terminal writes", taskID)
}
