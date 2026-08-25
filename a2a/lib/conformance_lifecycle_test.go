package lib

// Lifecycle and correlation conformance assertions 12-15 and 18. The
// executor side is played by the library's own TaskExecution helpers — the
// same code path W4's worker adapter will use.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

// taskHarness wires a requester and an executor onto one task the way the
// gateway and worker adapter will: requester publishes to .in and folds via
// TasksGet, executor consumes .in and publishes events.
type taskHarness struct {
	t         *testing.T
	ctx       context.Context
	requester *Client
	executor  *Client
	taskID    string
	addressee string     // the executor's name: the subject token (0.4)
	inbox     *collector // what the executor received on .in
	exec      *TaskExecution
	origin    *Envelope
}

func newTaskHarness(t *testing.T, url, taskID string) *taskHarness {
	t.Helper()
	ctx := testCtx(t)
	req, err := Connect(ctx, url, WithName("requester"))
	if err != nil {
		t.Fatalf("Connect requester: %v", err)
	}
	t.Cleanup(req.Close)
	ex, err := Connect(ctx, url, WithName("executor"))
	if err != nil {
		t.Fatalf("Connect executor: %v", err)
	}
	t.Cleanup(ex.Close)

	h := &taskHarness{t: t, ctx: ctx, requester: req, executor: ex, taskID: taskID,
		addressee: "worker-" + taskID, inbox: &collector{}}
	_, err = ex.SubscribeDurable(ctx, SubscribeConfig{
		Stream:  "TASKS",
		Subject: TaskInSubject(h.addressee, taskID),
		Durable: "exec-" + taskID,
		Session: h.addressee,
	}, h.inbox.handle)
	if err != nil {
		t.Fatalf("executor subscribe: %v", err)
	}
	return h
}

// submit publishes the task submission and builds the executor's
// TaskExecution from the received origin envelope.
func (h *taskHarness) submit(correlationID string) {
	h.t.Helper()
	// The submission is addressed: to must agree with the subject's addressee
	// token (0.4).
	env, err := NewMessageEnvelope(Party{Session: "chatops"}, h.taskID, "ctx-"+h.taskID, correlationID,
		validMessagePayload(), WithTo(Party{Session: h.addressee}))
	if err != nil {
		h.t.Fatalf("build submission: %v", err)
	}
	if err := h.requester.Publish(h.ctx, TaskInSubject(h.addressee, h.taskID), env); err != nil {
		h.t.Fatalf("publish submission: %v", err)
	}
	waitFor(h.t, 5e9, "submission delivery", func() bool { return h.inbox.count() >= 1 })
	origin := h.inbox.all()[0]
	exec, err := h.executor.NewTaskExecution(origin, Party{Session: h.addressee, AgentType: "claude-code"}, h.addressee)
	if err != nil {
		h.t.Fatalf("NewTaskExecution: %v", err)
	}
	h.exec = exec
	h.origin = origin
}

// followUp publishes a follow-up message on the same taskId and waits for the
// executor to receive it.
func (h *taskHarness) followUp(text string) {
	h.t.Helper()
	before := h.inbox.count()
	payload := fmt.Sprintf(`{"role": "user", "parts": [{"kind": "text", "text": %q}], "messageId": "m-%d"}`, text, before)
	// Follow-ups and steers reuse the task's original correlationId (0.4
	// field rule); the helper encodes it.
	env, err := NewFollowUpEnvelope(h.origin, Party{Session: "chatops"}, json.RawMessage(payload))
	if err != nil {
		h.t.Fatalf("build follow-up: %v", err)
	}
	if env.CorrelationID != h.origin.CorrelationID {
		h.t.Fatalf("follow-up correlationId = %q, want the task's original %q", env.CorrelationID, h.origin.CorrelationID)
	}
	if err := h.requester.Publish(h.ctx, TaskInSubject(h.addressee, h.taskID), env); err != nil {
		h.t.Fatalf("publish follow-up: %v", err)
	}
	waitFor(h.t, 5e9, "follow-up delivery", func() bool { return h.inbox.count() > before })
}

func (h *taskHarness) status(state TaskState, final bool) {
	h.t.Helper()
	if err := h.exec.PublishStatus(h.ctx, state, final); err != nil {
		h.t.Fatalf("publish status %s: %v", state, err)
	}
}

func (h *taskHarness) artifact(name, text string) {
	h.t.Helper()
	err := h.exec.PublishArtifact(h.ctx, Artifact{
		ArtifactID: "art-" + name,
		Name:       name,
		Parts:      []Part{{Kind: "text", Text: text}},
	})
	if err != nil {
		h.t.Fatalf("publish artifact %s: %v", name, err)
	}
}

func (h *taskHarness) get() *Task {
	h.t.Helper()
	task, err := h.requester.TasksGet(h.ctx, h.addressee, h.taskID)
	if err != nil {
		h.t.Fatalf("TasksGet: %v", err)
	}
	return task
}

// Assertion 12: a follow-up message with the same taskId resumes an
// input-required task, and the next status event is working. A follow-up
// during working (steering) is delivered to the executor and does not by
// itself change task state.
func TestAssertion12_FollowUpAndSteering(t *testing.T) {
	s := startServer(t)
	provisionTasksStream(t, clientURL(s))
	h := newTaskHarness(t, clientURL(s), "task-a12")

	h.submit("corr-lc")
	h.status(StateSubmitted, false)
	h.status(StateWorking, false)
	h.status(StateInputRequired, false)

	h.followUp("here is the input you asked for")
	h.status(StateWorking, false) // executor resumes

	h.followUp("actually, also check X") // steering mid-working
	// Steering produced no status event; the executor received it and keeps
	// working. Then it finishes.
	h.artifact(ArtifactResult, "done")
	h.status(StateCompleted, true)

	task := h.get()
	wantHistory := []TaskState{StateSubmitted, StateWorking, StateInputRequired, StateWorking, StateCompleted}
	if len(task.StatusHistory) != len(wantHistory) {
		t.Fatalf("status history = %v, want %v", task.StatusHistory, wantHistory)
	}
	for i, st := range wantHistory {
		if task.StatusHistory[i] != st {
			t.Fatalf("status history = %v, want %v", task.StatusHistory, wantHistory)
		}
	}
	// The transition after input-required is working: 12's first half.
	// The steering follow-up added no transition: 12's second half — the
	// history holds exactly one working after input-required, nothing between
	// it and completed.
	if h.inbox.count() != 3 { // submission + follow-up + steer all reached the executor
		t.Errorf("executor received %d messages on .in, want 3", h.inbox.count())
	}
}

// Assertion 13: a cancel always results in a terminal event — canceled, or
// completed if the race was lost — never a silent stop.
func TestAssertion13_CancelTerminal(t *testing.T) {
	s := startServer(t)
	provisionTasksStream(t, clientURL(s))

	t.Run("cancel_wins", func(t *testing.T) {
		h := newTaskHarness(t, clientURL(s), "task-a13a")
		h.submit("corr-lc")
		h.status(StateSubmitted, false)
		h.status(StateWorking, false)

		cancel, err := NewCancelEnvelope(Party{Session: "chatops"}, h.taskID, "ctx-"+h.taskID, "corr-lc")
		if err != nil {
			t.Fatal(err)
		}
		if err := h.requester.Publish(h.ctx, TaskInSubject(h.addressee, h.taskID), cancel); err != nil {
			t.Fatalf("publish cancel: %v", err)
		}
		waitFor(t, 5e9, "cancel delivery", func() bool {
			for _, e := range h.inbox.all() {
				if e.Kind == KindCancel {
					return true
				}
			}
			return false
		})
		h.status(StateCanceled, true)

		task := h.get()
		if task.State != StateCanceled || !task.Final {
			t.Errorf("task = %s final=%v, want canceled final=true", task.State, task.Final)
		}
	})

	t.Run("completion_wins_race", func(t *testing.T) {
		h := newTaskHarness(t, clientURL(s), "task-a13b")
		h.submit("corr-lc")
		h.status(StateSubmitted, false)
		h.status(StateWorking, false)
		h.artifact(ArtifactResult, "finished before the cancel arrived")
		h.status(StateCompleted, true)

		cancel, err := NewCancelEnvelope(Party{Session: "chatops"}, h.taskID, "ctx-"+h.taskID, "corr-lc")
		if err != nil {
			t.Fatal(err)
		}
		if err := h.requester.Publish(h.ctx, TaskInSubject(h.addressee, h.taskID), cancel); err != nil {
			t.Fatalf("publish cancel: %v", err)
		}
		// The cancel still reaches the executor - losing the race means the
		// terminal event wins, not that the cancel was silently dropped.
		waitFor(t, 5e9, "late cancel delivery", func() bool {
			for _, e := range h.inbox.all() {
				if e.Kind == KindCancel {
					return true
				}
			}
			return false
		})

		task := h.get()
		if task.State != StateCompleted || !task.Final {
			t.Errorf("task = %s final=%v, want completed final=true (terminal event wins)", task.State, task.Final)
		}
	})
}

// Assertion 14: correlationId is preserved verbatim across every hop the
// library mediates, and a child task created through the library inherits its
// parent's value.
func TestAssertion14_CorrelationPreserved(t *testing.T) {
	origin, err := NewMessageEnvelope(Party{Session: "chatops"}, "task-a14", "ctx-a14",
		"corr-original-do-not-remint", validMessagePayload())
	if err != nil {
		t.Fatal(err)
	}

	exec, err := (&Client{}).NewTaskExecution(origin, Party{Session: "worker-a14"}, "worker-a14")
	if err != nil {
		t.Fatalf("NewTaskExecution: %v", err)
	}

	status, err := exec.StatusEnvelope(StateWorking, false)
	if err != nil {
		t.Fatal(err)
	}
	if status.CorrelationID != origin.CorrelationID {
		t.Errorf("status correlationId = %q, want %q", status.CorrelationID, origin.CorrelationID)
	}

	child, err := NewChildTaskEnvelope(origin, Party{Session: "worker-a14"}, "task-a14-child", "ctx-a14",
		validMessagePayload())
	if err != nil {
		t.Fatal(err)
	}
	if child.CorrelationID != origin.CorrelationID {
		t.Errorf("child correlationId = %q, want parent's %q", child.CorrelationID, origin.CorrelationID)
	}

	// 0.4 tightening: a follow-up or steer reuses the task's original
	// correlationId, never a re-mint.
	followUp, err := NewFollowUpEnvelope(origin, Party{Session: "chatops"}, validMessagePayload())
	if err != nil {
		t.Fatal(err)
	}
	if followUp.CorrelationID != origin.CorrelationID {
		t.Errorf("follow-up correlationId = %q, want the task's original %q", followUp.CorrelationID, origin.CorrelationID)
	}
	if followUp.TaskID != origin.TaskID || followUp.ContextID != origin.ContextID {
		t.Errorf("follow-up ids = %q/%q, want origin's %q/%q", followUp.TaskID, followUp.ContextID, origin.TaskID, origin.ContextID)
	}
}

// Assertion 15: every event a task emits carries the taskId and correlationId
// of its originating message.
func TestAssertion15_EventsCarryIDs(t *testing.T) {
	origin, err := NewMessageEnvelope(Party{Session: "chatops"}, "task-a15", "ctx-a15", "corr-a15",
		validMessagePayload())
	if err != nil {
		t.Fatal(err)
	}
	exec, err := (&Client{}).NewTaskExecution(origin, Party{Session: "worker-a15"}, "worker-a15")
	if err != nil {
		t.Fatal(err)
	}

	status, err := exec.StatusEnvelope(StateSubmitted, false)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := exec.ArtifactEnvelope(Artifact{
		ArtifactID: "a1", Name: ArtifactResult, Parts: []Part{{Kind: "text", Text: "x"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, env := range []*Envelope{status, artifact} {
		if env.TaskID != "task-a15" {
			t.Errorf("%s taskId = %q, want task-a15", env.Kind, env.TaskID)
		}
		if env.CorrelationID != "corr-a15" {
			t.Errorf("%s correlationId = %q, want corr-a15", env.Kind, env.CorrelationID)
		}
		if env.ContextID != "ctx-a15" {
			t.Errorf("%s contextId = %q, want ctx-a15", env.Kind, env.ContextID)
		}
	}
}

// Assertion 18: every completed task carries at least one result artifact,
// and the reserved artifact names are used only for their defined content.
func TestAssertion18_ReservedArtifacts(t *testing.T) {
	fold := func(t *testing.T, artifacts []Artifact, terminal TaskState) error {
		t.Helper()
		origin, err := NewMessageEnvelope(Party{Session: "chatops"}, "task-a18", "ctx-a18", "corr-a18",
			validMessagePayload())
		if err != nil {
			t.Fatal(err)
		}
		exec, err := (&Client{}).NewTaskExecution(origin, Party{Session: "worker-a18"}, "worker-a18")
		if err != nil {
			t.Fatal(err)
		}
		var events []*Envelope
		mk := func(state TaskState, final bool) *Envelope {
			e, err := exec.StatusEnvelope(state, final)
			if err != nil {
				t.Fatal(err)
			}
			return e
		}
		events = append(events, mk(StateSubmitted, false), mk(StateWorking, false))
		for _, a := range artifacts {
			e, err := exec.ArtifactEnvelope(a)
			if err != nil {
				return err
			}
			events = append(events, e)
		}
		events = append(events, mk(terminal, true))
		task, err := FoldTask("task-a18", events)
		if err != nil {
			return err
		}
		return task.ValidateArtifacts()
	}

	textPart := []Part{{Kind: "text", Text: "some text"}}
	dataPart := []Part{{Kind: "data", Data: json.RawMessage(`{"tool": "kubectl", "args": ["get", "pods"]}`)}}

	cases := []struct {
		name      string
		artifacts []Artifact
		terminal  TaskState
		wantErr   bool
	}{
		{"completed_with_result", []Artifact{{ArtifactID: "r", Name: ArtifactResult, Parts: textPart}}, StateCompleted, false},
		{"completed_without_result", []Artifact{{ArtifactID: "p", Name: ArtifactProgress, Parts: textPart}}, StateCompleted, true},
		{"failed_without_result_is_fine", nil, StateFailed, false},
		{"reserved_names_used_correctly", []Artifact{
			{ArtifactID: "r", Name: ArtifactResult, Parts: textPart},
			{ArtifactID: "t", Name: ArtifactThinking, Parts: textPart},
			{ArtifactID: "a", Name: ArtifactActivity, Parts: dataPart},
			{ArtifactID: "p", Name: ArtifactProgress, Parts: textPart},
		}, StateCompleted, false},
		{"thinking_with_data_part", []Artifact{
			{ArtifactID: "r", Name: ArtifactResult, Parts: textPart},
			{ArtifactID: "t", Name: ArtifactThinking, Parts: dataPart},
		}, StateCompleted, true},
		{"activity_with_text_part", []Artifact{
			{ArtifactID: "r", Name: ArtifactResult, Parts: textPart},
			{ArtifactID: "a", Name: ArtifactActivity, Parts: textPart},
		}, StateCompleted, true},
		{"progress_with_data_part", []Artifact{
			{ArtifactID: "r", Name: ArtifactResult, Parts: textPart},
			{ArtifactID: "p", Name: ArtifactProgress, Parts: dataPart},
		}, StateCompleted, true},
		{"non_reserved_name_unconstrained", []Artifact{
			{ArtifactID: "r", Name: ArtifactResult, Parts: textPart},
			{ArtifactID: "x", Name: "custom-diagnostics", Parts: dataPart},
		}, StateCompleted, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := fold(t, tc.artifacts, tc.terminal)
			if tc.wantErr {
				var perr *ProtocolError
				if !errors.As(err, &perr) {
					t.Fatalf("want ProtocolError, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
