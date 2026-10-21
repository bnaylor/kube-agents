package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

const (
	fakeAddressee = "platform"
	statusQuery   = "status?"
	answerText    = "Three pods are crash-looping in `checkout`."
)

var fakeGateway = lib.Party{Session: "gateway", AgentType: "gateway"}
var fakeExecutor = lib.Party{Session: fakeAddressee, Profile: fakeAddressee}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func authorityFor(t *testing.T, conversation string, via bool) json.RawMessage {
	t.Helper()
	a := map[string]any{
		"requester": map[string]any{"principal": "nats:console", "backend": "console"},
		"audience":  map[string]any{"conversation": conversation, "kind": "dm"},
		"grants":    []any{},
	}
	if via {
		a["via"] = map[string]any{"taskId": "task-parent", "session": fakeAddressee}
	}
	return mustJSON(t, a)
}

// publishSubmission puts a submission on TASKS as the gateway would.
func publishSubmission(t *testing.T, js jetstream.JetStream, taskID, conversation string, via bool) {
	t.Helper()
	msg := mustJSON(t, lib.Message{Role: "user", Parts: []lib.Part{{Kind: "text", Text: "x"}}, MessageID: "m-" + taskID})
	env, err := lib.NewMessageEnvelope(fakeGateway, taskID, "ctx-"+taskID, "corr-"+taskID, msg, lib.WithAuthority(authorityFor(t, conversation, via)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(context.Background(), lib.TaskInSubject(fakeAddressee, taskID), mustJSON(t, env)); err != nil {
		t.Fatal(err)
	}
}

// publishRun puts a task's events on TASKS: submitted, working, the answer,
// completed.
func publishRun(t *testing.T, js jetstream.JetStream, taskID string, finish bool) {
	t.Helper()
	status := func(state lib.TaskState, final bool) {
		p := mustJSON(t, lib.StatusUpdate{TaskID: taskID, ContextID: "ctx-" + taskID, Status: lib.TaskStatus{State: state}, Final: final})
		env, err := lib.NewStatusUpdateEnvelope(fakeExecutor, taskID, "ctx-"+taskID, "corr-"+taskID, p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.Publish(context.Background(), lib.TaskEventsSubject(fakeAddressee, taskID), mustJSON(t, env)); err != nil {
			t.Fatal(err)
		}
	}
	status(lib.StateSubmitted, false)
	status(lib.StateWorking, false)
	if !finish {
		return
	}
	p := mustJSON(t, lib.ArtifactUpdate{TaskID: taskID, ContextID: "ctx-" + taskID,
		Artifact: lib.Artifact{Name: lib.ArtifactResult, Parts: []lib.Part{{Kind: "text", Text: answerText}}}, LastChunk: true})
	env, err := lib.NewArtifactUpdateEnvelope(fakeExecutor, taskID, "ctx-"+taskID, "corr-"+taskID, p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(context.Background(), lib.TaskEventsSubject(fakeAddressee, taskID), mustJSON(t, env)); err != nil {
		t.Fatal(err)
	}
	status(lib.StateCompleted, true)
}

// runFakeGateway answers console frames the way the gateway's console door
// does: a status question or a stop gets a notice and no task; anything else
// gets a submission for the conversation, preceded by two that are not the
// turn's own (another conversation's, and a child delegated in this one).
func runFakeGateway(t *testing.T, url string, js jetstream.JetStream, finish bool) *atomic.Int32 {
	t.Helper()
	nc, err := nats.Connect(url, nats.UserInfo("admin", "admin"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	var n atomic.Int32
	_, err = nc.Subscribe("chat.console.*.in", func(m *nats.Msg) {
		token := strings.Split(m.Subject, ".")[2]
		var f ConsoleInFrame
		if json.Unmarshal(m.Data, &f) != nil {
			return
		}
		notice := func(text string) {
			_ = nc.Publish(fmt.Sprintf(consoleOutSubject, token), mustJSON(t, ConsoleOutFrame{MessageID: "n1", Text: text}))
		}
		switch f.Text {
		case statusQuery:
			notice("Nothing is running in this conversation.")
			return
		case stopText:
			notice("Stopped the running task.")
			return
		}
		i := n.Add(1)
		notice("Working on it…")
		publishSubmission(t, js, fmt.Sprintf("task-other%d", i), "console:someone-else", false)
		publishSubmission(t, js, fmt.Sprintf("task-child%d", i), "console:"+token, true)
		root := fmt.Sprintf("task-root%d", i)
		publishSubmission(t, js, root, "console:"+token, false)
		publishRun(t, js, root, finish)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return &n
}

func callTool(t *testing.T, s *Server, name string, args any) CallToolResult {
	t.Helper()
	return s.CallTool(context.Background(), name, mustJSON(t, args))
}

func turnServer(t *testing.T, finish bool) (*Server, jetstream.JetStream, *atomic.Int32) {
	t.Helper()
	url, js := testBus(t)
	n := runFakeGateway(t, url, js, finish)
	b := dial(t, url)
	convs := NewConversations()
	return mustServer(t, SendMessageTool(b, convs), TaskControlTool(b, convs)), js, n
}

// A turn is sent as the console user, matched to its own root task (not
// another conversation's, not a delegated child), and answered from TASKS.
func TestSendMessageAnswersFromTheTurnsOwnTask(t *testing.T) {
	s, _, _ := turnServer(t, true)
	res := callTool(t, s, ToolSendMessage, map[string]any{"text": "what is broken?", "wait_seconds": 10})
	text := res.Content[0].Text
	if res.IsError {
		t.Fatalf("send_message failed: %s", text)
	}
	for _, want := range []string{"task-root1", "completed", answerText, "Working on it"} {
		if !strings.Contains(text, want) {
			t.Errorf("result does not contain %q:\n%s", want, text)
		}
	}
	for _, not := range []string{"task-other1", "task-child1"} {
		if strings.Contains(text, not) {
			t.Errorf("result names %s, which is not the turn's own task:\n%s", not, text)
		}
	}
}

// The same conversation name continues the same console conversation; a
// different name opens another.
func TestConversationsArePerName(t *testing.T) {
	s, _, _ := turnServer(t, true)
	token := func(conv string) string {
		text := callTool(t, s, ToolSendMessage, map[string]any{"text": "hi", "conversation": conv, "wait_seconds": 10}).Content[0].Text
		i := strings.Index(text, "token `")
		if i < 0 {
			t.Fatalf("no token in %q", text)
		}
		rest := text[i+len("token `"):]
		return rest[:strings.Index(rest, "`")]
	}
	a1, a2, b := token("a"), token("a"), token("b")
	if a1 != a2 {
		t.Errorf("conversation a got two tokens: %s, %s", a1, a2)
	}
	if a1 == b {
		t.Error("conversations a and b share a token")
	}
}

// A turn the gateway answers with a notice alone returns the notice, not a
// timeout.
func TestANoticeOnlyTurnReturnsTheNotice(t *testing.T) {
	s, _, n := turnServer(t, true)
	start := time.Now()
	res := callTool(t, s, ToolSendMessage, map[string]any{"text": statusQuery})
	if res.IsError || !strings.Contains(res.Content[0].Text, "Nothing is running") {
		t.Fatalf("result = %+v", res)
	}
	if elapsed := time.Since(start); elapsed > noticeGrace+2*time.Second {
		t.Errorf("a notice-only turn took %s", elapsed)
	}
	if n.Load() != 0 {
		t.Error("the fake gateway opened a task for a status question")
	}
}

// A turn still running when the wait ends returns its task id, and
// task_control get reads it.
func TestARunningTurnReturnsItsTaskForTaskControl(t *testing.T) {
	s, js, _ := turnServer(t, false)
	res := callTool(t, s, ToolSendMessage, map[string]any{"text": "long job", "wait_seconds": 3})
	if res.IsError || !strings.Contains(res.Content[0].Text, "still running") {
		t.Fatalf("result = %+v", res)
	}
	got := callTool(t, s, ToolTaskControl, map[string]any{"action": "get", "task_id": "task-root1"})
	if got.IsError || !strings.Contains(got.Content[0].Text, "working") {
		t.Errorf("get = %+v", got)
	}
	// It finishes; get with a wait sees the answer.
	publishRunTail(t, js, "task-root1")
	got = callTool(t, s, ToolTaskControl, map[string]any{"action": "get", "task_id": "task-root1", "wait_seconds": 10})
	if got.IsError || !strings.Contains(got.Content[0].Text, answerText) {
		t.Errorf("get after finish = %+v", got)
	}
}

func publishRunTail(t *testing.T, js jetstream.JetStream, taskID string) {
	t.Helper()
	p := mustJSON(t, lib.ArtifactUpdate{TaskID: taskID, ContextID: "ctx-" + taskID,
		Artifact: lib.Artifact{Name: lib.ArtifactResult, Parts: []lib.Part{{Kind: "text", Text: answerText}}}, LastChunk: true})
	env, _ := lib.NewArtifactUpdateEnvelope(fakeExecutor, taskID, "ctx-"+taskID, "corr-"+taskID, p)
	_, _ = js.Publish(context.Background(), lib.TaskEventsSubject(fakeAddressee, taskID), mustJSON(t, env))
	st := mustJSON(t, lib.StatusUpdate{TaskID: taskID, ContextID: "ctx-" + taskID, Status: lib.TaskStatus{State: lib.StateCompleted}, Final: true})
	env, _ = lib.NewStatusUpdateEnvelope(fakeExecutor, taskID, "ctx-"+taskID, "corr-"+taskID, st)
	_, _ = js.Publish(context.Background(), lib.TaskEventsSubject(fakeAddressee, taskID), mustJSON(t, env))
}

func TestTaskControlStopSendsTheStopWord(t *testing.T) {
	s, _, n := turnServer(t, true)
	res := callTool(t, s, ToolTaskControl, map[string]any{"action": "stop", "conversation": "ops"})
	if res.IsError || !strings.Contains(res.Content[0].Text, "Stopped the running task") {
		t.Fatalf("stop = %+v", res)
	}
	if n.Load() != 0 {
		t.Error("stop opened a task")
	}
}

func TestTurnToolsRefuseBadInput(t *testing.T) {
	s, _, _ := turnServer(t, true)
	for name, call := range map[string]func() CallToolResult{
		"an empty message": func() CallToolResult { return callTool(t, s, ToolSendMessage, map[string]any{"text": "  "}) },
		"an oversized message": func() CallToolResult {
			return callTool(t, s, ToolSendMessage, map[string]any{"text": strings.Repeat("x", consoleTextCap+1)})
		},
		"a bad token": func() CallToolResult {
			return callTool(t, s, ToolSendMessage, map[string]any{"text": "hi", "conversation_token": "a.b"})
		},
		"get without a task": func() CallToolResult { return callTool(t, s, ToolTaskControl, map[string]any{"action": "get"}) },
		"get with a bad task id": func() CallToolResult {
			return callTool(t, s, ToolTaskControl, map[string]any{"action": "get", "task_id": "a.>"})
		},
		"an unknown action": func() CallToolResult { return callTool(t, s, ToolTaskControl, map[string]any{"action": "steer"}) },
	} {
		if res := call(); !res.IsError {
			t.Errorf("%s was accepted: %+v", name, res)
		}
	}
}

// A task nobody submitted reads as not found, not as an empty task.
func TestReadTaskOfAnUnknownTask(t *testing.T) {
	url, _ := testBus(t)
	if _, err := dial(t, url).ReadTask(context.Background(), "task-nope"); err != ErrTaskNotFound {
		t.Errorf("err = %v, want ErrTaskNotFound", err)
	}
}

// Each task read costs one consumer, reaped when idle, never the two an
// ordered consumer leaves behind under the console grant.
func TestTaskReadsCostOneConsumerEach(t *testing.T) {
	url, js := testBus(t)
	publishSubmission(t, js, "task-r", "console:x", false)
	publishRun(t, js, "task-r", true)
	b := dial(t, url)
	const reads = 10
	start := time.Now()
	for i := 0; i < reads; i++ {
		if task, err := b.ReadTask(context.Background(), "task-r"); err != nil || !task.Final {
			t.Fatalf("read %d: %v %+v", i, err, task)
		}
	}
	s, err := js.Stream(context.Background(), lib.TasksStream)
	if err != nil {
		t.Fatal(err)
	}
	// All within the consumers' idle window, so each is still there to count.
	if elapsed := time.Since(start); elapsed >= taskConsumerIdle {
		t.Fatalf("%d reads took %s; a read must not wait out its fetch", reads, elapsed)
	}
	if got := s.CachedInfo().State.Consumers; got != reads {
		t.Errorf("%d reads left %d consumers on TASKS, want %d", reads, got, reads)
	}
}
