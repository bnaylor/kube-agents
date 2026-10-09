package workeradapter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// recordingStub is a harness that writes, per run, its argv and the opening
// stdin line into dir (argv.<n>, prompt.<n>), reports session id
// sess-<n> at init unless the file noinit.<n> exists, and answers
// "answer <n>". extra runs between the init and the read, for a stub that
// has to check something or wait.
func recordingStub(t *testing.T, dir, extra string) []string {
	t.Helper()
	return stub(t, fmt.Sprintf(`
dir=%q
n=$(( $(cat "$dir/n" 2>/dev/null || echo 0) + 1 ))
echo "$n" > "$dir/n"
printf '%%s\n' "$*" > "$dir/argv.$n"
if [ ! -e "$dir/noinit.$n" ]; then
  echo '{"type":"system","subtype":"init","session_id":"sess-'"$n"'"}'
fi
%s
read first || exit 1
printf '%%s' "$first" > "$dir/prompt.$n"
echo '{"type":"result","subtype":"success","result":"answer '"$n"'"}'
`, dir, extra))
}

func readRun(t *testing.T, dir, what string, n int) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("%s.%d", what, n)))
	if err != nil {
		t.Fatalf("harness run %d left no %s: %v", n, what, err)
	}
	return string(b)
}

func runCount(dir string) int {
	b, err := os.ReadFile(filepath.Join(dir, "n"))
	if err != nil {
		return 0
	}
	var n int
	fmt.Sscanf(strings.TrimSpace(string(b)), "%d", &n)
	return n
}

func reuseConfig(url, taskID, session string, harness []string) Config {
	cfg := adapterConfig(url, taskID, session, harness)
	cfg.SessionReuse = true
	return cfg
}

// publishIn puts a gateway envelope on a task's in subject: a steer when
// text is set, a cancel otherwise.
func publishIn(t *testing.T, c *lib.Client, session, taskID, text string) {
	t.Helper()
	var env *lib.Envelope
	var err error
	if text == "" {
		env, err = lib.NewCancelEnvelope(gatewayParty, taskID, "ctx-"+taskID, "corr-"+taskID,
			lib.WithTo(lib.Party{Session: session}))
	} else {
		payload, merr := json.Marshal(lib.Message{Role: "user", Parts: []lib.Part{{Kind: "text", Text: text}},
			MessageID: "msg-steer-" + taskID, TaskID: taskID, ContextID: "ctx-" + taskID})
		if merr != nil {
			t.Fatal(merr)
		}
		env, err = lib.NewMessageEnvelope(gatewayParty, taskID, "ctx-"+taskID, "corr-"+taskID, payload,
			lib.WithTo(lib.Party{Session: session}))
	}
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Publish(ctx, lib.TaskInSubject(session, taskID), env); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func finals(t *testing.T, url, session, taskID string) int {
	t.Helper()
	n := 0
	for _, env := range replayEvents(t, url, session, taskID) {
		if env.Kind == lib.KindStatusUpdate && statusOf(t, env).Final {
			n++
		}
	}
	return n
}

// The pod stays: after the first task's terminal, the next task submitted on
// the session's subjects runs in the same process, resuming the harness
// session the first run reported, without the primer, which only the first
// run reads. A SIGTERM while idle then ends the process with nothing
// published and a zero Result (exit 0).
func TestAReusedSessionPodRunsTheNextTurnInTheSameHarnessSession(t *testing.T) {
	url := startServer(t)
	c := testClient(t, url)
	const session = "chat-otter-r1u2"
	dir := t.TempDir()
	submit(t, c, session, "task-r-1", "first question")

	cfg := reuseConfig(url, "task-r-1", session, recordingStub(t, dir, ""))
	cfg.Primer = "EARLIER-TURNS"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAdapter(ctx, cfg)

	waitState(t, c, session, "task-r-1", lib.StateCompleted)
	submit(t, c, session, "task-r-2", "second question")
	waitState(t, c, session, "task-r-2", lib.StateCompleted)
	submit(t, c, session, "task-r-3", "third question")
	waitState(t, c, session, "task-r-3", lib.StateCompleted)

	if got := artifactText(foldTask(t, c, session, "task-r-2"), lib.ArtifactResult); got != "answer 2" {
		t.Errorf("second task's result = %q, want the second harness run's", got)
	}
	if a := readRun(t, dir, "argv", 1); strings.Contains(a, harnessResumeFlag) {
		t.Errorf("the first run resumed something: %q", a)
	}
	if p := readRun(t, dir, "prompt", 1); !strings.Contains(p, "EARLIER-TURNS") || !strings.Contains(p, "first question") {
		t.Errorf("the first run's prompt lacks the primer or the question: %q", p)
	}
	for n, want := range map[int]string{2: "sess-1", 3: "sess-2"} {
		if a := readRun(t, dir, "argv", n); !strings.HasSuffix(strings.TrimSpace(a), harnessResumeFlag+" "+want) {
			t.Errorf("run %d argv = %q, want it to resume %s", n, a, want)
		}
		if p := readRun(t, dir, "prompt", n); strings.Contains(p, "EARLIER-TURNS") {
			t.Errorf("run %d read the primer again: %q", n, p)
		}
	}

	select {
	case out := <-done:
		t.Fatalf("the adapter exited while its conversation was live: %+v", out)
	case <-time.After(500 * time.Millisecond):
	}
	cancel()
	out := waitOutcome(t, done, 30*time.Second)
	if out.err != nil || out.res != (Result{}) {
		t.Fatalf("SIGTERM while idle: %+v err=%v, want a zero Result and no error", out.res, out.err)
	}
	for _, task := range []string{"task-r-1", "task-r-2", "task-r-3"} {
		if n := finals(t, url, session, task); n != 1 {
			t.Errorf("%s has %d finals, want exactly 1", task, n)
		}
	}
}

// Everything else on the session's subjects is passed over: a steer or a
// cancel for a task that has ended is not a new task, and a cancel for the
// ended task does not reach the running one. A cancel for the running task
// still stops it, and the pod then takes the next task.
func TestAReusedPodRoutesSteerAndStopToTheirOwnTask(t *testing.T) {
	url := startServer(t)
	c := testClient(t, url)
	const session = "chat-heron-s7t8"
	dir := t.TempDir()
	submit(t, c, session, "task-s-1", "first")

	// Run 2 waits for its cancel; the others answer at once.
	harness := recordingStub(t, dir, `
if [ "$n" = 2 ]; then echo "$$" > "$dir/pid.2"; exec sleep 60; fi`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs lockedLog
	cfg := reuseConfig(url, "task-s-1", session, harness)
	cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	done := runAdapter(ctx, cfg)
	waitState(t, c, session, "task-s-1", lib.StateCompleted)

	publishIn(t, c, session, "task-s-1", "a late steer for the first task")
	publishIn(t, c, session, "task-s-1", "")
	submit(t, c, session, "task-s-2", "second")
	waitState(t, c, session, "task-s-2", lib.StateWorking)

	// A stop aimed at the ended task leaves this one running.
	publishIn(t, c, session, "task-s-1", "")
	time.Sleep(time.Second)
	if st := foldTask(t, c, session, "task-s-2").State; st != lib.StateWorking {
		t.Fatalf("task-s-2 is %s after a cancel for task-s-1, want still working", st)
	}
	publishIn(t, c, session, "task-s-2", "")
	waitState(t, c, session, "task-s-2", lib.StateCanceled)

	submit(t, c, session, "task-s-3", "third")
	waitState(t, c, session, "task-s-3", lib.StateCompleted)
	if n := runCount(dir); n != 3 {
		t.Errorf("harness ran %d times, want 3: the late steer and cancels must not start a task", n)
	}
	if n := finals(t, url, session, "task-s-1"); n != 1 {
		t.Errorf("task-s-1 has %d finals, want 1", n)
	}
	cancel()
	waitOutcome(t, done, 30*time.Second)
	// Passed over by the watcher itself, not caught one step later by the
	// respawn check: a steer taken as a task would run it as the request
	// if the ended task's terminal had not reached the stream.
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "next task taken") && strings.Contains(line, "task=task-s-1 ") {
			t.Fatalf("the watcher took a late steer or cancel for an ended task as a new task: %s", line)
		}
	}
}

// lockedLog is a log sink the adapter's goroutines and the test can share.
type lockedLog struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// SIGTERM in the middle of a later task is that task's eviction, exactly as
// in a one-task pod.
func TestASIGTERMMidTaskInAReusedPodIsAnEviction(t *testing.T) {
	url := startServer(t)
	c := testClient(t, url)
	const session = "chat-lynx-e1v2"
	dir := t.TempDir()
	submit(t, c, session, "task-e-1", "first")
	harness := recordingStub(t, dir, `if [ "$n" = 2 ]; then exec sleep 60; fi`)
	ctx, cancel := context.WithCancel(context.Background())
	done := runAdapter(ctx, reuseConfig(url, "task-e-1", session, harness))
	waitState(t, c, session, "task-e-1", lib.StateCompleted)
	submit(t, c, session, "task-e-2", "second")
	waitState(t, c, session, "task-e-2", lib.StateWorking)
	cancel()
	out := waitOutcome(t, done, 30*time.Second)
	if !out.res.Evicted || out.res.State != lib.StateFailed {
		t.Fatalf("run: %+v err=%v, want the eviction contract", out.res, out.err)
	}
	task := foldTask(t, c, session, "task-e-2")
	if !task.Final || task.State != lib.StateFailed {
		t.Fatalf("task-e-2: final=%v state=%s", task.Final, task.State)
	}
}

// A harness that reports no session leaves nothing to resume, and a next
// turn with neither the session nor the primer would know nothing of the
// conversation. The process exits after that task's terminal, so the next
// turn starts cold, with the primer.
func TestAReusedPodExitsWhenTheHarnessReportsNoSession(t *testing.T) {
	for _, tc := range []struct {
		name   string
		noInit int
		tasks  int
	}{
		{"on the first turn", 1, 1},
		{"on a resumed turn", 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := startServer(t)
			c := testClient(t, url)
			session := fmt.Sprintf("chat-vole-n%d", tc.noInit)
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("noinit.%d", tc.noInit)), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			submit(t, c, session, "task-n-1", "first")
			done := runAdapter(context.Background(), reuseConfig(url, "task-n-1", session, recordingStub(t, dir, "")))
			for i := 2; i <= tc.tasks; i++ {
				waitState(t, c, session, fmt.Sprintf("task-n-%d", i-1), lib.StateCompleted)
				submit(t, c, session, fmt.Sprintf("task-n-%d", i), "next")
			}
			out := waitOutcome(t, done, 30*time.Second)
			if out.err != nil || out.res.State != lib.StateCompleted {
				t.Fatalf("run: %+v err=%v, want the last task's completed terminal", out.res, out.err)
			}
		})
	}
}

// The delegate tool follows the task, not the pod: a wake (the gateway's
// task on the session's behalf, marked by authority.via) runs with the tool
// off, and the human turns either side of it in the same pod keep it. Off
// means what A2A_DELEGATE_TOOL=off means for a whole pod: the no-delegate
// argv, and no listener on the socket.
func TestTheDelegateToolIsDecidedPerTaskInAReusedPod(t *testing.T) {
	url := startServer(t)
	c := testClient(t, url)
	const session = "chat-stoat-d3w4"
	dir := t.TempDir()
	sock := delegateSock(t)
	check := fmt.Sprintf(`if [ -S %q ]; then echo listening > "$dir/sock.$n"; else echo absent > "$dir/sock.$n"; fi`, sock)
	with := recordingStub(t, dir, "echo WITH > \"$dir/tool.$n\"\n"+check)
	without := recordingStub(t, dir, "echo WITHOUT > \"$dir/tool.$n\"\n"+check)

	submit(t, c, session, "task-d-1", "human turn")
	cfg := reuseConfig(url, "task-d-1", session, with)
	cfg.HarnessCommandNoDelegate = without
	cfg.DelegateSocket = sock
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAdapter(ctx, cfg)
	waitState(t, c, session, "task-d-1", lib.StateCompleted)

	wake := authorityWithVia(t, c, "task-d-2", session)
	submitWithAuthority(t, c, session, "task-d-2", "the delegation came back", wake)
	waitState(t, c, session, "task-d-2", lib.StateCompleted)
	submit(t, c, session, "task-d-3", "human again")
	waitState(t, c, session, "task-d-3", lib.StateCompleted)

	for n, want := range map[int][2]string{1: {"WITH", "listening"}, 2: {"WITHOUT", "absent"}, 3: {"WITH", "listening"}} {
		if got := strings.TrimSpace(readRun(t, dir, "tool", n)); got != want[0] {
			t.Errorf("run %d used the %s argv, want %s", n, got, want[0])
		}
		if got := strings.TrimSpace(readRun(t, dir, "sock", n)); got != want[1] {
			t.Errorf("run %d saw the delegate socket %s, want %s", n, got, want[1])
		}
	}
	cancel()
	waitOutcome(t, done, 30*time.Second)
}

// authorityWithVia is the authority block wakeSession publishes: the task's
// capability, and via naming the delegated task and the session it ran on.
func authorityWithVia(t *testing.T, c *lib.Client, taskID, session string) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"grants": map[string]any{"capability": mintFor(t, c, taskID, session)},
		"via":    map[string]string{"taskId": "task-child-1", "session": session},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestDelegateOffForTaskReadsTheWakeMark(t *testing.T) {
	for _, tc := range []struct {
		name      string
		authority string
		off       bool
	}{
		{"no authority block", "", false},
		{"a human turn", `{"requester":{"principal":"p"},"grants":null}`, false},
		{"via null", `{"via":null,"grants":null}`, false},
		{"a wake", `{"via":{"taskId":"t","session":"s"},"grants":null}`, true},
		{"an unreadable block", `{"via":`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := &lib.Envelope{Authority: json.RawMessage(tc.authority)}
			if got := delegateOffForTask(env); got != tc.off {
				t.Errorf("delegateOffForTask(%s) = %v, want %v", tc.authority, got, tc.off)
			}
		})
	}
}

// The watcher's consumer is a named ephemeral like the others, so a
// nats-server restart or a long disconnect drops it while the pod is idle.
// The next task must still arrive: the watcher rebuilds it from where it
// left off.
func TestAReusedPodSurvivesItsWatcherConsumerBeingDropped(t *testing.T) {
	url := startServer(t)
	c := testClient(t, url)
	const session = "chat-marten-w5x6"
	dir := t.TempDir()
	submit(t, c, session, "task-w-1", "first")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAdapter(ctx, reuseConfig(url, "task-w-1", session, recordingStub(t, dir, "")))
	waitState(t, c, session, "task-w-1", lib.StateCompleted)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	name := lib.SessionConsumerName(session, lib.SessionConsumerOrigin)
	dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dcancel()
	deadline := time.Now().Add(waitDeadline)
	for {
		if err := js.DeleteConsumer(dctx, lib.TasksStream, name); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the watcher's consumer %s never appeared", name)
		}
		time.Sleep(50 * time.Millisecond)
	}
	submit(t, c, session, "task-w-2", "second")
	waitState(t, c, session, "task-w-2", lib.StateCompleted)
	cancel()
	waitOutcome(t, done, 30*time.Second)
}

// A profile pod's addressee is shared by every pod of its profile, so a
// watcher there would take its siblings' tasks.
func TestSessionReuseIsRefusedWithoutASessionOfItsOwn(t *testing.T) {
	cfg := Config{SessionReuse: true, ProfileExecutor: true, Profile: "p", PodName: "p-1"}
	if err := cfg.validate(); err == nil {
		t.Fatal("SessionReuse on a profile pod validated")
	}
	cfg = Config{SessionReuse: true, Profile: "p"}
	if err := cfg.validate(); err == nil {
		t.Fatal("SessionReuse with no session validated")
	}
}
