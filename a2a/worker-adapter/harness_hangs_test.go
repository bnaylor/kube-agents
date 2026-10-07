package workeradapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

const (
	// superviseReapTaskDeadline is past superviseReapReturnWithin on purpose:
	// the task deadline must not be what ends the test. It would not end the
	// hang anyway, since the group kill it sends does not reach the escaped
	// sleep, but a reason of deadline-exceeded would hide which bound worked.
	superviseReapTaskDeadline = 60 * time.Second
	// superviseReapReturnWithin is how long the run may take before the reap
	// counts as unbounded. The escaped sleep lasts 120s; a bounded reap ends
	// about KillGrace (500ms in adapterConfig) after the stub exits.
	superviseReapReturnWithin = 30 * time.Second

	// overflowDeadlockWithin is the deadline for startHarness against a
	// harness that overflows the scanner before it reads its prompt. With the
	// deadlock in place startHarness never returns, so this is how long the
	// test waits before calling it one and dumping the goroutines. A working
	// run returns in about one reapBoundForTest; the margin is for -race.
	overflowDeadlockWithin = 15 * time.Second
	// overflowLineBytes is the one stdout line the overflow stub writes
	// before it reads stdin: twice scannerMaxBytes, so the stub is still
	// blocked writing it when the scanner gives up at the ceiling.
	overflowLineBytes = 2 * scannerMaxBytes
)

// TestLifecycle_SuperviseReapIsBounded: a harness that reads its prompt,
// leaves a child in another process group holding stderr, and exits 7. The
// reap in supervise reads stderr to EOF, so unbounded it waits for the
// escaped sleep, past the task deadline and the group kill, which do not
// reach it. Bounded by KillGrace, the task fails promptly with the exit
// status, the note that the reap ran the full bound, and the stderr tail.
func TestLifecycle_SuperviseReapIsBounded(t *testing.T) {
	url := startServer(t)
	c := testClient(t, url)
	const session, taskID = "chat-quokka-e1f2", "task-reap-1"
	submit(t, c, session, taskID, "read this, then leave")

	harness, pidFile := escapingChildStub(t, "read first || exit 1", 7, "stub read its prompt and left a child holding stderr")
	cfg := adapterConfig(url, taskID, session, harness)
	cfg.TaskDeadline = superviseReapTaskDeadline
	started := time.Now()
	out := waitOutcome(t, runAdapter(context.Background(), cfg), superviseReapReturnWithin)
	t.Logf("adapter finished after %s", time.Since(started).Round(time.Millisecond))
	requireEscaped(t, pidFile)
	if out.res.State != lib.StateFailed {
		t.Fatalf("state %q err %v", out.res.State, out.err)
	}
	events := replayEvents(t, url, session, taskID)
	text := statusOf(t, events[len(events)-1]).Status.Message.Parts[0].Text
	for _, want := range []string{
		"reason: stream-ended-without-result - exit status 7 - the reap ran the full 500ms; the stderr tail may stop there",
		"\nstderr tail:\nstub read its prompt and left a child holding stderr",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("terminal reason missing %q:\n%s", want, text)
		}
	}
	// After a failed exit Wait cannot tell a held stderr from a slow reap,
	// and Go's own WaitDelay message is not evidence about the harness.
	for _, unwanted := range []string{"kept stderr open", "WaitDelay", "deadline-exceeded"} {
		if strings.Contains(text, unwanted) {
			t.Errorf("terminal reason carries %q:\n%s", unwanted, text)
		}
	}
}

// TestStartHarness_OverflowBeforePromptReadDoesNotDeadlock: a harness that
// writes a stdout line over scannerMaxBytes before it reads a prompt larger
// than the pipe buffer. The opening-prompt write blocks on the full stdin
// pipe, and the scanner, on reaching the ceiling, closes stdin to end the
// turn. When writeUser held the lock that closing path takes, each waited on
// the other and startHarness never returned. Now the close fails the write,
// startHarness reaps the harness, and the error names both the write and the
// ceiling that caused it.
func TestStartHarness_OverflowBeforePromptReadDoesNotDeadlock(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "harness.pid")
	harness := stub(t, fmt.Sprintf(`
echo $$ > %q
head -c %d /dev/zero | tr '\0' x
printf '\n'
while read -r _line; do :; done
`, pidFile, overflowLineBytes))
	// The stub leads its own process group, so a hung run is cleaned up by
	// killing the group; startHarness never handed back a proc to kill.
	t.Cleanup(func() {
		raw, err := os.ReadFile(pidFile)
		if err != nil {
			return
		}
		if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
	})

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		p, err := startHarness(harness, os.Environ(), strings.Repeat("x", openingPromptBytes()), reapBoundForTest, log)
		if p != nil {
			p.kill(0)
			_ = p.cmd.Wait()
			p.reaped()
		}
		done <- err
	}()
	var err error
	select {
	case err = <-done:
	case <-time.After(overflowDeadlockWithin):
		var dump bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&dump, 2)
		t.Fatalf("startHarness did not return within overflowDeadlockWithin (%s): the opening-prompt write and the scanner's overflow path are deadlocked\n%s", overflowDeadlockWithin, dump.String())
	}
	t.Logf("startHarness returned after %s: %v", time.Since(start).Round(time.Millisecond), err)
	if err == nil {
		t.Fatal("startHarness succeeded against a harness whose stdout overflowed before it read its prompt")
	}
	msg := err.Error()
	for _, want := range []string{
		"write opening prompt: ",
		fmt.Sprintf("over the %d-byte limit", scannerMaxBytes),
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error missing %q:\n%s", want, msg)
		}
	}
}
