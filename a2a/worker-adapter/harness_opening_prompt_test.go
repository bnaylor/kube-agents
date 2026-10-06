package workeradapter

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// openingPromptBytes is larger than any pipe buffer the adapter can meet, so
// the opening-prompt write cannot complete into the buffer and return before
// the stub exits. The write blocks until the stub's exit closes the read end,
// then fails with EPIPE, every run. A prompt that fits the buffer would let
// the write succeed and send the task down supervise's failure arm instead,
// which is not the path under test. Linux pipes default to 64 KiB and macOS
// pipes grow to 64 KiB at most; 256 KiB is four times either, and the
// submission envelope stays well under the embedded server's 1 MB payload cap.
const openingPromptBytes = 256 * 1024

// TestLifecycle_OpeningPromptWriteFailureKeepsEvidence: a harness that dies
// before reading its prompt fails the opening-prompt write, and the terminal
// reason still carries its exit status and stderr tail, as every other
// harness failure does (TestLifecycle_FailedWithEvidence). The token stays
// spawn-failed: the eval harness classifies on it.
func TestLifecycle_OpeningPromptWriteFailureKeepsEvidence(t *testing.T) {
	url := startServer(t)
	c := testClient(t, url)
	const session, taskID = "chat-okapi-c5d6", "task-openfail-1"
	submit(t, c, session, taskID, strings.Repeat("x", openingPromptBytes))

	// Never reads stdin: bash reads the script from its path, not stdin.
	harness := stub(t, `
echo "stub died before reading its prompt" >&2
exit 7
`)
	out := waitOutcome(t, runAdapter(context.Background(), adapterConfig(url, taskID, session, harness)), 30*time.Second)
	if out.res.State != lib.StateFailed {
		t.Fatalf("state %q err %v", out.res.State, out.err)
	}
	events := replayEvents(t, url, session, taskID)
	last := statusOf(t, events[len(events)-1])
	if !last.Final || last.Status.State != lib.StateFailed {
		t.Fatalf("last event %+v", last)
	}
	text := last.Status.Message.Parts[0].Text
	for _, want := range []string{
		"reason: spawn-failed - write opening prompt: ",
		" - exit status 7",
		"\nstderr tail:\nstub died before reading its prompt",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("terminal reason missing %q:\n%s", want, text)
		}
	}
	if !strings.HasPrefix(text, "reason: spawn-failed ") {
		t.Errorf("terminal reason does not lead with the spawn-failed token:\n%s", text)
	}
}

// TestStartHarness_OpeningPromptReapIsBounded: the reap after a failed
// opening-prompt write cannot be held open by a descendant the process-group
// kill does not reach. The stub backgrounds a sleep under job control (its
// own process group, out of the kill's reach) that inherits stderr, then
// exits without reading stdin. Unbounded, Wait would read stderr until the
// sleep exits; bounded, startHarness returns about one reapBound after the
// stub's exit, still carrying the exit status and the stderr tail.
func TestStartHarness_OpeningPromptReapIsBounded(t *testing.T) {
	const (
		reapBound = 500 * time.Millisecond
		// The sleep outlives the test's patience by a wide margin, so an
		// unbounded reap fails on returnWithin rather than finishing late.
		sleepSeconds = 120
		returnWithin = 30 * time.Second
	)
	pidFile := filepath.Join(t.TempDir(), "escaped.pid")
	harness := stub(t, fmt.Sprintf(`
set -m
sleep %d </dev/null >/dev/null &
echo $! > %q
echo "stub left a child holding stderr" >&2
exit 7
`, sleepSeconds, pidFile))
	t.Cleanup(func() {
		raw, err := os.ReadFile(pidFile)
		if err != nil {
			return
		}
		if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := startHarness(harness, os.Environ(), strings.Repeat("x", openingPromptBytes), reapBound, log)
		done <- err
	}()
	var err error
	select {
	case err = <-done:
	case <-time.After(returnWithin):
		t.Fatalf("startHarness still reaping after %s: the reap is unbounded", returnWithin)
	}
	if err == nil {
		t.Fatal("startHarness succeeded against a harness that never read its prompt")
	}
	// The premise: the sleep led its own process group, so the group kill
	// could not reach it and only the bound ended the reap. Without this, a
	// shell whose background jobs stayed in the stub's group would let the
	// test pass with the bound removed.
	raw, rerr := os.ReadFile(pidFile)
	if rerr != nil {
		t.Fatalf("escaped child pid: %v", rerr)
	}
	pid, perr := strconv.Atoi(strings.TrimSpace(string(raw)))
	if perr != nil {
		t.Fatalf("escaped child pid %q: %v", raw, perr)
	}
	if pgid, gerr := syscall.Getpgid(pid); gerr != nil || pgid != pid {
		t.Fatalf("background sleep %d is not its own process group leader (pgid %d, err %v); the test proves nothing", pid, pgid, gerr)
	}
	msg := err.Error()
	for _, want := range []string{
		"write opening prompt: ",
		" - exit status 7",
		"\nstderr tail:\nstub left a child holding stderr",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error missing %q:\n%s", want, msg)
		}
	}
	t.Logf("startHarness returned after %s: %s", time.Since(start).Round(time.Millisecond), msg)
}
