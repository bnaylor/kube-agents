package hermesbridge

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gke-labs/kube-agents/a2a/capability"
	"github.com/gke-labs/kube-agents/a2a/lib"
)

const (
	// heldStdoutTaskDeadline ends the deadline test's stub, which would
	// otherwise sleep 60s. The clean-exit test's stub exits at once.
	heldStdoutTaskDeadline = time.Second
	// heldStdoutKillGrace is the SIGTERM-to-SIGKILL grace and so, through
	// cmd.WaitDelay, the bound on the reap.
	heldStdoutKillGrace = 500 * time.Millisecond
	// heldStdoutTerminalWithin is how long the task may take to finalize
	// before the reap counts as unbounded. The escaped sleep lasts 120s; a
	// bounded run ends about heldStdoutTaskDeadline plus one
	// heldStdoutKillGrace after it starts.
	heldStdoutTerminalWithin = 15 * time.Second
)

// TestLifecycle_DeadlineReapIsBoundedWithAHeldStdout: a hermes run that
// leaves a child in another process group holding stdout, then outlives the
// task deadline. The deadline's group kill ends the run but not the escaped
// sleep, and Wait copies stdout to EOF, so unbounded it waits for the sleep,
// with no terminal event in the meantime. Bounded by KillGrace, the task
// fails with deadline-exceeded about one grace after the kill, and the
// reason says the reap ran the full bound.
func TestLifecycle_DeadlineReapIsBoundedWithAHeldStdout(t *testing.T) {
	task := runHeldStdoutTask(t, "task-held-stdout-deadline", "exec sleep 60")
	if task.State != lib.StateFailed {
		t.Fatalf("state = %s, want failed", task.State)
	}
	want := fmt.Sprintf("reason: deadline-exceeded - killed after %s; the reap after the kill ran the full %s, so a process hermes started may still hold its output",
		heldStdoutTaskDeadline, heldStdoutKillGrace)
	if got := terminalReason(t, task); got != want {
		t.Fatalf("reason:\n got %q\nwant %q", got, want)
	}
}

// TestLifecycle_CleanExitWithAHeldStdoutCompletes: the same escaped child,
// but hermes prints its answer and exits 0. Unbounded, Wait waits for the
// sleep. Bounded, Wait returns exec.ErrWaitDelay one grace after the exit.
// Everything hermes wrote was copied during that grace, so the task
// completes with the answer rather than failing over a cut that only lost
// the escaped child's output.
func TestLifecycle_CleanExitWithAHeldStdoutCompletes(t *testing.T) {
	task := runHeldStdoutTask(t, "task-held-stdout-clean", "echo the whole answer\nexit 0")
	if task.State != lib.StateCompleted {
		t.Fatalf("state = %s msg = %v, want completed", task.State, task.FinalMessage)
	}
	if got := task.Artifact(lib.ArtifactResult).Parts[0].Text; got != "the whole answer\n" {
		t.Fatalf("result = %q, want the stub's whole answer", got)
	}
}

// runHeldStdoutTask runs one task against a stub that backgrounds a sleep
// holding stdout in its own process group, out of the group kill's reach,
// and then runs rest. It fails the test if the task does not finalize within
// heldStdoutTerminalWithin (an unbounded reap) or if the sleep did not
// escape the group (the premise), and returns the task.
func runHeldStdoutTask(t *testing.T, taskID, rest string) *lib.Task {
	t.Helper()
	_, url := startServer(t)
	pidFile := filepath.Join(t.TempDir(), "escaped.pid")
	path := filepath.Join(t.TempDir(), "hermes-stub")
	// set -m gives the background sleep its own process group; it keeps
	// stdout and drops stderr. exec in rest keeps a foreground command in
	// the stub's own group, so the group kill reaches it.
	body := fmt.Sprintf("#!/bin/bash\nset -m\nsleep 120 </dev/null 2>/dev/null &\necho $! > %q\n%s\n", pidFile, rest)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	startBridgeConfig(t, Config{
		NATSURL:      url,
		Command:      []string{"/bin/bash", path},
		TaskDeadline: heldStdoutTaskDeadline,
		KillGrace:    heldStdoutKillGrace,
		Scope:        capability.NamespaceScope(""),
	}, nil)
	// Registered after the bridge so it runs first: an unbounded reap holds
	// the bridge's shutdown until this sleep is gone.
	t.Cleanup(func() {
		raw, err := os.ReadFile(pidFile)
		if err != nil {
			return
		}
		if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	c := gatewayClient(t, url)

	started := time.Now()
	submit(t, c, taskID, "leave a child holding stdout")
	var task *lib.Task
	waitFor(t, heldStdoutTerminalWithin, "terminal event on "+taskID+" (an unbounded reap never sends one)", func() bool {
		got, err := c.TasksGet(testCtx(t), "platform", taskID)
		if err != nil {
			return false
		}
		task = got
		return task.Final
	})
	t.Logf("task finalized after %s", time.Since(started).Round(time.Millisecond))

	// The premise: the sleep led its own group, so only the bound ended the
	// reap. Without it the test would pass with the bound removed.
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("escaped child pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("escaped child pid %q: %v", raw, err)
	}
	if pgid, err := syscall.Getpgid(pid); err != nil || pgid != pid {
		t.Fatalf("background sleep %d is not its own process group leader (pgid %d, err %v); the test proves nothing", pid, pgid, err)
	}
	return task
}
