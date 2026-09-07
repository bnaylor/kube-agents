package workeradapter

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// TestLifecycle_OversizeResultLineStallsToTheDeadline answers the question naming
// scannerMaxBytes raised but did not settle: a result line past the 8 MiB
// ceiling does not truncate, so what does the task become?
//
// The bad answer would be an apparently-successful empty result — the harness
// exits 0 having written its deliverable, the adapter never parses it, and the
// task completes with nothing in it. That is worse than losing the text,
// because a caller cannot tell it happened.
//
// The actual answer is worse than either, and this pins it so a fix can be
// measured against it. The scanner stops with bufio.ErrTooLong and nothing
// drains stdout afterwards, so the harness BLOCKS forever writing into a full
// pipe. cmd.Wait never returns, the adapter never reaches its
// stream-ended-without-result path, and the task sits until TaskDeadline —
// 1800s in production (A2A_TASK_DEADLINE_SECONDS) — before reporting
// `deadline-exceeded`.
//
// So the cost is not just the lost deliverable:
//   - the reason is actively misleading; `deadline-exceeded` reads as "the
//     model took too long", not "the answer was too big to read",
//   - the pod holds its session slot, and its shared bus credential, for the
//     whole deadline doing nothing,
//   - and `scannerMaxBytes` is therefore not a truncation limit but a stall
//     trigger.
//
// The fix is to keep draining stdout after a scan error (discarding), so the
// harness can exit and the adapter can report the real cause. Recorded rather
// than fixed here: it is a behaviour change on the worker's terminal path.
func TestLifecycle_OversizeResultLineStallsToTheDeadline(t *testing.T) {
	url := startServer(t)
	c := testClient(t, url)
	const session, taskID = "chat-tapir-oversize", "task-oversize-1"
	submit(t, c, session, taskID, "write something enormous")

	// One result line comfortably past scannerMaxBytes (8 MiB), emitted the
	// way a real deliverable would be: valid JSON, exit 0, nothing wrong with
	// the harness at all.
	harness := stub(t, `
echo '{"type":"system","subtype":"init","session_id":"stub-oversize"}'
printf '{"type":"result","subtype":"success","result":"'
head -c 9000000 /dev/zero | tr '\0' 'x'
printf '"}\n'
exit 0
`)
	out := waitOutcome(t, runAdapter(context.Background(), adapterConfig(url, taskID, session, harness)), 120*time.Second)
	if out.res.State != lib.StateFailed {
		t.Fatalf("state %q, want failed: an oversize deliverable must not read as success", out.res.State)
	}
	task := foldTask(t, c, session, taskID)
	if task.State != lib.StateFailed || !task.Final {
		t.Fatalf("folded %+v, want a final failed task", task)
	}
	events := replayEvents(t, url, session, taskID)
	text := statusOf(t, events[len(events)-1]).Status.Message.Parts[0].Text
	// Pinned as-is, deliberately. When the drain lands, this flips to
	// stream-ended-without-result naming bufio.ErrTooLong, and this test
	// failing is the signal that it worked.
	if !strings.Contains(text, "deadline-exceeded") {
		t.Errorf("expected the documented current behaviour (a stall to the task "+
			"deadline); if this now names the scanner error instead, the drain "+
			"landed and this test should be inverted: %q", text)
	}
	if strings.Contains(text, "token too long") {
		t.Errorf("the scanner error now reaches the reason — good; invert this test")
	}
}
