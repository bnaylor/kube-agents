// Package workeradapter is the in-pod shim between the bus and the harness
// (spec-subagent-profiles.md, "The adapter"): it fetches its one task by
// subject, publishes the lifecycle events, drives the harness over the
// headless stream-json contract, forwards steering and follow-ups onto the
// harness stdin, and exits with a code matching the terminal state. One task
// per process; the pod exists because the message is already durable.
package workeradapter

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// DefaultHarnessPath is where the worker image carries the harness: the
// native binary shipped inside the agent SDK's platform package (launch-card
// constants; it is a self-contained executable, not a cli.js).
const DefaultHarnessPath = "/app/node_modules/@anthropic-ai/claude-agent-sdk-linux-x64/claude"

// harnessEvent is one stream-json line from the harness stdout. Only the
// fields the mapper consults; unknown fields and unknown types pass through
// the decoder untouched and are ignored, mirroring the envelope's own
// unknown-field rule.
type harnessEvent struct {
	Type    string `json:"type"`
	Subtype string `json:"subtype"`
	// type:"assistant" carries an API Message; content blocks are inspected
	// for text / thinking / tool_use.
	Message *harnessMessage `json:"message,omitempty"`
	// type:"result" fields.
	Result  string `json:"result,omitempty"`
	IsError bool   `json:"is_error,omitempty"`
	// type:"system" subtype:"init".
	SessionID string `json:"session_id,omitempty"`
}

type harnessMessage struct {
	Content []harnessBlock `json:"content"`
}

type harnessBlock struct {
	Type     string          `json:"type"`
	Text     string          `json:"text,omitempty"`
	Thinking string          `json:"thinking,omitempty"`
	Name     string          `json:"name,omitempty"`  // tool_use
	Input    json.RawMessage `json:"input,omitempty"` // tool_use
}

// userMessage is the stream-json stdin shape for both the opening prompt and
// every steer: the SDK serializes user turns exactly like this, and a line
// written mid-run is absorbed at the harness's next turn boundary - which is
// the payload spec's steering rule made concrete.
type userMessage struct {
	Type    string          `json:"type"`
	Message userMessageBody `json:"message"`
}

type userMessageBody struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// harnessProc supervises one harness subprocess: stdin writer, stdout
// scanner, stderr tail, process-group kill.
type harnessProc struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	events <-chan harnessEvent
	// scanDone closes when the stdout scanner has hit EOF - Wait must not
	// run before it, or the pipe teardown races the last buffered events
	// (the result line, typically) out of existence.
	scanDone chan struct{}
	// scanErr surfaces a stdout read/parse failure after events closes.
	scanErr func() error
	stderr  *tailBuffer
	log     *slog.Logger

	mu         sync.Mutex
	stdinDead  bool
	killTimers []*time.Timer
}

// startHarness launches argv with the given extra environment appended to
// the parent's, writes the opening prompt as the first stdin line, and
// starts the stdout scanner. The process runs in its own process group so a
// kill reaches the harness's own children.
func startHarness(argv []string, env []string, prompt string, log *slog.Logger) (*harnessProc, error) {
	if len(argv) == 0 {
		return nil, fmt.Errorf("empty harness command")
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("harness stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("harness stdout: %w", err)
	}
	stderr := &tailBuffer{max: 2048}
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("spawn harness: %w", err)
	}

	events := make(chan harnessEvent, 64)
	scanDone := make(chan struct{})
	var scanFailed error
	var scanMu sync.Mutex
	go func() {
		defer close(scanDone)
		defer close(events)
		sc := bufio.NewScanner(stdout)
		// Result lines carry a whole turn's text; give them room.
		sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
		for sc.Scan() {
			line := sc.Bytes()
			if len(line) == 0 {
				continue
			}
			var ev harnessEvent
			if err := json.Unmarshal(line, &ev); err != nil {
				// Non-JSON chatter on stdout is logged, never fatal - the
				// harness owns its stdout and the contract owns only the
				// JSON lines.
				log.Warn("harness emitted non-JSON stdout line", "line", truncate(string(line), 200))
				continue
			}
			events <- ev
		}
		if err := sc.Err(); err != nil {
			scanMu.Lock()
			scanFailed = err
			scanMu.Unlock()
		}
	}()

	p := &harnessProc{
		cmd:      cmd,
		stdin:    stdin,
		events:   events,
		scanDone: scanDone,
		scanErr: func() error {
			scanMu.Lock()
			defer scanMu.Unlock()
			return scanFailed
		},
		stderr: stderr,
		log:    log,
	}
	if err := p.writeUser(prompt); err != nil {
		p.kill(0)
		_ = cmd.Wait()
		return nil, fmt.Errorf("write opening prompt: %w", err)
	}
	return p, nil
}

// writeUser writes one user message line onto the harness stdin. Steers
// reuse it verbatim: same shape, later turn.
func (p *harnessProc) writeUser(text string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stdinDead {
		return fmt.Errorf("harness stdin closed")
	}
	line, err := json.Marshal(userMessage{
		Type:    "user",
		Message: userMessageBody{Role: "user", Content: text},
	})
	if err != nil {
		return err
	}
	if _, err := p.stdin.Write(append(line, '\n')); err != nil {
		p.stdinDead = true
		return err
	}
	return nil
}

// closeStdin ends the harness's input stream - the signal that the
// conversation is over and it should finish and exit.
func (p *harnessProc) closeStdin() {
	p.mu.Lock()
	p.stdinDead = true
	p.mu.Unlock()
	_ = p.stdin.Close()
}

// kill delivers SIGTERM to the process group, escalating to SIGKILL after
// grace. Timers are retained so the reaper can stop them before the pgid is
// recycled (the bridge's lesson, W7).
func (p *harnessProc) kill(grace time.Duration) {
	pid := p.cmd.Process.Pid
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	if grace <= 0 {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		return
	}
	t := time.AfterFunc(grace, func() {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	})
	p.mu.Lock()
	p.killTimers = append(p.killTimers, t)
	p.mu.Unlock()
}

// reaped stops any armed escalation timers; called after Wait so a recycled
// process group can't be killed by a stale timer.
func (p *harnessProc) reaped() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range p.killTimers {
		t.Stop()
	}
	p.killTimers = nil
}

// tailBuffer keeps the last max bytes written - failure evidence without
// unbounded memory (W7's pattern).
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (t *tailBuffer) Write(b []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, b...)
	if len(t.buf) > t.max {
		t.buf = t.buf[len(t.buf)-t.max:]
	}
	return len(b), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
