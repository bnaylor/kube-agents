// Package hermesbridge is the stand-in executor for tasks addressed to the
// platform profile: it consumes a2a.tasks.{profile}.*.in, runs one
// `hermes -p {profile} chat -q <prompt>` per task, and publishes the payload
// spec's lifecycle events with the output as the result artifact. It is
// scaffolding for the Hermes-first world - when the stage-3 dispatcher and
// the W4 worker adapter land, the bridge retires. Design:
// a2a/docs/hermes-bridge.md.
package hermesbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// Config wires one bridge. Zero values get playground defaults in Run.
type Config struct {
	// NATSURL is the bus address.
	NATSURL string
	// Profile is the addressee token the bridge executes for ("platform").
	Profile string
	// Command is the invocation prefix; the task prompt is appended as the
	// final argument. Default: ["hermes", "-p", <profile>, "chat", "-q"].
	Command []string
	// Concurrency caps simultaneous hermes subprocesses (default 2, the
	// platform profile's concurrency in the profiles spec).
	Concurrency int
	// TaskDeadline is the per-invocation wall-clock ceiling (default 7200s,
	// matching the platform profile's activeDeadlineSeconds).
	TaskDeadline time.Duration
	// KillGrace is SIGTERM-to-SIGKILL grace on cancel/deadline (default 10s).
	KillGrace time.Duration
	// KVBucket holds the in-flight registry the sweep reads (default
	// "runtime-state", the provisioned bucket).
	KVBucket string
	// ResultChunkSize bounds one result artifact-update's text part so a
	// large answer never trips the client-side max-message-size gate
	// (default 256KiB).
	ResultChunkSize int
	// NATSOptions carries credentials etc; applied to both connections.
	NATSOptions []nats.Option
	Logger      *slog.Logger
}

func (c *Config) defaults() {
	if c.Profile == "" {
		c.Profile = "platform"
	}
	if len(c.Command) == 0 {
		// -Q is hermes's programmatic mode: no banner, no spinner, no TUI
		// box around the answer - stdout is the response.
		c.Command = []string{"hermes", "-p", c.Profile, "chat", "-Q", "-q"}
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 2
	}
	if c.TaskDeadline <= 0 {
		c.TaskDeadline = 7200 * time.Second
	}
	if c.KillGrace <= 0 {
		c.KillGrace = 10 * time.Second
	}
	if c.KVBucket == "" {
		c.KVBucket = "runtime-state"
	}
	if c.ResultChunkSize <= 0 {
		c.ResultChunkSize = 256 * 1024
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// runState is a task's position in the bridge, guarded by taskRun.mu.
type runState int

const (
	statePending runState = iota // accepted, submitted published, queued
	stateRunning                 // subprocess spawned
	stateDone                    // terminal event published
)

type taskRun struct {
	origin *lib.Envelope
	exec   *lib.TaskExecution

	mu    sync.Mutex
	state runState
	proc  *exec.Cmd

	canceled    atomic.Bool
	deadlineHit atomic.Bool
}

// Bridge is one running instance. Two connections by design: the lib client
// owns the task plane (consume, publish, resilience contract), and a raw
// jetstream handle owns what the lib doesn't speak yet - the KV in-flight
// registry and the sweep's CAS publish.
type Bridge struct {
	cfg  Config
	from lib.Party

	c  *lib.Client
	nc *nats.Conn
	js jetstream.JetStream
	kv jetstream.KeyValue

	mu    sync.Mutex
	tasks map[string]*taskRun
	queue chan *taskRun
	wg    sync.WaitGroup
}

// New connects and sweeps but does not consume yet; Run does.
func New(ctx context.Context, cfg Config) (*Bridge, error) {
	cfg.defaults()
	b := &Bridge{
		cfg: cfg,
		from: lib.Party{
			Session:   cfg.Profile + "-bridge",
			AgentType: "hermes-bridge",
			Profile:   cfg.Profile,
		},
		tasks: make(map[string]*taskRun),
		queue: make(chan *taskRun, 1024),
	}
	var err error
	b.c, err = lib.Connect(ctx, cfg.NATSURL,
		lib.WithName(b.from.Session),
		lib.WithLogger(cfg.Logger),
		lib.WithNATSOptions(cfg.NATSOptions...))
	if err != nil {
		return nil, err
	}
	b.nc, err = nats.Connect(cfg.NATSURL, append([]nats.Option{
		nats.Name(b.from.Session + "-kv"), nats.MaxReconnects(-1),
	}, cfg.NATSOptions...)...)
	if err != nil {
		b.c.Close()
		return nil, fmt.Errorf("kv connection: %w", err)
	}
	b.js, err = jetstream.New(b.nc)
	if err != nil {
		b.close()
		return nil, fmt.Errorf("kv jetstream: %w", err)
	}
	b.kv, err = b.js.KeyValue(ctx, cfg.KVBucket)
	if err != nil {
		b.close()
		return nil, fmt.Errorf("kv bucket %s: %w", cfg.KVBucket, err)
	}
	return b, nil
}

func (b *Bridge) close() {
	b.c.Close()
	b.nc.Close()
}

// Run sweeps orphans from a prior incarnation, then consumes the profile's
// in subjects until ctx is canceled. On shutdown, in-flight tasks get
// terminal failed (reason: bridge-shutdown) - the eviction path the profiles
// spec requires of adapters, so a rollout stays distinguishable from a crash.
func (b *Bridge) Run(ctx context.Context) error {
	defer b.close()
	if err := b.sweep(ctx); err != nil {
		return fmt.Errorf("startup sweep: %w", err)
	}
	for i := 0; i < b.cfg.Concurrency; i++ {
		b.wg.Add(1)
		go b.worker(ctx)
	}
	sub, err := b.c.SubscribeDurable(ctx, lib.SubscribeConfig{
		Stream:  lib.TasksStream,
		Subject: fmt.Sprintf("a2a.tasks.%s.*.in", b.cfg.Profile),
		Durable: "bridge-" + b.cfg.Profile,
		Session: b.cfg.Profile,
	}, func(env *lib.Envelope) { b.handle(ctx, env) })
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	b.cfg.Logger.Info("hermes bridge consuming", "profile", b.cfg.Profile)
	<-ctx.Done()
	sub.Stop()
	close(b.queue)
	b.shutdownTasks()
	b.wg.Wait()
	return nil
}

// shutdownTasks kills running subprocesses and writes terminal failed for
// every non-done task.
func (b *Bridge) shutdownTasks() {
	b.mu.Lock()
	runs := make([]*taskRun, 0, len(b.tasks))
	for _, r := range b.tasks {
		runs = append(runs, r)
	}
	b.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, r := range runs {
		r.mu.Lock()
		wasRunning := r.state == stateRunning
		if r.state != stateDone {
			r.state = stateDone
		} else {
			r.mu.Unlock()
			continue
		}
		proc := r.proc
		r.mu.Unlock()
		if wasRunning && proc != nil && proc.Process != nil {
			_ = syscall.Kill(-proc.Process.Pid, syscall.SIGKILL)
			// The runner goroutine sees stateDone and leaves the terminal
			// event to us.
		}
		if err := b.publishTerminal(ctx, r, lib.StateFailed, "reason: bridge-shutdown - the bridge was terminated while this task was in flight"); err != nil {
			b.cfg.Logger.Error("shutdown terminal publish failed", "task", r.origin.TaskID, "err", err)
			continue
		}
		b.clearInFlight(ctx, r.origin.TaskID)
	}
}

// handle dispatches one envelope from the in subject. Anything it publishes
// happens before returning, ie before the consumer ack - a bridge death in
// here just redelivers.
func (b *Bridge) handle(ctx context.Context, env *lib.Envelope) {
	switch env.Kind {
	case lib.KindMessage:
		b.handleMessage(ctx, env)
	case lib.KindCancel:
		b.handleCancel(ctx, env)
	default:
		b.cfg.Logger.Warn("unexpected kind on in subject; ignoring",
			"kind", env.Kind, "task", env.TaskID)
	}
}

func (b *Bridge) handleMessage(ctx context.Context, env *lib.Envelope) {
	b.mu.Lock()
	run := b.tasks[env.TaskID]
	b.mu.Unlock()
	if run != nil {
		b.refuseSteer(ctx, run, env)
		return
	}
	// Unknown task: the dispatcher rule. Empty events subject means new;
	// terminal means acked with a warning; non-final events with no local run
	// is an orphan a follow-up cannot revive.
	task, err := b.c.TasksGet(ctx, b.cfg.Profile, env.TaskID)
	switch {
	case isTaskNotFound(err):
		b.accept(ctx, env)
	case err != nil:
		// The lib acks after this handler returns, so the submission is
		// dropped, not redelivered - no terminal event will follow. Honest
		// gap: the lib exposes no nak path yet.
		b.cfg.Logger.Error("events lookup failed; dropping submission",
			"task", env.TaskID, "err", err)
	case task.Final:
		b.cfg.Logger.Warn("message for a task with a terminal event; ignoring", "task", env.TaskID)
	default:
		b.cfg.Logger.Warn("message for an orphaned task this bridge is not running; ignoring",
			"task", env.TaskID, "state", task.State)
	}
}

// accept is the dispatcher half: register in-flight, publish submitted,
// queue for a worker. KV before submitted, deliberately - a crash between
// the two leaves a key the sweep deletes harmlessly, where the opposite
// order leaves a task the sweep cannot see.
func (b *Bridge) accept(ctx context.Context, env *lib.Envelope) {
	x, err := b.c.NewTaskExecution(env, b.from, b.cfg.Profile)
	if err != nil {
		b.cfg.Logger.Error("rejecting malformed submission", "task", env.TaskID, "err", err)
		return
	}
	run := &taskRun{origin: env, exec: x}
	if err := b.markInFlight(ctx, env.TaskID); err != nil {
		b.cfg.Logger.Error("in-flight registry write failed; dropping submission",
			"task", env.TaskID, "err", err)
		return
	}
	if err := x.PublishStatus(ctx, lib.StateSubmitted, false); err != nil {
		b.cfg.Logger.Error("submitted publish failed; dropping submission",
			"task", env.TaskID, "err", err)
		b.clearInFlight(ctx, env.TaskID)
		return
	}
	b.mu.Lock()
	b.tasks[env.TaskID] = run
	b.mu.Unlock()
	b.cfg.Logger.Info("task accepted", "task", env.TaskID, "correlation", env.CorrelationID, "from", env.From.Session)
	select {
	case b.queue <- run:
	default:
		// 1024 queued tasks on a playground bridge is a fault, not load.
		b.finishRun(ctx, run, lib.StateFailed, "reason: bridge-queue-overflow")
	}
}

func (b *Bridge) handleCancel(ctx context.Context, env *lib.Envelope) {
	b.mu.Lock()
	run := b.tasks[env.TaskID]
	b.mu.Unlock()
	if run == nil {
		b.cancelOrphan(ctx, env)
		return
	}
	run.canceled.Store(true)
	run.mu.Lock()
	if run.state == statePending {
		// Not yet spawned: terminal now; the worker skips done runs.
		run.state = stateDone
		run.mu.Unlock()
		b.terminalAndClear(ctx, run, lib.StateCanceled, "reason: canceled-before-start")
		return
	}
	proc := run.proc
	running := run.state == stateRunning
	run.mu.Unlock()
	if running && proc != nil && proc.Process != nil {
		b.killGroup(proc.Process.Pid)
	}
	// The runner goroutine publishes terminal canceled when the process exits.
}

// cancelOrphan handles cancel for a task the bridge is not running: if its
// events show it non-final (a prior incarnation died mid-task and the sweep
// has no key for it), synthesize terminal canceled under CAS. Terminal or
// absent tasks get a warning and nothing else.
func (b *Bridge) cancelOrphan(ctx context.Context, env *lib.Envelope) {
	task, err := b.c.TasksGet(ctx, b.cfg.Profile, env.TaskID)
	switch {
	case isTaskNotFound(err):
		b.cfg.Logger.Warn("cancel for a task with no events; ignoring", "task", env.TaskID)
	case err != nil:
		b.cfg.Logger.Error("cancel events lookup failed", "task", env.TaskID, "err", err)
	case task.Final:
		b.cfg.Logger.Warn("cancel for a task with a terminal event; ignoring", "task", env.TaskID)
	default:
		if err := b.synthesizeTerminal(ctx, env.TaskID, task, lib.StateCanceled,
			"reason: canceled-while-orphaned - no live executor held this task"); err != nil {
			b.cfg.Logger.Error("orphan cancel synthesis failed", "task", env.TaskID, "err", err)
		}
	}
}

// refuseSteer answers a mid-run follow-up honestly: hermes chat -q is
// one-shot, there is no stdin to inject into. Non-final working status, no
// state change (payload spec assertion 12's steering half).
func (b *Bridge) refuseSteer(ctx context.Context, run *taskRun, steer *lib.Envelope) {
	msg := "steering received but not absorbed: the Hermes CLI runs one-shot and cannot " +
		"accept mid-run input. The task continues on its original instruction; cancel if that is wrong."
	if err := b.publishStatusMessage(ctx, run, lib.StateWorking, false, msg); err != nil {
		b.cfg.Logger.Error("steer refusal publish failed", "task", steer.TaskID, "err", err)
	}
}

func (b *Bridge) worker(ctx context.Context) {
	defer b.wg.Done()
	for run := range b.queue {
		run.mu.Lock()
		if run.state != statePending {
			run.mu.Unlock()
			continue
		}
		run.state = stateRunning
		run.mu.Unlock()
		b.runTask(ctx, run)
	}
}

func (b *Bridge) runTask(ctx context.Context, run *taskRun) {
	taskID := run.origin.TaskID
	prompt, ok := promptFromMessage(run.origin.Payload)
	if !ok {
		b.finishRun(ctx, run, lib.StateRejected,
			"reason: no-text-parts - the submission message carries nothing the hermes CLI can be asked")
		return
	}
	if err := run.exec.PublishStatus(ctx, lib.StateWorking, false); err != nil {
		b.cfg.Logger.Error("working publish failed", "task", taskID, "err", err)
		b.finishRun(ctx, run, lib.StateFailed, "reason: bus-publish-failed at working")
		return
	}

	argv := append(append([]string(nil), b.cfg.Command...), prompt)
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout strings.Builder
	stderr := newTailBuffer(2048)
	cmd.Stdout = &stdout
	cmd.Stderr = stderr

	run.mu.Lock()
	if run.state != stateRunning {
		// Shutdown or cancel got here first.
		run.mu.Unlock()
		return
	}
	if err := cmd.Start(); err != nil {
		run.mu.Unlock()
		b.finishRun(ctx, run, lib.StateFailed, fmt.Sprintf("reason: spawn-failed - %v", err))
		return
	}
	run.proc = cmd
	run.mu.Unlock()

	// Cancel may have raced the spawn: its kill saw no process, so re-check.
	if run.canceled.Load() {
		b.killGroup(cmd.Process.Pid)
	}
	deadline := time.AfterFunc(b.cfg.TaskDeadline, func() {
		run.deadlineHit.Store(true)
		b.killGroup(cmd.Process.Pid)
	})
	err := cmd.Wait()
	deadline.Stop()

	run.mu.Lock()
	if run.state != stateRunning {
		// Shutdown owns the terminal event.
		run.mu.Unlock()
		return
	}
	run.state = stateDone
	run.mu.Unlock()

	switch {
	case err == nil:
		// A canceled task that finished anyway won the race: completed wins,
		// per the payload spec's cancel mapping.
		if perr := b.publishResult(ctx, run, stdout.String()); perr != nil {
			b.cfg.Logger.Error("result publish failed", "task", taskID, "err", perr)
			b.terminalAndClear(ctx, run, lib.StateFailed, "reason: bus-publish-failed at result")
			return
		}
		b.terminalAndClear(ctx, run, lib.StateCompleted, "")
	case run.deadlineHit.Load():
		b.terminalAndClear(ctx, run, lib.StateFailed,
			fmt.Sprintf("reason: deadline-exceeded - killed after %s", b.cfg.TaskDeadline))
	case run.canceled.Load():
		b.terminalAndClear(ctx, run, lib.StateCanceled, "reason: canceled-by-request")
	default:
		b.terminalAndClear(ctx, run, lib.StateFailed,
			fmt.Sprintf("reason: hermes-exited-nonzero - %v; stderr tail: %s", err, stderr.String()))
	}
}

// finishRun marks the run done (if a worker still owns it) and writes the
// terminal event.
func (b *Bridge) finishRun(ctx context.Context, run *taskRun, state lib.TaskState, msg string) {
	run.mu.Lock()
	run.state = stateDone
	run.mu.Unlock()
	b.terminalAndClear(ctx, run, state, msg)
}

func (b *Bridge) terminalAndClear(ctx context.Context, run *taskRun, state lib.TaskState, msg string) {
	if err := b.publishTerminal(ctx, run, state, msg); err != nil {
		// The task stays in the KV registry, so a restart's sweep writes the
		// terminal event this publish could not.
		b.cfg.Logger.Error("terminal publish failed; sweep will finalize",
			"task", run.origin.TaskID, "state", state, "err", err)
		return
	}
	b.clearInFlight(ctx, run.origin.TaskID)
	b.mu.Lock()
	delete(b.tasks, run.origin.TaskID)
	b.mu.Unlock()
	b.cfg.Logger.Info("task finished", "task", run.origin.TaskID, "state", state)
}

func (b *Bridge) publishTerminal(ctx context.Context, run *taskRun, state lib.TaskState, msg string) error {
	if msg == "" {
		return run.exec.PublishStatus(ctx, state, true)
	}
	return b.publishStatusMessage(ctx, run, state, true, msg)
}

// publishStatusMessage is PublishStatus with a status.message attached -
// the lib's TaskExecution doesn't carry one, and reasons ride there.
func (b *Bridge) publishStatusMessage(ctx context.Context, run *taskRun, state lib.TaskState, final bool, text string) error {
	origin := run.origin
	payload, err := json.Marshal(lib.StatusUpdate{
		TaskID:    origin.TaskID,
		ContextID: origin.ContextID,
		Status: lib.TaskStatus{
			State: state,
			Message: &lib.Message{
				Role:      "agent",
				MessageID: "msg-" + nuid.Next(),
				Parts:     []lib.Part{{Kind: "text", Text: text}},
				TaskID:    origin.TaskID,
				ContextID: origin.ContextID,
			},
		},
		Final: final,
	})
	if err != nil {
		return err
	}
	env, err := lib.NewStatusUpdateEnvelope(b.from, origin.TaskID, origin.ContextID, origin.CorrelationID, payload)
	if err != nil {
		return err
	}
	return b.c.Publish(ctx, lib.TaskEventsSubject(b.cfg.Profile, origin.TaskID), env)
}

// publishResult ships stdout as the result artifact, chunked per A2A rules
// so one huge answer never trips the max-message-size gate.
func (b *Bridge) publishResult(ctx context.Context, run *taskRun, output string) error {
	chunks := chunkString(output, b.cfg.ResultChunkSize)
	artifactID := "artifact-" + run.origin.TaskID + "-result"
	for i, chunk := range chunks {
		payload, err := json.Marshal(lib.ArtifactUpdate{
			TaskID:    run.origin.TaskID,
			ContextID: run.origin.ContextID,
			Artifact: lib.Artifact{
				ArtifactID: artifactID,
				Name:       lib.ArtifactResult,
				Parts:      []lib.Part{{Kind: "text", Text: chunk}},
			},
			Append:    i > 0,
			LastChunk: i == len(chunks)-1,
		})
		if err != nil {
			return err
		}
		env, err := lib.NewArtifactUpdateEnvelope(b.from, run.origin.TaskID, run.origin.ContextID, run.origin.CorrelationID, payload)
		if err != nil {
			return err
		}
		if err := b.c.Publish(ctx, lib.TaskEventsSubject(b.cfg.Profile, run.origin.TaskID), env); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bridge) killGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	time.AfterFunc(b.cfg.KillGrace, func() {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	})
}

// chunkString splits s into size-byte pieces on byte boundaries; an empty s
// is one empty chunk, because a completed task must still carry a result
// artifact (assertion 18).
func chunkString(s string, size int) []string {
	if len(s) <= size {
		return []string{s}
	}
	var out []string
	for len(s) > size {
		out = append(out, s[:size])
		s = s[size:]
	}
	return append(out, s)
}

// promptFromMessage joins the submission message's text parts. ok is false
// when there is nothing textual to ask.
func promptFromMessage(payload json.RawMessage) (string, bool) {
	var m lib.Message
	if err := json.Unmarshal(payload, &m); err != nil {
		return "", false
	}
	var texts []string
	for _, p := range m.Parts {
		if p.Kind == "text" && strings.TrimSpace(p.Text) != "" {
			texts = append(texts, p.Text)
		}
	}
	if len(texts) == 0 {
		return "", false
	}
	return strings.Join(texts, "\n\n"), true
}

func isTaskNotFound(err error) bool {
	var a2aErr *lib.A2AError
	return errors.As(err, &a2aErr) && a2aErr.Code == lib.CodeTaskNotFound
}

// tailBuffer keeps the last cap bytes written - stderr evidence for the
// failed reason without holding a runaway stream.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	cap int
}

func newTailBuffer(capacity int) *tailBuffer {
	return &tailBuffer{cap: capacity}
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.cap {
		t.buf = t.buf[len(t.buf)-t.cap:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}
