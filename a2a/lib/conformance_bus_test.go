package lib

// Server-backed conformance assertions: the passthrough half of 3, plus 4, 5,
// and the max-message-size half of 8. All run against a real nats-server with
// JetStream started in-process.

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// collector gathers delivered envelopes for assertions.
type collector struct {
	mu   sync.Mutex
	envs []*Envelope
}

func (c *collector) handle(env *Envelope) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.envs = append(c.envs, env)
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.envs)
}

func (c *collector) all() []*Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*Envelope(nil), c.envs...)
}

// Assertion 3 (passthrough half): inbound identity and authority values reach
// the application byte-identical, whatever a publisher put in them.
func TestAssertion03_PassthroughBus(t *testing.T) {
	s := startServer(t)
	provisionTasksStream(t, clientURL(s))
	ctx := testCtx(t)

	c, err := Connect(ctx, clientURL(s), WithName("test-consumer"))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	col := &collector{}
	_, err = c.SubscribeDurable(ctx, SubscribeConfig{
		Stream:  "TASKS",
		Subject: "a2a.tasks.task-pt.in",
		Durable: "pt-consumer",
		Session: "chatops",
	}, col.handle)
	if err != nil {
		t.Fatalf("SubscribeDurable: %v", err)
	}

	// Odd spacing and key order on purpose: byte-identical means these bytes.
	identity := `{"sub":  "spiffe://forged", "z":[1,null , true]}`
	authority := `{"requester":"discord:99",   "scope": {"a" :1}}`
	raw := validEnvelopeJSON(func(m map[string]any) {
		m["taskId"] = "task-pt"
		m["envelopeId"] = "env-pt-1"
	})
	// Splice raw identity/authority bytes in without normalizing them.
	raw = bytes.Replace(raw, []byte(`"identity":null`), []byte(`"identity":`+identity), 1)
	raw = bytes.Replace(raw, []byte(`"authority":null`), []byte(`"authority":`+authority), 1)
	publishRaw(t, clientURL(s), "a2a.tasks.task-pt.in", raw)

	waitFor(t, 5e9, "envelope delivery", func() bool { return col.count() == 1 })
	env := col.all()[0]
	if string(env.Identity) != identity {
		t.Errorf("identity not byte-identical:\n got: %s\nwant: %s", env.Identity, identity)
	}
	if string(env.Authority) != authority {
		t.Errorf("authority not byte-identical:\n got: %s\nwant: %s", env.Authority, authority)
	}
}

// Assertion 4: a consumer on a wildcard ignores envelopes whose to names
// another session.
func TestAssertion04_ToFiltering(t *testing.T) {
	s := startServer(t)
	provisionTasksStream(t, clientURL(s))
	ctx := testCtx(t)

	c, err := Connect(ctx, clientURL(s), WithName("test-consumer"))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	col := &collector{}
	_, err = c.SubscribeDurable(ctx, SubscribeConfig{
		Stream:  "TASKS",
		Subject: "a2a.tasks.*.in",
		Durable: "to-consumer",
		Session: "chatops",
	}, col.handle)
	if err != nil {
		t.Fatalf("SubscribeDurable: %v", err)
	}

	from := Party{Session: "worker-brisk-otter"}
	pub := func(taskID string, opts ...EnvelopeOption) {
		env, err := NewMessageEnvelope(from, taskID, "ctx-1", "corr-1", validMessagePayload(), opts...)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if err := c.Publish(ctx, TaskInSubject(taskID), env); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	pub("t-broadcast")                                        // no to: delivered
	pub("t-elsewhere", WithTo(Party{Session: "other-place"})) // ignored
	pub("t-mine", WithTo(Party{Session: "chatops"}))          // delivered

	waitFor(t, 5e9, "two deliveries", func() bool { return col.count() >= 2 })
	got := map[string]bool{}
	for _, e := range col.all() {
		got[e.TaskID] = true
	}
	if !got["t-broadcast"] || !got["t-mine"] || got["t-elsewhere"] {
		t.Errorf("delivered set = %v; want t-broadcast and t-mine only", got)
	}
	if col.count() != 2 {
		t.Errorf("delivered %d envelopes, want 2", col.count())
	}
}

// Assertion 5: a redelivered envelope (same envelopeId) reaches the
// application at most once.
func TestAssertion05_Dedup(t *testing.T) {
	s := startServer(t)
	provisionTasksStream(t, clientURL(s))
	ctx := testCtx(t)

	c, err := Connect(ctx, clientURL(s), WithName("test-consumer"))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	col := &collector{}
	_, err = c.SubscribeDurable(ctx, SubscribeConfig{
		Stream:  "TASKS",
		Subject: "a2a.tasks.task-dd.in",
		Durable: "dd-consumer",
		Session: "chatops",
	}, col.handle)
	if err != nil {
		t.Fatalf("SubscribeDurable: %v", err)
	}

	env, err := NewMessageEnvelope(Party{Session: "w"}, "task-dd", "ctx-1", "corr-1",
		validMessagePayload(), WithEnvelopeID("env-dup-1"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	// The same envelope hitting the subject twice is what JetStream redelivery
	// looks like to the application layer.
	publishRaw(t, clientURL(s), "a2a.tasks.task-dd.in", raw)
	publishRaw(t, clientURL(s), "a2a.tasks.task-dd.in", raw)
	// A distinct envelope proves delivery still flows after the duplicate.
	env2, err := NewMessageEnvelope(Party{Session: "w"}, "task-dd", "ctx-1", "corr-1",
		validMessagePayload(), WithEnvelopeID("env-dup-2"))
	if err != nil {
		t.Fatal(err)
	}
	raw2, _ := json.Marshal(env2)
	publishRaw(t, clientURL(s), "a2a.tasks.task-dd.in", raw2)

	waitFor(t, 5e9, "post-duplicate delivery", func() bool {
		for _, e := range col.all() {
			if e.EnvelopeID == "env-dup-2" {
				return true
			}
		}
		return false
	})
	seen := 0
	for _, e := range col.all() {
		if e.EnvelopeID == "env-dup-1" {
			seen++
		}
	}
	if seen != 1 {
		t.Errorf("envelope env-dup-1 reached the application %d times, want 1", seen)
	}
}

// Assertion 8 (size half): an envelope over the max message size fails
// client-side with an A2A error before publish.
func TestAssertion08_MaxMessageSize(t *testing.T) {
	s := runJetStreamServer(t, -1, t.TempDir(), func(o *natsserver.Options) {
		o.MaxPayload = 4096
	})
	t.Cleanup(s.Shutdown)
	provisionTasksStream(t, clientURL(s))
	ctx := testCtx(t)

	c, err := Connect(ctx, clientURL(s), WithName("test-pub"))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	big := bytes.Repeat([]byte("x"), 8192)
	payload := `{"role": "user", "parts": [{"kind": "text", "text": "` + string(big) + `"}], "messageId": "m-big"}`
	env, err := NewMessageEnvelope(Party{Session: "w"}, "task-big", "ctx-1", "corr-1", json.RawMessage(payload))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	err = c.Publish(ctx, TaskInSubject("task-big"), env)
	var a2aErr *A2AError
	if !errors.As(err, &a2aErr) {
		t.Fatalf("oversize publish: want A2AError, got %v", err)
	}
	// Client-side means the server never saw it.
	if n := streamMsgCount(t, clientURL(s), "TASKS"); n != 0 {
		t.Errorf("stream holds %d messages; oversize envelope must fail before publish", n)
	}
}

// Assertion 8, size half continued: the client-side gate must count the
// Nats-Msg-Id header the way the server does, so an envelope inside the
// header-width window under the max still fails with an A2A error, not a raw
// nats.go one.
func TestAssertion08_HeaderWidthWindow(t *testing.T) {
	const maxPayload = 4096
	s := runJetStreamServer(t, -1, t.TempDir(), func(o *natsserver.Options) {
		o.MaxPayload = maxPayload
	})
	t.Cleanup(s.Shutdown)
	provisionTasksStream(t, clientURL(s))
	ctx := testCtx(t)

	c, err := Connect(ctx, clientURL(s), WithName("test-pub"))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	// Two passes: measure, then pad the text so the marshaled envelope alone
	// is exactly maxPayload bytes - inside the gate if headers are ignored,
	// over the wire limit once the Nats-Msg-Id header is counted.
	build := func(fill int) (*Envelope, int) {
		payload := `{"role": "user", "parts": [{"kind": "text", "text": "` + strings.Repeat("x", fill) + `"}], "messageId": "m-w", "taskId": "task-win", "contextId": "ctx-1"}`
		env, err := NewMessageEnvelope(Party{Session: "w"}, "task-win", "ctx-1", "corr-1",
			json.RawMessage(payload), WithEnvelopeID("env-window-fixed"))
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		data, err := json.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		return env, len(data)
	}
	// The ts field marshals at variable width (RFC3339Nano trims zeros), so
	// converge on the target and then assert we landed inside the window:
	// data alone within the max, data plus the Nats-Msg-Id header over it.
	fill := 1000
	env, size := build(fill)
	for i := 0; i < 10 && size != maxPayload; i++ {
		fill += maxPayload - size
		env, size = build(fill)
	}
	overhead := msgIDHeaderOverhead + len("env-window-fixed")
	if size > maxPayload || size+overhead <= maxPayload {
		t.Fatalf("sizing pass built %d bytes; need within (%d, %d]", size, maxPayload-overhead, maxPayload)
	}

	err = c.Publish(ctx, TaskInSubject("task-win"), env)
	var a2aErr *A2AError
	if !errors.As(err, &a2aErr) {
		t.Fatalf("in-window oversize publish: want A2AError, got %v", err)
	}
	if n := streamMsgCount(t, clientURL(s), "TASKS"); n != 0 {
		t.Errorf("stream holds %d messages; the gate must fire before publish", n)
	}
}

// The to-filter cannot run without knowing our own session, so an anonymous
// durable subscription is refused rather than silently non-conforming.
func TestSubscribeDurable_RequiresSession(t *testing.T) {
	s := startServer(t)
	provisionTasksStream(t, clientURL(s))
	ctx := testCtx(t)
	c, err := Connect(ctx, clientURL(s), WithName("test-consumer"))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()
	_, err = c.SubscribeDurable(ctx, SubscribeConfig{
		Stream:  "TASKS",
		Subject: "a2a.tasks.task-ns.in",
		Durable: "ns-consumer",
	}, func(*Envelope) {})
	if err == nil {
		t.Fatal("SubscribeDurable accepted a config without Session")
	}
}
