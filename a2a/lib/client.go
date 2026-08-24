package lib

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TaskInSubject is where a requester publishes message and cancel envelopes
// for a task.
func TaskInSubject(taskID string) string {
	return fmt.Sprintf("a2a.tasks.%s.in", taskID)
}

// TaskEventsSubject is where the executor publishes status and artifact
// updates for a task.
func TaskEventsSubject(taskID string) string {
	return fmt.Sprintf("a2a.tasks.%s.events", taskID)
}

// AgentSubject carries an agent's card and its shutdown tombstone.
func AgentSubject(session string) string {
	return fmt.Sprintf("a2a.agents.%s", session)
}

// Client is the a2a-jetstream client: validated publish, durable subscribe
// with dedup, and the NR resilience contract on top of nats.go.
type Client struct {
	url  string
	opts clientOptions
	log  *slog.Logger

	mu      sync.RWMutex
	nc      *nats.Conn
	js      jetstream.JetStream
	subs    []*durableSub
	closing atomic.Bool

	// rebuilds counts terminal-close recoveries (NR-2); exposed for tests and
	// health.
	rebuilds atomic.Int64
}

// ClientOption configures Connect.
type ClientOption func(*clientOptions)

type clientOptions struct {
	name     string
	logger   *slog.Logger
	natsOpts []nats.Option
}

// WithName names the connection for server-side observability.
func WithName(name string) ClientOption {
	return func(o *clientOptions) { o.name = name }
}

// WithLogger routes the connection-event log lines NR-3 requires.
func WithLogger(l *slog.Logger) ClientOption {
	return func(o *clientOptions) { o.logger = l }
}

// WithNATSOptions appends raw nats.go options (reconnect tuning in tests).
// Connection callbacks are owned by the library and cannot be overridden.
func WithNATSOptions(opts ...nats.Option) ClientOption {
	return func(o *clientOptions) { o.natsOpts = append(o.natsOpts, opts...) }
}

// Connect dials NATS and establishes JetStream. All four connection callbacks
// — disconnected, reconnected, closed, error — are registered here and logged
// with the server error that triggered them (NR-3).
func Connect(ctx context.Context, url string, opts ...ClientOption) (*Client, error) {
	c := &Client{url: url}
	for _, opt := range opts {
		opt(&c.opts)
	}
	c.log = c.opts.logger
	if c.log == nil {
		c.log = slog.Default()
	}
	nc, js, err := c.dial()
	if err != nil {
		return nil, err
	}
	c.nc, c.js = nc, js
	return c, nil
}

// dial builds a fresh connection with the callback set. It never touches
// existing connection objects, so the rebuild path (NR-2) can call it against
// a dead predecessor.
func (c *Client) dial() (*nats.Conn, jetstream.JetStream, error) {
	base := []nats.Option{
		nats.Name(c.opts.name),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			// Transient (NR-1): nats.go reconnects; tear nothing down.
			c.log.Warn("nats disconnected", "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.log.Info("nats reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			subject := ""
			if sub != nil {
				subject = sub.Subject
			}
			c.log.Error("nats async error", "err", err, "subject", subject)
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			// Terminal (NR-1): the connection will never come back on its own.
			c.log.Error("nats connection closed", "err", nc.LastError())
			if !c.closing.Load() {
				go c.rebuild()
			}
		}),
	}
	nc, err := nats.Connect(c.url, append(base, c.opts.natsOpts...)...)
	if err != nil {
		return nil, nil, fmt.Errorf("nats connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("jetstream: %w", err)
	}
	return nc, js, nil
}

// rebuild is the terminal-close path (NR-2): construct a fresh client
// connection, re-establish JetStream, and re-subscribe every durable from its
// spec. Nothing is retried against objects bound to the dead connection.
func (c *Client) rebuild() {
	c.log.Warn("nats terminal close: rebuilding connection")
	nc, js, err := c.dial()
	for err != nil {
		if c.closing.Load() {
			return
		}
		c.log.Error("nats rebuild dial failed; retrying", "err", err)
		nc, js, err = c.dial()
	}
	c.mu.Lock()
	c.nc, c.js = nc, js
	subs := append([]*durableSub(nil), c.subs...)
	c.mu.Unlock()
	for _, s := range subs {
		if s.stopped.Load() {
			continue
		}
		if err := s.start(context.Background(), js); err != nil {
			c.log.Error("nats rebuild re-subscribe failed", "durable", s.cfg.Durable, "err", err)
		} else {
			c.log.Info("nats rebuild re-subscribed", "durable", s.cfg.Durable)
		}
	}
	c.rebuilds.Add(1)
	c.log.Info("nats rebuild complete")
}

// Close shuts the client down deliberately; no rebuild follows.
func (c *Client) Close() {
	c.closing.Store(true)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.subs {
		s.stopLocal()
	}
	c.subs = nil
	if c.nc != nil {
		c.nc.Close()
	}
}

// conn returns the current connection pair under the read lock, so callers
// never race a rebuild.
func (c *Client) conn() (*nats.Conn, jetstream.JetStream) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nc, c.js
}

// Publish validates env, enforces the server's max message size client-side
// (assertion 8 — the alternative is a silent drop at the server), and
// publishes to subject with the envelopeId as the JetStream dedup id.
func (c *Client) Publish(ctx context.Context, subject string, env *Envelope) error {
	if err := env.ValidateEmit(); err != nil {
		return err
	}
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	nc, js := c.conn()
	if max := nc.MaxPayload(); int64(len(data)) > max {
		return &A2AError{
			Code:    CodeContentTooLarge,
			Message: fmt.Sprintf("envelope is %d bytes; bus max message size is %d", len(data), max),
		}
	}
	_, err = js.Publish(ctx, subject, data, jetstream.WithMsgID(env.EnvelopeID))
	if err != nil {
		return fmt.Errorf("publish %s: %w", subject, err)
	}
	return nil
}

// SubscribeConfig describes a durable subscription.
type SubscribeConfig struct {
	Stream  string
	Subject string
	Durable string
	// Session is this consumer's own session name; envelopes addressed to
	// another session are ignored per assertion 4.
	Session string
}

// Subscription is a live durable subscription.
type Subscription interface {
	Stop()
}

// SubscribeDurable creates or binds the durable consumer and delivers each
// envelope to handler at most once per envelopeId (assertion 5). The
// subscription survives connection rebuilds: its spec, not its JetStream
// objects, is what the client retains.
func (c *Client) SubscribeDurable(ctx context.Context, cfg SubscribeConfig, handler func(*Envelope)) (Subscription, error) {
	s := &durableSub{c: c, cfg: cfg, handler: handler, seen: newDedupSet(dedupWindow)}
	_, js := c.conn()
	if err := s.start(ctx, js); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.subs = append(c.subs, s)
	c.mu.Unlock()
	return s, nil
}

type durableSub struct {
	c       *Client
	cfg     SubscribeConfig
	handler func(*Envelope)
	seen    *dedupSet
	stopped atomic.Bool

	mu sync.Mutex
	cc jetstream.ConsumeContext
}

// start creates or updates the durable consumer on js and begins consuming.
// Called at subscribe time and again by rebuild with a fresh js; it holds no
// reference to any prior connection's objects.
func (s *durableSub) start(ctx context.Context, js jetstream.JetStream) error {
	cons, err := js.CreateOrUpdateConsumer(ctx, s.cfg.Stream, jetstream.ConsumerConfig{
		Durable:       s.cfg.Durable,
		FilterSubject: s.cfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return fmt.Errorf("consumer %s on %s: %w", s.cfg.Durable, s.cfg.Stream, err)
	}
	cc, err := cons.Consume(s.deliver)
	if err != nil {
		return fmt.Errorf("consume %s: %w", s.cfg.Durable, err)
	}
	s.mu.Lock()
	old := s.cc
	s.cc = cc
	s.mu.Unlock()
	_ = old // the old context died with its connection; never called again
	return nil
}

func (s *durableSub) deliver(msg jetstream.Msg) {
	env, err := ParseEnvelope(msg.Data())
	if err != nil {
		// Poison messages are surfaced and terminated, not redelivered forever.
		s.c.log.Error("a2a envelope rejected", "subject", msg.Subject(), "err", err)
		_ = msg.Term()
		return
	}
	// Assertion 4: a wildcard consumer ignores envelopes addressed elsewhere.
	if env.To != nil && s.cfg.Session != "" && env.To.Session != s.cfg.Session {
		_ = msg.Ack()
		return
	}
	// Assertion 5: at most once per envelopeId across redeliveries.
	if !s.seen.add(env.EnvelopeID) {
		_ = msg.Ack()
		return
	}
	s.handler(env)
	_ = msg.Ack()
}

// Stop ends the subscription and removes it from the rebuild registry.
func (s *durableSub) Stop() {
	s.stopLocal()
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	for i, sub := range s.c.subs {
		if sub == s {
			s.c.subs = append(s.c.subs[:i], s.c.subs[i+1:]...)
			break
		}
	}
}

func (s *durableSub) stopLocal() {
	s.stopped.Store(true)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cc != nil {
		s.cc.Stop()
	}
}
