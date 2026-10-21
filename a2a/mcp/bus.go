package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

const (
	// streamDirectory is the agent-card stream the operator provisions (its
	// subjects are lib.AgentSubject's). lib names the others.
	streamDirectory = "DIRECTORY"
	// directorySubjects is every agent's card subject.
	directorySubjects = "a2a.agents.>"

	// consumerBridgePlatform is the platform bridge's durable on TASKS
	// (a2a/hermes-bridge). The gateway's is gatewayRelayDurable.
	consumerBridgePlatform = "bridge-platform"
	gatewayRelayDurable    = "gateway-relay"

	// directoryReadTimeout bounds one status call's read of the agent cards.
	// The stream holds one message per agent, so it is a short read or a
	// stalled bus, and the status reports either.
	directoryReadTimeout = 3 * time.Second
	directoryFetchBatch  = 256
)

// statusStreams are the streams the console principal may read the info of,
// in the order the status shows them.
var statusStreams = []string{lib.TasksStream, streamDirectory, lib.StreamTopicsState, lib.StreamTopicsJournal}

// statusConsumers are the durables whose liveness says whether the install is
// answering: the gateway's relay and the platform bridge, both on TASKS.
var statusConsumers = []string{gatewayRelayDurable, consumerBridgePlatform}

// Deployment names the one install this server is a client of. It is fixed at
// start: a caller cannot point the server at another bus.
type Deployment struct {
	Agent     string `json:"agent"`
	Namespace string `json:"namespace"`
	Cluster   string `json:"cluster,omitempty"`
}

// BusConfig is how the server reaches the bus. The TLS trust comes from the
// environment lib reads (NATS_CA_FILE), as for every bus client.
type BusConfig struct {
	URL        string
	User       string
	Password   string
	Deployment Deployment
	Logger     *slog.Logger
}

// Bus is what the status tool needs from the bus, so the tool can be tested
// without one.
type Bus interface {
	Status(ctx context.Context) BusStatus
}

// NATSBus is the server's one bus connection, as the console principal.
type NATSBus struct {
	client *lib.Client
	cfg    BusConfig
}

// DialBus connects to the bus. It fails if the first connect does, so a
// misconfigured pod crash-loops visibly instead of serving a status that only
// ever says "disconnected"; after that, lib reconnects on its own.
func DialBus(ctx context.Context, cfg BusConfig) (*NATSBus, error) {
	if cfg.URL == "" || cfg.User == "" || cfg.Password == "" {
		return nil, errors.New("the bus needs a URL, a user and a password")
	}
	opts := []lib.ClientOption{lib.WithUserPassword(cfg.User, cfg.Password)}
	if cfg.Logger != nil {
		opts = append(opts, lib.WithLogger(cfg.Logger))
	}
	client, err := lib.Connect(ctx, cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to the bus at %s as %s: %w", cfg.URL, cfg.User, err)
	}
	return &NATSBus{client: client, cfg: cfg}, nil
}

// Close drops the connection.
func (b *NATSBus) Close() { b.client.Close() }

// Status reads the bus's state now. Every part is read fresh and every part
// that fails is named in Problems rather than failing the whole report, so a
// half-working bus still tells the caller which half.
func (b *NATSBus) Status(ctx context.Context) BusStatus {
	st := BusStatus{
		Deployment: b.cfg.Deployment,
		URL:        b.cfg.URL,
		User:       b.cfg.User,
		Connected:  b.client.Conn().IsConnected(),
	}
	if !st.Connected {
		st.Problems = append(st.Problems, "not connected to the bus; the client is reconnecting")
		return st
	}
	if cs, err := b.client.Conn().TLSConnectionState(); err == nil {
		st.TLSVersion = tlsVersionName(cs.Version)
	}
	js := b.client.JetStream()
	for _, name := range statusStreams {
		s, err := js.Stream(ctx, name)
		if err != nil {
			st.Problems = append(st.Problems, fmt.Sprintf("stream %s: %v", name, err))
			continue
		}
		info := s.CachedInfo()
		st.Streams = append(st.Streams, StreamHealth{
			Name:      name,
			Messages:  info.State.Msgs,
			Bytes:     info.State.Bytes,
			MaxBytes:  info.Config.MaxBytes,
			Consumers: info.State.Consumers,
		})
		if name != lib.TasksStream {
			continue
		}
		for _, cname := range statusConsumers {
			c, err := s.Consumer(ctx, cname)
			if err != nil {
				st.Problems = append(st.Problems, fmt.Sprintf("consumer %s/%s: %v", name, cname, err))
				continue
			}
			ci := c.CachedInfo()
			st.Consumers = append(st.Consumers, ConsumerHealth{
				Stream:        name,
				Name:          cname,
				NumPending:    ci.NumPending,
				NumAckPending: ci.NumAckPending,
				Bound:         ci.PushBound || ci.NumWaiting > 0,
			})
		}
	}
	cards, err := b.agentCards(ctx)
	if err != nil {
		st.Problems = append(st.Problems, fmt.Sprintf("agent cards: %v", err))
	}
	st.AgentCards = cards
	return st
}

// agentCards reads the latest card of every agent off DIRECTORY with an
// ordered consumer, which the console grant allows and which the server
// removes itself when it goes idle. A closed card removes the agent.
func (b *NATSBus) agentCards(ctx context.Context) (map[string]AgentCard, error) {
	ctx, cancel := context.WithTimeout(ctx, directoryReadTimeout)
	defer cancel()
	js := b.client.JetStream()
	s, err := js.Stream(ctx, streamDirectory)
	if err != nil {
		return nil, err
	}
	pending := s.CachedInfo().State.Msgs
	cards := map[string]AgentCard{}
	if pending == 0 {
		return cards, nil
	}
	cons, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{directorySubjects},
		DeliverPolicy:  jetstream.DeliverLastPerSubjectPolicy,
	})
	if err != nil {
		return nil, err
	}
	for {
		batch, err := cons.Fetch(directoryFetchBatch, jetstream.FetchMaxWait(directoryReadTimeout))
		if err != nil {
			return cards, err
		}
		n := 0
		for msg := range batch.Messages() {
			n++
			addCard(cards, msg.Subject(), msg.Data())
			if md, err := msg.Metadata(); err == nil && md.NumPending == 0 {
				return cards, nil
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
			return cards, err
		}
		if n == 0 {
			return cards, nil
		}
	}
}

// addCard folds one DIRECTORY message into cards.
func addCard(cards map[string]AgentCard, subject string, data []byte) {
	// The profile is shown in a code span, so only a valid subject token,
	// which cannot carry a backtick or a space, is accepted.
	profile, ok := strings.CutPrefix(subject, lib.AgentSubject(""))
	if !ok || !lib.ValidSubjectToken(profile) {
		return
	}
	env, err := lib.ParseEnvelope(data)
	if err != nil {
		return
	}
	if env.Kind == lib.KindAgentClosed {
		delete(cards, profile)
		return
	}
	var card AgentCard
	if json.Unmarshal(env.Payload, &card) != nil {
		return
	}
	cards[profile] = card
}
