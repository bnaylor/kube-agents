package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// gatewayParty is the gateway's own identity in from — routing and display
// only, never an authorization input. Its supervisor events carry it so
// replay always distinguishes "the worker said failed" from "the supervisor
// declared it dead".
var gatewayParty = lib.Party{Session: "gateway", AgentType: "a2a-gateway"}

// Gateway wires the adapter, the session manager, and the bus client.
type Gateway struct {
	cfg     *Config
	client  *lib.Client
	reg     *Registry
	adapter Adapter
	pm      *PrincipalMap
	ps      *Pseudonymizer
	log     *slog.Logger
	spawner spawner // nil until SpawnSessions arms (W4)

	// runCtx is Run's context; queue workers derive their timeouts from it.
	runCtx context.Context

	// inbox orders inbound messages per conversation (the backend delivers
	// events on unordered goroutines) and events orders relay work per
	// session, so no conversation can block another.
	inbox  *keyedQueue[InboundMessage]
	events *keyedQueue[*lib.Envelope]

	mu sync.Mutex
	// sessionLocks serializes work per conversation; tasks serialize per
	// session by construction (a message during a running task is a steer,
	// never a second task).
	sessionLocks map[string]*sync.Mutex
	// taskSessions caches taskId -> session key; the KV task index is the
	// durable copy a restart falls back to. Entries retire with the task.
	taskSessions map[string]string
	// relays holds per-task render state for the rolling progress line.
	relays map[string]*relayState

	// backend names the chat backend for authority blocks.
	backend string
}

// Options are the injectable pieces; tests provide fakes.
type Options struct {
	Client  *lib.Client
	Adapter Adapter
	Config  *Config
	Logger  *slog.Logger
	Backend string
}

// New assembles a gateway.
func New(o Options) (*Gateway, error) {
	if o.Client == nil || o.Adapter == nil || o.Config == nil {
		return nil, fmt.Errorf("gateway needs a client, an adapter, and a config")
	}
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	pm, err := LoadPrincipalMap(o.Config.PrincipalMapPath)
	if err != nil {
		return nil, err
	}
	if pm.Len() == 0 {
		log.Warn("principal map is empty; every inbound message will be dropped at verification",
			"path", o.Config.PrincipalMapPath)
	}
	backend := o.Backend
	if backend == "" {
		backend = "discord"
	}
	g := &Gateway{
		cfg:          o.Config,
		client:       o.Client,
		reg:          NewRegistry(o.Client),
		adapter:      o.Adapter,
		pm:           pm,
		ps:           NewPseudonymizer(o.Config.AttributionSalt),
		log:          log,
		runCtx:       context.Background(),
		sessionLocks: map[string]*sync.Mutex{},
		taskSessions: map[string]string{},
		relays:       map[string]*relayState{},
		backend:      backend,
	}
	g.inbox = newKeyedQueue(func(_ string, batch []InboundMessage) {
		for _, msg := range batch {
			g.handleInbound(msg)
		}
	})
	g.events = newKeyedQueue(g.relayBatch)
	if o.Config.SpawnSessions {
		s, err := newPodSpawner(o.Config, log)
		if err != nil {
			return nil, fmt.Errorf("session-pod spawning is enabled but the k8s client failed: %w", err)
		}
		g.spawner = s
	}
	if o.Config.DefaultAddressee == RouteSession && g.spawner == nil {
		return nil, fmt.Errorf("A2A_DEFAULT_ADDRESSEE=%s requires A2A_SPAWN_SESSIONS=true: without a spawner the sentinel would publish tasks to a literal %q addressee no executor owns", RouteSession, RouteSession)
	}
	return g, nil
}

// Run subscribes the event relay, starts the reap and sweep loops, and runs
// the adapter until ctx is done.
func (g *Gateway) Run(ctx context.Context) error {
	g.runCtx = ctx
	sub, err := g.client.SubscribeDurable(ctx, lib.SubscribeConfig{
		Stream:  lib.TasksStream,
		Subject: "a2a.tasks.*.*.events",
		Durable: "gateway-relay",
		Session: gatewayParty.Session,
	}, func(env *lib.Envelope) { g.relayEvent(ctx, env) })
	if err != nil {
		return fmt.Errorf("event relay subscription: %w", err)
	}
	defer sub.Stop()

	go g.reapLoop(ctx)
	if g.spawner != nil {
		go g.sweepLoop(ctx)
	}

	// The adapter's delivery goroutines only enqueue; per-conversation order
	// is the queue's job, not the backend's.
	return g.adapter.Run(ctx, func(msg InboundMessage) { g.inbox.enqueue(msg.Conversation, msg) })
}

// lockSession returns the per-conversation mutex, minting it on first use.
func (g *Gateway) lockSession(key string) *sync.Mutex {
	g.mu.Lock()
	defer g.mu.Unlock()
	l, ok := g.sessionLocks[key]
	if !ok {
		l = &sync.Mutex{}
		g.sessionLocks[key] = l
	}
	return l
}

// handleInbound is one user turn: verify the sender, resolve the session,
// and route the message — status query by replay, stop, steer, or a new
// task. Runs on the conversation's inbox worker, in arrival order.
func (g *Gateway) handleInbound(msg InboundMessage) {
	// Verify against the backend's identity mechanism — for Discord, the
	// test mapping table — and drop the message if we can't (gateway design,
	// turns-and-tasks step 1).
	principal := g.pm.Resolve(msg.AuthorID)
	if principal == "" {
		g.log.Warn("dropping message from unmapped sender",
			"backend", g.backend, "author", msg.AuthorID, "conversation", msg.Conversation)
		return
	}

	l := g.lockSession(msg.Conversation)
	l.Lock()
	defer l.Unlock()

	ctx, cancel := context.WithTimeout(g.runCtx, 60*time.Second)
	defer cancel()

	rec, err := g.reg.Get(ctx, msg.Conversation)
	if err != nil {
		g.log.Error("session lookup failed", "conversation", msg.Conversation, "err", err)
		return
	}
	if rec == nil {
		// First contact: contextId is minted here and never changes — the
		// durable name of the conversation on the bus, across every pod
		// incarnation.
		rec = &SessionRecord{
			Key:       msg.Conversation,
			ContextID: "ctx-" + randHex(12),
			Addressee: g.cfg.DefaultAddressee,
			Kind:      msg.Kind,
		}
		// The W4 switch: with spawning armed and the route set to "session",
		// the conversation gets its own executor. The bus session name is
		// minted per incarnation (spawn time), not here — reaping and
		// respawning changes the pod and the bus session name; contextId
		// persists (gateway design).
		if g.spawner != nil && rec.Addressee == RouteSession {
			rec.SessionRouted = true
			rec.Profile = "chat"
		}
	}
	rec.LastActivity = time.Now().UTC()

	rosterIDs, rosterComplete, err := g.adapter.Roster(msg.Conversation)
	if err != nil {
		g.log.Warn("roster read failed; snapshotting requester only",
			"conversation", msg.Conversation, "err", err)
		rosterIDs, rosterComplete = nil, false
	}
	// The audience snapshot always contains at least the requester — an
	// empty roster would erase exactly the person the classifier's "who
	// could have read this" starts from.
	if !slices.Contains(rosterIDs, msg.AuthorID) {
		rosterIDs = append(rosterIDs, msg.AuthorID)
	}
	authority := BuildAuthority(g.ps, g.pm, principal, g.backend, msg.AuthorID,
		"principal-map", msg.Conversation, rec.Kind, rosterIDs, rosterComplete)
	rec.Roster = hashRoster(g.ps, g.pm, rosterIDs)

	// Heal a stale ActiveTask before routing: if the task is already
	// terminal on the stream (the relay's ack raced a transient failure, or
	// the gateway was down when the terminal fired and the redelivery
	// hasn't landed), release the serialization instead of steering the
	// user into a finished task. Only the serialization: the task index
	// stays until the relay retires it, so a queued terminal event still
	// posts its result.
	if active := rec.ActiveTask; active != nil && !active.Detached {
		if task, err := g.client.TasksGet(ctx, rec.Addressee, active.TaskID); err == nil && task.Final {
			g.log.Info("healing stale active task", "taskId", active.TaskID, "state", task.State)
			rec.ActiveTask = nil
		}
	}

	active := rec.ActiveTask
	switch {
	case active != nil && isStatusQuery(msg.Text):
		g.answerStatusByReplay(ctx, rec)
	case active != nil && !active.Detached && isStop(msg.Text):
		g.cancelTask(ctx, rec, authority)
	case active != nil && !active.Detached:
		g.steerTask(ctx, rec, msg, authority)
	default:
		// A session-routed conversation with no live incarnation gets a
		// fresh bus session name before the task's addressee is chosen.
		if rec.SessionRouted && rec.PodName == "" {
			rec.BusSession = mintSessionName(rec.Profile)
			rec.Addressee = rec.BusSession
		}
		g.startTask(ctx, rec, msg, principal, authority)
	}

	if err := withRetry(3, func() error { return g.reg.Put(ctx, rec) }); err != nil {
		g.log.Error("session record write failed", "conversation", rec.Key, "err", err)
	}
}

// startTask mints the identifiers, publishes the submission, and posts the
// placeholder the relay will edit.
func (g *Gateway) startTask(ctx context.Context, rec *SessionRecord, msg InboundMessage, principal string, authority []byte) {
	taskID := "task-" + randHex(8)
	// correlationId is minted here and nowhere else — the originating user
	// interaction (payload spec field rule).
	correlationID := "corr-" + randHex(12)

	// The ingress log is the plaintext join: backend message id against
	// correlationId, so the audit chain runs chat message -> correlationId ->
	// every hop -> change. Plaintext stays local; the bus gets pseudonyms.
	g.log.Info("ingress",
		"correlationId", correlationID,
		"taskId", taskID,
		"backendMessageId", msg.MessageID,
		"principal", principal,
		"conversation", msg.Conversation,
		"addressee", rec.Addressee)

	payload, err := messagePayload(msg.Text, taskID, rec.ContextID)
	if err != nil {
		g.log.Error("message payload build failed", "err", err)
		return
	}
	env, err := lib.NewMessageEnvelope(gatewayParty, taskID, rec.ContextID, correlationID, payload,
		lib.WithTo(lib.Party{Session: rec.Addressee}),
		lib.WithAuthority(authority))
	if err != nil {
		g.log.Error("envelope build failed", "err", err)
		return
	}

	// Placeholder first, so the rolling line exists before the first event
	// can arrive (the demo posts one while the pod cold-starts; same idea).
	statusMsgID, err := g.adapter.Post(rec.Key, "⏳ submitted…")
	if err != nil {
		g.log.Error("placeholder post failed", "conversation", rec.Key, "err", err)
	}

	// Register the task everywhere the relay looks BEFORE publishing: a fast
	// executor's submitted event must never race the mapping, because the
	// relay acks what it cannot route and the durable won't redeliver it.
	rec.ActiveTask = &ActiveTask{TaskID: taskID, CorrelationID: correlationID, StatusMsgID: statusMsgID,
		Ask: truncateRunes(msg.Text, askCap), SubmittedAt: time.Now()}
	rec.Tasks = append(rec.Tasks, TaskRef{ID: taskID, Addressee: rec.Addressee})
	if len(rec.Tasks) > taskHistoryCap {
		rec.Tasks = rec.Tasks[len(rec.Tasks)-taskHistoryCap:]
	}
	g.mu.Lock()
	g.taskSessions[taskID] = rec.Key
	g.relays[taskID] = &relayState{}
	g.mu.Unlock()
	if err := g.reg.IndexTask(ctx, taskID, rec.Key); err != nil {
		g.log.Error("task index write failed", "taskId", taskID, "err", err)
	}
	if err := g.reg.Put(ctx, rec); err != nil {
		g.log.Error("session record write failed", "conversation", rec.Key, "err", err)
	}

	if err := g.client.Publish(ctx, lib.TaskInSubject(rec.Addressee, taskID), env); err != nil {
		g.log.Error("task publish failed", "taskId", taskID, "err", err)
		if statusMsgID != "" {
			_ = g.adapter.Edit(rec.Key, statusMsgID, "❌ could not reach the bus; try again")
		}
		rec.ActiveTask = nil
		g.mu.Lock()
		delete(g.taskSessions, taskID)
		delete(g.relays, taskID)
		g.mu.Unlock()
		return
	}

	// Session-addressed routes get an incarnation; fixed addressees (the
	// Hermes-first "platform") have their own executor and spawn nothing.
	if g.spawner != nil && rec.SessionRouted {
		g.ensureSessionPod(ctx, rec, taskID)
	}
}

// steerTask forwards a message that arrived while the task runs as a
// follow-up on the same taskId — injected, absorbed at the executor's next
// turn boundary (decided 8/24). It reuses the task's correlationId; the
// steer is attributed by its own envelope and authority block.
func (g *Gateway) steerTask(ctx context.Context, rec *SessionRecord, msg InboundMessage, authority []byte) {
	active := rec.ActiveTask
	payload, err := messagePayload(msg.Text, active.TaskID, rec.ContextID)
	if err != nil {
		g.log.Error("steer payload build failed", "err", err)
		return
	}
	env, err := lib.NewMessageEnvelope(gatewayParty, active.TaskID, rec.ContextID, active.CorrelationID, payload,
		lib.WithTo(lib.Party{Session: rec.Addressee}),
		lib.WithAuthority(authority))
	if err != nil {
		g.log.Error("steer envelope build failed", "err", err)
		return
	}
	if err := g.client.Publish(ctx, lib.TaskInSubject(rec.Addressee, active.TaskID), env); err != nil {
		g.log.Error("steer publish failed", "taskId", active.TaskID, "err", err)
	}
}

// cancelTask publishes kind:cancel — the hard interrupt — and detaches the
// session. Detaching matters in the Hermes-first world: platform tasks have
// no janitor yet (the dispatcher arrives at stage 3), so a dead executor
// would otherwise wedge the conversation forever. The gateway never forges a
// terminal event for a task it doesn't supervise; it just stops letting that
// task serialize new ones.
func (g *Gateway) cancelTask(ctx context.Context, rec *SessionRecord, authority []byte) {
	active := rec.ActiveTask
	env, err := lib.NewCancelEnvelope(gatewayParty, active.TaskID, rec.ContextID, active.CorrelationID,
		lib.WithTo(lib.Party{Session: rec.Addressee}),
		lib.WithAuthority(authority))
	if err != nil {
		g.log.Error("cancel envelope build failed", "err", err)
		return
	}
	if err := g.client.Publish(ctx, lib.TaskInSubject(rec.Addressee, active.TaskID), env); err != nil {
		g.log.Error("cancel publish failed", "taskId", active.TaskID, "err", err)
		return
	}
	active.Detached = true
	g.post(rec.Key, "🛑 cancel sent — the task ends when the executor confirms")
}

// answerStatusByReplay answers "what is it doing" from the stream, not from
// a live connection — tasks/get materialized by replay is the durability
// payoff, and beat 2 of something-working.
func (g *Gateway) answerStatusByReplay(ctx context.Context, rec *SessionRecord) {
	active := rec.ActiveTask
	task, err := g.client.TasksGet(ctx, rec.Addressee, active.TaskID)
	if err != nil {
		if _, ok := err.(*lib.A2AError); ok {
			g.post(rec.Key, "📭 no events on the stream yet — the task was just submitted")
			return
		}
		g.log.Error("status replay failed", "taskId", active.TaskID, "err", err)
		g.post(rec.Key, "⚠️ replay failed; see gateway logs")
		return
	}
	g.post(rec.Key, formatTaskStatus(task, active.Ask, active.SubmittedAt))
}

// messagePayload builds the A2A Message for one chat turn.
func messagePayload(text, taskID, contextID string) ([]byte, error) {
	return marshalMessage(lib.Message{
		Role:      "user",
		Parts:     []lib.Part{{Kind: "text", Text: text}},
		MessageID: "msg-" + randHex(8),
		TaskID:    taskID,
		ContextID: contextID,
	})
}

func hashRoster(ps *Pseudonymizer, pm *PrincipalMap, ids []string) []string {
	out := make([]string, 0, min(len(ids), rosterCap))
	for _, id := range ids {
		if len(out) >= rosterCap {
			break
		}
		entry := id
		if p := pm.Resolve(id); p != "" {
			entry = p
		}
		out = append(out, ps.Hash(entry))
	}
	return out
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err)) // process entropy failure; nothing sane to do
	}
	return hex.EncodeToString(b)
}
