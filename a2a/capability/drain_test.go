package capability

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Shutdown, and the one thing it must not do: turn a request that was fine into
// a refusal.
//
// The verifier fails closed, which is right — a store it cannot read is not a
// reason to permit anything. But "the store is unreachable" and "this process
// is going away" are different facts, and the fail-closed branch cannot tell
// them apart. Every path into the resolver takes a context, so cancelling the
// handlers' context before draining makes the second fact arrive dressed as the
// first: each request still queued is answered `allowed: false`, and a broker
// reads that as the capability being bad and rejects a task nothing was wrong
// with. The drain exists precisely to stop that, so the drain has to outlive
// the signal.

// gateStore blocks in the read until it is released, and — unlike fakeStore —
// honours the context it is given. Both halves are the point: the block is what
// puts a request in flight at the moment of shutdown, and the context is what
// the shutdown order decides the fate of.
type gateStore struct {
	inner   Store
	entered chan struct{}
	release chan struct{}
}

func (g *gateStore) GetRevision(ctx context.Context, key string, rev uint64) ([]byte, error) {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	select {
	case <-g.release:
		return g.inner.GetRevision(ctx, key, rev)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func drainTestServer(t *testing.T) string {
	t.Helper()
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server did not come up")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// TestAnInFlightRequestIsAnsweredAcrossADrain is the shutdown order cmd/verifier
// implements, asserted end to end on a real bus: a request whose store read is
// still outstanding when shutdown begins gets its real answer.
//
// Two connections, deliberately. The verifier's is the one that drains; the
// asker's is separate, because draining the connection the reply subscription
// lives on would lose the reply for a reason that has nothing to do with the
// bug under test.
func TestAnInFlightRequestIsAnsweredAcrossADrain(t *testing.T) {
	inner, _, hop := twoHop(t)
	gate := &gateStore{inner: inner, entered: make(chan struct{}, 1), release: make(chan struct{})}

	url := drainTestServer(t)
	verifierConn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("verifier connect: %v", err)
	}
	askerConn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("asker connect: %v", err)
	}
	t.Cleanup(askerConn.Close)

	// The shutdown order under test. handlerCtx is NOT the signal context:
	// it outlives the drain and is cancelled only after it finishes.
	handlerCtx, handlerCancel := context.WithCancel(context.Background())
	defer handlerCancel()

	svc := &Service{Resolver: &Resolver{Store: gate}}
	if _, err := svc.Subscribe(handlerCtx, verifierConn); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	subj, err := VerifySubject(podB)
	if err != nil {
		t.Fatalf("subject: %v", err)
	}
	body, err := json.Marshal(Request{Ref: hop, Verb: VerbTaskExecute, Resource: "project/P/cluster/C"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	replySubj := ReplyPrefix + podB + ".drainprobe"
	replySub, err := askerConn.SubscribeSync(replySubj)
	if err != nil {
		t.Fatalf("reply subscribe: %v", err)
	}
	if err := askerConn.PublishRequest(subj, replySubj, body); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := askerConn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The request is now inside the store read. This is the moment SIGTERM
	// is the interesting one.
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never reached the store; the test never set up the race it is about")
	}

	drained := make(chan struct{})
	verifierConn.SetClosedHandler(func(*nats.Conn) { close(drained) })
	if err := verifierConn.Drain(); err != nil {
		t.Fatalf("drain: %v", err)
	}
	close(gate.release)

	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection never finished draining")
	}
	// Only now, which is the ordering the whole fix is.
	handlerCancel()

	m, err := replySub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("the in-flight request got no answer at all across the drain: %v", err)
	}
	var resp Response
	if err := json.Unmarshal(m.Data, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !resp.Allowed {
		t.Fatalf("a request that was fine came back refused because the process was shutting down: %q. "+
			"That is the broker rejecting a good task, which is the outcome draining exists to prevent.", resp.Reason)
	}
}

// TestACancelledHandlerContextRefusesWhatItShouldHaveAnswered is the negative
// control, and it is what makes the test above mean something: it runs the same
// request through the same service with the handlers' context cancelled first,
// which is what passing the signal context to Subscribe amounts to once SIGTERM
// has fired. The answer inverts. So the passing case above is the ordering
// doing work, not the request being easy.
func TestACancelledHandlerContextRefusesWhatItShouldHaveAnswered(t *testing.T) {
	inner, _, hop := twoHop(t)
	released := make(chan struct{})
	close(released)
	gate := &gateStore{inner: inner, entered: make(chan struct{}, 1), release: released}

	svc := &Service{Resolver: &Resolver{Store: gate}}
	body, err := json.Marshal(Request{Ref: hop, Verb: VerbTaskExecute, Resource: "project/P/cluster/C"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	subj, err := VerifySubject(podB)
	if err != nil {
		t.Fatalf("subject: %v", err)
	}

	// Control first: live context, same everything else.
	if resp := svc.Answer(context.Background(), subj, body); !resp.Allowed {
		t.Fatalf("baseline: the request is refused even with a live context (%q); "+
			"the inversion below would prove nothing", resp.Reason)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	resp := svc.Answer(cancelled, subj, body)
	if resp.Allowed {
		t.Fatal("expected the fail-closed branch; the store fake is not honouring its context " +
			"and this control is vacuous")
	}
	if resp.Reason != WalkRefused {
		t.Fatalf("refused, but not through the branch the hazard runs through: %q", resp.Reason)
	}
}
