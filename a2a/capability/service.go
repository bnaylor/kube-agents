package capability

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// The verification service, and the client brokers reach it with.
//
// 09 §4 makes the verifier the only component holding read across the bucket,
// which is why it is its own workload rather than a library every broker links.
// The rule it enforces that nothing else can — "the verifier authenticates its
// caller" — needs an answer to a question NATS does not answer on its own: a
// message does not carry its publisher.
//
// The answer is the subject, and it is the mechanism A3 already shipped for the
// task plane. A caller reaches the verifier at `a2a.cap.verify.<its-own-name>`,
// and the server refuses any principal publishing on a token that is not its
// own, with the permissions it attached when that principal authenticated. So
// the last token of the subject the request arrived on IS the authenticated
// caller — not a claim in the payload, which the caller writes and could write
// anything into. This is why A3b is sequential after A3 and not parallel with
// it: without subject-derived identity there is nothing here to authenticate
// against.
const (
	// VerifyPrefix is the request namespace. One token follows: the caller.
	VerifyPrefix = "a2a.cap.verify."
	// VerifySubscribe is what the verifier listens on.
	VerifySubscribe = VerifyPrefix + "*"
	// VerifyQueue lets the verifier scale horizontally without duplicating
	// answers.
	VerifyQueue = "cap-verifier"
)

// VerifySubject is where a caller asks. The caller's own name is the subject's
// last token, and the server is what makes that true.
func VerifySubject(caller string) (string, error) {
	if err := checkToken("caller", caller); err != nil {
		return "", err
	}
	return VerifyPrefix + caller, nil
}

// callerFromSubject reads the authenticated caller off the request subject.
func callerFromSubject(subject string) (string, error) {
	rest, ok := strings.CutPrefix(subject, VerifyPrefix)
	if !ok || rest == "" || strings.Contains(rest, ".") {
		return "", refuse("the request did not arrive on a caller-scoped verify subject")
	}
	return rest, nil
}

// Request is what a broker asks. It carries no identity: the subject carries
// that, and a payload field would be one the caller writes.
type Request struct {
	Ref      Ref   `json:"ref"`
	Verb     Verb  `json:"verb"`
	Resource Scope `json:"resource"`
}

// Response is a verdict and the rule behind it. It deliberately does NOT carry
// the resolved tier or scope: the broker asked whether a verb is permitted, and
// answering with the capability itself would hand the store's contents to the
// one party 09 §4 withholds them from.
type Response struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason,omitempty"`
}

// Service answers verification requests. It holds the only Store in the
// deployment.
type Service struct {
	Resolver *Resolver
	Log      *slog.Logger
}

// Answer applies the rules to one request. Split out from the subscription so
// the rules can be tested without a bus.
func (s *Service) Answer(ctx context.Context, subject string, data []byte) Response {
	caller, err := callerFromSubject(subject)
	if err != nil {
		return Response{Reason: Reason(err)}
	}
	var req Request
	if err := json.Unmarshal(data, &req); err != nil {
		return Response{Reason: "the request is not well-formed"}
	}
	// The walk and the verb check are answered differently on purpose, and
	// the split is the whole anti-oracle argument.
	//
	// Every way the WALK can fail tells the caller something about a chain
	// it has not shown any right to: "no entry at the pinned revision" says
	// the key is not there, "the entry does not name the caller as its
	// delegate" says it is. Request ids are not secrets, but a verifier that
	// answers "does this one exist" to anyone who can name one is a lookup
	// service for live requests. So every walk refusal is one sentence, and
	// the rule that actually fired goes to the verifier's own log.
	//
	// The VERB check is the other side of that line. Reaching it means the
	// walk succeeded, which means this caller is the principal the chain
	// names — it already knows the capability exists, because it holds it.
	// Telling it which verb or which scope was out of bounds gives away
	// nothing it did not bring, and it is the difference between a
	// debuggable refusal and a mystery.
	leaf, err := s.Resolver.Resolve(ctx, caller, req.Ref)
	switch {
	case err == nil:
	case errors.Is(err, ErrRefused):
		if s.Log != nil {
			s.Log.Info("capability walk refused",
				"caller", caller, "key", clip(req.Ref.Key), "revision", req.Ref.Revision,
				"rule", Reason(err))
		}
		return Response{Reason: WalkRefused}
	default:
		// Store trouble is not a denial, but it is answered as one. The
		// operator's signal is the log line; the caller's is a verdict.
		if s.Log != nil {
			s.Log.Error("capability store unavailable; failing closed", "err", err)
		}
		return Response{Reason: WalkRefused}
	}
	if err := Permits(leaf, req.Verb, req.Resource); err != nil {
		return Response{Reason: Reason(err)}
	}
	return Response{Allowed: true}
}

// WalkRefused is the single answer every chain-walk refusal gets. One string
// for the whole class, so a caller cannot tell a key that is not there from
// one that is not its own.
const WalkRefused = "the capability does not authorize this caller"

// clip bounds a caller-supplied value on its way into the verifier's log. The
// key is chosen by whoever sent the request and the log is not.
func clip(s string) string {
	const max = 96
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// Subscribe starts answering and returns as soon as the subscription is
// established on the server. Callers that want to block should use Serve;
// this exists so a caller can know the verifier is listening before it lets
// anything ask, because a request that races the subscription is answered by
// the client's timeout, and the client reads a timeout as a denial.
func (s *Service) Subscribe(ctx context.Context, nc *nats.Conn) (*nats.Subscription, error) {
	sub, err := nc.QueueSubscribe(VerifySubscribe, VerifyQueue, func(m *nats.Msg) {
		s.handle(ctx, m)
	})
	if err != nil {
		return nil, err
	}
	if err := nc.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return nil, err
	}
	return sub, nil
}

// Serve subscribes and answers until ctx is done.
func (s *Service) Serve(ctx context.Context, nc *nats.Conn) error {
	sub, err := s.Subscribe(ctx, nc)
	if err != nil {
		return err
	}
	defer func() { _ = sub.Drain() }()
	<-ctx.Done()
	return nil
}

func (s *Service) handle(ctx context.Context, m *nats.Msg) {
	caller, cerr := callerFromSubject(m.Subject)
	// The reply subject is chosen by the caller, and the verifier's publish
	// grant is broader than any one inbox. Answering wherever asked would
	// let a broker have the verifier deliver into another principal's
	// inbox. session.go documents the same shape on the JetStream path;
	// this is the one place on the capability path where it would apply, so
	// it is closed here rather than inherited.
	if cerr == nil && !strings.HasPrefix(m.Reply, "_INBOX."+caller+".") {
		if s.Log != nil {
			s.Log.Warn("verify request asked for a reply outside the caller's inbox; dropped",
				"caller", caller)
		}
		return
	}
	resp := s.Answer(ctx, m.Subject, m.Data)
	b, err := json.Marshal(resp)
	if err != nil {
		return
	}
	if err := m.Respond(b); err != nil && s.Log != nil {
		s.Log.Warn("verify reply failed", "err", err)
	}
}

// DefaultTimeout bounds a broker's wait. The verifier is on the request path,
// so a broker that waits forever turns a verifier outage into a wedged task
// rather than a refused one.
const DefaultTimeout = 5 * time.Second

// Client is the broker side. It knows its own name because the operator told
// the pod, and it cannot lie about it: it can only publish on its own subject.
type Client struct {
	nc      *nats.Conn
	self    string
	subject string
	Timeout time.Duration
}

// NewClient builds the broker's verifier client. self is the principal's own
// name — the session pod name for a session, which is also the name the
// gateway wrote into the capability's delegate field.
func NewClient(nc *nats.Conn, self string) (*Client, error) {
	subj, err := VerifySubject(self)
	if err != nil {
		return nil, err
	}
	return &Client{nc: nc, self: self, subject: subj}, nil
}

// Check asks whether the capability at ref permits verb v on resource r.
// A nil error means permitted. Everything else — refused, unreachable,
// malformed — is a denial, because the alternative is a broker that proceeds
// when it could not find out.
func (c *Client) Check(ctx context.Context, ref Ref, v Verb, r Scope) error {
	b, err := json.Marshal(Request{Ref: ref, Verb: v, Resource: r})
	if err != nil {
		return err
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// The reply inbox is spelled here rather than taken from the
	// connection's prefix. The verifier answers only into the caller's own
	// inbox, and a broker that had connected without
	// nats.CustomInboxPrefix would otherwise ask from a default inbox, be
	// dropped, and read the drop as the verifier being down — a
	// fail-closed bug, but one that would take a deployment out silently.
	reply := "_INBOX." + c.self + "." + nuid.Next()
	sub, err := c.nc.SubscribeSync(reply)
	if err != nil {
		return refuse("the verifier could not be reached")
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := c.nc.PublishRequest(c.subject, reply, b); err != nil {
		return refuse("the verifier could not be reached")
	}
	msg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return refuse("the verifier could not be reached")
	}
	var resp Response
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return refuse("the verifier's answer was not well-formed")
	}
	if !resp.Allowed {
		return &Refusal{Rule: resp.Reason}
	}
	return nil
}
