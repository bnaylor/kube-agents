package authcallout

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/gke-labs/kube-agents/a2a/capability"
	"github.com/gke-labs/kube-agents/a2a/lib"
)

// docs/architecture/09-capability-envelope.md §9's conformance tests, run
// against the operator's real render on a real server.
//
// 09 states three properties and they are not independent claims — they are
// the whole of why an unsigned capability in a shared KV bucket is safe:
//
//  1. no broker writes in another principal's namespace;
//  2. no broker READS the store, by any path;
//  3. the permissions that make (1) and (2) true are actually configured.
//
// (3) is not a formality. Every subject in the cap design is a `$KV.…` or
// `$JS.API.…` subject, and a test written against the bare key names — `root.x`
// rather than `$KV.cap.root.x` — matches no subject any KV operation touches.
// Such a test passes against a server with no capability permissions at all,
// which is exactly the failure this file exists to make impossible. So every
// assertion below is a client outcome on a wire subject, and each principal
// that is refused something is also shown reaching something else on the same
// connection.
//
// The map is the operator's rendered fixture rather than a hand-written one:
// the grants under test are the shipped grants or the test is theatre.

// capMap is the operator's render, read from the same fixture contract_test.go
// parses. The identities it carries are provision, session (narrowed) and
// verifier; gateway, worker and seed are static users in the rendered
// nats.conf and are reached with connectStatic instead.
func capMap(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("testdata/rendered-identity-map.json")
	if err != nil {
		t.Fatalf("reading the operator's rendered identity map: %v", err)
	}
	return string(b)
}

const (
	verifierSA  = "system:serviceaccount:kubeagents-system:agent-a2a-verifier"
	provisionSA = "system:serviceaccount:kubeagents-system:agent-a2a-provision"

	tokenVerifier  = "token-for-the-verifier-serviceaccount-padded-to-a-realistic-len"
	tokenProvision = "token-for-the-provision-serviceaccount-padded-to-a-realistic-l"
)

// capTokens attests the three ServiceAccounts the rendered map keys on. The
// session pods reuse the pod-bound tokens the narrowing tests already define,
// so a session's grants here are the derived ones, not a fixture.
func capTokens() map[string]Attested {
	return map[string]Attested{
		tokenVerifier:  {ServiceAccount: verifierSA},
		tokenProvision: {ServiceAccount: provisionSA},
		tokenPodA:      {ServiceAccount: sessionSA, PodName: podA, PodUID: "uid-a"},
		tokenPodB:      {ServiceAccount: sessionSA, PodName: podB, PodUID: "uid-b"},
	}
}

// capReadSubjects is every JetStream API subject that would read, copy or
// destroy the cap bucket. Written at both depths the stream name occupies and
// with and without a trailing token, because that is the shape the deny in
// platformagent_a2a_identities.go is written against and a deny that missed a
// depth would leave a working read behind.
func capReadSubjects() []string {
	s := capability.Stream
	return []string{
		"$JS.API.STREAM.INFO." + s,
		"$JS.API.DIRECT.GET." + s,
		"$JS.API.DIRECT.GET." + s + "." + capability.Subject("root.task-1"),
		"$JS.API.STREAM.MSG.GET." + s,
		"$JS.API.CONSUMER.CREATE." + s + ".spy",
		"$JS.API.CONSUMER.CREATE." + s + ".spy." + capability.SubjectPrefix + ">",
		"$JS.API.CONSUMER.MSG.NEXT." + s + ".spy",
		"$JS.API.STREAM.SNAPSHOT." + s,
		"$JS.API.STREAM.PURGE." + s,
		"$JS.API.STREAM.DELETE." + s,
	}
}

func refuseAll(subjects []string) map[string]bool {
	m := make(map[string]bool, len(subjects))
	for _, s := range subjects {
		m[s] = true
	}
	return m
}

// 09 §9 (2). No broker reads the store — not the three subjects the verifier
// holds, not a consumer, not a snapshot, and not the bucket's own subject
// space.
//
// `gateway` holds `$JS.API.>`, the whole JetStream API on every stream. That
// wildcard is a standing debt (gke-labs#1306) and this test is what stops it
// from also being the capability design's undoing: the deny subtracted from it
// is the only thing between a broker and every capability in flight, and it is
// asserted here rather than read. `worker` and `seed` were wildcards too until
// gke-labs#1316 enumerated them, and they are still asked the same question —
// an enumerated list is narrower by construction but only until someone adds a
// line to it.
func TestNoBrokerCanReadTheCapabilityStore(t *testing.T) {
	h, serverLog := startHarnessWithServerLogMap(t, capMap(t), capTokens())

	for _, user := range []string{"gateway", "worker", "seed"} {
		t.Run(user, func(t *testing.T) {
			nc, violations := connectStatic(t, h, user, "pw-"+user)
			want := refuseAll(capReadSubjects())
			if user == "seed" {
				// The one exception in this whole test, and it is asserted
				// as an allow rather than dropped, so that taking it away
				// fails here instead of in an install.
				//
				// seed PROVISIONS this bucket. `kv info cap || kv add cap`
				// is the provision script's idempotency guard, so STREAM.INFO
				// on the bucket is a grant it has to hold. What that returns
				// is stream state — a message count, a subject list, a first
				// and last sequence — and none of it is a capability. Every
				// subject that does return one (DIRECT.GET, STREAM.MSG.GET,
				// each CONSUMER verb), the copies (SNAPSHOT), the destructive
				// ones and the subscribe on the bucket's subject space all
				// stay refused below, which is what makes this a carve-out
				// and not a hole.
				want["$JS.API.STREAM.INFO."+capability.Stream] = false
			}
			checkPublish(t, nc, violations, want)
			if !subscribeRefused(t, nc, violations, capability.SubjectPrefix+">") {
				t.Errorf("%s may subscribe to %s>; it would see every capability as it is minted",
					user, capability.SubjectPrefix)
			}
			// The control for this whole subtest: the connection is
			// live and the same JetStream API works on a stream that is
			// not the cap bucket. Without this, a broken credential
			// would pass every assertion above.
			checkPublish(t, nc, violations, map[string]bool{
				"$JS.API.STREAM.INFO.TASKS": false,
			})
		})
	}

	t.Run("session", func(t *testing.T) {
		nc, violations := h.connectAs(t, podA, tokenPodA)
		checkPublish(t, nc, violations, refuseAll(capReadSubjects()))
		if !subscribeRefused(t, nc, violations, capability.SubjectPrefix+">") {
			t.Error("a session may subscribe to the cap bucket's subject space")
		}
		// The control: this session's own task subject still works, so
		// the refusals above are this pod's narrowed grants at work and
		// not a connection that can publish nothing.
		checkPublish(t, nc, violations, map[string]bool{
			lib.TaskEventsSubject(podA, "task-1"): false,
		})
	})

	if !serverLog.sawViolationFor(capability.Stream) {
		t.Error("the server logged no violation naming KV_cap; the refusals have to be visible where an operator looks")
	}
}

// 09 §9 (1). A broker writes in its own namespace and nowhere else.
//
// The gateway's grant is `$KV.cap.root.*` — one token, so one request id, and
// no reach into the hop namespace at all. A session's is nothing: there is no
// second hop in the product yet, so the write a hop would need is deliberately
// ungranted rather than granted early (see sessionGrants).
func TestACapabilityWriterCannotWriteOutsideItsOwnNamespace(t *testing.T) {
	h, _ := startHarnessWithServerLogMap(t, capMap(t), capTokens())

	gw, gwViolations := connectStatic(t, h, "gateway", renderedGatewayPassword)
	checkPublish(t, gw, gwViolations, map[string]bool{
		// Its own namespace, one token deep.
		capability.Subject("root.task-1"): false,
		// Two tokens: `*` is one token, so a request id with a dot in it
		// is refused rather than silently landing somewhere else.
		capability.Subject("root.task.1"): true,
		// The hop namespace, which belongs to whoever is attenuating.
		capability.Subject("hop." + podA + ".0"): true,
		capability.Subject("hop.gateway.0"):      true,
		// Nothing above the namespace.
		"$KV.cap.anything": true,
	})

	session, sessionViolations := h.connectAs(t, podA, tokenPodA)
	checkPublish(t, session, sessionViolations, map[string]bool{
		// A session cannot mint a root, which would be a task
		// authorizing itself.
		capability.Subject("root.task-1"): true,
		// Nor a hop, its own included: the rules for attenuation exist
		// and are tested, but no shipped component performs one, so the
		// grant lands with the caller rather than ahead of it.
		capability.Subject("hop." + podA + ".0"): true,
		capability.Subject("hop." + podB + ".0"): true,
	})

	// The worker credential is the one a compromised agent-side sidecar
	// holds. It writes nothing in this namespace either.
	worker, workerViolations := connectStatic(t, h, "worker", "pw-worker")
	checkPublish(t, worker, workerViolations, map[string]bool{
		capability.Subject("root.task-1"):        true,
		capability.Subject("hop." + podA + ".0"): true,
	})
}

// 09 §9 (3), and the DoD's live half in unit-test form: the real verifier,
// under the grants the operator renders for it, answering a real session's
// real client over the shipped permission set.
//
// This is the test the other two are meaningless without. They assert that a
// long list of subjects is refused; refusing every subject on the bus would
// satisfy them. What this one asserts is that after all that refusing the
// mechanism still works end to end — the gateway mints, the verifier reads,
// the session asks and is answered, and the answer is right.
func TestTheShippedGrantsLetTheVerifierWorkAndNobodyElse(t *testing.T) {
	h, _ := startHarnessWithServerLogMap(t, capMap(t), capTokens())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The bucket, created by the principal that creates it in the
	// deployment. The provision Job's grants include STREAM.CREATE.KV_cap
	// and nothing that reads an entry, and this is where that is checked.
	provision, _ := h.connectAs(t, "provision", tokenProvision)
	pjs, err := jetstream.New(provision)
	if err != nil {
		t.Fatalf("jetstream as provision: %v", err)
	}
	if _, err := pjs.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: capability.Bucket, History: 1,
	}); err != nil {
		t.Fatalf("the provision principal could not create the cap bucket: %v", err)
	}

	startCapabilityVerifier(t, ctx, h)

	// The gateway mints, as the gateway: a static nats.conf principal with
	// exactly one grant on this path.
	gw, _ := connectStatic(t, h, "gateway", renderedGatewayPassword)
	gjs, err := jetstream.New(gw)
	if err != nil {
		t.Fatalf("jetstream as gateway: %v", err)
	}
	ref, err := capability.NewMinter(gjs).Mint(ctx, "task-1", capability.Entry{
		Tier:     capability.TierDeveloperTeam,
		Scope:    capability.NamespaceScope("kubeagents-system"),
		Delegate: podA,
	})
	if err != nil {
		t.Fatalf("the gateway could not mint under its rendered grant: %v", err)
	}

	// And the session asks, over its own derived verify subject.
	session, _ := h.connectAs(t, podA, tokenPodA)
	client, err := capability.NewClient(session, podA)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.Check(ctx, ref, capability.VerbTaskExecute,
		capability.NamespaceScope("kubeagents-system")); err != nil {
		t.Fatalf("the session's own capability was refused over the shipped grants: %v", err)
	}

	// A verb the capability does not carry, refused by the verifier rather
	// than by the bus — which is the distinction that matters: the session
	// reached the verifier and got an answer.
	if err := client.Check(ctx, ref, capability.VerbFleetRead,
		capability.NamespaceScope("kubeagents-system")); err == nil {
		t.Error("a developer-team capability authorized a platform verb")
	}

	// The other session's client, holding a reference it could only have
	// obtained by reading somebody else's envelope. It reaches the verifier
	// — it has its own verify subject — and the verifier refuses it,
	// because the entry names podA and the subject says podB.
	other, _ := h.connectAs(t, podB, tokenPodB)
	otherClient, err := capability.NewClient(other, podB)
	if err != nil {
		t.Fatalf("NewClient(podB): %v", err)
	}
	err = otherClient.Check(ctx, ref, capability.VerbTaskExecute,
		capability.NamespaceScope("kubeagents-system"))
	if err == nil {
		t.Fatal("podB used a capability minted for podA")
	}
	if !strings.Contains(err.Error(), capability.WalkRefused) {
		t.Errorf("podB's refusal names something other than the walk: %v", err)
	}

	// And podB cannot ask in podA's name, which is what makes the
	// verifier's subject-derived caller identity sound rather than
	// decorative.
	forged, err := capability.NewClient(other, podA)
	if err != nil {
		t.Fatalf("NewClient(podB-as-podA): %v", err)
	}
	short, shortCancel := context.WithTimeout(ctx, 2*time.Second)
	defer shortCancel()
	if err := forged.Check(short, ref, capability.VerbTaskExecute,
		capability.NamespaceScope("kubeagents-system")); err == nil {
		t.Fatal("podB asked the verifier a question in podA's name and was answered")
	}
}

// startHarnessWithServerLogMap is startHarnessWithServerLog with the map and
// tokens chosen by the caller. The original hard-codes the narrowing suite's
// pair; this file needs the operator's render.
func startHarnessWithServerLogMap(t *testing.T, identityMap string, tokens map[string]Attested) (*harness, *violationLog) {
	t.Helper()
	h := startHarness(t, identityMap, tokens)
	vl := &violationLog{lines: make(chan string, 256)}
	h.server.SetLoggerV2(vl, false, false, false)
	return h, vl
}

// startCapabilityVerifier runs a real verifier on the harness, under the
// verifier identity the operator renders, and returns once it is listening.
//
// Two things are load-bearing about doing it this way rather than with a stub.
// The bind is the first: NewStore is a `$JS.API.STREAM.INFO.KV_cap` call, so a
// grant list that is missing it fails here, in the same place and for the same
// reason the Deployment would fail on a cluster. And the subscribe is the
// second: the verifier subscribes `a2a.cap.verify.*`, which is a grant no other
// principal on this bus holds, so a caller reaching it at all is evidence that
// the derived session grants and the rendered verifier grants meet.
//
// Subscribe rather than Serve: it returns after the subscription is
// established on the server, and a request that races the subscription is
// answered by the client's timeout, which every broker reads as a denial.
func startCapabilityVerifier(t *testing.T, ctx context.Context, h *harness) {
	t.Helper()
	nc, _ := h.connectAs(t, "verifier", tokenVerifier)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream as verifier: %v", err)
	}
	store, err := capability.NewStore(ctx, js)
	if err != nil {
		t.Fatalf("the verifier could not bind the cap bucket under its rendered grants: %v", err)
	}
	svc := &capability.Service{Resolver: &capability.Resolver{Store: store}}
	sub, err := svc.Subscribe(ctx, nc)
	if err != nil {
		t.Fatalf("the verifier could not subscribe under its rendered grants: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
}
