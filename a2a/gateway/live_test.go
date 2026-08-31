package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// TestLiveAgainstInstallNATS runs the gateway (fake chat adapter, real bus
// client) against a real deployment's NATS — the W6 install via
// port-forward — under the REAL gateway user's deny-by-default grants, with
// a stand-in executor on the worker user. This is the half of the DoD unit
// tests cannot prove: that every JetStream interaction the gateway performs
// (durable consumer on TASKS, ordered replay for tasks/get, KV on
// session-state, acks, inbox traffic) survives the permission lists.
//
// Skipped unless the env is set:
//
//	A2A_LIVE_NATS_URL=nats://127.0.0.1:4222 \
//	A2A_LIVE_GATEWAY_PASSWORD=... A2A_LIVE_WORKER_PASSWORD=... \
//	go test ./gateway -run TestLive -v -count=1
func TestLiveAgainstInstallNATS(t *testing.T) {
	url := os.Getenv("A2A_LIVE_NATS_URL")
	gwPass := os.Getenv("A2A_LIVE_GATEWAY_PASSWORD")
	wkPass := os.Getenv("A2A_LIVE_WORKER_PASSWORD")
	if url == "" || gwPass == "" || wkPass == "" {
		t.Skip("live NATS env not set; see comment")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := lib.Connect(ctx, url,
		lib.WithName("a2a-gateway-livetest"),
		lib.WithNATSOptions(
			nats.UserInfo("gateway", gwPass),
			nats.CustomInboxPrefix("_INBOX.gateway"),
		))
	if err != nil {
		t.Fatalf("gateway connect: %v", err)
	}
	defer client.Close()

	worker, err := lib.Connect(ctx, url,
		lib.WithName("a2a-worker-livetest"),
		lib.WithNATSOptions(
			nats.UserInfo("worker", wkPass),
			nats.CustomInboxPrefix("_INBOX.worker"),
		))
	if err != nil {
		t.Fatalf("worker connect: %v", err)
	}
	defer worker.Close()

	mapFile := t.TempDir() + "/principal-map"
	if err := os.WriteFile(mapFile, []byte("1001 test:bnaylor\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	adapter := newFakeAdapter()
	cfg := &Config{
		NATSURL:          url,
		PrincipalMapPath: mapFile,
		DefaultAddressee: "platform",
		IdleTTL:          30 * time.Minute,
		AttributionSalt:  []byte("live-test-salt"),
	}
	g, err := New(Options{Client: client, Adapter: adapter, Config: cfg, Backend: "discord"})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.Run(ctx) }()
	time.Sleep(2 * time.Second) // let the durable bind before traffic

	// Beat 1 shape: a chat message becomes a task addressed to platform; the
	// executor answers over the bus; the reply relays back.
	marker := "live grants check " + time.Now().UTC().Format(time.RFC3339)
	adapter.inbox <- InboundMessage{
		Conversation: "discord:live/thread-livetest", Kind: "group",
		AuthorID: "1001", MessageID: "live-1", Text: marker,
	}

	// The stand-in executor finds the task the way W7's bridge will: from
	// the stream, under the worker user. Match on the marker so a stale task
	// from an earlier run can never satisfy this.
	var origin *lib.Envelope
	waitFor(t, "task on the real TASKS stream", func() bool {
		task, err := findLatestLiveTask(wkPass, url)
		if err != nil || task == nil {
			return false
		}
		var m lib.Message
		if json.Unmarshal(task.Payload, &m) != nil || joinTextParts(m.Parts) != marker {
			return false
		}
		origin = task
		return true
	})

	exec, err := worker.NewTaskExecution(origin, lib.Party{Session: "platform", AgentType: "livetest-executor"}, "platform")
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.PublishStatus(ctx, lib.StateSubmitted, false); err != nil {
		t.Fatalf("submitted under worker grants: %v", err)
	}
	if err := exec.PublishStatus(ctx, lib.StateWorking, false); err != nil {
		t.Fatal(err)
	}
	if err := exec.PublishArtifact(ctx, lib.Artifact{Name: lib.ArtifactProgress, Parts: []lib.Part{{Kind: "text", Text: "live step 1"}}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "rolling line edit through real grants", func() bool {
		for _, e := range adapter.editTexts() {
			if strings.Contains(e, "live step 1") {
				return true
			}
		}
		return false
	})

	// Beat 2 shape: "what is it doing" answered by replay under the gateway
	// user's grants (ordered consumer + stream msg-get on TASKS).
	adapter.inbox <- InboundMessage{
		Conversation: "discord:live/thread-livetest", Kind: "group",
		AuthorID: "1001", MessageID: "live-2", Text: "what is it doing",
	}
	waitFor(t, "status by replay", func() bool {
		for _, p := range adapter.postTexts() {
			if strings.Contains(p, "replay") && strings.Contains(p, "working") {
				return true
			}
		}
		return false
	})

	if err := exec.PublishArtifact(ctx, lib.Artifact{Name: lib.ArtifactResult, Parts: []lib.Part{{Kind: "text", Text: "live result: grants hold"}}}); err != nil {
		t.Fatal(err)
	}
	if err := exec.PublishStatus(ctx, lib.StateCompleted, true); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "result relayed", func() bool {
		for _, p := range adapter.postTexts() {
			if p == "live result: grants hold" {
				return true
			}
		}
		return false
	})
	t.Log("live DoD (bus half) held: submit under gateway grants, execute under worker grants, relay + replay under gateway grants")
}

// findLatestLiveTask fetches the newest message on the platform in subjects
// using the worker user (stream msg-get by last_by_subj; wildcards are
// legal there).
func findLatestLiveTask(workerPass, url string) (*lib.Envelope, error) {
	nc, err := nats.Connect(url, nats.UserInfo("worker", workerPass), nats.CustomInboxPrefix("_INBOX.worker"))
	if err != nil {
		return nil, err
	}
	defer nc.Close()
	msg, err := nc.Request("$JS.API.STREAM.MSG.GET.TASKS", []byte(`{"last_by_subj":"a2a.tasks.platform.*.in"}`), 5*time.Second)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Message struct {
			Data []byte `json:"data"`
		} `json:"message"`
		Error *struct {
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, nil
	}
	return lib.ParseEnvelope(resp.Message.Data)
}

// TestLiveEndToEndThroughBridge is the W3 DoD's bus path with no stand-ins:
// the gateway (fake chat adapter in Discord's place) submits to platform on
// the real install, W7's bridge drives the real platform agent, and the
// real answer relays back — plus "what is it doing" answered by replay
// while the task runs. Requires the same env as TestLiveAgainstInstallNATS
// (worker password unused here but kept for the shared gate).
func TestLiveEndToEndThroughBridge(t *testing.T) {
	url := os.Getenv("A2A_LIVE_NATS_URL")
	gwPass := os.Getenv("A2A_LIVE_GATEWAY_PASSWORD")
	if url == "" || gwPass == "" || os.Getenv("A2A_LIVE_BRIDGE") != "true" {
		t.Skip("live bridge env not set (A2A_LIVE_BRIDGE=true)")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := lib.Connect(ctx, url,
		lib.WithName("a2a-gateway-livee2e"),
		lib.WithNATSOptions(nats.UserInfo("gateway", gwPass), nats.CustomInboxPrefix("_INBOX.gateway")))
	if err != nil {
		t.Fatalf("gateway connect: %v", err)
	}
	defer client.Close()

	mapFile := t.TempDir() + "/principal-map"
	if err := os.WriteFile(mapFile, []byte("1001 test:bnaylor\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	adapter := newFakeAdapter()
	cfg := &Config{
		NATSURL:          url,
		PrincipalMapPath: mapFile,
		DefaultAddressee: "platform",
		IdleTTL:          30 * time.Minute,
		AttributionSalt:  []byte("live-test-salt"),
	}
	g, err := New(Options{Client: client, Adapter: adapter, Config: cfg, Backend: "discord"})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.Run(ctx) }()
	time.Sleep(2 * time.Second)

	conv := "discord:live/e2e-" + time.Now().UTC().Format("150405")
	adapter.inbox <- InboundMessage{
		Conversation: conv, Kind: "group", AuthorID: "1001", MessageID: "e2e-1",
		Text: "What is the upgrade readiness of the fleet? Answer from the upgrade-readiness topic.",
	}

	// Beat 2 while it runs: status by replay, never forwarded to the bridge.
	time.Sleep(8 * time.Second)
	adapter.inbox <- InboundMessage{
		Conversation: conv, Kind: "group", AuthorID: "1001", MessageID: "e2e-2",
		Text: "what is it doing",
	}
	waitFor(t, "status answered by replay", func() bool {
		for _, p := range adapter.postTexts() {
			if strings.Contains(p, "replay") {
				return true
			}
		}
		return false
	})

	// Beat 1: the real platform agent's answer, relayed back. Hermes takes
	// its time; give it the full window.
	deadline := time.Now().Add(4 * time.Minute)
	for time.Now().Before(deadline) {
		for _, p := range adapter.postTexts() {
			low := strings.ToLower(p)
			if strings.Contains(low, "readiness") && !strings.Contains(p, "replay") && !strings.HasPrefix(p, "⏳") {
				t.Logf("real answer relayed (%d chars): %.200s", len(p), p)
				return
			}
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("no real answer relayed; posts so far: %q", adapter.postTexts())
}

// TestLiveSessionCapOnInstall proves S8's DoD against the real install: with
// the cap set to 2, two concurrent Delegates spawn real worker pods and a
// third is refused with the honest message while the first two keep running
// — then both complete on the real bus, so the path being capped still works
// end to end. The gateway here is in-process (fake chat adapter in Discord's
// place, the W3 convention) with the REAL pod spawner pointed at the install
// through a pinned kube context. It never calls Run, so the deployed gateway
// keeps the shared relay durable and Discord service is undisturbed;
// completion is asserted by replay (TasksGet), which rides an ephemeral
// ordered consumer of its own.
//
//	kubectl --context "$CTX" -n kubeagents-system port-forward svc/platform-agent-a2a-nats 14222:4222 &
//	A2A_LIVE_NATS_URL=nats://127.0.0.1:14222 \
//	A2A_LIVE_GATEWAY_PASSWORD=... \
//	A2A_LIVE_KUBECONTEXT=gke_bnaylor-kagents-dev_northamerica-northeast1_a2a-next-dev \
//	go test ./gateway -run TestLiveSessionCapOnInstall -v -count=1 -timeout 15m
func TestLiveSessionCapOnInstall(t *testing.T) {
	url := os.Getenv("A2A_LIVE_NATS_URL")
	gwPass := os.Getenv("A2A_LIVE_GATEWAY_PASSWORD")
	kubeCtx := os.Getenv("A2A_LIVE_KUBECONTEXT")
	if url == "" || gwPass == "" || kubeCtx == "" {
		t.Skip("live cap env not set; see comment")
	}
	ns := envOr("A2A_LIVE_NAMESPACE", "kubeagents-system")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := lib.Connect(ctx, url,
		lib.WithName("a2a-gateway-captest"),
		lib.WithNATSOptions(
			nats.UserInfo("gateway", gwPass),
			nats.CustomInboxPrefix("_INBOX.gateway"),
		))
	if err != nil {
		t.Fatalf("gateway connect: %v", err)
	}
	defer client.Close()

	rc, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{CurrentContext: kubeCtx},
	).ClientConfig()
	if err != nil {
		t.Fatalf("kubeconfig for context %q: %v", kubeCtx, err)
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		t.Fatal(err)
	}

	mapFile := t.TempDir() + "/principal-map"
	if err := os.WriteFile(mapFile, []byte("1001 test:bnaylor\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		// The spawned pods dial the bus in-cluster; only this process rides
		// the port-forward.
		NATSURL:          envOr("A2A_LIVE_INCLUSTER_NATS_URL", "nats://platform-agent-a2a-nats."+ns+".svc:4222"),
		PrincipalMapPath: mapFile,
		DefaultAddressee: "platform",
		MaxSessions:      2,
		IdleTTL:          30 * time.Minute,
		AttributionSalt:  []byte("live-test-salt"),
		Namespace:        ns,
		WorkerImage:      envOr("A2A_LIVE_WORKER_IMAGE", "northamerica-northeast1-docker.pkg.dev/bnaylor-kagents-dev/a2a-demo/worker-next:latest"),
		NATSCredsSecret:  envOr("A2A_NATS_CREDS_SECRET", "platform-agent-a2a-nats-creds"),
	}
	adapter := newFakeAdapter()
	sp := &podSpawner{cfg: cfg, client: cs, log: slog.Default()}
	g, err := New(Options{Client: client, Adapter: adapter, Config: cfg, Backend: "discord", Spawner: sp})
	if err != nil {
		t.Fatal(err)
	}

	if live, err := sp.LiveSessions(ctx); err != nil {
		t.Fatalf("live count against the install: %v", err)
	} else if live != 0 {
		t.Skipf("install busy: %d live session pods; rerun when quiet", live)
	}

	convA, convB, convC := "discord:livecap/s8-a", "discord:livecap/s8-b", "discord:livecap/s8-c"
	turn := func(conv, id, text string) {
		t.Helper()
		g.handleInbound(InboundMessage{Conversation: conv, Kind: "group", AuthorID: "1001", MessageID: id, Text: text})
	}
	// A failed run must not leave Running workers holding the bus
	// credential; a clean one retires its task indexes. (Session records
	// themselves age out through the deployed gateway's reaper.)
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), time.Minute)
		defer ccancel()
		for _, conv := range []string{convA, convB, convC} {
			rec, err := g.reg.Get(cctx, conv)
			if err != nil || rec == nil {
				continue
			}
			if rec.ActiveTask != nil {
				_ = g.reg.DropTask(cctx, rec.ActiveTask.TaskID)
			}
			if rec.PodName != "" {
				_ = sp.Delete(cctx, rec.PodName)
			}
		}
	})

	turn(convA, "s8-live-1", "Delegate: write a haiku about pod quotas")
	turn(convB, "s8-live-2", "Delegate: write a haiku about resource limits")
	waitLive(t, "two live session pods on the install", 2*time.Minute, func() bool {
		n, err := sp.LiveSessions(ctx)
		return err == nil && n == 2
	})

	turn(convC, "s8-live-3", "Delegate: write a haiku about refusals")
	refused := false
	for _, p := range adapter.postTexts() {
		if strings.Contains(p, "2 session workers") && strings.Contains(p, "(cap 2)") {
			refused = true
		}
	}
	if !refused {
		t.Fatalf("no honest refusal at the cap; posts: %q", adapter.postTexts())
	}
	if n, err := sp.LiveSessions(ctx); err != nil || n != 2 {
		t.Fatalf("third pod appeared past the cap (n=%d, err=%v)", n, err)
	}
	// A refused turn changes nothing: conversation C never got a record in
	// the real session KV.
	if rec, err := g.reg.Get(ctx, convC); err != nil || rec != nil {
		t.Fatalf("refused conversation left a record: %+v (err=%v)", rec, err)
	}

	// The first two keep running to completion on the real bus — the path
	// being capped still works end to end (worker pod -> LiteLLM -> result).
	for _, conv := range []string{convA, convB} {
		rec, err := g.reg.Get(ctx, conv)
		if err != nil || rec == nil || rec.ActiveTask == nil {
			t.Fatalf("no active task for %s: %+v (err=%v)", conv, rec, err)
		}
		taskID, addressee := rec.ActiveTask.TaskID, rec.Addressee
		waitLive(t, conv+" completes", 5*time.Minute, func() bool {
			task, err := g.client.TasksGet(ctx, addressee, taskID)
			return err == nil && task.Final && task.State == lib.StateCompleted
		})
	}
}

// waitLive is waitFor with a live-install clock: pod pulls and model calls
// take longer than the in-memory suite's 10s budget.
func waitLive(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for %s", what)
}
