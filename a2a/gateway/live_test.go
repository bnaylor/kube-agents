package gateway

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

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
