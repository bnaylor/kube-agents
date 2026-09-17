package workeradapter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// TestFetchOriginTakesASteerOnceTheCapEvictsTheSubmission pins a degradation,
// not a behaviour anyone wants.
//
// TASKS carries max_msgs_per_subject with discard=old, and that cap lands on a
// task's ...in subject as much as its ...events one. The oldest message on
// ...in is the originating kind:message, so a task that takes more inbound
// messages than the cap loses its submission first. fetchOrigin reads the
// subject from the beginning and takes the first kind:message it finds; steers
// are kind:message too and no envelope field marks the submission, so what it
// hands back after that eviction is a steer, and the worker runs against it.
// Nothing observes the swap - the events side has lib.Task.SubmittedMissing,
// this side has nothing, because nothing folds ...in.
//
// spec-nats-deployment.md carries this as an open question. Closing it means
// marking the submission or moving it off the steers' subject, and either way
// this test changes with the fix - a failure here is that fix landing, not a
// regression.
func TestFetchOriginTakesASteerOnceTheCapEvictsTheSubmission(t *testing.T) {
	js, cleanup := capTasksStream(t, 1)
	defer cleanup()

	const session, taskID = "sess", "t1"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	in := lib.TaskInSubject(session, taskID)
	publishMessage(ctx, t, js, in, "the originating request")
	publishMessage(ctx, t, js, in, "a steer sent later")

	a := &adapter{cfg: Config{Session: session, TaskID: taskID}, log: slog.Default(), js: js}
	origin, seq, err := a.fetchOrigin(ctx)
	if err != nil {
		t.Fatalf("fetchOrigin: %v", err)
	}
	if !strings.Contains(string(origin.Payload), "a steer sent later") {
		t.Fatalf("expected the surviving steer back as the origin, got seq=%d %s", seq, origin.Payload)
	}
	if strings.Contains(string(origin.Payload), "the originating request") {
		t.Fatalf("the submission should have been evicted by the cap, got %s", origin.Payload)
	}
}

// capTasksStream is the render's TASKS with max_msgs_per_subject dialled down
// to perSubject, so a handful of messages is "past the cap". Every other flag matches
// what platformagent_a2a_manifests.go creates.
func capTasksStream(t *testing.T, perSubject int64) (jetstream.JetStream, func()) {
	t.Helper()
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true,
		StoreDir: t.TempDir(), NoLog: true, NoSigs: true}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server not ready")
	}
	nc, err := nats.Connect(fmt.Sprintf("nats://%s", s.Addr().String()))
	if err != nil {
		s.Shutdown()
		t.Fatalf("connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		s.Shutdown()
		t.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: lib.TasksStream, Subjects: []string{"a2a.tasks.>"},
		Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardOld,
		MaxAge: 72 * time.Hour, MaxMsgsPerSubject: perSubject, AllowDirect: true,
	}); err != nil {
		nc.Close()
		s.Shutdown()
		t.Fatalf("create TASKS: %v", err)
	}
	return js, func() { nc.Close(); s.Shutdown() }
}

func publishMessage(ctx context.Context, t *testing.T, js jetstream.JetStream, subject, text string) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"role":      "user",
		"messageId": text,
		"parts":     []map[string]string{{"kind": "text", "text": text}},
	})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	env, err := lib.NewMessageEnvelope(
		lib.Party{Session: "requester", AgentType: "agent"}, "t1", "ctx-1", "corr-1", payload)
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := js.Publish(ctx, subject, raw); err != nil {
		t.Fatalf("publish %s: %v", subject, err)
	}
}
