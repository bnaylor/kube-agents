package mcp

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// consolePublish and consoleSubscribe are the console principal's grants as
// the operator renders them. TestTheGrantsAreTheRenderedOnes holds them to the
// rendered nats.conf, so a status read that needs more than the console has
// fails here rather than on a cluster.
var consolePublish = []string{
	"$JS.API.INFO",
	"$JS.API.STREAM.INFO.TASKS",
	"$JS.API.STREAM.INFO.DIRECTORY",
	"$JS.API.STREAM.INFO.TOPICS-STATE",
	"$JS.API.STREAM.INFO.TOPICS-JOURNAL",
	"$JS.API.CONSUMER.CREATE.TASKS.>",
	"$JS.API.CONSUMER.CREATE.DIRECTORY.>",
	"$JS.API.CONSUMER.CREATE.TOPICS-STATE.>",
	"$JS.API.CONSUMER.CREATE.TOPICS-JOURNAL.>",
	"$JS.API.CONSUMER.INFO.TASKS.*",
	"$JS.API.CONSUMER.INFO.DIRECTORY.*",
	"$JS.API.CONSUMER.INFO.TOPICS-STATE.*",
	"$JS.API.CONSUMER.INFO.TOPICS-JOURNAL.*",
	"$JS.API.CONSUMER.MSG.NEXT.TASKS.*",
	"$JS.API.CONSUMER.MSG.NEXT.DIRECTORY.*",
	"$JS.API.CONSUMER.MSG.NEXT.TOPICS-STATE.*",
	"$JS.API.CONSUMER.MSG.NEXT.TOPICS-JOURNAL.*",
	"chat.console.*.in",
	"_INBOX.console.>",
}

var consoleSubscribe = []string{"a2a.>", "chat.console.*.out", "_INBOX.console.>"}

// renderedConf is the operator's render the callout suite starts from.
const renderedConf = "../authcallout/testdata/rendered-nats.conf"

func TestTheGrantsAreTheRenderedOnes(t *testing.T) {
	raw, err := os.ReadFile(renderedConf)
	if err != nil {
		t.Fatal(err)
	}
	conf := string(raw)
	start := strings.Index(conf, "user: console")
	if start < 0 {
		t.Fatal("the rendered conf has no console user")
	}
	block := conf[start:]
	block = block[:strings.Index(block, "\n      }\n")]
	for _, g := range append(append([]string{}, consolePublish...), consoleSubscribe...) {
		if !strings.Contains(block, `"`+g+`"`) {
			t.Errorf("grant %q is not in the rendered console user; update the test's copy", g)
		}
	}
	if got, want := strings.Count(block, `"`)/2-1, len(consolePublish)+len(consoleSubscribe); got != want {
		t.Errorf("the rendered console user has %d quoted values beside its password, the test has %d grants", got, want)
	}
}

const (
	testUser = "console"
	testPass = "pw-console"
)

// testBus starts a JetStream server with the operator's message streams and
// a console user holding the rendered grants, and returns its URL and an
// admin connection for seeding.
func testBus(t *testing.T) (string, jetstream.JetStream) {
	t.Helper()
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true}
	opts.Users = []*natsserver.User{
		{Username: testUser, Password: testPass, Permissions: &natsserver.Permissions{
			Publish:   &natsserver.SubjectPermission{Allow: consolePublish},
			Subscribe: &natsserver.SubjectPermission{Allow: consoleSubscribe},
		}},
		{Username: "admin", Password: "admin"},
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server did not start")
	}
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL(), nats.UserInfo("admin", "admin"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for name, subjects := range map[string][]string{
		lib.TasksStream:         {"a2a.tasks.>"},
		streamDirectory:         {directorySubjects},
		lib.StreamTopicsState:   {"a2a.topics.state.>"},
		lib.StreamTopicsJournal: {"a2a.topics.journal.>"},
	} {
		cfg := jetstream.StreamConfig{Name: name, Subjects: subjects, MaxBytes: 1 << 20}
		if name == streamDirectory {
			cfg.MaxMsgsPerSubject = 1
		}
		if _, err := js.CreateStream(ctx, cfg); err != nil {
			t.Fatalf("stream %s: %v", name, err)
		}
	}
	return srv.ClientURL(), js
}

func publishCard(t *testing.T, js jetstream.JetStream, profile string, card map[string]any) {
	t.Helper()
	payload, _ := json.Marshal(card)
	env, err := lib.NewAgentCardEnvelope(lib.Party{Session: profile, Profile: profile}, "corr-"+profile, payload)
	if err != nil {
		t.Fatal(err)
	}
	publishEnvelope(t, js, profile, env)
}

func publishEnvelope(t *testing.T, js jetstream.JetStream, profile string, env *lib.Envelope) {
	t.Helper()
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(context.Background(), lib.AgentSubject(profile), data); err != nil {
		t.Fatal(err)
	}
}

func dial(t *testing.T, url string) *NATSBus {
	t.Helper()
	b, err := DialBus(context.Background(), BusConfig{
		URL: url, User: testUser, Password: testPass,
		Deployment: Deployment{Agent: "platform-agent", Namespace: "kubeagents-system"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	return b
}

// The status reads everything it shows through the console grants alone.
func TestStatusReadsTheBusAsTheConsole(t *testing.T) {
	url, js := testBus(t)
	publishCard(t, js, "platform", map[string]any{"name": "Platform agent", "description": "does | things\nwell"})
	publishCard(t, js, "cluster-a", map[string]any{"name": "Cluster A"})
	publishCard(t, js, "retired", map[string]any{"name": "Retired"})
	closed, err := lib.NewAgentClosedEnvelope(lib.Party{Session: "retired", Profile: "retired"}, "corr-retired")
	if err != nil {
		t.Fatal(err)
	}
	publishEnvelope(t, js, "retired", closed)
	// The gateway's durable exists; the bridge's does not, which the status
	// names rather than failing on.
	if _, err := js.CreateOrUpdateConsumer(context.Background(), lib.TasksStream, jetstream.ConsumerConfig{Durable: gatewayRelayDurable}); err != nil {
		t.Fatal(err)
	}

	st := dial(t, url).Status(context.Background())

	if !st.Connected {
		t.Fatal("status says disconnected")
	}
	if len(st.Streams) != len(statusStreams) {
		t.Errorf("streams = %+v, want all %d", st.Streams, len(statusStreams))
	}
	if len(st.Consumers) != 1 || st.Consumers[0].Name != gatewayRelayDurable {
		t.Errorf("consumers = %+v, want the gateway relay alone", st.Consumers)
	}
	if len(st.Problems) != 1 || !strings.Contains(st.Problems[0], consumerBridgePlatform) {
		t.Errorf("problems = %v, want the missing bridge consumer named", st.Problems)
	}
	if len(st.AgentCards) != 2 || st.AgentCards["platform"].text(cardName) != "Platform agent" || st.AgentCards["cluster-a"] == nil {
		t.Errorf("cards = %v, want platform and cluster-a (retired closed its card)", st.AgentCards)
	}

	md := FormatBusStatusMarkdown(st)
	for _, want := range []string{"`kubeagents-system/platform-agent`", "| `platform` | Platform agent | does \\| things well |", "consumer TASKS/" + consumerBridgePlatform} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown does not contain %q:\n%s", want, md)
		}
	}
}

func TestStatusWithNoAgentsSaysSo(t *testing.T) {
	url, _ := testBus(t)
	st := dial(t, url).Status(context.Background())
	if len(st.AgentCards) != 0 {
		t.Errorf("cards = %v, want none", st.AgentCards)
	}
	if !strings.Contains(FormatBusStatusMarkdown(st), "No agent cards on DIRECTORY") {
		t.Error("an empty directory is not stated")
	}
}

func TestDialBusRefusesAMissingCredentialOrABus(t *testing.T) {
	url, _ := testBus(t)
	for name, cfg := range map[string]BusConfig{
		"no URL":           {User: testUser, Password: testPass},
		"no password":      {URL: url, User: testUser},
		"a wrong password": {URL: url, User: testUser, Password: "nope"},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if b, err := DialBus(ctx, cfg); err == nil {
			b.Close()
			t.Errorf("DialBus accepted %s", name)
		}
		cancel()
	}
}

func TestTheStatusToolRendersTheBus(t *testing.T) {
	url, _ := testBus(t)
	s := mustServer(t, ConnectionStatusTool(dial(t, url)))
	res := s.CallTool(context.Background(), ToolConnectionStatus, nil)
	if res.IsError || !strings.Contains(res.Content[0].Text, "connected") {
		t.Errorf("result = %+v", res)
	}
}

func TestACardUnderAnInvalidProfileIsDropped(t *testing.T) {
	cards := map[string]AgentCard{}
	payload, _ := json.Marshal(map[string]any{"name": "x"})
	env, err := lib.NewAgentCardEnvelope(lib.Party{Session: "p", Profile: "p"}, "corr-p", payload)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(env)
	for _, subject := range []string{lib.AgentSubject(""), lib.AgentSubject("a.b"), lib.AgentSubject("a`b"), "a2a.tasks.x"} {
		addCard(cards, subject, data)
	}
	if len(cards) != 0 {
		t.Errorf("cards = %v, want none from invalid subjects", cards)
	}
	addCard(cards, lib.AgentSubject("good-profile"), data)
	if len(cards) != 1 {
		t.Errorf("a valid profile was not added: %v", cards)
	}
}
