package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
)

// fakeSlackAPI fakes the six Web API calls the adapter makes; tests assert
// on what was posted/updated and canned replies drive the root check.
type fakeSlackAPI struct {
	replies  map[string][]slack.Message // channel+"/"+threadTS -> msgs, root first
	posted   []struct{ channel, thread, text string }
	updated  []struct{ channel, ts, text string }
	members  []string
	cursor   string
	openedIM string
	// repliesCalls counts conversations.replies reads. That call is the one
	// synchronous Web API round trip made on the event pump's own goroutine,
	// so tests assert on how often it happens, not only on its answer.
	repliesCalls int
}

func (f *fakeSlackAPI) AuthTestContext(context.Context) (*slack.AuthTestResponse, error) {
	return &slack.AuthTestResponse{UserID: "UBOT", User: "kage"}, nil
}

func (f *fakeSlackAPI) PostMessage(channelID string, options ...slack.MsgOption) (string, string, error) {
	_, values, err := slack.UnsafeApplyMsgOptions("tok", channelID, "https://slack.example/api/", options...)
	if err != nil {
		return "", "", err
	}
	f.posted = append(f.posted, struct{ channel, thread, text string }{
		values.Get("channel"), values.Get("thread_ts"), values.Get("text"),
	})
	return channelID, "999.001", nil
}

func (f *fakeSlackAPI) UpdateMessage(channelID, timestamp string, options ...slack.MsgOption) (string, string, string, error) {
	_, values, err := slack.UnsafeApplyMsgOptions("tok", channelID, "https://slack.example/api/", options...)
	if err != nil {
		return "", "", "", err
	}
	f.updated = append(f.updated, struct{ channel, ts, text string }{
		values.Get("channel"), timestamp, values.Get("text"),
	})
	return channelID, timestamp, "", nil
}

func (f *fakeSlackAPI) GetUsersInConversation(params *slack.GetUsersInConversationParameters) ([]string, string, error) {
	return f.members, f.cursor, nil
}

func (f *fakeSlackAPI) OpenConversation(params *slack.OpenConversationParameters) (*slack.Channel, bool, bool, error) {
	ch := &slack.Channel{}
	ch.ID = f.openedIM
	return ch, false, false, nil
}

// GetConversationRepliesContext honours ctx the way the real client does —
// a cancelled or expired context comes back as ctx.Err() and no messages —
// so tests can drive the shutdown and timeout paths of isSessionThread.
func (f *fakeSlackAPI) GetConversationRepliesContext(ctx context.Context, params *slack.GetConversationRepliesParameters) ([]slack.Message, bool, string, error) {
	f.repliesCalls++
	if err := ctx.Err(); err != nil {
		return nil, false, "", err
	}
	return f.replies[params.ChannelID+"/"+params.Timestamp], false, "", nil
}

func newTestSlackAdapter(api *fakeSlackAPI) *SlackAdapter {
	return &SlackAdapter{api: api, log: slog.Default(), botUserID: "UBOT",
		sessionThreads: map[string]bool{}, seen: map[string]bool{}}
}

// stalledSlackStub is a Slack Web API stub that parks every request until
// unblock is called, then answers invalid_auth (one of the four errors
// socketmode does not retry). unblock is idempotent and is also registered
// as a cleanup AHEAD of srv.Close, so a t.Fatal anywhere in the test releases
// the parked request before Close waits on it. Without that ordering a
// failing test hung in Close for the package timeout — ten minutes and a
// "blocked in Close after 5 seconds" line — instead of printing its Fatalf.
func stalledSlackStub(t *testing.T) (srv *httptest.Server, unblock func()) {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	unblock = func() { once.Do(func() { close(release) }) }
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":false,"error":"invalid_auth"}`))
	}))
	t.Cleanup(func() { unblock(); srv.Close() })
	return srv, unblock
}

// TestSlackWebAPIClientIsBounded: a Post through the real constructor against
// a server that never answers returns by slackAPITimeout instead of parking.
// Driven through newSlackAdapter, not slackHTTPClient, because the claim is
// about the client the constructor hands slack-go: deleting the
// OptionHTTPClient line, or letting an option in extra displace it, leaves
// the helper intact and the adapter unbounded. The production bound is also
// pinned, since the test shortens it.
func TestSlackWebAPIClientIsBounded(t *testing.T) {
	if slackAPITimeout <= 0 || slackAPITimeout > time.Minute {
		t.Fatalf("slackAPITimeout = %v, want a bound within a minute", slackAPITimeout)
	}
	orig := slackAPITimeout
	slackAPITimeout = 300 * time.Millisecond
	t.Cleanup(func() { slackAPITimeout = orig })

	srv, _ := stalledSlackStub(t)
	a := newSlackAdapter("xoxb-stub", "xapp-stub", slog.Default(), slack.OptionAPIURL(srv.URL+"/"))
	done := make(chan error, 1)
	go func() {
		_, err := a.Post("slack:C1/1.0", "hello")
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Post against a server that never answers returned nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Post parked past the bound: the constructor did not hand slack-go the bounded client")
	}
}

// TestSlackDMMentionIsStripped: the bot's handle is addressing in a DM too,
// where Slack's composer offers it by autocomplete. "<@UBOT> stop" must reach
// the gateway as "stop" so the cancel affordance matches, and a bare mention
// in a DM is not a turn, as it is not in a channel.
func TestSlackDMMentionIsStripped(t *testing.T) {
	a := newTestSlackAdapter(&fakeSlackAPI{})
	got, ok := a.inbound(context.Background(), slackMsg("im", "D1", "U1", "<@UBOT> stop", "1.0", ""))
	if !ok || got.Text != "stop" || got.Conversation != "slack:dm/D1" {
		t.Fatalf("DM with a mention: delivered=%v text=%q conv=%q, want text \"stop\"", ok, got.Text, got.Conversation)
	}
	if _, ok := a.inbound(context.Background(), slackMsg("im", "D1", "U1", "<@UBOT>", "2.0", "")); ok {
		t.Fatal("a bare mention in a DM has nothing to run")
	}
	got, ok = a.inbound(context.Background(), slackMsg("im", "D1", "U1", "plain ask", "3.0", ""))
	if !ok || got.Text != "plain ask" {
		t.Fatalf("DM without a mention: delivered=%v text=%q", ok, got.Text)
	}
}

// TestSlackRunAuthTestHonoursCancel: auth.test is the one Web API call Run
// makes before the pump exists. Against a server that never answers, a
// cancelled ctx must still bring Run back — that is SIGTERM during a
// stalled boot, which previously waited on the kubelet to kill the pod.
func TestSlackRunAuthTestHonoursCancel(t *testing.T) {
	srv, _ := stalledSlackStub(t)
	a := newSlackAdapter("xoxb-stub", "xapp-stub", slog.Default(), slack.OptionAPIURL(srv.URL+"/"))

	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan error, 1)
	go func() { returned <- a.Run(ctx, func(InboundMessage) {}) }()

	select {
	case err := <-returned:
		t.Fatalf("Run returned before cancel against a stalled auth.test: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v after cancel, want a context.Canceled-wrapped auth.test error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel: auth.test is not under ctx")
	}
}

// TestSlackEmptyPrincipalMapWarnsAtBoot: the boot-time warning for an empty
// map fires for every backend whose identity join IS the map — Slack as
// much as Discord — and not for gchat, which never reads it. Nothing renders
// the Slack map yet, so a Slack gateway with a missing map file is the
// ordinary case until #2099, and it must not pass boot silently and then
// drop every sender.
func TestSlackEmptyPrincipalMapWarnsAtBoot(t *testing.T) {
	s := startServer(t)
	url := s.ClientURL()
	provision(t, url)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	for _, tc := range []struct {
		backend string
		warns   bool
	}{
		{slackBackend, true},
		{discordBackend, true},
		{gchatBackend, false},
	} {
		t.Run(tc.backend, func(t *testing.T) {
			client, err := lib.Connect(ctx, url, lib.WithName("gateway-test-"+tc.backend), lib.WithAgreementPolicy(SupervisorAgreement(nil)))
			if err != nil {
				t.Fatalf("gateway client: %v", err)
			}
			t.Cleanup(client.Close)
			logs := &recordingHandler{}
			cfg := &Config{
				NATSURL:          url,
				PrincipalMapPath: filepath.Join(t.TempDir(), "no-such-map"),
				DefaultAddressee: "platform",
				IdleTTL:          30 * time.Minute,
				AttributionSalt:  []byte("test-salt"),
			}
			if _, err := New(Options{Client: client, Adapter: newFakeAdapter(), Config: cfg, Backend: tc.backend, Logger: slog.New(logs)}); err != nil {
				t.Fatalf("New: %v", err)
			}
			lvl, found := logs.level("principal map is empty")
			if found != tc.warns {
				t.Fatalf("backend %q: empty-map warning found=%t, want %t", tc.backend, found, tc.warns)
			}
			if found && lvl != slog.LevelWarn {
				t.Fatalf("backend %q: empty-map notice at %v, want WARN", tc.backend, lvl)
			}
		})
	}
}

// recordingHandler captures log records so a test can assert on the LEVEL a
// message came out at, not only on its text — the shutdown-path filters are
// entirely about level, and a test that only matched the words would pass
// against the WARN-on-every-SIGTERM behaviour they exist to remove. Mutexed
// because the pump logs from its own goroutine.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// level reports the level of the first record whose message contains sub.
func (h *recordingHandler) level(sub string) (slog.Level, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if strings.Contains(r.Message, sub) {
			return r.Level, true
		}
	}
	return 0, false
}

// slackEnvelope builds the socketmode.Event shape a real EventsAPI delivery
// has, Request and all — the field TestSlackRunAwaitsPumpGoroutine leaves nil
// and so routes around the ack entirely.
func slackEnvelope(envelopeID string, m *slackevents.MessageEvent) socketmode.Event {
	return socketmode.Event{
		Type:    socketmode.EventTypeEventsAPI,
		Request: &socketmode.Request{EnvelopeID: envelopeID},
		Data: slackevents.EventsAPIEvent{
			Type:       slackevents.CallbackEvent,
			InnerEvent: slackevents.EventsAPIInnerEvent{Data: m},
		},
	}
}

func slackMsg(channelType, channel, user, text, ts, threadTS string) *slackevents.MessageEvent {
	return &slackevents.MessageEvent{
		ChannelType: channelType, Channel: channel, User: user,
		Text: text, TimeStamp: ts, ThreadTimeStamp: threadTS,
	}
}

func TestSlackPostThreadsAndTranslates(t *testing.T) {
	api := &fakeSlackAPI{}
	a := newTestSlackAdapter(api)
	ts, err := a.Post("slack:C1/100.1", "⚙️ **working**")
	if err != nil || ts != "999.001" {
		t.Fatalf("post: ts=%q err=%v", ts, err)
	}
	p := api.posted[0]
	if p.channel != "C1" || p.thread != "100.1" || p.text != "⚙️ *working*" {
		t.Errorf("post = %+v", p)
	}
	if _, err := a.Post("slack:dm/D1", "hi"); err != nil {
		t.Fatal(err)
	}
	if api.posted[1].thread != "" {
		t.Error("DM posts must not set thread_ts")
	}
	if _, err := a.Post("discord:1/2", "x"); err == nil {
		t.Error("malformed conversation must error")
	}
}

func TestSlackEditTranslates(t *testing.T) {
	api := &fakeSlackAPI{}
	a := newTestSlackAdapter(api)
	if err := a.Edit("slack:C1/100.1", "100.2", "✅ **completed**"); err != nil {
		t.Fatal(err)
	}
	u := api.updated[0]
	if u.channel != "C1" || u.ts != "100.2" || u.text != "✅ *completed*" {
		t.Errorf("update = %+v", u)
	}
	if err := a.Edit("nonsense", "1", "x"); err == nil {
		t.Error("malformed conversation must error")
	}
}

func TestSlackRosterReadsChannelMembers(t *testing.T) {
	api := &fakeSlackAPI{members: []string{"U1", "U2"}}
	a := newTestSlackAdapter(api)
	ids, complete, err := a.Roster("slack:C1/100.1")
	if err != nil || !complete || len(ids) != 2 {
		t.Fatalf("roster = %v %v %v", ids, complete, err)
	}
	api.cursor = "more"
	if _, complete, _ = a.Roster("slack:C1/100.1"); complete {
		t.Error("a next cursor means the roster is incomplete")
	}
	if _, _, err := a.Roster("discord:1/2"); err == nil {
		t.Error("malformed conversation must error")
	}
}

func TestSlackOpenDirect(t *testing.T) {
	api := &fakeSlackAPI{openedIM: "D9"}
	a := newTestSlackAdapter(api)
	conv, err := a.OpenDirect("U1")
	if err != nil || conv != "slack:dm/D9" {
		t.Fatalf("openDirect = %q, %v", conv, err)
	}
}

// TestSlackInboundAffordanceRule pins which messages become turns: DMs
// always; channel messages only when they mention the bot (the ask roots
// the session thread); thread replies when they mention the bot or the
// thread is already a session thread (session threads carry every message —
// the parity with Discord's bot-created threads).
//
// The cases run in order against one adapter, because the rule is stateful:
// a mention in a thread makes that thread a session thread for the cases
// after it. 200.1 is the thread the bot is pulled into mid-conversation and
// 300.1 the one it is never addressed in, and they are separate threads for
// exactly that reason.
func TestSlackInboundAffordanceRule(t *testing.T) {
	api := &fakeSlackAPI{replies: map[string][]slack.Message{
		"C1/100.1": {{Msg: slack.Msg{Text: "<@UBOT> check the nodes", User: "U1"}}},
		"C1/200.1": {{Msg: slack.Msg{Text: "lunch?", User: "U2"}}},
		"C1/300.1": {{Msg: slack.Msg{Text: "anyone seen the changelog?", User: "U2"}}},
	}}
	a := newTestSlackAdapter(api)

	// adopted is the gateway's side of the mid-thread case below: the
	// mentioned ask on slack:C1/200.1 was verified and started a task, and
	// TaskStarted is how the adapter learns that thread is now a session
	// thread. Run before the case that needs it.
	adopted := func() { a.TaskStarted("slack:C1/200.1", "task-1") }
	cases := []struct {
		name   string
		before func()
		m      *slackevents.MessageEvent
		want   bool
		conv   string
		kind   string
		text   string
	}{
		{"dm delivers", nil, slackMsg("im", "D1", "U1", "hi", "1.0", ""), true, "slack:dm/D1", "dm", "hi"},
		{"channel without mention drops", nil, slackMsg("channel", "C1", "U1", "hello", "2.0", ""), false, "", "", ""},
		{"channel mention roots a thread on the ask", nil, slackMsg("channel", "C1", "U1", "<@UBOT> do a thing", "3.5", ""), true, "slack:C1/3.5", "group", "do a thing"},
		{"display-name mention form strips", nil, slackMsg("channel", "C1", "U1", "<@UBOT|kage> do it", "3.6", ""), true, "slack:C1/3.6", "group", "do it"},
		{"thread reply with mention delivers", nil, slackMsg("channel", "C1", "U1", "<@UBOT> and this", "4.0", "200.1"), true, "slack:C1/200.1", "group", "and this"},
		// The mention above minted a session on slack:C1/200.1 and the
		// gateway started a task there (adopted), so that thread now
		// carries every message — the follow-up the user expects to be able
		// to steer or stop with.
		{"unmentioned follow-up in an adopted thread delivers", adopted, slackMsg("channel", "C1", "U1", "stop", "4.5", "200.1"), true, "slack:C1/200.1", "group", "stop"},
		{"reply in bot-rooted thread delivers unmentioned", nil, slackMsg("channel", "C1", "U3", "steer it", "5.0", "100.1"), true, "slack:C1/100.1", "group", "steer it"},
		{"reply in plain thread drops", nil, slackMsg("channel", "C1", "U3", "chatter", "6.0", "300.1"), false, "", "", ""},
		{"bare mention drops", nil, slackMsg("channel", "C1", "U1", "<@UBOT>", "7.0", ""), false, "", "", ""},
		// Slack transmits &, < and > entity-encoded; the ask must reach the
		// executor as the user typed it.
		{"entities decode in a dm", nil, slackMsg("im", "D1", "U1", "get pods -n foo &amp;&amp; describe node &lt;name&gt;", "10.0", ""), true, "slack:dm/D1", "dm", "get pods -n foo && describe node <name>"},
		{"entities decode after the mention strip", nil, slackMsg("channel", "C1", "U1", "<@UBOT> scale web if cpu &gt; 80%", "11.0", ""), true, "slack:C1/11.0", "group", "scale web if cpu > 80%"},
		{"entities decode in a thread steer", nil, slackMsg("channel", "C1", "U3", "and &lt;this&gt; too", "12.0", "100.1"), true, "slack:C1/100.1", "group", "and <this> too"},
	}
	for _, c := range cases {
		if c.before != nil {
			c.before()
		}
		got, ok := a.inbound(context.Background(), c.m)
		if ok != c.want {
			t.Errorf("%s: delivered=%v want %v", c.name, ok, c.want)
			continue
		}
		if ok && (got.Conversation != c.conv || got.Text != c.text || got.Kind != c.kind ||
			got.AuthorID != c.m.User || got.MessageID != c.m.TimeStamp) {
			t.Errorf("%s: got %+v", c.name, got)
		}
	}
}

func TestSlackInboundFilters(t *testing.T) {
	a := newTestSlackAdapter(&fakeSlackAPI{})
	if _, ok := a.inbound(context.Background(), slackMsg("im", "D1", "UBOT", "self", "1.0", "")); ok {
		t.Error("own messages must drop")
	}
	bot := slackMsg("im", "D1", "U9", "from an app", "2.0", "")
	bot.BotID = "B123"
	if _, ok := a.inbound(context.Background(), bot); ok {
		t.Error("bot messages must drop")
	}
	edited := slackMsg("im", "D1", "U1", "edited", "3.0", "")
	edited.SubType = "message_changed"
	if _, ok := a.inbound(context.Background(), edited); ok {
		t.Error("non-empty subtypes must drop")
	}
	dup := slackMsg("im", "D1", "U1", "once", "4.0", "")
	if _, ok := a.inbound(context.Background(), dup); !ok {
		t.Fatal("first delivery expected")
	}
	if _, ok := a.inbound(context.Background(), dup); ok {
		t.Error("socket mode is at-least-once; a duplicate (channel,ts) must drop")
	}
	if _, ok := a.inbound(context.Background(), slackMsg("channel", "", "U1", "<@UBOT> x", "5.0", "")); ok {
		t.Error("empty channel must drop")
	}
	if _, ok := a.inbound(context.Background(), slackMsg("im", "D1", "", "ghost", "6.0", "")); ok {
		t.Error("empty user must drop")
	}
}

func TestSlackConversationIDRoundTrip(t *testing.T) {
	cases := []struct {
		channelType, channel, threadTS string
		want                           string
		wantChannel, wantThread        string
	}{
		{"im", "D0AB1", "", "slack:dm/D0AB1", "D0AB1", ""},
		{"channel", "C042", "1725193344.000100", "slack:C042/1725193344.000100", "C042", "1725193344.000100"},
		{"group", "G777", "1700.42", "slack:G777/1700.42", "G777", "1700.42"},
		{"mpim", "C9", "1700.43", "slack:C9/1700.43", "C9", "1700.43"},
	}
	for _, c := range cases {
		got := slackConversationID(c.channelType, c.channel, c.threadTS)
		if got != c.want {
			t.Errorf("slackConversationID(%q,%q,%q) = %q, want %q", c.channelType, c.channel, c.threadTS, got, c.want)
		}
		ch, ts, ok := slackChannelThread(got)
		if !ok || ch != c.wantChannel || ts != c.wantThread {
			t.Errorf("slackChannelThread(%q) = %q,%q,%v want %q,%q,true", got, ch, ts, ok, c.wantChannel, c.wantThread)
		}
	}
	for _, bad := range []string{"discord:1/2", "slack:", "slack:C1", "slack:C1/", "slack:dm/", "slack:/100.1"} {
		if _, _, ok := slackChannelThread(bad); ok {
			t.Errorf("slackChannelThread(%q) parsed; must refuse", bad)
		}
	}
}

// The DoD's registry round-trip: a Slack key contains '.' and '/' and ':',
// all outside the KV token charset — it must survive kvKey's tokenization
// as one token, and distinct keys must not collide through the substitution.
func TestSlackKeySurvivesKVKeyTokenization(t *testing.T) {
	key := "slack:C042/1725193344.000100"
	tok := kvKey(key)
	if !strings.HasPrefix(tok, "sessions.") {
		t.Fatalf("kvKey(%q) = %q, want sessions. prefix", key, tok)
	}
	if strings.ContainsAny(tok[len("sessions."):], "./: ") {
		t.Errorf("kvKey(%q) = %q leaks non-token characters", key, tok)
	}
	if kvKey("slack:C042/1725193344_000100") == tok {
		t.Errorf("distinct slack keys collide after sanitization")
	}
}

func TestToMrkdwn(t *testing.T) {
	cases := map[string]string{
		"⚙️ **working** — checking nodes":    "⚙️ *working* — checking nodes",
		"see [the doc](https://x.example/p)": "see <https://x.example/p|the doc>",
		"plain text":                         "plain text",
		"**a** and **b**":                    "*a* and *b*",
		// Executor text is model output; Slack control sequences in it must
		// arrive escaped, or a prompt-injected result pings the room.
		"<!channel> deploy done": "&lt;!channel&gt; deploy done",
		"ping <@U999> now":       "ping &lt;@U999&gt; now",
		"a & b < c":              "a &amp; b &lt; c",
	}
	for in, want := range cases {
		if got := toMrkdwn(in); got != want {
			t.Errorf("toMrkdwn(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestSlackTurnSubtypes: thread_broadcast is a steer with "also send to
// channel" checked and file_share is an ask with an attachment — both are
// genuine turns and must not vanish silently. Edits stay dropped.
func TestSlackTurnSubtypes(t *testing.T) {
	api := &fakeSlackAPI{replies: map[string][]slack.Message{
		"C1/100.1": {{Msg: slack.Msg{Text: "<@UBOT> check the nodes", User: "U1"}}},
	}}
	a := newTestSlackAdapter(api)

	broadcast := slackMsg("channel", "C1", "U2", "also try the east cluster", "8.0", "100.1")
	broadcast.SubType = "thread_broadcast"
	if _, ok := a.inbound(context.Background(), broadcast); !ok {
		t.Error("thread_broadcast reply in a bot-rooted thread must deliver")
	}

	file := slackMsg("im", "D1", "U1", "here is the manifest", "9.0", "")
	file.SubType = "file_share"
	if _, ok := a.inbound(context.Background(), file); !ok {
		t.Error("file_share with text must deliver")
	}
}

// TestSlackUnescaper: the decode is the exact inverse of Slack's own inbound
// escaping and nothing wider. The literal cases are the reason this is one
// strings.Replacer and not a sequence of ReplaceAll calls — a Replacer scans
// the input once and never rescans its own output, so "&amp;lt;" (what Slack
// sends for a typed "&lt;") comes back as "&lt;" instead of collapsing to
// "<" the way &amp;-then-&lt; passes would leave it.
func TestSlackUnescaper(t *testing.T) {
	cases := []struct {
		name string
		wire string
		want string
	}{
		{"plain text untouched", "restart the api deployment", "restart the api deployment"},
		{"ampersand", "get pods -n foo &amp;&amp; describe node", "get pods -n foo && describe node"},
		{"angles", "describe node &lt;name&gt;", "describe node <name>"},
		{"greater than in a condition", "scale web if cpu &gt; 80%", "scale web if cpu > 80%"},
		{"typed &lt; survives", "type &amp;lt; for a left angle", "type &lt; for a left angle"},
		{"typed &amp; survives", "write &amp;amp; not &amp;", "write &amp; not &"},
		{"typed &gt; survives", "the &amp;gt; entity", "the &gt; entity"},
		{"non-slack entities are left alone", "&copy; 2026 &#123; &nbsp;", "&copy; 2026 &#123; &nbsp;"},
		{"bare ampersand is not an entity", "cats & dogs", "cats & dogs"},
		{"round trip through the outbound escaper", slackEscaper.Replace("a & b < c > d"), "a & b < c > d"},
	}
	for _, c := range cases {
		if got := slackUnescaper.Replace(c.wire); got != c.want {
			t.Errorf("%s: slackUnescaper(%q) = %q, want %q", c.name, c.wire, got, c.want)
		}
	}
}

// TestSlackAskEchoIsNotDoubleEscaped: the inbound text becomes ActiveTask.Ask
// and formatTaskStatus echoes it back through Post -> toMrkdwn, which escapes
// again. Decoding on the way in is what keeps that one escape rather than
// two, so the user sees their own words and not "cpu &amp;gt; 80%".
func TestSlackAskEchoIsNotDoubleEscaped(t *testing.T) {
	api := &fakeSlackAPI{}
	a := newTestSlackAdapter(api)
	msg, ok := a.inbound(context.Background(), slackMsg("im", "D1", "U1", "scale web if cpu &gt; 80% &amp;&amp; nodes ok", "20.0", ""))
	if !ok {
		t.Fatal("dm must deliver")
	}
	ask := truncateRunes(msg.Text, askCap)
	if ask != "scale web if cpu > 80% && nodes ok" {
		t.Fatalf("ask = %q", ask)
	}
	card := formatTaskStatus(&lib.Task{ID: "t-1", State: lib.StateWorking}, ask, time.Time{})
	if _, err := a.Post(msg.Conversation, card); err != nil {
		t.Fatal(err)
	}
	wire := api.posted[0].text
	// One escape on the wire, which Slack renders back as the typed text.
	if !strings.Contains(wire, "cpu &gt; 80% &amp;&amp; nodes ok") {
		t.Errorf("status card echo = %q", wire)
	}
	if strings.Contains(wire, "&amp;gt;") || strings.Contains(wire, "&amp;amp;") {
		t.Errorf("status card echo is double-escaped: %q", wire)
	}
}

// TestSlackEmptyTurnSkipsRootLookup: an attachment-only or whitespace-only
// reply is dropped either way, so it must not pay for the thread-root read
// first. That read runs on the event pump's goroutine under
// slackRepliesTimeout, and the next envelope's ack waits behind it — a
// two-second stall spent to discard the message. The control case — a reply
// with text, in a thread of its OWN — proves the guard still reads when the
// answer matters.
//
// The control's thread is separate on purpose. Sharing 300.1 with the empty
// cases made the final count assertion worthless: with the guard removed the
// attachment case does the read and caches the answer, the whitespace and
// control cases then both hit that cache, and repliesCalls lands on exactly
// the 1 the test wanted. It passed on a cache hit while claiming to prove a
// read. In its own uncached thread the control has to spend the call, so the
// same "want 1" now reads 2 the moment the guard goes.
func TestSlackEmptyTurnSkipsRootLookup(t *testing.T) {
	api := &fakeSlackAPI{replies: map[string][]slack.Message{
		"C1/300.1": {{Msg: slack.Msg{Text: "<@UBOT> watch the rollout", User: "U1"}}},
		"C1/310.1": {{Msg: slack.Msg{Text: "<@UBOT> and this one too", User: "U1"}}},
	}}
	a := newTestSlackAdapter(api)

	// A file_share reply with no caption, in a thread nothing has cached.
	attachment := slackMsg("channel", "C1", "U2", "", "301.0", "300.1")
	attachment.SubType = "file_share"
	if _, ok := a.inbound(context.Background(), attachment); ok {
		t.Error("an empty reply is not a turn")
	}
	if api.repliesCalls != 0 {
		t.Errorf("empty reply made %d conversations.replies calls, want 0", api.repliesCalls)
	}

	whitespace := slackMsg("channel", "C1", "U2", "   \n\t ", "302.0", "300.1")
	if _, ok := a.inbound(context.Background(), whitespace); ok {
		t.Error("a whitespace-only reply is not a turn")
	}
	if api.repliesCalls != 0 {
		t.Errorf("whitespace reply made %d conversations.replies calls, want 0", api.repliesCalls)
	}

	// Control: a different thread, uncached by anything above, with text.
	// The read must happen, and the reply must deliver.
	if _, ok := a.inbound(context.Background(), slackMsg("channel", "C1", "U2", "steer it", "313.0", "310.1")); !ok {
		t.Error("an unmentioned reply in a bot-rooted thread must deliver")
	}
	if api.repliesCalls != 1 {
		t.Errorf("made %d conversations.replies calls, want 1 — only the control should read", api.repliesCalls)
	}
}

// TestSlackBareMentionStillRootsTheThread pins the side effect the empty-text
// check above must not skip past. A bare "@bot" is not a turn, but it does
// root a thread, and recording that costs no API call — so the first real
// reply under it delivers from cache rather than spending
// slackRepliesTimeout re-reading a root we already saw.
func TestSlackBareMentionStillRootsTheThread(t *testing.T) {
	api := &fakeSlackAPI{}
	a := newTestSlackAdapter(api)
	if _, ok := a.inbound(context.Background(), slackMsg("channel", "C1", "U1", "<@UBOT>", "400.0", "")); ok {
		t.Fatal("a bare mention has nothing to run")
	}
	if _, ok := a.inbound(context.Background(), slackMsg("channel", "C1", "U2", "here is the ask", "401.0", "400.0")); !ok {
		t.Error("a reply under a bare mention must deliver: the root mentioned the bot")
	}
	// api.replies is empty, so a lookup would have answered false and
	// dropped the reply. Delivering proves it came from the cache fill.
	if api.repliesCalls != 0 {
		t.Errorf("root was re-read %d times; the bare mention should have cached it", api.repliesCalls)
	}
}

// TestToMrkdwnEscapesAmpersandsInsideLinkURLs pins behaviour that reads like
// a bug and is not one: the ampersand in a link's query string goes out as
// "&amp;", inside the <url|label> form.
//
// That is what Slack asks for and what Slack itself emits. Slack's formatting
// spec names exactly three characters to entity-encode — &, < and > — with no
// carve-out for the URL portion of a control sequence, and the archived
// version of that page states the invariant from the other side: "Because the
// ampersands and angled brackets are already escaped, no further translation
// need take place (for a web-client). The server ensures that no extra
// un-escaped angled brackets or ampersands are included in the message."
// (slackhq/slack-api-docs, page_formatting.md; the live page is
// docs.slack.dev/messaging/formatting-message-text.) The rendering algorithm
// on that same page — find <(.*?)>, split on the pipe, treat the head as a
// URL — runs over the already-escaped text, so the client decodes the entity
// when it builds the href.
//
// Confirmed from the other direction by slackapi/bolt-js#2103, where an app
// posted a link containing a RAW "&" and Slack's own server normalised it to
// "&amp;" in the stored message; Slack staff labelled the resulting broken
// link a "server-side-issue" in the iOS client, not a sender error, and
// desktop and web resolved the same link correctly. Emitting a bare "&" here
// would therefore be re-escaped by Slack anyway.
//
// So: do not "fix" this by leaving the URL unescaped. Anything that stops
// escaping inside <...> also stops escaping <!channel>, which is the reason
// slackEscaper exists — see the case below.
func TestToMrkdwnEscapesAmpersandsInsideLinkURLs(t *testing.T) {
	cases := map[string]string{
		"[Trace](https://monitor.local/query?a=1&b=2)": "<https://monitor.local/query?a=1&amp;b=2|Trace>",
		"[Logs](https://x.example/l?a=1&b=2&c=3)":      "<https://x.example/l?a=1&amp;b=2&amp;c=3|Logs>",
		// A bare URL is not rewritten; Slack auto-links it. The ampersand
		// is still escaped, for the same reason.
		"see https://x.example/l?a=1&b=2": "see https://x.example/l?a=1&amp;b=2",
	}
	for in, want := range cases {
		if got := toMrkdwn(in); got != want {
			t.Errorf("toMrkdwn(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestToMrkdwnNeutralisesControlSequences is the property the escaping exists
// for and the one no link-handling change may cost us. Relayed text is
// executor output — model output — so a prompt-injected result containing
// <!channel> must reach Slack as inert characters, not as an @channel ping to
// the whole room. Same for <!here>, <!everyone>, a user mention, and a
// subteam handle. Held alongside a link in the same string, since a link fix
// is the plausible way to break it.
func TestToMrkdwnNeutralisesControlSequences(t *testing.T) {
	cases := map[string]string{
		"<!channel> deploy done":  "&lt;!channel&gt; deploy done",
		"<!here> heads up":        "&lt;!here&gt; heads up",
		"<!everyone> all hands":   "&lt;!everyone&gt; all hands",
		"<!subteam^S123|@sre> up": "&lt;!subteam^S123|@sre&gt; up",
		"ping <@U999> now":        "ping &lt;@U999&gt; now",
		"join <#C123|general>":    "join &lt;#C123|general&gt;",
		// The mixed case: a real link is rewritten, the injected control
		// sequence beside it is not.
		"<!channel> see [Trace](https://monitor.local/q?a=1&b=2)": "&lt;!channel&gt; see <https://monitor.local/q?a=1&amp;b=2|Trace>",
	}
	for in, want := range cases {
		got := toMrkdwn(in)
		if got != want {
			t.Errorf("toMrkdwn(%q) = %q, want %q", in, got, want)
		}
		// Belt and braces, independent of the table: the only control
		// sequences left on the wire are URL links. No mention, channel
		// link or special command survives as one.
		for _, opener := range []string{"<!", "<@", "<#"} {
			if strings.Contains(got, opener) {
				t.Errorf("toMrkdwn(%q) = %q leaves a live %q control sequence", in, got, opener)
			}
		}
	}
}

// TestSlackRunAwaitsPumpGoroutine: Run must not return while the event pump
// it started is still working. Before the WaitGroup it did — RunContext
// returning (an invalid token, an unrecoverable socket error) unblocked Run
// while a handler call was mid-flight, so an embedder that treats "Run
// returned" as "this adapter is finished" could tear down state the pump was
// still writing to.
//
// The sequence is forced, not timed: apps.connections.open blocks until the
// test releases it, so the pump is guaranteed to be inside handler before
// RunContext fails. Then the test asserts Run is still blocked, releases the
// handler, and reads a variable the pump wrote with no synchronisation of its
// own — under -race, only Run's wg.Wait can order that write before this
// read.
func TestSlackRunAwaitsPumpGoroutine(t *testing.T) {
	srv, unblock := stalledSlackStub(t)

	a := newTestSlackAdapter(&fakeSlackAPI{})
	// A real socketmode client, pointed at the stub: its Events channel is
	// the pump's input, and its connect fails fatally (invalid_auth is one
	// of the four errors socketmode does not retry) as soon as we release.
	a.sm = socketmode.New(slack.New("xoxb-stub", slack.OptionAPIURL(srv.URL+"/")))

	var pumpFinishedHandler bool // deliberately unsynchronised; see above
	entered := make(chan struct{})
	proceed := make(chan struct{})
	handler := func(InboundMessage) {
		close(entered)
		<-proceed
		pumpFinishedHandler = true
	}

	// Queue one real turn for the pump before Run starts; Events is buffered.
	a.sm.Events <- socketmode.Event{
		Type: socketmode.EventTypeEventsAPI,
		Data: slackevents.EventsAPIEvent{
			Type: slackevents.CallbackEvent,
			InnerEvent: slackevents.EventsAPIInnerEvent{
				Data: slackMsg("im", "D1", "U1", "hello", "500.0", ""),
			},
		},
	}

	returned := make(chan error, 1)
	go func() { returned <- a.Run(context.Background(), handler) }()

	select {
	case <-entered:
	case err := <-returned:
		t.Fatalf("Run returned before the pump reached the handler: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("pump never reached the handler")
	}

	// RunContext can now fail, which sends Run into its deferred cancel and
	// wait while the handler is still parked.
	unblock()
	select {
	case err := <-returned:
		t.Fatalf("Run returned with the pump still in the handler: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	close(proceed)
	select {
	case err := <-returned:
		if err == nil {
			t.Error("Run should surface the connect failure")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run never returned; the deferred cancel and wait are out of order")
	}
	if !pumpFinishedHandler {
		t.Error("Run returned before the pump finished")
	}
}

// TestSlackPumpDropsATurnItCouldNotAck is the duplicate-turn guard. An
// envelope we failed to ack is one Slack will redeliver; handling it here as
// well turns that safe redelivery into two turns from one user message,
// because the instance that gets the redelivery has an empty alreadySeen map
// and cannot suppress it. So a failed ack must drop the turn, not log and
// carry on.
//
// The ack failure is forced without racing a context cancellation: socketmode
// refuses to write a Socket Mode response of 20KB or more (Slack silently
// drops those), so AckCtx on an oversized envelope ID fails deterministically,
// before the response ever reaches the send channel. A second, ackable
// envelope behind it proves the drop is a drop and not a dead pump — and,
// since Events is FIFO and the pump is single-threaded, seeing the second turn
// means the first was already decided.
func TestSlackPumpDropsATurnItCouldNotAck(t *testing.T) {
	srv, unblock := stalledSlackStub(t)

	logs := &recordingHandler{}
	a := newTestSlackAdapter(&fakeSlackAPI{})
	a.log = slog.New(logs)
	a.sm = socketmode.New(slack.New("xoxb-stub", slack.OptionAPIURL(srv.URL+"/")))

	delivered := make(chan InboundMessage, 4)

	a.sm.Events <- slackEnvelope(strings.Repeat("E", 32*1024), slackMsg("im", "D1", "U1", "unacked", "600.0", ""))
	a.sm.Events <- slackEnvelope("Env-ok", slackMsg("im", "D1", "U1", "acked", "601.0", ""))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan error, 1)
	go func() { returned <- a.Run(ctx, func(m InboundMessage) { delivered <- m }) }()

	select {
	case m := <-delivered:
		if m.MessageID == "600.0" {
			t.Fatalf("the unacked turn reached the handler: %+v — Slack will redeliver it, so this is the duplicate", m)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the ackable turn never reached the handler")
	}
	select {
	case m := <-delivered:
		t.Errorf("a second turn reached the handler: %+v", m)
	default:
	}

	// An ack that failed for anything other than shutdown is a real failure
	// and keeps its WARN.
	if lvl, ok := logs.level("ack failed"); !ok || lvl != slog.LevelWarn {
		t.Errorf("oversized-envelope ack logged at %v (found=%v), want WARN", lvl, ok)
	}

	cancel()
	unblock()
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("Run never returned")
	}
}

// slackInvalidAuthServer answers every Web API call with invalid_auth, which
// socketmode's connect() treats as fatal — so RunContext gives up on the first
// attempt instead of backing off and redialling, and a Run against it returns
// in microseconds.
func slackInvalidAuthServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":false,"error":"invalid_auth"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// slackCancelledPumpRun drives one whole SlackAdapter.Run against a context
// that is already cancelled, with a single ackable DM envelope sitting in
// Events, and reports whether the pump took that envelope off the channel.
//
// The Socket Mode client is the real one, not a fake, because the behaviour
// under test is the real one's: AckCtx on a cancelled context with room in the
// 20-deep socketModeResponses buffer returns nil roughly half the time. Run's
// deferred wg.Wait means the pump goroutine has finished by the time this
// returns, so the caller's counters need no locking of their own.
func slackCancelledPumpRun(apiURL, envelopeID, ts string, logs *recordingHandler, handler func(InboundMessage)) bool {
	a := newTestSlackAdapter(&fakeSlackAPI{})
	a.log = slog.New(logs)
	a.sm = socketmode.New(slack.New("xoxb-stub", slack.OptionAPIURL(apiURL)))
	a.sm.Events <- slackEnvelope(envelopeID, slackMsg("im", "D1", "U1", "hello", ts, ""))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = a.Run(ctx, handler)

	// Whatever RunContext pushed onto Events on its way out — connecting, and
	// possibly connection_error — carries no Request, so matching on the
	// envelope ID answers exactly one question: did the pump take OUR
	// envelope, or did its top-of-loop select take ctx.Done instead?
	for {
		select {
		case evt := <-a.sm.Events:
			if evt.Request != nil && evt.Request.EnvelopeID == envelopeID {
				return true
			}
		default:
			return false
		}
	}
}

// slackCancelledPumpRuns is how many shutdowns the two tests below each drive,
// and the count is the whole reason they are trustworthy. One run cannot be
// enough: with ctx already done AND an envelope already buffered, both cases
// of the pump's top-of-loop select are ready, and a Go select picks uniformly
// at random among ready cases — so about half of all runs leave at the select
// and never reach the code under test, and a one-shot test would go green
// against the broken version every other time it ran. Fifty independent runs
// make "no turn was handled" and "no ack was attempted" deterministic to about
// 2^-50. That is also why the branches are not pinned by pre-filling the
// response buffer instead: pre-filling forces AckCtx to FAIL, and the branch
// that matters here is the one where it succeeds.
const slackCancelledPumpRuns = 50

// TestSlackPumpStartsNoTurnOnACancelledContext is the shutdown-race guard —
// what 9239f5df set out to do and did only about half the time.
//
// The trap: AckCtx returning nil never meant Slack has the ack. It means the
// response was QUEUED. SendCtx races ctx.Done against a send into the 20-deep
// socketModeResponses channel, runResponseSender keeps that channel drained so
// in production there is always room, and once ctx is cancelled both cases are
// ready and the runtime picks at random — measured against slack-go v0.29.0,
// 483 of 1000 such calls came back nil. Each of those used to fall straight
// through to the handler and start or steer a task on an instance that is
// exiting, while runResponseSender — whose select has the same shape — left
// without flushing the queued ack. Slack redelivers to a new instance whose
// alreadySeen map is empty, and one user message becomes two agent sessions
// doing real work.
//
// So the contract is: a cancelled pump reaches no handler, whatever AckCtx
// says. Two guards in Run enforce it — the ctx.Err() re-check after the Events
// receive, and the ctx.Err() re-check after a successful ack — and this test
// asserts the contract rather than either guard.
// TestSlackPumpDoesNotAckOnACancelledContext below pins the first one alone.
func TestSlackPumpStartsNoTurnOnACancelledContext(t *testing.T) {
	srv := slackInvalidAuthServer(t)
	logs := &recordingHandler{}

	// handled is written by each run's pump goroutine and read here; Run's
	// deferred wg.Wait orders every write before this function sees it.
	handled, took := 0, 0
	for i := 0; i < slackCancelledPumpRuns; i++ {
		if slackCancelledPumpRun(srv.URL+"/", fmt.Sprintf("Env-cancel-%d", i),
			fmt.Sprintf("800.%03d", i), logs, func(InboundMessage) { handled++ }) {
			took++
		}
	}
	if handled != 0 {
		t.Errorf("a cancelled pump handled %d of %d turns; every one is a duplicate waiting to happen, because nothing flushed the ack and Slack will redeliver the envelope",
			handled, slackCancelledPumpRuns)
	}
	// Without this the test could pass for the wrong reason: if every run
	// happened to leave at the top-of-loop select, nothing below it ran.
	if took == 0 {
		t.Errorf("not one of the %d runs took the envelope off Events, so the code under test never executed", slackCancelledPumpRuns)
	}
	t.Logf("%d of %d cancelled pumps took the envelope past the top-of-loop select", took, slackCancelledPumpRuns)
}

// TestSlackPumpDoesNotAckOnACancelledContext pins the first guard on its own:
// the ctx.Err() re-check immediately after the Events receive, which is what
// defeats the pseudo-random select. A pump that is already cancelled must not
// so much as ATTEMPT the ack — an ack queued now goes into a buffer whose
// drain goroutine is exiting, so it is at best a no-op, and at worst the thing
// that makes the pump believe the turn is safe to run.
//
// Both shutdown log lines are checked because an attempted ack announces
// itself one way or the other: Canceled from AckCtx logs "ack abandoned", and
// a nil arriving on a dead context logs "ack queued but not flushed". Over
// fifty runs, an ack attempted at all produces one of them.
func TestSlackPumpDoesNotAckOnACancelledContext(t *testing.T) {
	srv := slackInvalidAuthServer(t)
	logs := &recordingHandler{}
	for i := 0; i < slackCancelledPumpRuns; i++ {
		slackCancelledPumpRun(srv.URL+"/", fmt.Sprintf("Env-noack-%d", i),
			fmt.Sprintf("810.%03d", i), logs, func(InboundMessage) {})
	}
	if lvl, ok := logs.level("ack abandoned"); ok {
		t.Errorf("a cancelled pump attempted an ack and had it refused (logged at %v); the receive is not re-checking ctx", lvl)
	}
	if lvl, ok := logs.level("ack queued but not flushed"); ok {
		t.Errorf("a cancelled pump queued an ack nothing will flush (logged at %v); the receive is not re-checking ctx", lvl)
	}
}

// TestSlackAckCtxQueuesAnAckOnACancelledContext is the half of the premise
// that actually describes production, and the one the pre-filled-buffer test
// below cannot show. With ROOM in socketModeResponses — the normal state,
// since runResponseSender drains it — AckCtx on a cancelled context has two
// ready cases in its select and comes back nil a large fraction of the time.
// Nil means queued, never delivered: runResponseSender exits on the same ctx
// without flushing what is in the buffer.
//
// Asserted as "not an error every single time" rather than "about half", so
// the assertion is not itself a coin toss — two hundred draws all landing on
// the error case is 2^-200 if the select is fair, and a certainty if slack-go
// has changed. Should this one ever start failing, the post-ack ctx.Err()
// guard in the pump has lost its reason to exist and can go.
func TestSlackAckCtxQueuesAnAckOnACancelledContext(t *testing.T) {
	const draws = 200
	queued := 0
	for i := 0; i < draws; i++ {
		sm := socketmode.New(slack.New("xoxb-stub"))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := sm.AckCtx(ctx, "Env-1", nil); err == nil {
			queued++
		}
	}
	if queued == 0 {
		t.Errorf("AckCtx on a cancelled context with an empty response buffer errored on all %d draws; SendCtx no longer races ctx.Done against the buffered send", draws)
	}
	t.Logf("AckCtx returned nil — queued, not delivered — on %d of %d cancelled-context calls", queued, draws)
}

// TestSlackAckCtxFailsOnACancelledContext pins one premise the shutdown filter
// rests on: AckCtx really does come back context.Canceled once the context is
// done and the response channel cannot take the write. Plain Ack could not —
// it passes context.TODO(), and marshalling a forty-byte struct does not fail
// — which is why the error branch was unreachable before the switch to AckCtx.
//
// Read the pre-filled buffer below for what it is and no more. It forces the
// error branch by making ctx.Done the ONLY ready case in AckCtx's select, and
// that is not what a real shutdown looks like: in production
// runResponseSender keeps socketModeResponses drained, so the buffered send is
// ready too and AckCtx comes back nil about half the time
// (TestSlackAckCtxQueuesAnAckOnACancelledContext). This test is proof that
// "AckCtx can return Canceled", NOT proof that the shutdown path is covered —
// the coverage for that is TestSlackPumpStartsNoTurnOnACancelledContext and
// TestSlackPumpDoesNotAckOnACancelledContext.
func TestSlackAckCtxFailsOnACancelledContext(t *testing.T) {
	sm := socketmode.New(slack.New("xoxb-stub"))
	// socketModeResponses is 20 deep and its drain goroutine only runs under
	// RunContext, so twenty sends leave the buffer full and ctx.Done the only
	// ready case in AckCtx's select.
	for i := 0; i < 20; i++ {
		if err := sm.Send(socketmode.Response{EnvelopeID: "filler"}); err != nil {
			t.Fatalf("filling the response buffer: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sm.AckCtx(ctx, "Env-1", nil); !errors.Is(err, context.Canceled) {
		t.Errorf("AckCtx on a cancelled context = %v, want context.Canceled", err)
	}
}

// TestSlackRootLookupLogLevels: the thread-root read fires on every
// unmentioned thread reply, so its failure log is the noisiest thing in the
// adapter and a WARN on every pod termination is a false positive for anything
// alerting on logs. Cancelled is demoted. DeadlineExceeded is NOT — at this
// site that is slackRepliesTimeout genuinely expiring on a slow
// conversations.replies, which cost a user their reply and is the real
// operational signal a blanket "any context error" filter would swallow.
func TestSlackRootLookupLogLevels(t *testing.T) {
	// Shutdown: the parent context is already cancelled.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	shutdown := &recordingHandler{}
	a := newTestSlackAdapter(&fakeSlackAPI{})
	a.log = slog.New(shutdown)
	if a.isSessionThread(cancelled, "C1", "700.1") {
		t.Error("a failed lookup must report false")
	}
	if lvl, ok := shutdown.level("thread root lookup"); !ok || lvl != slog.LevelDebug {
		t.Errorf("cancelled lookup logged at %v (found=%v), want DEBUG", lvl, ok)
	}

	// The timeout, modelled with a parent whose deadline has already passed
	// so the derived context reports DeadlineExceeded rather than Canceled.
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	timedOut := &recordingHandler{}
	b := newTestSlackAdapter(&fakeSlackAPI{})
	b.log = slog.New(timedOut)
	if b.isSessionThread(expired, "C1", "700.2") {
		t.Error("a timed-out lookup must report false")
	}
	if lvl, ok := timedOut.level("thread root lookup"); !ok || lvl != slog.LevelWarn {
		t.Errorf("timed-out lookup logged at %v (found=%v), want WARN — slackRepliesTimeout expiring is a dropped reply", lvl, ok)
	}

	// And a plain empty answer, with no context involved at all, still warns.
	empty := &recordingHandler{}
	c := newTestSlackAdapter(&fakeSlackAPI{})
	c.log = slog.New(empty)
	if c.isSessionThread(context.Background(), "C1", "700.3") {
		t.Error("an unknown thread root must report false")
	}
	if lvl, ok := empty.level("thread root lookup"); !ok || lvl != slog.LevelWarn {
		t.Errorf("empty root lookup logged at %v (found=%v), want WARN", lvl, ok)
	}
}

// TestSlackDecodedTextDrivesTheAffordances pins a consequence of decoding the
// inbound entities that nothing else in the suite notices. normalize() drops
// every non-alphanumeric, so the entity escaping used to survive it as
// letters: "&lt;stop&gt;" normalized to "ltstopgt" and matched nothing.
// Decoded first, the same wire text normalizes to "stop" — a hard task cancel.
// Kept deliberately: the affordances should match what the user typed, not
// what Slack's transport did to it. The same shift shortens normalized text,
// so an ask can newly fall under isStatusQuery's wideMatchLenCap.
func TestSlackDecodedTextDrivesTheAffordances(t *testing.T) {
	a := newTestSlackAdapter(&fakeSlackAPI{})

	// What Slack puts on the wire when a user types "<stop>".
	const stopWire = "&lt;stop&gt;"
	if got := normalize(stopWire); got != "ltstopgt" || isStop(stopWire) {
		t.Fatalf("premise: normalize(%q) = %q, isStop = %v", stopWire, got, isStop(stopWire))
	}
	msg, ok := a.inbound(context.Background(), slackMsg("im", "D1", "U1", stopWire, "800.0", ""))
	if !ok {
		t.Fatal("dm must deliver")
	}
	if msg.Text != "<stop>" {
		t.Fatalf("inbound text = %q, want the decoded form", msg.Text)
	}
	if !isStop(msg.Text) {
		t.Error("a typed <stop> must cancel: the decode is what lets normalize see \"stop\"")
	}

	// The length half. Same words, entity-encoded and not.
	const pokeWire = "any update on the &lt;prod&gt; rollout &amp; the canary?"
	if isStatusQuery(pokeWire, true) {
		t.Errorf("premise: the wire form normalizes to %d chars, over the %d cap", len(normalize(pokeWire)), wideMatchLenCap)
	}
	poke, ok := a.inbound(context.Background(), slackMsg("im", "D1", "U1", pokeWire, "801.0", ""))
	if !ok {
		t.Fatal("dm must deliver")
	}
	if !isStatusQuery(poke.Text, true) {
		t.Errorf("decoded %q normalizes to %d chars and must read as a status poke", poke.Text, len(normalize(poke.Text)))
	}
}

// TestSlackMidThreadMentionAdoptsThread pins the sequence that mints a
// session in a thread the bot did not root: a user mentions the bot in
// someone else's thread, which delivers a turn keyed on that thread, the
// gateway starts a task there and says so (TaskStarted), and the user then
// follows up unmentioned — a steer, or "stop". That follow-up has to reach
// the gateway, because a session is already running there. Before the
// adapter recorded the thread, the follow-up hit the root check, the root
// read found a message with no mention, and the message was discarded
// without even a drop notice: nothing reached handleInbound.
//
// The record is the gateway's TaskStarted, not the mention: the adapter
// used to record on the mention alone, before anyone had verified the
// sender, and TestSlackBareMentionInForeignThreadAdoptsNothing pins the
// other half of that change.
//
// Every shape that can carry a mention into a foreign thread is here, since
// the bug is "a session minted at a key the adapter never recorded" and a
// plain reply is only one way to reach it.
func TestSlackMidThreadMentionAdoptsThread(t *testing.T) {
	mention := func(text, ts, thread string) *slackevents.MessageEvent {
		return slackMsg("channel", "C1", "U1", text, ts, thread)
	}
	broadcast := func(text, ts, thread string) *slackevents.MessageEvent {
		m := mention(text, ts, thread)
		m.SubType = "thread_broadcast"
		return m
	}
	fileShare := func(text, ts, thread string) *slackevents.MessageEvent {
		m := mention(text, ts, thread)
		m.SubType = "file_share"
		return m
	}
	cases := []struct {
		name string
		msg  func(text, ts, thread string) *slackevents.MessageEvent
	}{
		{"plain reply", mention},
		{"thread_broadcast", broadcast},
		{"file_share", fileShare},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			api := &fakeSlackAPI{replies: map[string][]slack.Message{
				"C1/200.1": {{Msg: slack.Msg{Text: "lunch?", User: "U2"}}},
			}}
			a := newTestSlackAdapter(api)

			got, ok := a.inbound(context.Background(), c.msg("<@UBOT> drain node 3", "4.0", "200.1"))
			if !ok || got.Conversation != "slack:C1/200.1" {
				t.Fatalf("mention in a foreign thread: delivered=%v conv=%q", ok, got.Conversation)
			}
			// The gateway verified the sender and started a task on that key.
			a.TaskStarted(got.Conversation, "task-1")
			got, ok = a.inbound(context.Background(), slackMsg("channel", "C1", "U1", "stop", "5.0", "200.1"))
			if !ok || got.Conversation != "slack:C1/200.1" {
				t.Fatalf("unmentioned follow-up in the session's own thread: delivered=%v conv=%q", ok, got.Conversation)
			}
			if got.Text != "stop" {
				t.Errorf("follow-up text = %q, want the affordance word intact", got.Text)
			}
		})
	}
}

// TestSlackBareMentionInForeignThreadAdoptsNothing: a bare "@bot" inside
// someone else's thread is not a turn (nothing to run), and it does not make
// that thread a session thread either. The adapter used to record the
// thread on the mention alone — before the gateway had verified the sender
// or decided anything — so anyone who could type "<@bot>" in a thread turned
// its every later message into a delivery. Now the thread becomes a session
// thread when the gateway starts a task in it and says so (TaskStarted);
// until then an unmentioned reply is answered from the root, which does not
// mention the bot, and drops.
func TestSlackBareMentionInForeignThreadAdoptsNothing(t *testing.T) {
	api := &fakeSlackAPI{replies: map[string][]slack.Message{
		"C1/200.1": {{Msg: slack.Msg{Text: "lunch?", User: "U2"}}},
	}}
	a := newTestSlackAdapter(api)

	if _, ok := a.inbound(context.Background(), slackMsg("channel", "C1", "U1", "<@UBOT>", "4.0", "200.1")); ok {
		t.Fatal("a bare mention has nothing to run and must not deliver")
	}
	if v, cached := a.sessionThreads["C1/200.1"]; cached {
		t.Fatalf("a bare mention in a foreign thread recorded the thread as %v; it must record nothing", v)
	}
	if _, ok := a.inbound(context.Background(), slackMsg("channel", "C1", "U1", "drain node 3", "5.0", "200.1")); ok {
		t.Fatal("an unmentioned reply after a bare mention delivered: the mention adopted the thread")
	}
	if api.repliesCalls != 1 {
		t.Errorf("the unmentioned reply made %d conversations.replies reads, want exactly 1 — the root is the answer", api.repliesCalls)
	}
	// A mentioned ask is a turn whoever rooted the thread; that has not
	// changed. What has: the thread is a session thread once the gateway
	// starts a task there, and not before.
	got, ok := a.inbound(context.Background(), slackMsg("channel", "C1", "U1", "<@UBOT> drain node 3", "6.0", "200.1"))
	if !ok || got.Conversation != "slack:C1/200.1" {
		t.Fatalf("a mentioned ask in a foreign thread: delivered=%v conv=%q", ok, got.Conversation)
	}
	a.TaskStarted(got.Conversation, "task-1")
	if _, ok := a.inbound(context.Background(), slackMsg("channel", "C1", "U1", "stop", "7.0", "200.1")); !ok {
		t.Fatal("an unmentioned follow-up after TaskStarted must deliver")
	}
}

// TestSlackMentionUnpoisonsCachedFalse: the root check caches its answer, so
// an unmentioned reply that arrives BEFORE the bot is pulled into the thread
// leaves a false behind. The gateway starting a task there (TaskStarted) has
// to overwrite it, or the thread is dropped for the life of the cache entry
// — including the "stop" for the session that task started.
func TestSlackMentionUnpoisonsCachedFalse(t *testing.T) {
	api := &fakeSlackAPI{replies: map[string][]slack.Message{
		"C1/200.1": {{Msg: slack.Msg{Text: "lunch?", User: "U2"}}},
	}}
	a := newTestSlackAdapter(api)

	if _, ok := a.inbound(context.Background(), slackMsg("channel", "C1", "U3", "chatter", "3.0", "200.1")); ok {
		t.Fatal("chatter in a thread the bot is not in must drop")
	}
	if v, cached := a.sessionThreads["C1/200.1"]; !cached || v {
		t.Fatalf("want a cached false for the thread; cached=%v value=%v", cached, v)
	}
	got, ok := a.inbound(context.Background(), slackMsg("channel", "C1", "U1", "<@UBOT> drain node 3", "4.0", "200.1"))
	if !ok {
		t.Fatal("mention in the thread must deliver")
	}
	// The un-poisoning is the gateway's TaskStarted, not the mention.
	a.TaskStarted(got.Conversation, "task-1")
	if _, ok := a.inbound(context.Background(), slackMsg("channel", "C1", "U1", "stop", "5.0", "200.1")); !ok {
		t.Fatal("the cached false outlived the session it silenced")
	}
	// One entry, one eviction slot: the overwrite must not double-book the
	// ring or the cache would evict short of its cap.
	if n := len(a.threadsOrder); n != 1 {
		t.Errorf("threadsOrder = %d entries, want 1", n)
	}
}

// TestSlackSessionLookupAnswersAColdCache: the session registry is the
// source of truth for which threads the gateway is in, and a cache miss asks
// it before it reads the thread root. That is what makes a thread the
// gateway adopted mid-conversation — root by someone else, no mention in it
// — survive a restart of the adapter's process: the root read alone would
// answer false, and every follow-up would drop.
func TestSlackSessionLookupAnswersAColdCache(t *testing.T) {
	api := &fakeSlackAPI{replies: map[string][]slack.Message{
		"C1/200.1": {{Msg: slack.Msg{Text: "lunch?", User: "U2"}}},
		"C1/210.1": {{Msg: slack.Msg{Text: "coffee?", User: "U2"}}},
		"C1/220.1": {{Msg: slack.Msg{Text: "<@UBOT> watch the rollout", User: "U1"}}},
	}}
	a := newTestSlackAdapter(api)
	var asked []string
	a.sessions = func(_ context.Context, conversation string) (bool, error) {
		asked = append(asked, conversation)
		switch conversation {
		case "slack:C1/200.1":
			return true, nil
		case "slack:C1/220.1":
			return false, errors.New("kv unavailable")
		}
		return false, nil
	}

	// The registry holds the thread: delivered, and the root was never read.
	got, ok := a.inbound(context.Background(), slackMsg("channel", "C1", "U1", "stop", "5.0", "200.1"))
	if !ok || got.Conversation != "slack:C1/200.1" {
		t.Fatalf("a reply in a thread the registry holds: delivered=%v conv=%q", ok, got.Conversation)
	}
	if api.repliesCalls != 0 {
		t.Errorf("the registry answered, yet %d conversations.replies reads were made; want 0", api.repliesCalls)
	}
	if !a.sessionThreads["C1/200.1"] {
		t.Error("the registry's answer was not cached")
	}

	// The registry does not hold the thread, and the root does not mention
	// the bot: dropped, after exactly the one root read.
	if _, ok := a.inbound(context.Background(), slackMsg("channel", "C1", "U1", "chatter", "6.0", "210.1")); ok {
		t.Fatal("a reply in a thread neither the registry nor the root claims delivered")
	}
	if api.repliesCalls != 1 {
		t.Errorf("a registry false must fall through to the root: %d replies reads, want 1", api.repliesCalls)
	}

	// The registry fails: not a drop. The root read still answers for a
	// thread the bot rooted.
	got, ok = a.inbound(context.Background(), slackMsg("channel", "C1", "U1", "and the canary", "7.0", "220.1"))
	if !ok || got.Conversation != "slack:C1/220.1" {
		t.Fatalf("a reply in a bot-rooted thread with the registry failing: delivered=%v conv=%q", ok, got.Conversation)
	}
	if api.repliesCalls != 2 {
		t.Errorf("a registry error must fall through to the root: %d replies reads, want 2", api.repliesCalls)
	}
	if len(asked) != 3 {
		t.Errorf("the registry was consulted for %v; want once per cold thread", asked)
	}
}

// TestSlackTaskStartedMarksOnlyThreads: TaskStarted records the thread a
// task started in, and nothing for a DM, which is the whole session and
// needs no record.
func TestSlackTaskStartedMarksOnlyThreads(t *testing.T) {
	a := newTestSlackAdapter(&fakeSlackAPI{})
	a.TaskStarted("slack:dm/D1", "task-dm")
	a.TaskStarted("not-a-slack-key", "task-other")
	if n := len(a.sessionThreads); n != 0 {
		t.Fatalf("a DM or a foreign key marked %d threads, want 0: %v", n, a.sessionThreads)
	}
	a.TaskStarted("slack:C1/9.0", "task-thread")
	if !a.sessionThreads["C1/9.0"] {
		t.Fatalf("a task in a thread did not mark it: %v", a.sessionThreads)
	}
	if n := len(a.threadsOrder); n != 1 {
		t.Errorf("threadsOrder = %d entries, want 1", n)
	}
}

// TestSlackGatewayAdoptsThreadOnStartedTaskAndSurvivesRestart is the
// adopt-a-foreign-thread sequence through the real gateway rather than the
// adapter alone: New wires the registry lookup, a verified sender's mentioned
// ask in someone else's thread starts a task and TaskStarted marks the
// thread, and after the adapter's cache is wiped — a restart — the registry
// still answers for the unmentioned "stop". An unverified sender's ask in a
// fresh thread starts nothing and marks nothing.
func TestSlackGatewayAdoptsThreadOnStartedTaskAndSurvivesRestart(t *testing.T) {
	s := startServer(t)
	url := s.ClientURL()
	provision(t, url)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	mapFile := filepath.Join(t.TempDir(), "principal-map")
	if err := os.WriteFile(mapFile, []byte("U1 test:jayanti\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := lib.Connect(ctx, url, lib.WithName("gateway-test-slack"), lib.WithAgreementPolicy(SupervisorAgreement(nil)))
	if err != nil {
		t.Fatalf("gateway client: %v", err)
	}
	t.Cleanup(client.Close)

	api := &fakeSlackAPI{replies: map[string][]slack.Message{
		"C1/200.1": {{Msg: slack.Msg{Text: "lunch?", User: "U2"}}},
	}}
	a := newTestSlackAdapter(api)
	cfg := &Config{
		NATSURL:          url,
		PrincipalMapPath: mapFile,
		DefaultAddressee: "platform",
		IdleTTL:          30 * time.Minute,
		AttributionSalt:  []byte("test-salt"),
	}
	g, err := New(Options{Client: client, Adapter: a, Config: cfg, Backend: slackBackend, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if a.sessions == nil {
		t.Fatal("New did not offer the Slack adapter the session lookup")
	}

	// The mentioned ask, as inbound would hand it over, driven through the
	// gateway: verified, minted, task started, and the adapter told.
	g.handleInbound(InboundMessage{Conversation: "slack:C1/200.1", Kind: "group", AuthorID: "U1", MessageID: "4.0", Text: "drain node 3"})
	marked := func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.sessionThreads["C1/200.1"]
	}
	deadline := time.Now().Add(5 * time.Second)
	for !marked() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !marked() {
		t.Fatalf("the gateway started a task in C1/200.1 and the adapter never marked it: %v", a.sessionThreads)
	}

	// A restart: the cache is gone, the registry is not.
	a.mu.Lock()
	a.sessionThreads = map[string]bool{}
	a.threadsOrder = nil
	a.mu.Unlock()
	got, ok := a.inbound(ctx, slackMsg("channel", "C1", "U1", "stop", "5.0", "200.1"))
	if !ok || got.Conversation != "slack:C1/200.1" {
		t.Fatalf("an unmentioned follow-up after a restart: delivered=%v conv=%q — the registry did not answer", ok, got.Conversation)
	}
	if api.repliesCalls != 0 {
		t.Errorf("the root was read %d times; the registry should have answered first", api.repliesCalls)
	}

	// An unmapped sender's ask starts nothing, so it marks nothing.
	g.handleInbound(InboundMessage{Conversation: "slack:C1/300.1", Kind: "group", AuthorID: "U9", MessageID: "6.0", Text: "drain node 4"})
	a.mu.Lock()
	v, cached := a.sessionThreads["C1/300.1"]
	a.mu.Unlock()
	if cached {
		t.Fatalf("an unverified sender's ask marked its thread as %v; it must mark nothing", v)
	}
	if held, err := a.sessions(ctx, "slack:C1/300.1"); err != nil || held {
		t.Fatalf("the registry holds a session for the unverified sender's thread: held=%v err=%v", held, err)
	}
}
