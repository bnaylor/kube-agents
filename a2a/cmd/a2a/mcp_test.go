package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// jrpc builds one newline-terminated JSON-RPC request line. A nil id makes it
// a notification.
func jrpc(id any, method string, params any) string {
	msg := map[string]any{"jsonrpc": "2.0", "method": method}
	if id != nil {
		msg["id"] = id
	}
	if params != nil {
		msg["params"] = params
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return string(raw) + "\n"
}

// exchange runs the MCP server over the given input and returns one decoded
// JSON object per output line.
func exchange(t *testing.T, input string) []map[string]any {
	t.Helper()
	var out bytes.Buffer
	if err := serveMCP(strings.NewReader(input), &out); err != nil {
		t.Fatalf("serveMCP: %v", err)
	}
	var msgs []map[string]any
	for _, line := range strings.Split(strings.TrimRight(out.String(), "\n"), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("output line is not JSON: %q: %v", line, err)
		}
		msgs = append(msgs, m)
	}
	return msgs
}

// handshake is the client's opening: initialize, then the initialized
// notification. Prepended to every exchange that goes on to call tools.
const testProtocolVersion = "2025-06-18"

func handshake() string {
	return jrpc(1, "initialize", map[string]any{
		"protocolVersion": testProtocolVersion,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "test", "version": "0"},
	}) + jrpc(nil, "notifications/initialized", nil)
}

func result(t *testing.T, msg map[string]any) map[string]any {
	t.Helper()
	res, ok := msg["result"].(map[string]any)
	if !ok {
		t.Fatalf("no result in %v", msg)
	}
	return res
}

func toolNames(t *testing.T, listResult map[string]any) []string {
	t.Helper()
	tools, ok := listResult["tools"].([]any)
	if !ok {
		t.Fatalf("no tools array in %v", listResult)
	}
	var names []string
	for _, tool := range tools {
		names = append(names, tool.(map[string]any)["name"].(string))
	}
	return names
}

// callResult unpacks a tools/call result into its first text content and the
// isError flag.
func callResult(t *testing.T, msg map[string]any) (string, bool) {
	t.Helper()
	res := result(t, msg)
	content, ok := res["content"].([]any)
	if !ok || len(content) == 0 {
		t.Fatalf("no content in %v", res)
	}
	first := content[0].(map[string]any)
	if kind := first["type"]; kind != "text" {
		t.Fatalf("content type %v, want text", kind)
	}
	isErr, _ := res["isError"].(bool)
	return first["text"].(string), isErr
}

// --- server-free behaviour -------------------------------------------------

func TestMCPInitializeEchoesProtocolAndAdvertisesTools(t *testing.T) {
	t.Setenv("NATS_URL", "")
	msgs := exchange(t, handshake())
	if len(msgs) != 1 {
		t.Fatalf("want 1 response (the notification is silent), got %d: %v", len(msgs), msgs)
	}
	res := result(t, msgs[0])
	if v := res["protocolVersion"]; v != testProtocolVersion {
		t.Errorf("protocolVersion %v, want %v", v, testProtocolVersion)
	}
	caps, ok := res["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("no capabilities in %v", res)
	}
	if _, ok := caps["tools"]; !ok {
		t.Error("capabilities do not advertise tools")
	}
	info, ok := res["serverInfo"].(map[string]any)
	if !ok || info["name"] == "" {
		t.Errorf("serverInfo missing or unnamed: %v", res["serverInfo"])
	}
}

func TestMCPToolsListIsEmptyWithoutABus(t *testing.T) {
	// The mode switch's promise is that a today install cannot tell the
	// feature exists. No NATS_URL means no bus, so no tools are advertised.
	t.Setenv("NATS_URL", "")
	msgs := exchange(t, handshake()+jrpc(2, "tools/list", nil))
	res := result(t, msgs[1])
	tools, ok := res["tools"].([]any)
	if !ok {
		t.Fatalf("tools/list result has no tools array: %v", res)
	}
	if len(tools) != 0 {
		t.Errorf("a busless install advertises %d tools, want 0: %v", len(tools), tools)
	}
}

func TestMCPBusGateTreatsAnUninterpolatedPlaceholderAsUnset(t *testing.T) {
	// The registration declares NATS_URL as "${NATS_URL}" in the mcp_servers
	// env block. Whether the harness interpolates an UNSET variable to empty
	// or passes the literal through is not pinned by anything in this repo,
	// so the gate accepts either spelling of "no bus".
	t.Setenv("NATS_URL", "${NATS_URL}")
	msgs := exchange(t, handshake()+
		jrpc(2, "tools/list", nil)+
		jrpc(3, "tools/call", map[string]any{
			"name": "topics_read", "arguments": map[string]any{"topic": "upgrade-readiness"},
		}))
	if tools := result(t, msgs[1])["tools"].([]any); len(tools) != 0 {
		t.Errorf("a placeholder NATS_URL advertises %d tools, want 0", len(tools))
	}
	text, isErr := callResult(t, msgs[2])
	if !isErr || !strings.Contains(text, "NATS_URL is not set") {
		t.Errorf("placeholder call: isError=%v text=%q, want the honest no-bus answer", isErr, text)
	}
}

func TestMCPToolsListAdvertisesTheReaders(t *testing.T) {
	// Presence of NATS_URL is the gate; tools/list does not dial.
	t.Setenv("NATS_URL", "nats://127.0.0.1:1")
	msgs := exchange(t, handshake()+jrpc(2, "tools/list", nil))
	names := toolNames(t, result(t, msgs[1]))
	want := []string{"topics_list", "topics_read"}
	if len(names) != len(want) {
		t.Fatalf("tool names %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("tool names %v, want %v", names, want)
		}
	}
	// topics_read requires its one argument by schema.
	tools := result(t, msgs[1])["tools"].([]any)
	read := tools[1].(map[string]any)
	schema := read["inputSchema"].(map[string]any)
	required, _ := schema["required"].([]any)
	if len(required) != 1 || required[0] != "topic" {
		t.Errorf("topics_read required %v, want [topic]", required)
	}
}

func TestMCPCallWithoutABusIsAnHonestError(t *testing.T) {
	// A stale Hermes schema cache can hand the model a tool the current env
	// no longer backs (a next->today flip). The answer is the skill
	// doctrine's sentence, not a stack trace.
	t.Setenv("NATS_URL", "")
	input := handshake() + jrpc(2, "tools/call", map[string]any{
		"name": "topics_read", "arguments": map[string]any{"topic": "upgrade-readiness"},
	})
	msgs := exchange(t, input)
	text, isErr := callResult(t, msgs[1])
	if !isErr {
		t.Error("busless call did not set isError")
	}
	if !strings.Contains(text, "NATS_URL is not set") {
		t.Errorf("busless call text %q does not name the missing env", text)
	}
}

func TestMCPUnknownToolIsAProtocolError(t *testing.T) {
	t.Setenv("NATS_URL", "nats://127.0.0.1:1")
	msgs := exchange(t, handshake()+jrpc(2, "tools/call", map[string]any{
		"name": "shell_exec", "arguments": map[string]any{},
	}))
	errObj, ok := msgs[1]["error"].(map[string]any)
	if !ok {
		t.Fatalf("unknown tool did not produce a JSON-RPC error: %v", msgs[1])
	}
	if code := errObj["code"].(float64); code != -32602 {
		t.Errorf("error code %v, want -32602", code)
	}
}

func TestMCPUnknownMethodAndMalformedInput(t *testing.T) {
	t.Setenv("NATS_URL", "")
	input := handshake() +
		jrpc(2, "no/such/method", nil) +
		jrpc(nil, "notifications/unknown", nil) + // notification: no reply, known or not
		"this is not json\n" +
		jrpc(3, "ping", nil)
	msgs := exchange(t, input)
	// initialize, -32601, -32700, ping - and nothing for the notification.
	if len(msgs) != 4 {
		t.Fatalf("want 4 responses, got %d: %v", len(msgs), msgs)
	}
	if code := msgs[1]["error"].(map[string]any)["code"].(float64); code != -32601 {
		t.Errorf("unknown method code %v, want -32601", code)
	}
	if code := msgs[2]["error"].(map[string]any)["code"].(float64); code != -32700 {
		t.Errorf("malformed line code %v, want -32700", code)
	}
	if _, ok := msgs[3]["result"]; !ok {
		t.Errorf("ping got no result: %v", msgs[3])
	}
}

func TestMCPConnectErrorsDoNotEchoTheBusAddress(t *testing.T) {
	// NATS URLs may carry userinfo credentials, and url.Parse quotes the whole
	// raw URL in its error. A malformed credentialed address must not arrive
	// in model-visible tool output.
	t.Setenv("NATS_URL", "nats://user:supersecret@[::1:1")
	msgs := exchange(t, handshake()+jrpc(2, "tools/call", map[string]any{
		"name": "topics_read", "arguments": map[string]any{"topic": "upgrade-readiness"},
	}))
	text, isErr := callResult(t, msgs[1])
	if !isErr {
		t.Error("connect failure did not set isError")
	}
	if strings.Contains(text, "supersecret") {
		t.Errorf("connect error echoed the credential: %q", text)
	}
	if !strings.Contains(text, "cannot connect") {
		t.Errorf("connect error text %q does not name the failure class", text)
	}
}

func TestMCPOverlongLineEndsTheSessionLoudly(t *testing.T) {
	// Dying on a >1MiB frame is the chosen posture (a real client never sends
	// one through Hermes); this pins that the limit fails the session rather
	// than hanging it, so dropping the limit or swallowing the error shows up.
	t.Setenv("NATS_URL", "")
	input := handshake() + strings.Repeat("x", mcpMaxLineBytes+1) + "\n" + jrpc(2, "ping", nil)
	var out bytes.Buffer
	err := serveMCP(strings.NewReader(input), &out)
	if !errors.Is(err, bufio.ErrTooLong) {
		t.Fatalf("serveMCP returned %v, want bufio.ErrTooLong", err)
	}
	if got := strings.Count(out.String(), "\n"); got != 1 {
		t.Errorf("wrote %d responses, want 1 (initialize only; the session dies at the fault)", got)
	}
}

// --- against a real bus ------------------------------------------------------

const testTopicSubject = "a2a.topics.shared.upgrade-readiness"

// startTopicsServer runs a real nats-server with JetStream and provisions the
// state stream the way the deployment does: the configured subject list is
// the registry.
func startTopicsServer(t *testing.T) string {
	t.Helper()
	opts := &natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true,
		StoreDir: t.TempDir(), NoLog: true, NoSigs: true,
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server not ready")
	}
	t.Cleanup(s.Shutdown)
	url := fmt.Sprintf("nats://127.0.0.1:%d", s.Addr().(*net.TCPAddr).Port)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              lib.StreamTopicsState,
		Subjects:          []string{testTopicSubject},
		MaxMsgsPerSubject: 8,
	}); err != nil {
		t.Fatalf("create %s: %v", lib.StreamTopicsState, err)
	}
	return url
}

func seedEntry(t *testing.T, url, summary string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := lib.Connect(ctx, url, lib.WithName("mcp-test-seed"))
	if err != nil {
		t.Fatalf("lib.Connect: %v", err)
	}
	defer c.Close()
	artifact, err := lib.NewTopicArtifact("upgrade-readiness", summary,
		map[string]any{"ready": 2, "of": 3})
	if err != nil {
		t.Fatalf("NewTopicArtifact: %v", err)
	}
	err = c.PublishTopic(ctx, testTopicSubject,
		lib.Party{Session: "seed"}, "", "", lib.NewCorrelationID(), artifact)
	if err != nil {
		t.Fatalf("PublishTopic: %v", err)
	}
}

func TestMCPTopicsReadRendersTheEntry(t *testing.T) {
	url := startTopicsServer(t)
	seedEntry(t, url, "two of three clusters are ready")
	t.Setenv("NATS_URL", url)
	msgs := exchange(t, handshake()+jrpc(2, "tools/call", map[string]any{
		"name": "topics_read", "arguments": map[string]any{"topic": "upgrade-readiness"},
	}))
	text, isErr := callResult(t, msgs[1])
	if isErr {
		t.Fatalf("read reported isError: %q", text)
	}
	// The rendered shape is the skill's contract: provenance first, then the
	// summary, then the structured state.
	for _, want := range []string{
		"topic:       upgrade-readiness (state class, " + testTopicSubject + ")",
		"written by:  seed at ",
		"two of three clusters are ready",
		"\"ready\": 2",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("rendered entry missing %q in:\n%s", want, text)
		}
	}
}

func TestMCPTopicsReadEmptyTopicIsAnAnswerNotAFailure(t *testing.T) {
	url := startTopicsServer(t)
	t.Setenv("NATS_URL", url)
	msgs := exchange(t, handshake()+jrpc(2, "tools/call", map[string]any{
		"name": "topics_read", "arguments": map[string]any{"topic": "upgrade-readiness"},
	}))
	text, isErr := callResult(t, msgs[1])
	if isErr {
		t.Errorf("an empty provisioned topic is a real answer, not isError (text %q)", text)
	}
	if !strings.Contains(text, "provisioned but has no entries yet") {
		t.Errorf("empty-topic text %q does not say so", text)
	}
}

func TestMCPTopicsReadUnknownTopicIsAToolError(t *testing.T) {
	url := startTopicsServer(t)
	t.Setenv("NATS_URL", url)
	msgs := exchange(t, handshake()+jrpc(2, "tools/call", map[string]any{
		"name": "topics_read", "arguments": map[string]any{"topic": "no-such-topic"},
	}))
	text, isErr := callResult(t, msgs[1])
	if !isErr {
		t.Error("unknown topic did not set isError")
	}
	if !strings.Contains(text, "no provisioned topic") {
		t.Errorf("unknown-topic text %q does not name the refusal", text)
	}
}

func TestMCPTopicsReadEnvelopeReturnsTheRawEnvelope(t *testing.T) {
	url := startTopicsServer(t)
	seedEntry(t, url, "raw check")
	t.Setenv("NATS_URL", url)
	msgs := exchange(t, handshake()+jrpc(2, "tools/call", map[string]any{
		"name":      "topics_read",
		"arguments": map[string]any{"topic": "upgrade-readiness", "envelope": true},
	}))
	text, isErr := callResult(t, msgs[1])
	if isErr {
		t.Fatalf("envelope read reported isError: %q", text)
	}
	var env map[string]any
	if err := json.Unmarshal([]byte(text), &env); err != nil {
		t.Fatalf("envelope text is not JSON: %v\n%s", err, text)
	}
	if env["kind"] != "topic-update" {
		t.Errorf("envelope kind %v, want topic-update", env["kind"])
	}
}

func TestMCPTopicsListRendersTheRegistry(t *testing.T) {
	url := startTopicsServer(t)
	t.Setenv("NATS_URL", url)
	msgs := exchange(t, handshake()+jrpc(2, "tools/call", map[string]any{
		"name": "topics_list", "arguments": map[string]any{},
	}))
	text, isErr := callResult(t, msgs[1])
	if isErr {
		t.Fatalf("list reported isError: %q", text)
	}
	for _, want := range []string{"TOPIC", "upgrade-readiness", "state", testTopicSubject} {
		if !strings.Contains(text, want) {
			t.Errorf("registry rendering missing %q in:\n%s", want, text)
		}
	}
}
