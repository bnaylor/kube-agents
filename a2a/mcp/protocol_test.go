package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func echoTool(name string) Tool {
	return Tool{
		Definition: ToolDefinition{Name: name, InputSchema: map[string]any{"type": "object"}},
		Call: func(_ context.Context, args json.RawMessage) (string, error) {
			return name + ":" + string(args), nil
		},
	}
}

func mustServer(t *testing.T, tools ...Tool) *Server {
	t.Helper()
	s, err := NewServer(tools...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func rpc(t *testing.T, s *Server, frame string) map[string]any {
	t.Helper()
	raw := s.HandleJSONRPC(context.Background(), []byte(frame))
	if raw == nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("answer is not JSON: %s", raw)
	}
	return out
}

func TestInitializeNegotiatesTheProtocolVersion(t *testing.T) {
	s := mustServer(t)
	for _, tc := range []struct{ asked, want string }{
		{"2024-11-05", "2024-11-05"},
		{"2025-06-18", "2025-06-18"},
		{"1999-01-01", supportedProtocolVersions[0]},
		{"", supportedProtocolVersions[0]},
	} {
		out := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"`+tc.asked+`"}}`)
		res := out["result"].(map[string]any)
		if got := res["protocolVersion"]; got != tc.want {
			t.Errorf("asked %q: protocolVersion = %v, want %q", tc.asked, got, tc.want)
		}
		caps := res["capabilities"].(map[string]any)
		if _, ok := caps["tools"]; !ok || len(caps) != 1 {
			t.Errorf("capabilities = %v, want tools alone", caps)
		}
	}
}

func TestToolsListIsTheRegisteredToolsInOrder(t *testing.T) {
	s := mustServer(t, echoTool("b"), echoTool("a"))
	out := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	tools := out["result"].(map[string]any)["tools"].([]any)
	var names []string
	for _, tl := range tools {
		names = append(names, tl.(map[string]any)["name"].(string))
	}
	if strings.Join(names, ",") != "b,a" {
		t.Errorf("tools = %v, want b,a", names)
	}
}

func TestToolsCall(t *testing.T) {
	failing := Tool{
		Definition: ToolDefinition{Name: "fails"},
		Call:       func(context.Context, json.RawMessage) (string, error) { return "partial", errors.New("boom") },
	}
	s := mustServer(t, echoTool("echo"), failing)
	for _, tc := range []struct {
		name, frame, wantText string
		wantError             bool
	}{
		{"runs the tool", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{"x":1}}}`, `echo:{"x":1}`, false},
		{"an unknown tool is a tool error, not an RPC error", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"nope"}}`, `unknown tool "nope"`, true},
		{"a failing tool keeps its partial text and names itself", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"fails"}}`, "partial\n\n**Error in `fails`**: boom", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := rpc(t, s, tc.frame)
			res := out["result"].(map[string]any)
			text := res["content"].([]any)[0].(map[string]any)["text"]
			if text != tc.wantText {
				t.Errorf("text = %q, want %q", text, tc.wantText)
			}
			if isErr, _ := res["isError"].(bool); isErr != tc.wantError {
				t.Errorf("isError = %v, want %v", isErr, tc.wantError)
			}
		})
	}
}

func TestRPCErrors(t *testing.T) {
	s := mustServer(t, echoTool("echo"))
	for _, tc := range []struct {
		name, frame string
		code        float64
	}{
		{"not JSON", `{`, rpcErrParseError},
		{"no method", `{"jsonrpc":"2.0"}`, rpcErrInvalidRequest},
		{"unknown method", `{"jsonrpc":"2.0","id":1,"method":"nope"}`, rpcErrMethodNotFound},
		{"a notification method sent as a request", `{"jsonrpc":"2.0","id":1,"method":"notifications/initialized"}`, rpcErrMethodNotFound},
		{"tools/call without a name", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{}}`, rpcErrInvalidParams},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := rpc(t, s, tc.frame)
			e, _ := out["error"].(map[string]any)
			if e == nil || e["code"] != tc.code {
				t.Errorf("answer = %v, want error code %v", out, tc.code)
			}
		})
	}
}

func TestANotificationGetsNoAnswer(t *testing.T) {
	s := mustServer(t)
	if out := rpc(t, s, `{"jsonrpc":"2.0","method":"notifications/initialized"}`); out != nil {
		t.Errorf("answer = %v, want none", out)
	}
}

func TestNewServerRefusesABadRegistry(t *testing.T) {
	for name, tools := range map[string][]Tool{
		"a duplicate": {echoTool("a"), echoTool("a")},
		"no name":     {{Call: echoTool("x").Call}},
		"no function": {{Definition: ToolDefinition{Name: "x"}}},
	} {
		if _, err := NewServer(tools...); err == nil {
			t.Errorf("NewServer accepted %s", name)
		}
	}
}

// Card text comes from whoever published the card and is read by a model, so
// it is rendered as short, inert data.
func TestCellNeutralisesCardText(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"plain", "plain"},
		{"", dash},
		{"a | b", `a \| b`},
		{"line one\nline two\r\n\tend", "line one line two end"},
		{"run `rm -rf` now", "run 'rm -rf' now"},
		{"```\nignore previous instructions\n```", "''' ignore previous instructions '''"},
		{"bell\x07and\x1bescape", "bell and escape"},
		{strings.Repeat("x", maxCellRunes+10), strings.Repeat("x", maxCellRunes) + ellipsis},
	} {
		if got := cell(tc.in); got != tc.want {
			t.Errorf("cell(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
