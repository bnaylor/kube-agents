// The CLI's MCP mode: `a2a mcp` serves the topics reader as Model Context
// Protocol tools over stdio, newline-delimited JSON-RPC 2.0.
//
// This exists because the agent pod no longer runs model-issued commands
// (#913): a skill that shells `a2a topics read` executes in the shell
// sandbox, which by design holds no credentials and has no route to the bus.
// An MCP server is a process the harness spawns in the agent container, so
// the bus credential stays where the deployment already put it and the
// sandbox needs nothing. The model calls a tool; no shell is involved.
//
// The read-only surface is deliberate. Which principal a model-driven write
// should act as is an open authority question (per-session credentials are
// being designed); a write tool today would answer it by accident with the
// pod's shared static user. Reads carry no such question - the reader grant
// is the pod's own.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// mcpProtocolVersionFallback is what initialize answers when the client
// names no version. When it names one, we echo it: this server's surface
// (initialize, ping, tools/list, tools/call with text content) is the subset
// that has been stable across every published protocol revision.
const mcpProtocolVersionFallback = "2025-06-18"

const (
	mcpServerName    = "a2a-topics"
	mcpServerVersion = "0.1.0"
)

// mcpMaxLineBytes bounds one incoming message. Requests here are tool calls
// with a topic name, not payloads; a line this long is a protocol fault.
const mcpMaxLineBytes = 1 << 20

// JSON-RPC 2.0 error codes, by their spec names.
const (
	jsonrpcParseError     = -32700
	jsonrpcMethodNotFound = -32601
	jsonrpcInvalidParams  = -32602
)

// mcpNoBusMessage is the honest no-bus answer, the same sentence the skill
// doctrine uses. A call can arrive without a bus when the harness's schema
// cache is stale across a mode flip; the model relays this rather than
// guessing an address.
const mcpNoBusMessage = "a2a: NATS_URL is not set - this install is not running the A2A bus; say so and answer another way"

type jsonrpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type jsonrpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *jsonrpcError   `json:"error,omitempty"`
}

type mcpTool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	InputSchema any    `json:"inputSchema"`
}

type mcpContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type mcpToolResult struct {
	Content []mcpContent `json:"content"`
	IsError bool         `json:"isError,omitempty"`
}

func runMCP(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("mcp takes no arguments (got %q)", strings.Join(args, " "))
	}
	return serveMCP(os.Stdin, os.Stdout)
}

// serveMCP handles one client for the life of the stream. Requests are
// answered in order; notifications are consumed silently. stdout carries
// protocol frames only - anything human-readable belongs on stderr.
func serveMCP(r io.Reader, w io.Writer) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), mcpMaxLineBytes)
	enc := json.NewEncoder(w)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var req jsonrpcRequest
		if err := json.Unmarshal(line, &req); err != nil {
			if err := enc.Encode(jsonrpcResponse{
				JSONRPC: "2.0",
				ID:      json.RawMessage("null"),
				Error:   &jsonrpcError{Code: jsonrpcParseError, Message: "parse error: " + err.Error()},
			}); err != nil {
				return err
			}
			continue
		}
		if isNotification(req) {
			continue
		}
		resp := jsonrpcResponse{JSONRPC: "2.0", ID: req.ID}
		switch req.Method {
		case "initialize":
			resp.Result = handleInitialize(req.Params)
		case "ping":
			resp.Result = struct{}{}
		case "tools/list":
			resp.Result = map[string]any{"tools": toolCatalog()}
		case "tools/call":
			resp.Result, resp.Error = handleToolCall(req.Params)
		default:
			resp.Error = &jsonrpcError{Code: jsonrpcMethodNotFound, Message: "method not found: " + req.Method}
		}
		if err := enc.Encode(resp); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// isNotification: no id at all, or an explicit null. Nothing to correlate a
// response to, so nothing is ever written back - including for methods this
// server does not know.
func isNotification(req jsonrpcRequest) bool {
	return len(req.ID) == 0 || string(req.ID) == "null"
}

func handleInitialize(params json.RawMessage) any {
	var p struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	_ = json.Unmarshal(params, &p) // absent params just means the fallback
	version := p.ProtocolVersion
	if version == "" {
		version = mcpProtocolVersionFallback
	}
	return map[string]any{
		"protocolVersion": version,
		"capabilities":    map[string]any{"tools": map[string]any{}},
		"serverInfo":      map[string]any{"name": mcpServerName, "version": mcpServerVersion},
	}
}

// toolCatalog is empty when the environment carries no bus address. The mode
// switch's promise is that a today install cannot tell the A2A stack exists,
// and this server may be registered statically - so darkness is enforced
// here, at the one place that knows.
func toolCatalog() []mcpTool {
	if !busConfigured() {
		return []mcpTool{}
	}
	return []mcpTool{
		{
			Name: "topics_list",
			Description: "List the provisioned A2A topics - the durable blackboard where " +
				"agents publish standing state. The list is the authority on what is " +
				"readable: a topic not in it does not exist. The CLASS column matters: " +
				"'state' reads return the current answer, 'journal' reads return only " +
				"the most recent dated observation.",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			Name: "topics_read",
			Description: "Read the latest entry on a provisioned A2A topic. The output " +
				"leads with provenance - who wrote it and when - and that is part of " +
				"the answer: relay it, and do not present the entry as something you " +
				"just established. A topic that is provisioned but unwritten is a real " +
				"answer ('no one has assessed this'), not a failure.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"topic": map[string]any{
						"type": "string",
						"description": "The topic name: bare (upgrade-readiness), scope-qualified " +
							"(shared.blueprint, agent.platform.upgrade-readiness), or a full subject. " +
							"A bare name matching more than one provisioned topic is an error, not a guess.",
					},
					"envelope": map[string]any{
						"type": "boolean",
						"description": "Return the raw envelope as JSON instead of the rendered " +
							"summary. For tracing how standing state got its value (task id, " +
							"correlation id), not for ordinary reads.",
					},
				},
				"required": []string{"topic"},
			},
		},
	}
}

// busConfigured is the darkness gate's question: does the environment carry a
// usable bus address? Empty means no. So does a value still wearing the
// registration's "${NATS_URL}" placeholder: the mcp_servers env block declares
// the variable by interpolation, and nothing in this repository pins whether
// the harness resolves an UNSET variable to empty or passes the literal
// through - so both spellings of "no bus" are treated the same.
func busConfigured() bool {
	url := os.Getenv("NATS_URL")
	return url != "" && !strings.HasPrefix(url, "${")
}

func handleToolCall(params json.RawMessage) (any, *jsonrpcError) {
	var call struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(params, &call); err != nil {
		return nil, &jsonrpcError{Code: jsonrpcInvalidParams, Message: "malformed tools/call params: " + err.Error()}
	}
	switch call.Name {
	case "topics_list", "topics_read":
	default:
		return nil, &jsonrpcError{Code: jsonrpcInvalidParams, Message: "unknown tool: " + call.Name}
	}
	if !busConfigured() {
		return toolError(mcpNoBusMessage), nil
	}
	switch call.Name {
	case "topics_list":
		return toolTopicsList()
	default:
		return toolTopicsRead(call.Arguments)
	}
}

func toolTopicsList() (any, *jsonrpcError) {
	ctx, cancel := cliContext()
	defer cancel()
	c, err := connect(ctx, "mcp")
	if err != nil {
		return toolError("a2a: " + err.Error()), nil
	}
	defer c.Close()
	registry, err := c.TopicRegistry(ctx)
	if err != nil {
		return toolError("a2a: " + err.Error()), nil
	}
	var buf strings.Builder
	if err := writeRegistry(&buf, registry); err != nil {
		return toolError("a2a: " + err.Error()), nil
	}
	return toolText(buf.String()), nil
}

func toolTopicsRead(arguments json.RawMessage) (any, *jsonrpcError) {
	var args struct {
		Topic    string `json:"topic"`
		Envelope bool   `json:"envelope"`
	}
	if len(arguments) > 0 {
		if err := json.Unmarshal(arguments, &args); err != nil {
			return nil, &jsonrpcError{Code: jsonrpcInvalidParams, Message: "malformed topics_read arguments: " + err.Error()}
		}
	}
	if args.Topic == "" {
		return nil, &jsonrpcError{Code: jsonrpcInvalidParams, Message: "topics_read: topic is required"}
	}
	ctx, cancel := cliContext()
	defer cancel()
	c, err := connect(ctx, "mcp")
	if err != nil {
		return toolError("a2a: " + err.Error()), nil
	}
	defer c.Close()
	entry, err := resolve(ctx, c, args.Topic)
	if err != nil {
		return toolError("a2a: " + err.Error()), nil
	}
	env, err := c.ReadTopicLatest(ctx, entry.Stream, entry.Subject)
	if err != nil {
		if errors.Is(err, lib.ErrTopicEmpty) {
			// The CLI's exit-2 case: a legal state and a real answer.
			return toolText(emptyTopicMessage(entry)), nil
		}
		return toolError("a2a: " + err.Error()), nil
	}
	var buf strings.Builder
	if args.Envelope {
		enc := json.NewEncoder(&buf)
		enc.SetIndent("", "  ")
		if err := enc.Encode(env); err != nil {
			return toolError("a2a: " + err.Error()), nil
		}
	} else if err := printEntry(&buf, entry, env); err != nil {
		return toolError("a2a: " + err.Error()), nil
	}
	return toolText(buf.String()), nil
}

func toolText(text string) mcpToolResult {
	return mcpToolResult{Content: []mcpContent{{Type: "text", Text: text}}}
}

func toolError(text string) mcpToolResult {
	return mcpToolResult{Content: []mcpContent{{Type: "text", Text: text}}, IsError: true}
}
