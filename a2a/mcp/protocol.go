// Package mcp is the kube-agents Model Context Protocol server: a JSON-RPC
// 2.0 surface over HTTP that lets an external coding agent or IDE host talk to
// one in-cluster `next` install as a console client.
//
// It runs as its own Deployment (a2a/cmd/mcp), holding the console credential
// and nothing else, and only when the install opts in. A caller authenticates
// with the server's own bearer token; the bus never sees that token, and the
// server never takes a caller's word for which bus to dial.
//
// This is the core: the protocol, the transport, the one bus connection and
// the connection_status tool. The tools that send turns, steer tasks and read
// topics are registered on top of it (gke-labs/kube-agents#3653 is where they
// come from).
package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

const (
	jsonRPCVersion = "2.0"

	// ServerName and ServerVersion are what initialize reports.
	ServerName    = "kube-agents"
	ServerVersion = "0.5.0"

	methodInitialize    = "initialize"
	methodPing          = "ping"
	methodToolsList     = "tools/list"
	methodToolsCall     = "tools/call"
	methodResourcesList = "resources/list"
	methodPromptsList   = "prompts/list"

	rpcErrParseError     = -32700
	rpcErrInvalidRequest = -32600
	rpcErrMethodNotFound = -32601
	rpcErrInvalidParams  = -32602
	rpcErrInternal       = -32603

	contentTypeText = "text"

	msgInvalidFrame   = "invalid JSON-RPC frame"
	msgMissingMethod  = "missing method"
	msgNeedsToolName  = "tools/call requires a tool name"
	msgUnknownMethod  = "unknown method %q"
	msgUnknownTool    = "unknown tool %q"
	msgToolFailedHead = "**Error in `%s`**: %v"
)

// supportedProtocolVersions are the MCP revisions this server speaks, newest
// first. initialize answers with the client's requested revision when it is
// one of these and with the newest otherwise, which is the spec's negotiation:
// the client then decides whether it can proceed.
var supportedProtocolVersions = []string{"2025-06-18", "2025-03-26", "2024-11-05"}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ToolDefinition is one entry of tools/list.
type ToolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// TextContent is one content block of a tool result.
type TextContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// CallToolResult is what tools/call answers with.
type CallToolResult struct {
	Content []TextContent `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

// Tool is one registered tool: its schema and the function that runs it. The
// function returns markdown for the caller to read; an error becomes an
// isError result naming the tool, so a failed tool never fails the RPC.
type Tool struct {
	Definition ToolDefinition
	Call       func(ctx context.Context, args json.RawMessage) (string, error)
}

// Server is the protocol core: it dispatches JSON-RPC frames to the tools it
// was built with. It holds no transport and no bus; those are the tools' and
// the HTTP handler's.
type Server struct {
	tools map[string]Tool
	order []string
}

// NewServer builds a Server over tools, listed in the order given.
func NewServer(tools ...Tool) (*Server, error) {
	s := &Server{tools: make(map[string]Tool, len(tools))}
	for _, t := range tools {
		name := t.Definition.Name
		if name == "" || t.Call == nil {
			return nil, errors.New("a tool needs a name and a function")
		}
		if _, dup := s.tools[name]; dup {
			return nil, fmt.Errorf("tool %q registered twice", name)
		}
		s.tools[name] = t
		s.order = append(s.order, name)
	}
	return s, nil
}

// HandleJSONRPC answers one JSON-RPC frame. A notification (no id) gets no
// answer, which the caller reports as nil.
func (s *Server) HandleJSONRPC(ctx context.Context, raw []byte) []byte {
	var req rpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return marshalRPCError(json.RawMessage("null"), rpcErrParseError, msgInvalidFrame)
	}
	if len(req.ID) == 0 || string(req.ID) == "null" {
		if req.Method == "" {
			return marshalRPCError(json.RawMessage("null"), rpcErrInvalidRequest, msgMissingMethod)
		}
		return nil
	}

	switch req.Method {
	case methodInitialize:
		var params struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &params)
		version := supportedProtocolVersions[0]
		if slices.Contains(supportedProtocolVersions, params.ProtocolVersion) {
			version = params.ProtocolVersion
		}
		return marshalRPCResult(req.ID, map[string]any{
			"protocolVersion": version,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": ServerName, "version": ServerVersion},
		})
	case methodPing:
		return marshalRPCResult(req.ID, map[string]any{})
	case methodToolsList:
		defs := make([]ToolDefinition, 0, len(s.order))
		for _, name := range s.order {
			defs = append(defs, s.tools[name].Definition)
		}
		return marshalRPCResult(req.ID, map[string]any{"tools": defs})
	case methodToolsCall:
		var params struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil || params.Name == "" {
			return marshalRPCError(req.ID, rpcErrInvalidParams, msgNeedsToolName)
		}
		return marshalRPCResult(req.ID, s.CallTool(ctx, params.Name, params.Arguments))
	// Neither is a capability this server declares; an empty list is the
	// polite answer to a client that asks anyway.
	case methodResourcesList:
		return marshalRPCResult(req.ID, map[string]any{"resources": []any{}})
	case methodPromptsList:
		return marshalRPCResult(req.ID, map[string]any{"prompts": []any{}})
	default:
		return marshalRPCError(req.ID, rpcErrMethodNotFound, fmt.Sprintf(msgUnknownMethod, req.Method))
	}
}

// CallTool runs one tool by name.
func (s *Server) CallTool(ctx context.Context, name string, args json.RawMessage) CallToolResult {
	t, ok := s.tools[name]
	if !ok {
		return CallToolResult{Content: []TextContent{{Type: contentTypeText, Text: fmt.Sprintf(msgUnknownTool, name)}}, IsError: true}
	}
	text, err := t.Call(ctx, args)
	if err != nil {
		msg := fmt.Sprintf(msgToolFailedHead, name, err)
		if text != "" {
			msg = text + "\n\n" + msg
		}
		return CallToolResult{Content: []TextContent{{Type: contentTypeText, Text: msg}}, IsError: true}
	}
	return CallToolResult{Content: []TextContent{{Type: contentTypeText, Text: text}}}
}

func marshalRPCResult(id json.RawMessage, result any) []byte {
	b, err := json.Marshal(rpcResponse{JSONRPC: jsonRPCVersion, ID: id, Result: result})
	if err != nil {
		return marshalRPCError(id, rpcErrInternal, err.Error())
	}
	return b
}

func marshalRPCError(id json.RawMessage, code int, msg string) []byte {
	b, _ := json.Marshal(rpcResponse{JSONRPC: jsonRPCVersion, ID: id, Error: &rpcError{Code: code, Message: msg}})
	return b
}
