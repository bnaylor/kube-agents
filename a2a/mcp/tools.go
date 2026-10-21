package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// The turn tools: send a turn as a console user, and read or stop its task.
const (
	ToolSendMessage = "send_message"
	ToolTaskControl = "task_control"

	actionGet  = "get"
	actionStop = "stop"
	// stopText is the console door's cancel: a turn whose whole text is a
	// stop word cancels the conversation's running task (chattext.IsStop).
	stopText = "stop"

	defaultWaitSeconds = 60
	maxWaitSeconds     = 300
	taskPollEvery      = 2 * time.Second

	// maxAnswerBytes caps the answer shown in one result; the full answer
	// stays on the task.
	maxAnswerBytes = 64 * 1024
)

// MaxTurnWait is the longest a turn tool holds a request: the wait for the
// gateway's submission plus the longest answer wait a caller may ask for.
const MaxTurnWait = submissionWait + maxWaitSeconds*time.Second

// TurnBus is what the turn tools need from the bus.
type TurnBus interface {
	SendTurn(ctx context.Context, convs *Conversations, token, text string) (TurnResult, error)
	ReadTask(ctx context.Context, taskID string) (*lib.Task, error)
	WaitForTask(ctx context.Context, taskID string, wait, every time.Duration) (*lib.Task, error)
}

type sendMessageArgs struct {
	Text              string `json:"text"`
	Conversation      string `json:"conversation"`
	ConversationToken string `json:"conversation_token"`
	WaitSeconds       int    `json:"wait_seconds"`
}

// SendMessageTool sends one turn to the install's agents and waits a while
// for its answer.
func SendMessageTool(bus TurnBus, convs *Conversations) Tool {
	return Tool{
		Definition: ToolDefinition{
			Name: ToolSendMessage,
			Description: "Send a message to this kube-agents install, as a console user, and wait for the answer. " +
				"A turn that is still running when the wait ends returns its task id; read it later with task_control.",
			InputSchema: map[string]any{
				"type":     "object",
				"required": []string{"text"},
				"properties": map[string]any{
					"text":               map[string]any{"type": "string", "description": "The message."},
					"conversation":       map[string]any{"type": "string", "description": "A name for the conversation; turns with the same name continue it. Default: " + DefaultConversation + "."},
					"conversation_token": map[string]any{"type": "string", "description": "Continue a conversation by the token a previous answer reported, instead of by name."},
					"wait_seconds":       map[string]any{"type": "integer", "description": fmt.Sprintf("How long to wait for the answer, %d at most. Default %d.", maxWaitSeconds, defaultWaitSeconds)},
				},
			},
		},
		Call: func(ctx context.Context, raw json.RawMessage) (string, error) {
			var a sendMessageArgs
			if err := json.Unmarshal(orEmptyObject(raw), &a); err != nil {
				return "", fmt.Errorf("arguments: %w", err)
			}
			token, err := convs.Resolve(a.Conversation, a.ConversationToken)
			if err != nil {
				return "", err
			}
			res, err := bus.SendTurn(ctx, convs, token, a.Text)
			if err != nil {
				return "", err
			}
			var sb strings.Builder
			writeConversation(&sb, a.Conversation, token)
			writeNotices(&sb, res.Notices)
			if res.TaskID == "" {
				if len(res.Notices) == 0 {
					sb.WriteString("The gateway answered with no task.\n")
				}
				return sb.String(), nil
			}
			task, err := bus.WaitForTask(ctx, res.TaskID, waitFor(a.WaitSeconds), taskPollEvery)
			if err != nil {
				fmt.Fprintf(&sb, "Task `%s` was submitted, but reading it failed: %v\n", res.TaskID, err)
				return sb.String(), nil
			}
			writeTask(&sb, res.TaskID, task)
			return sb.String(), nil
		},
	}
}

type taskControlArgs struct {
	Action            string `json:"action"`
	TaskID            string `json:"task_id"`
	Conversation      string `json:"conversation"`
	ConversationToken string `json:"conversation_token"`
	WaitSeconds       int    `json:"wait_seconds"`
}

// TaskControlTool reads a task, or stops a conversation's running task.
func TaskControlTool(bus TurnBus, convs *Conversations) Tool {
	return Tool{
		Definition: ToolDefinition{
			Name: ToolTaskControl,
			Description: "Read a task's state and answer by id (action get, optionally waiting for it to finish), " +
				"or stop the running task of a conversation (action stop).",
			InputSchema: map[string]any{
				"type":     "object",
				"required": []string{"action"},
				"properties": map[string]any{
					"action":             map[string]any{"type": "string", "enum": []string{actionGet, actionStop}},
					"task_id":            map[string]any{"type": "string", "description": "The task to read (get)."},
					"conversation":       map[string]any{"type": "string", "description": "The conversation whose task to stop (stop). Default: " + DefaultConversation + "."},
					"conversation_token": map[string]any{"type": "string", "description": "The conversation, by token (stop)."},
					"wait_seconds":       map[string]any{"type": "integer", "description": fmt.Sprintf("For get: wait up to this long for the task to finish, %d at most. Default 0.", maxWaitSeconds)},
				},
			},
		},
		Call: func(ctx context.Context, raw json.RawMessage) (string, error) {
			var a taskControlArgs
			if err := json.Unmarshal(orEmptyObject(raw), &a); err != nil {
				return "", fmt.Errorf("arguments: %w", err)
			}
			switch a.Action {
			case actionGet:
				if a.TaskID == "" {
					return "", errors.New("get needs a task_id")
				}
				var task *lib.Task
				var err error
				if a.WaitSeconds > 0 {
					task, err = bus.WaitForTask(ctx, a.TaskID, waitFor(a.WaitSeconds), taskPollEvery)
				} else {
					task, err = bus.ReadTask(ctx, a.TaskID)
				}
				if err != nil {
					return "", err
				}
				var sb strings.Builder
				writeTask(&sb, a.TaskID, task)
				return sb.String(), nil
			case actionStop:
				token, err := convs.Resolve(a.Conversation, a.ConversationToken)
				if err != nil {
					return "", err
				}
				res, err := bus.SendTurn(ctx, convs, token, stopText)
				if err != nil {
					return "", err
				}
				var sb strings.Builder
				writeConversation(&sb, a.Conversation, token)
				sb.WriteString("Sent stop.\n")
				writeNotices(&sb, res.Notices)
				return sb.String(), nil
			default:
				return "", fmt.Errorf("unknown action %q; use %s or %s", a.Action, actionGet, actionStop)
			}
		},
	}
}

func orEmptyObject(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return json.RawMessage("{}")
	}
	return raw
}

func waitFor(seconds int) time.Duration {
	switch {
	case seconds <= 0:
		seconds = defaultWaitSeconds
	case seconds > maxWaitSeconds:
		seconds = maxWaitSeconds
	}
	return time.Duration(seconds) * time.Second
}

func writeConversation(sb *strings.Builder, alias, token string) {
	if alias == "" {
		alias = DefaultConversation
	}
	fmt.Fprintf(sb, "Conversation `%s` (token `%s`).\n\n", alias, token)
}

func writeNotices(sb *strings.Builder, notices []string) {
	if len(notices) == 0 {
		return
	}
	sb.WriteString("Gateway notices:\n")
	for _, n := range notices {
		fmt.Fprintf(sb, "- %s\n", cell(n))
	}
	sb.WriteString("\n")
}

// writeTask renders a task's state and, once it has one, its answer. The
// answer is the agents' own markdown, shown as written: it is what the
// caller asked for. Progress shows while the task runs.
func writeTask(sb *strings.Builder, taskID string, t *lib.Task) {
	if t == nil {
		fmt.Fprintf(sb, "Task `%s`: no events yet.\n", taskID)
		return
	}
	fmt.Fprintf(sb, "Task `%s`: **%s**", taskID, t.State)
	if !t.Final {
		sb.WriteString(" (still running; read it again with task_control get)")
	}
	sb.WriteString("\n\n")
	if answer := artifactText(t, lib.ArtifactResult); answer != "" {
		sb.WriteString(capText(answer))
		sb.WriteString("\n")
	} else if t.Final && t.FinalMessage != nil {
		if msg := partsText(t.FinalMessage.Parts); msg != "" {
			sb.WriteString(capText(msg))
			sb.WriteString("\n")
		}
	} else if p := artifactText(t, lib.ArtifactProgress); p != "" && !t.Final {
		fmt.Fprintf(sb, "Progress: %s\n", cell(lastLine(p)))
	}
}

func artifactText(t *lib.Task, name string) string {
	a := t.Artifact(name)
	if a == nil {
		return ""
	}
	return partsText(a.Parts)
}

func partsText(parts []lib.Part) string {
	var sb strings.Builder
	for _, p := range parts {
		if p.Text != "" {
			sb.WriteString(p.Text)
		}
	}
	return strings.TrimSpace(sb.String())
}

func lastLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.LastIndex(s, "\n"); i >= 0 {
		return s[i+1:]
	}
	return s
}

func capText(s string) string {
	if len(s) <= maxAnswerBytes {
		return s
	}
	cut := maxAnswerBytes
	for cut > 0 && !utf8RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "\n\n…(answer shortened; the full answer is on the task)"
}

func utf8RuneStart(b byte) bool { return b&0xC0 != 0x80 }
