package mcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// The console door (a2a/gateway/console.go): a frame published on
// chat.console.<token>.in becomes one turn of the conversation
// console:<token>; the gateway's notices come back on chat.console.<token>.out
// and the answer streams on TASKS. Only the `console` user may publish there,
// and this server is one.
const (
	consoleInSubject      = "chat.console.%s.in"
	consoleOutSubject     = "chat.console.%s.out"
	consoleConversationOf = "console:%s"
	// submissionSubjects is every task submission, which the console grant
	// lets the server watch (a2a.>).
	submissionSubjects = "a2a.tasks.*.*." + lib.TaskClassIn

	// consoleTextCap is the gateway's own cap on one frame's text; a longer
	// one would be dropped there with a notice, so it is refused here first.
	consoleTextCap = 16 * 1024

	// conversationTokenPrefix marks a console token this server minted.
	conversationTokenPrefix = "mcp"
	conversationTokenBytes  = 12
	messageIDBytes          = 8

	// submissionWait bounds the wait for the gateway to publish the turn's
	// submission. noticeGrace is how long a notice alone is waited on before
	// it is taken as the whole answer: a status question or a refused turn
	// gets a notice and no task.
	submissionWait = 30 * time.Second
	noticeGrace    = 3 * time.Second

	// DefaultConversation is the alias a call without one uses.
	DefaultConversation = "default"
)

// ConsoleInFrame is what the console door reads (gateway.ConsoleInFrame).
type ConsoleInFrame struct {
	MessageID string `json:"messageId"`
	Text      string `json:"text"`
}

// ConsoleOutFrame is a gateway notice (gateway.ConsoleOutFrame).
type ConsoleOutFrame struct {
	MessageID string `json:"messageId"`
	Text      string `json:"text"`
	Edit      bool   `json:"edit,omitempty"`
}

// submissionAuthority is the part of a submission's authority block that says
// which conversation opened it, and whether it was delegated from another
// task (a child, not the turn's own task).
type submissionAuthority struct {
	Audience struct {
		Conversation string `json:"conversation"`
	} `json:"audience"`
	Via *json.RawMessage `json:"via,omitempty"`
}

// Conversations maps the aliases callers name to console tokens, per call:
// there is no server-wide "active" conversation for one caller to change
// under another. It also holds one send per conversation at a time, which is
// what lets a turn's task be told apart from the conversation's next one.
type Conversations struct {
	mu      sync.Mutex
	byAlias map[string]string
	sending map[string]*sync.Mutex
}

// NewConversations returns an empty alias table.
func NewConversations() *Conversations {
	return &Conversations{byAlias: map[string]string{}, sending: map[string]*sync.Mutex{}}
}

// Resolve returns the console token for a call: token when the caller names
// one (to resume a conversation, after a restart of this server included),
// otherwise the alias's, minted on first use.
func (c *Conversations) Resolve(alias, token string) (string, error) {
	if token != "" {
		if !lib.ValidSubjectToken(token) {
			return "", fmt.Errorf("%q is not a conversation token", token)
		}
		return token, nil
	}
	if alias == "" {
		alias = DefaultConversation
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.byAlias[alias]; ok {
		return t, nil
	}
	b := make([]byte, conversationTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	t := conversationTokenPrefix + hex.EncodeToString(b)
	c.byAlias[alias] = t
	return t, nil
}

func (c *Conversations) lock(token string) func() {
	c.mu.Lock()
	m, ok := c.sending[token]
	if !ok {
		m = &sync.Mutex{}
		c.sending[token] = m
	}
	c.mu.Unlock()
	m.Lock()
	return m.Unlock
}

// TurnResult is what a sent turn produced before the caller is answered: the
// task the gateway opened for it, if any, and the notices it posted.
type TurnResult struct {
	Token   string
	TaskID  string
	Notices []string
}

// SendTurn publishes text as one turn of the conversation token and waits for
// the gateway's answer to it: the root task it submitted for the turn, or,
// when it answers with notices alone (a status question, a refusal, a stop),
// those.
func (b *NATSBus) SendTurn(ctx context.Context, convs *Conversations, token, text string) (TurnResult, error) {
	res := TurnResult{Token: token}
	if strings.TrimSpace(text) == "" {
		return res, errors.New("empty message")
	}
	if len(text) > consoleTextCap {
		return res, fmt.Errorf("message is %d bytes; the console door takes at most %d", len(text), consoleTextCap)
	}
	unlock := convs.lock(token)
	defer unlock()

	nc := b.client.Conn()
	subs := make(chan *nats.Msg, 64)
	notices := make(chan *nats.Msg, 16)
	sub, err := nc.ChanSubscribe(submissionSubjects, subs)
	if err != nil {
		return res, err
	}
	defer func() { _ = sub.Unsubscribe() }()
	nsub, err := nc.ChanSubscribe(fmt.Sprintf(consoleOutSubject, token), notices)
	if err != nil {
		return res, err
	}
	defer func() { _ = nsub.Unsubscribe() }()
	// The subscriptions are live on the server before the frame goes out, so
	// a fast submission is not missed.
	if err := nc.Flush(); err != nil {
		return res, err
	}

	id := make([]byte, messageIDBytes)
	if _, err := rand.Read(id); err != nil {
		return res, err
	}
	frame, _ := json.Marshal(ConsoleInFrame{MessageID: conversationTokenPrefix + "-" + hex.EncodeToString(id), Text: text})
	if err := nc.Publish(fmt.Sprintf(consoleInSubject, token), frame); err != nil {
		return res, err
	}

	conversation := fmt.Sprintf(consoleConversationOf, token)
	deadline := time.NewTimer(submissionWait)
	defer deadline.Stop()
	var grace <-chan time.Time
	for {
		select {
		case m := <-subs:
			if taskID, ok := rootSubmission(m, conversation); ok {
				res.TaskID = taskID
				return res, nil
			}
		case m := <-notices:
			var f ConsoleOutFrame
			if json.Unmarshal(m.Data, &f) == nil && strings.TrimSpace(f.Text) != "" {
				res.Notices = append(res.Notices, f.Text)
				if grace == nil {
					grace = time.After(noticeGrace)
				}
			}
		case <-grace:
			return res, nil
		case <-deadline.C:
			if len(res.Notices) > 0 {
				return res, nil
			}
			return res, fmt.Errorf("the gateway did not answer the turn within %s", submissionWait)
		case <-ctx.Done():
			return res, ctx.Err()
		}
	}
}

// rootSubmission reports whether m is the submission of a turn's own task in
// conversation: its audience is that conversation and it was not delegated
// from another task.
func rootSubmission(m *nats.Msg, conversation string) (string, bool) {
	env, err := lib.ParseEnvelope(m.Data)
	if err != nil || env.TaskID == "" || len(env.Authority) == 0 {
		return "", false
	}
	var a submissionAuthority
	if json.Unmarshal(env.Authority, &a) != nil {
		return "", false
	}
	if a.Audience.Conversation != conversation || a.Via != nil {
		return "", false
	}
	return env.TaskID, true
}

// WaitForTask reads the task until it is final or wait has passed, and
// returns the last read.
func (b *NATSBus) WaitForTask(ctx context.Context, taskID string, wait, every time.Duration) (*lib.Task, error) {
	deadline := time.Now().Add(wait)
	for {
		t, err := b.ReadTask(ctx, taskID)
		if err != nil && !errors.Is(err, ErrTaskNotFound) {
			return nil, err
		}
		if t != nil && t.Final {
			return t, nil
		}
		if time.Now().Add(every).After(deadline) {
			if t == nil {
				return nil, err
			}
			return t, nil
		}
		select {
		case <-ctx.Done():
			return t, ctx.Err()
		case <-time.After(every):
		}
	}
}
