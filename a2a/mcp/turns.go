package mcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gke-labs/kube-agents/a2a/lib"
	"github.com/gke-labs/kube-agents/a2a/lib/chattext"
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
	// submissionSubjects is every task's .in subject, which the console grant
	// lets the server watch (a2a.>): submissions, follow-ups and cancels.
	submissionSubjects = "a2a.tasks.*.*." + lib.TaskClassIn
	// submissionSubjectParts is a2a.tasks.<addressee>.<task>.in; the
	// addressee is field 2.
	submissionSubjectParts = 5
	submissionAddressee    = 2

	// consoleTextCap is the gateway's own cap on one frame's text; a longer
	// one would be dropped there with a notice, so it is refused here first.
	consoleTextCap = 16 * 1024

	conversationTokenPrefix = "mcp"
	conversationTokenBytes  = 12
	messageIDBytes          = 8
	// maxConversations bounds the alias and send-lock tables; past it a new
	// conversation is refused rather than the tables left to grow.
	maxConversations = 1024

	// submissionWait bounds the wait for the gateway to publish the turn.
	// noticeGrace is how long notices alone are waited on before they are
	// taken as the whole answer (a status question or a refused turn gets
	// notices and no task); the gateway can post one and then do a little
	// work before a submission, so the grace is generous, and a late
	// submission cannot be mistaken for a later turn's, whose text differs.
	submissionWait = 30 * time.Second
	noticeGrace    = 10 * time.Second
	// maxFollowedTasks bounds the delegated tasks one wait follows.
	maxFollowedTasks = 8

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
// which conversation opened it, and which task it was delegated from.
type submissionAuthority struct {
	Audience struct {
		Conversation string `json:"conversation"`
	} `json:"audience"`
	Via *struct {
		TaskID string `json:"taskId"`
	} `json:"via,omitempty"`
}

// Conversations maps the aliases callers name to console tokens, per call:
// there is no server-wide "active" conversation for one caller to change under
// another. It also holds one send per conversation at a time, which, with the
// text, is what tells a turn's own submission from the conversation's next.
// Callers share one bearer token and so one alias namespace: two callers that
// use the same name share a conversation.
type Conversations struct {
	mu      sync.Mutex
	byAlias map[string]string
	sending map[string]chan struct{}
}

// NewConversations returns an empty alias table.
func NewConversations() *Conversations {
	return &Conversations{byAlias: map[string]string{}, sending: map[string]chan struct{}{}}
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
	if len(c.byAlias) >= maxConversations {
		return "", fmt.Errorf("this server already holds %d conversations; continue one by its token", maxConversations)
	}
	b := make([]byte, conversationTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	t := conversationTokenPrefix + hex.EncodeToString(b)
	c.byAlias[alias] = t
	return t, nil
}

// lock takes the conversation's send lock, or gives up when ctx does.
func (c *Conversations) lock(ctx context.Context, token string) (func(), error) {
	c.mu.Lock()
	ch, ok := c.sending[token]
	if !ok {
		if len(c.sending) >= maxConversations {
			c.mu.Unlock()
			return nil, fmt.Errorf("this server is already sending on %d conversations", maxConversations)
		}
		ch = make(chan struct{}, 1)
		c.sending[token] = ch
	}
	c.mu.Unlock()
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("another turn is being sent on this conversation: %w", ctx.Err())
	}
}

// TurnResult is what a sent turn produced: the task the gateway opened, or
// steered, for it, and the notices it posted.
type TurnResult struct {
	Token   string
	Task    *TaskRef
	Notices []string
}

// turnFate is what the gateway does with a turn's text, by the same rules it
// applies (a2a/gateway/gateway.go's command switch, which the console page's
// turnFate mirrors too): settled turns are answered with a notice and open no
// task; any other turn opens or steers one under one of texts, the text as
// sent or, for a delegate turn, its stripped rest.
func turnFate(text string) (settled bool, texts []string) {
	t := strings.TrimSpace(text)
	if rest, ok := chattext.IsFeedbackCommand(t); ok && strings.TrimSpace(rest) == "" {
		return true, nil
	}
	if rest, ok := chattext.IsSessionCommand(t); ok {
		rest = strings.TrimSpace(rest)
		if rest == "" || chattext.IsSessionOff(rest) || chattext.IsStop(rest) {
			return true, nil
		}
		t = rest
	}
	if chattext.IsStop(t) {
		return true, nil
	}
	texts = []string{t}
	if rest, ok := chattext.IsDelegate(t); ok && strings.TrimSpace(rest) != "" {
		texts = append(texts, strings.TrimSpace(rest))
	}
	return false, texts
}

// turnWatch is a sent turn's view of the conversation's submissions: the
// turn's own task once it appears, then the tasks delegated from it.
type turnWatch struct {
	conversation string
	texts        []string
	subs         chan *nats.Msg
	notices      chan *nats.Msg
	unsubscribe  func()

	// mu guards followed and known, which the follower goroutine fills
	// from the conversation's submissions once the turn's task is known.
	mu       sync.Mutex
	followed []TaskRef
	known    map[string]bool
	stop     chan struct{}
}

// openTurn subscribes to the conversation's submissions and notices, live on
// the server before it returns so that nothing published after is missed.
func (b *NATSBus) openTurn(token string, texts []string) (*turnWatch, error) {
	nc := b.client.Conn()
	w := &turnWatch{
		conversation: fmt.Sprintf(consoleConversationOf, token),
		texts:        texts,
		subs:         make(chan *nats.Msg, 256),
		notices:      make(chan *nats.Msg, 64),
		known:        map[string]bool{},
		stop:         make(chan struct{}),
	}
	sub, err := nc.ChanSubscribe(submissionSubjects, w.subs)
	if err != nil {
		return nil, err
	}
	nsub, err := nc.ChanSubscribe(fmt.Sprintf(consoleOutSubject, token), w.notices)
	if err != nil {
		_ = sub.Unsubscribe()
		return nil, err
	}
	w.unsubscribe = func() { _ = sub.Unsubscribe(); _ = nsub.Unsubscribe() }
	if err := nc.Flush(); err != nil {
		w.unsubscribe()
		return nil, err
	}
	return w, nil
}

// SendTurn publishes text as one turn of the conversation and waits for the
// gateway's answer to it: the task carrying the turn (a new one, or a running
// one the text was sent into), or, when the gateway answers with notices
// alone (a status question, a refusal), those. The returned watch keeps
// following the turn's delegations until it is closed.
func (b *NATSBus) SendTurn(ctx context.Context, convs *Conversations, token, text string) (TurnResult, *turnWatch, error) {
	res := TurnResult{Token: token}
	if strings.TrimSpace(text) == "" {
		return res, nil, errors.New("empty message")
	}
	if len(text) > consoleTextCap {
		return res, nil, fmt.Errorf("message is %d bytes; the console door takes at most %d", len(text), consoleTextCap)
	}
	settled, texts := turnFate(text)
	if settled {
		res, err := b.sendSettled(ctx, convs, token, text)
		return res, nil, err
	}
	unlock, err := convs.lock(ctx, token)
	if err != nil {
		return res, nil, err
	}
	defer unlock()
	// A caller that gave up while waiting for the lock must not send.
	if err := ctx.Err(); err != nil {
		return res, nil, err
	}
	w, err := b.openTurn(token, texts)
	if err != nil {
		return res, nil, err
	}
	if err := b.publishFrame(token, text); err != nil {
		w.close()
		return res, nil, err
	}

	deadline := time.NewTimer(submissionWait)
	defer deadline.Stop()
	var grace <-chan time.Time
	for {
		select {
		case m := <-w.subs:
			if ref, ok := w.ownSubmission(m); ok {
				res.Task = &ref
				go w.follow()
				return res, w, nil
			}
		case m := <-w.notices:
			if n := noticeText(m); n != "" {
				res.Notices = append(res.Notices, n)
				if grace == nil {
					grace = time.After(noticeGrace)
				}
			}
		case <-grace:
			w.close()
			return res, nil, nil
		case <-deadline.C:
			w.close()
			if len(res.Notices) > 0 {
				return res, nil, nil
			}
			return res, nil, fmt.Errorf("the gateway did not answer the turn within %s", submissionWait)
		case <-ctx.Done():
			w.close()
			return res, nil, ctx.Err()
		}
	}
}

// SendStop sends the console door's stop word.
func (b *NATSBus) SendStop(ctx context.Context, convs *Conversations, token string) (TurnResult, error) {
	return b.sendSettled(ctx, convs, token, stopText)
}

// sendSettled sends a turn the gateway answers with a notice and no task (a
// stop, a bare /session, /session off, a bare /feedback) and returns the
// notice, or nothing if none arrives within noticeGrace.
func (b *NATSBus) sendSettled(ctx context.Context, convs *Conversations, token, text string) (TurnResult, error) {
	res := TurnResult{Token: token}
	unlock, err := convs.lock(ctx, token)
	if err != nil {
		return res, err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return res, err
	}
	w, err := b.openTurn(token, nil)
	if err != nil {
		return res, err
	}
	defer w.close()
	if err := b.publishFrame(token, text); err != nil {
		return res, err
	}
	timeout := time.After(noticeGrace)
	for {
		select {
		case m := <-w.notices:
			if n := noticeText(m); n != "" {
				res.Notices = append(res.Notices, n)
				return res, nil
			}
		case <-timeout:
			return res, nil
		case <-ctx.Done():
			return res, ctx.Err()
		}
	}
}

func (b *NATSBus) publishFrame(token, text string) error {
	id := make([]byte, messageIDBytes)
	if _, err := rand.Read(id); err != nil {
		return err
	}
	frame, _ := json.Marshal(ConsoleInFrame{MessageID: conversationTokenPrefix + "-" + hex.EncodeToString(id), Text: text})
	return b.client.Conn().Publish(fmt.Sprintf(consoleInSubject, token), frame)
}

func noticeText(m *nats.Msg) string {
	var f ConsoleOutFrame
	if json.Unmarshal(m.Data, &f) != nil {
		return ""
	}
	return strings.TrimSpace(f.Text)
}

func (w *turnWatch) close() {
	if w == nil {
		return
	}
	if w.unsubscribe != nil {
		w.unsubscribe()
	}
	select {
	case <-w.stop:
	default:
		close(w.stop)
	}
}

// submission parses an .in message into its task, conversation, the task it
// was delegated from (if any) and its text. Only a message counts: a cancel,
// or anything else on the subject, is not a turn.
func submission(m *nats.Msg) (ref TaskRef, a submissionAuthority, text string, ok bool) {
	parts := strings.Split(m.Subject, ".")
	if len(parts) != submissionSubjectParts {
		return
	}
	env, err := lib.ParseEnvelope(m.Data)
	if err != nil || env.Kind != lib.KindMessage || env.TaskID == "" || len(env.Authority) == 0 {
		return
	}
	if json.Unmarshal(env.Authority, &a) != nil {
		return
	}
	var msg lib.Message
	if json.Unmarshal(env.Payload, &msg) != nil {
		return
	}
	return TaskRef{Addressee: parts[submissionAddressee], TaskID: env.TaskID}, a, strings.TrimSpace(partsText(msg.Parts)), true
}

// ownSubmission reports whether m carries this turn: a message in this
// conversation, not delegated from another task, with this turn's text. A
// message for a task already running is the gateway sending the text into
// it, and that task is where the turn's answer is.
func (w *turnWatch) ownSubmission(m *nats.Msg) (TaskRef, bool) {
	ref, a, text, ok := submission(m)
	if !ok || a.Audience.Conversation != w.conversation || a.Via != nil || !slices.Contains(w.texts, text) {
		return TaskRef{}, false
	}
	w.known[ref.TaskID] = true
	return ref, true
}

// follow records the tasks delegated, directly or not, from the tasks this
// turn already knows, as their submissions arrive, until the watch closes. It
// keeps the subscription drained, so a busy install's submissions never back
// up into a slow-consumer drop.
func (w *turnWatch) follow() {
	for {
		select {
		case <-w.stop:
			return
		case m := <-w.subs:
			ref, a, _, ok := submission(m)
			if !ok || a.Audience.Conversation != w.conversation || a.Via == nil {
				continue
			}
			w.mu.Lock()
			if w.known[a.Via.TaskID] && !w.known[ref.TaskID] && len(w.followed) < maxFollowedTasks {
				w.known[ref.TaskID] = true
				w.followed = append(w.followed, ref)
			}
			w.mu.Unlock()
		}
	}
}

func (w *turnWatch) followedTasks() []TaskRef {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.followed)
}

// TurnOutcome is a turn's task and the tasks delegated from it, each as last
// read, and which of them holds the answer.
type TurnOutcome struct {
	Root     TaskRef
	Task     *lib.Task
	Followed []FollowedTask
	// AnswerFrom is the task whose result is the turn's answer: the last
	// delegated task to finish with one (a delegation's answer lands on the
	// task that wakes the delegator), else the root.
	AnswerFrom *TaskRef
	Answer     *lib.Task
}

// FollowedTask is one delegated task as last read.
type FollowedTask struct {
	Ref  TaskRef
	Task *lib.Task
	Err  error
}

// delegationSettle is how long, once every followed task has finished,
// WaitForTurn waits for another delegation before taking the last answer.
const delegationSettle = 2 * time.Second

// WaitForTurn waits, within wait, for the turn's task to finish and, when it
// delegated (it carries a delegate or handoff artifact), for the tasks
// delegated from it: the answer of a delegation lands on the task that wakes
// the delegator, which may come minutes after the delegator itself finished.
// Followed tasks are read one at a time on one reader slot. It closes the
// watch.
func (b *NATSBus) WaitForTurn(ctx context.Context, w *turnWatch, root TaskRef, wait time.Duration) (TurnOutcome, error) {
	defer w.close()
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	out := TurnOutcome{Root: root}
	t, err := b.WaitForTask(ctx, root, wait)
	if err != nil && !errors.Is(err, ErrTaskNotFound) {
		return out, err
	}
	out.Task = t
	out.AnswerFrom, out.Answer = &out.Root, t
	if t == nil || !t.Final || !delegated(t) {
		return out, nil
	}
	read := map[string]FollowedTask{}
	var order []TaskRef
	settledAt := time.Time{}
	for ctx.Err() == nil {
		progressed := false
		for _, ref := range w.followedTasks() {
			if f, ok := read[ref.TaskID]; ok && f.Task != nil && f.Task.Final {
				continue
			}
			if _, ok := read[ref.TaskID]; !ok {
				order = append(order, ref)
			}
			ft, ferr := b.WaitForTask(ctx, ref, time.Until(deadlineOf(ctx)))
			read[ref.TaskID] = FollowedTask{Ref: ref, Task: ft, Err: ferr}
			progressed = true
			if ferr == nil && ft != nil && ft.Final && ft.Artifact(lib.ArtifactResult) != nil {
				r := ref
				out.AnswerFrom, out.Answer = &r, ft
			}
		}
		if progressed {
			settledAt = time.Time{}
			continue
		}
		if out.AnswerFrom != nil && *out.AnswerFrom != root {
			if settledAt.IsZero() {
				settledAt = time.Now()
			} else if time.Since(settledAt) >= delegationSettle {
				break
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(minFetchWait * 2):
		}
	}
	for _, ref := range order {
		out.Followed = append(out.Followed, read[ref.TaskID])
	}
	return out, nil
}

// delegated reports whether a finished task handed its work on.
func delegated(t *lib.Task) bool {
	return t.Artifact(lib.ArtifactDelegate) != nil || t.Artifact(lib.ArtifactHandoff) != nil
}

func deadlineOf(ctx context.Context) time.Time {
	if dl, ok := ctx.Deadline(); ok {
		return dl
	}
	return time.Now()
}

// ResolveAddressee finds the addressee of a task named by id alone, from its
// submission: the .in message the gateway wrote for it. More than one
// addressee holding a submission under that id is refused, rather than one
// picked: the id alone does not say whose task it is.
func (b *NATSBus) ResolveAddressee(ctx context.Context, taskID string) (string, error) {
	if !lib.ValidSubjectToken(taskID) {
		return "", fmt.Errorf("%q is not a task id", taskID)
	}
	release, err := b.readerSlot()
	if err != nil {
		return "", err
	}
	defer release()
	ctx, cancel := context.WithTimeout(ctx, taskReadTimeout)
	defer cancel()
	s, err := b.client.JetStream().Stream(ctx, lib.TasksStream)
	if err != nil {
		return "", err
	}
	cons, err := s.CreateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject:     fmt.Sprintf("a2a.tasks.*.%s.%s", taskID, lib.TaskClassIn),
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		AckPolicy:         jetstream.AckNonePolicy,
		MemoryStorage:     true,
		InactiveThreshold: taskConsumerIdle,
	})
	if err != nil {
		return "", err
	}
	pending := cons.CachedInfo().NumPending
	if pending == 0 {
		return "", ErrTaskNotFound
	}
	batch, err := cons.Fetch(int(min(pending, taskFetchBatch)), jetstream.FetchMaxWait(taskFetchWait))
	if err != nil {
		return "", err
	}
	addressees := map[string]bool{}
	for msg := range batch.Messages() {
		parts := strings.Split(msg.Subject(), ".")
		if len(parts) != submissionSubjectParts || agreedEnvelope(msg.Subject(), msg.Data()) == nil {
			continue
		}
		addressees[parts[submissionAddressee]] = true
	}
	switch len(addressees) {
	case 0:
		return "", ErrTaskNotFound
	case 1:
		for a := range addressees {
			return a, nil
		}
	}
	return "", fmt.Errorf("task id %s is submitted to more than one addressee; name it as <addressee>%s%s", taskID, taskRefSep, taskID)
}
