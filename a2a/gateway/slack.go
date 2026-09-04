package gateway

import (
	"log/slog"
	"regexp"
	"strings"
	"sync"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
)

// slackDMPrefix marks a DM conversation key. The whole DM is the session,
// like Discord's — "a DM, or a thread in a group space" (gateway design).
const slackDMPrefix = "slack:dm/"

// slackSeenCap bounds the at-least-once dedupe ring: Socket Mode redelivers
// unacked envelopes, so delivered (channel, ts) pairs are remembered and
// re-deliveries dropped. Sized to roughly a busy hour of messages.
const slackSeenCap = 2048

// slackAPI is the slice of the Slack Web API the adapter uses; *slack.Client
// satisfies it, tests fake it.
type slackAPI interface {
	AuthTest() (*slack.AuthTestResponse, error)
	PostMessage(channelID string, options ...slack.MsgOption) (string, string, error)
	UpdateMessage(channelID, timestamp string, options ...slack.MsgOption) (string, string, string, error)
	GetUsersInConversation(params *slack.GetUsersInConversationParameters) ([]string, string, error)
	OpenConversation(params *slack.OpenConversationParameters) (*slack.Channel, bool, bool, error)
	GetConversationReplies(params *slack.GetConversationRepliesParameters) ([]slack.Message, bool, string, error)
}

// SlackAdapter is the first real mapped-identity backend. Transport is
// Socket Mode — an outbound websocket, so no inbound endpoint on the
// cluster and no ingress to secure, the property that made Discord cheap.
// The sender is whatever user_id Slack's authenticated connection asserted;
// joining it to a principal (or dropping it) is the session manager's job
// against the install's mapping table. Never profile.email: whether that
// field is IdP-asserted or user-editable is workspace configuration we do
// not control, and a user-editable field feeding a principal is an
// impersonation primitive (gateway design, identity section).
type SlackAdapter struct {
	api       slackAPI
	sm        *socketmode.Client // nil in unit tests
	log       *slog.Logger
	botUserID string

	mu sync.Mutex
	// sessionRoots caches whether a thread's root message mentions the bot
	// — the rule that lets a bot-rooted thread carry every message without
	// making every thread in a joined channel a session.
	sessionRoots map[string]bool
	// seen and seenOrder are the at-least-once dedupe ring over (channel, ts).
	seen      map[string]bool
	seenOrder []string
}

// slackLinkRE rewrites the markdown links the relay emits into mrkdwn's
// <url|text> form; anything fancier is presentation polish, not this card.
var slackLinkRE = regexp.MustCompile(`\[([^\]\n]+)\]\((https?://[^)\s]+)\)`)

// slackConversationID is the backend-qualified session key. A channel is
// not a session; a thread in it is — and Slack threads are implicit
// (replying with thread_ts creates one), so a channel mention binds the
// session to the mention message's own ts as thread root, with no
// thread-creation failure mode to handle.
func slackConversationID(channelType, channel, threadTS string) string {
	if channelType == "im" {
		return slackDMPrefix + channel
	}
	return "slack:" + channel + "/" + threadTS
}

// slackChannelThread inverts slackConversationID for the adapter's own use;
// threadTS is "" for DMs.
func slackChannelThread(conversation string) (channel, threadTS string, ok bool) {
	if dm, found := strings.CutPrefix(conversation, slackDMPrefix); found {
		return dm, "", dm != ""
	}
	rest, found := strings.CutPrefix(conversation, "slack:")
	if !found {
		return "", "", false
	}
	channel, threadTS, found = strings.Cut(rest, "/")
	if !found || channel == "" || threadTS == "" {
		return "", "", false
	}
	return channel, threadTS, true
}

// toMrkdwn translates the two markdown forms the relay emits (bold pairs,
// links) into Slack mrkdwn. Deterministic and narrow on purpose: full
// markdown fidelity is presentation polish, and the legacy Hermes path's
// converter is not this code path's to reuse.
func toMrkdwn(text string) string {
	text = strings.ReplaceAll(text, "**", "*")
	return slackLinkRE.ReplaceAllString(text, "<$2|$1>")
}

// slackMentionsBot reports whether text mentions the bot user. Slack encodes
// mentions as <@U123> or <@U123|display>; requiring the closing form keeps a
// longer id sharing the prefix (<@U123X>) from matching.
func slackMentionsBot(text, botID string) bool {
	marker := "<@" + botID
	for {
		i := strings.Index(text, marker)
		if i < 0 {
			return false
		}
		rest := text[i+len(marker):]
		if strings.HasPrefix(rest, ">") || strings.HasPrefix(rest, "|") {
			return true
		}
		text = text[i+1:]
	}
}

// stripSlackMention removes every mention of the bot (both encoded forms)
// and trims the remainder — the task text is the ask, not the addressing.
func stripSlackMention(text, botID string) string {
	marker := "<@" + botID
	var b strings.Builder
	for {
		i := strings.Index(text, marker)
		if i < 0 {
			break
		}
		rest := text[i+len(marker):]
		switch {
		case strings.HasPrefix(rest, ">"):
			b.WriteString(text[:i])
			text = rest[1:]
		case strings.HasPrefix(rest, "|"):
			j := strings.Index(rest, ">")
			if j < 0 {
				b.WriteString(text[:i+len(marker)])
				text = rest
				continue
			}
			b.WriteString(text[:i])
			text = rest[j+1:]
		default:
			// A longer id sharing the prefix; keep it and move past.
			b.WriteString(text[:i+len(marker)])
			text = rest
		}
	}
	b.WriteString(text)
	return strings.TrimSpace(b.String())
}

// alreadySeen records and reports (channel, ts) pairs — Socket Mode is
// at-least-once, and a redelivered ask must not become a steer.
func (s *SlackAdapter) alreadySeen(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[key] {
		return true
	}
	s.seen[key] = true
	s.seenOrder = append(s.seenOrder, key)
	if len(s.seenOrder) > slackSeenCap {
		delete(s.seen, s.seenOrder[0])
		s.seenOrder = s.seenOrder[1:]
	}
	return false
}

// isSessionRoot reports whether a thread's root message mentions the bot,
// via cache or one conversations.replies read. An API failure reports
// false without caching: dropping is safe (the user can @mention), and the
// next reply retries.
func (s *SlackAdapter) isSessionRoot(channel, threadTS string) bool {
	key := channel + "/" + threadTS
	s.mu.Lock()
	if v, ok := s.sessionRoots[key]; ok {
		s.mu.Unlock()
		return v
	}
	s.mu.Unlock()
	msgs, _, _, err := s.api.GetConversationReplies(&slack.GetConversationRepliesParameters{
		ChannelID: channel, Timestamp: threadTS, Limit: 1, Inclusive: true,
	})
	if err != nil || len(msgs) == 0 {
		s.log.Warn("thread root lookup failed; reply not delivered", "channel", channel, "thread", threadTS, "err", err)
		return false
	}
	root := slackMentionsBot(msgs[0].Text, s.botUserID)
	s.markSessionRoot(key, root)
	return root
}

func (s *SlackAdapter) markSessionRoot(key string, isRoot bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessionRoots[key] = isRoot
}

// inbound normalizes one message event, or reports it not-a-turn. The
// affordance rule, deterministic: DMs carry every message; a channel
// message must mention the bot, and the ask's own ts becomes the session
// thread's root (Slack threads are implicit); a thread reply is a turn when
// it mentions the bot or the thread root did. Everything else — bots, our
// own posts, edits and other subtypes, redeliveries — is not a turn.
func (s *SlackAdapter) inbound(m *slackevents.MessageEvent) (InboundMessage, bool) {
	if m.SubType != "" || m.BotID != "" || m.User == "" || m.User == s.botUserID ||
		m.Channel == "" || m.TimeStamp == "" {
		return InboundMessage{}, false
	}
	if s.alreadySeen(m.Channel + "/" + m.TimeStamp) {
		return InboundMessage{}, false
	}
	text := strings.TrimSpace(m.Text)
	if m.ChannelType == "im" {
		if text == "" {
			return InboundMessage{}, false
		}
		return InboundMessage{
			Conversation: slackConversationID(m.ChannelType, m.Channel, ""),
			Kind:         "dm",
			AuthorID:     m.User,
			MessageID:    m.TimeStamp,
			Text:         text,
		}, true
	}
	mentioned := slackMentionsBot(text, s.botUserID)
	if mentioned {
		text = stripSlackMention(text, s.botUserID)
	}
	isReply := m.ThreadTimeStamp != "" && m.ThreadTimeStamp != m.TimeStamp
	threadTS := m.ThreadTimeStamp
	if !isReply {
		if !mentioned {
			return InboundMessage{}, false
		}
		// The ask roots the session thread; remember that so the first
		// unmentioned reply needn't re-read it from the API.
		threadTS = m.TimeStamp
		s.markSessionRoot(m.Channel+"/"+threadTS, true)
	} else if !mentioned && !s.isSessionRoot(m.Channel, threadTS) {
		return InboundMessage{}, false
	}
	if text == "" {
		// A bare mention has nothing to run; same shape as Discord's rule.
		return InboundMessage{}, false
	}
	return InboundMessage{
		Conversation: slackConversationID(m.ChannelType, m.Channel, threadTS),
		Kind:         "group",
		AuthorID:     m.User,
		MessageID:    m.TimeStamp,
		Text:         text,
	}, true
}
