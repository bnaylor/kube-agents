package gateway

import (
	"regexp"
	"strings"
)

// slackDMPrefix marks a DM conversation key. The whole DM is the session,
// like Discord's — "a DM, or a thread in a group space" (gateway design).
const slackDMPrefix = "slack:dm/"

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
