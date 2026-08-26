package gateway

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// The gateway holds no model, so its affordances are literal: a small set of
// normalized phrases, deterministic by construction. Anything richer belongs
// in the executors.

// normalize lowercases and strips everything but letters, digits, and single
// spaces, so "What is it doing?!" and "what is it doing" are the same ask.
func normalize(s string) string {
	var b strings.Builder
	lastSpace := true
	for _, r := range strings.ToLower(strings.TrimSpace(s)) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '\'':
			if r != '\'' { // drop apostrophes: "what's" -> "whats"
				b.WriteRune(r)
			}
			lastSpace = false
		case r == ' ' || r == '\t' || r == '\n':
			if !lastSpace {
				b.WriteRune(' ')
			}
			lastSpace = true
		}
	}
	return strings.TrimSpace(b.String())
}

var statusQueries = map[string]bool{
	"what is it doing":   true,
	"whats it doing":     true,
	"what are you doing": true,
	"whats happening":    true,
	"what is happening":  true,
	"status":             true,
}

// isStatusQuery reports whether the turn is the "what is it doing" ask,
// answered by stream replay rather than forwarded as steering.
func isStatusQuery(text string) bool {
	return statusQueries[normalize(text)]
}

var stopWords = map[string]bool{
	"stop":   true,
	"cancel": true,
	"abort":  true,
}

// isStop reports whether the turn is the cancel affordance — the hard
// interrupt, mapped to kind:cancel (gateway design).
func isStop(text string) bool {
	return stopWords[normalize(text)]
}

// statusHistoryCap bounds the rendered transition history — a long-running
// task accumulates one state per event and the answer must stay one chat
// message.
const statusHistoryCap = 12

// formatTaskStatus renders a replayed Task for chat: current state, the
// transition history, and the latest progress line.
func formatTaskStatus(t *lib.Task) string {
	var b strings.Builder
	fmt.Fprintf(&b, "🔎 task `%s` is **%s**", t.ID, t.State)
	if len(t.StatusHistory) > 0 {
		history := t.StatusHistory
		prefix := ""
		if len(history) > statusHistoryCap {
			history = history[len(history)-statusHistoryCap:]
			prefix = "… → "
		}
		states := make([]string, len(history))
		for i, s := range history {
			states[i] = string(s)
		}
		fmt.Fprintf(&b, " (history: %s%s)", prefix, strings.Join(states, " → "))
	}
	if p := t.Artifact(lib.ArtifactProgress); p != nil {
		if text := lastTextPart(p.Parts); text != "" {
			fmt.Fprintf(&b, "\n📋 latest progress: %s", truncateRunes(text, progressCap))
		}
	}
	if r := t.Artifact(lib.ArtifactResult); r != nil {
		fmt.Fprintf(&b, "\n📦 result so far: %d part(s)", len(r.Parts))
	}
	b.WriteString("\n_(answered by stream replay — no live connection to the executor)_")
	return b.String()
}

func lastTextPart(parts []lib.Part) string {
	for i := len(parts) - 1; i >= 0; i-- {
		if parts[i].Kind == "text" && parts[i].Text != "" {
			return parts[i].Text
		}
	}
	return ""
}

func joinTextParts(parts []lib.Part) string {
	var b strings.Builder
	for _, p := range parts {
		if p.Kind == "text" {
			b.WriteString(p.Text)
		}
	}
	return b.String()
}

// chatChunks splits text for backends with a message size cap (Discord:
// 2000); the chunk size leaves headroom for decoration. Cuts land on line
// breaks where possible and never inside a UTF-8 sequence — a split rune is
// an invalid payload the backend may refuse outright.
func chatChunks(text string, size int) []string {
	if text == "" {
		return nil
	}
	var chunks []string
	for len(text) > size {
		cut := strings.LastIndex(text[:size], "\n")
		if cut < size/2 {
			cut = size
			for cut > 0 && !utf8.RuneStart(text[cut]) {
				cut--
			}
		}
		chunks = append(chunks, text[:cut])
		text = text[cut:]
	}
	return append(chunks, text)
}

// truncateRunes bounds s to n bytes at a rune boundary, with an ellipsis
// marking the loss.
func truncateRunes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

func marshalMessage(m lib.Message) ([]byte, error) {
	return json.Marshal(m)
}
