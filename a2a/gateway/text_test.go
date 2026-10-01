package gateway

import (
	"strings"
	"testing"
)

func TestIsStatusQuery(t *testing.T) {
	// Exact phrases are the affordance everywhere - including narrow mode,
	// the posture for executors that absorb steers.
	exact := []string{
		"what is it doing", "What's it doing?", "status", "Status?",
		"how's it going", "how is it going", "whats going on",
		"any progress", "any updates?", "progress", "where are we",
	}
	// Interrogative shapes match only under the wide posture, where the
	// executor refuses steers and a false positive costs nothing.
	wideOnly := []string{
		"what is the agent doing", // the live miss that created this test
		"what is kage doing",
		"any update on the rollout",
	}
	// Steers the wide rule mistakes for status asks - the documented cost
	// of the width bias, and why steer-absorbing executors get narrow.
	wideCost := []string{
		"any update to the config should be reverted",
		"how about doing the upgrade instead",
	}
	never := []string{
		"also check the memory limits",
		"actually, focus on the kube-system namespace instead",
		"stop",
		"what is the memory limit on the nats pod and can you also check its restarts", // long compound: steer
		"delete the deployment",
	}
	for _, s := range exact {
		if !isStatusQuery(s, false) || !isStatusQuery(s, true) {
			t.Errorf("expected status query in both modes: %q", s)
		}
	}
	for _, s := range append(wideOnly, wideCost...) {
		if !isStatusQuery(s, true) {
			t.Errorf("expected wide status query: %q", s)
		}
		if isStatusQuery(s, false) {
			t.Errorf("expected narrow mode to forward as a steer: %q", s)
		}
	}
	for _, s := range never {
		if isStatusQuery(s, true) || isStatusQuery(s, false) {
			t.Errorf("expected NOT a status query in any mode: %q", s)
		}
	}
}

func TestIsDelegate(t *testing.T) {
	yes := map[string]string{
		"Delegate: write a haiku about message buses": "write a haiku about message buses",
		"delegate write a haiku":                      "write a haiku",
		"DELEGATE - check the fleet, then report":     "check the fleet, then report",
		"  delegate,  summarize #930  ":               "summarize #930",
		"Delegate:\nmultiline task":                   "multiline task",
	}
	for in, want := range yes {
		got, ok := isDelegate(in)
		if !ok || got != want {
			t.Errorf("isDelegate(%q) = (%q, %v), want (%q, true)", in, got, ok, want)
		}
	}
	no := []string{
		"delegate",
		"delegate:",
		"Delegated tasks are neat",
		"can you delegate this",
		"delegation is the demo",
		"what is it doing",
		"",
	}
	for _, in := range no {
		if got, ok := isDelegate(in); ok {
			t.Errorf("isDelegate(%q) = (%q, true), want false", in, got)
		}
	}
}

// TestChatChunksKeepFencesBalanced: the adapters translate each chunk alone,
// so a cut inside a fenced block must close the block at the end of the
// chunk and reopen it, info string and all, at the start of the next. Every
// chunk then parses as it would in the whole; no chunk exceeds the cap even
// with the fences added; and the text between the inserted fences is the
// original, byte for byte.
func TestChatChunksKeepFencesBalanced(t *testing.T) {
	logs := strings.Repeat("log line\n", 300)
	cases := map[string]struct {
		text string
		tag  string // the info string the reopened fence must carry
	}{
		"two blocks with prose between": {
			text: "intro\n```\n" + logs + "```\nsee <https://evil.example|https://good.example>\n```\nkubectl get pods\n```\n",
		},
		"one block then prose": {
			text: "```\n" + logs + "```\n**Summary:** see [runbook](https://x.example/r)",
		},
		"a language tag": {
			text: "```yaml\n" + logs + "```\ndone",
			tag:  "yaml",
		},
	}
	for name, tc := range cases {
		chunks := chatChunks(tc.text, discordChunk)
		if len(chunks) < 2 {
			t.Fatalf("%s: chatChunks gave %d chunks; the block must be cut for the test to mean anything", name, len(chunks))
		}
		var joined strings.Builder
		closed := false
		for i, chunk := range chunks {
			if len(chunk) > discordChunk {
				t.Errorf("%s: chunk %d is %d bytes, over the cap of %d", name, i, len(chunk), discordChunk)
			}
			lines := strings.Split(chunk, "\n")
			fences := 0
			for _, l := range lines {
				fences += strings.Count(l, "```")
			}
			if fences%2 != 0 {
				t.Errorf("%s: chunk %d has %d fence lines; a chunk must be balanced to parse alone:\n%q", name, i, fences, chunk)
			}
			body := chunk
			if closed {
				reopen := "```" + tc.tag
				if !strings.HasPrefix(body, reopen) {
					t.Errorf("%s: chunk %d follows a cut block and does not reopen it with %q: %q", name, i, reopen, body[:min(len(body), 40)])
				}
				body = strings.TrimPrefix(body, reopen)
				if !strings.HasPrefix(body, "\n") {
					t.Errorf("%s: chunk %d: the reopened fence does not end its line: %q", name, i, chunk[:min(len(chunk), 40)])
				}
				body = strings.TrimPrefix(body, "\n")
			}
			closed = false
			if i < len(chunks)-1 && strings.HasSuffix(body, "\n```") {
				// The cut fell inside the block: the inserted closer, after
				// a line that was the block's own.
				body = strings.TrimSuffix(body, "\n```")
				closed = true
			}
			joined.WriteString(body)
			if closed {
				joined.WriteString("\n") // the line break the cut landed on
			}
		}
		if joined.String() != tc.text {
			t.Errorf("%s: the chunks, with the inserted fences removed, are not the original text:\n got %q\nwant %q", name, joined.String(), tc.text)
		}
	}
}

// TestChatChunksUnchangedOutsideFences: text with no fence open at the cut
// is split as it always was, so a result that is prose and closed blocks
// posts the same chunks as before the chunker learned about fences.
func TestChatChunksUnchangedOutsideFences(t *testing.T) {
	text := strings.Repeat("a line of prose that goes on\n", 200) + "```\nshort block\n```\n" + strings.Repeat("more prose\n", 100)
	chunks := chatChunks(text, discordChunk)
	if got := strings.Join(chunks, ""); got != text {
		t.Fatalf("chunks do not join back to the text")
	}
	for i, c := range chunks {
		if strings.Contains(c, "\n```\n```") || len(c) > discordChunk {
			t.Errorf("chunk %d carries an inserted fence or is over the cap: %q", i, c)
		}
	}
	// A hard cut (no line break to land on) inside a fence still closes and
	// reopens, with the closer on its own line, and makes progress.
	long := "```\n" + strings.Repeat("x", 5000) + "\n```\n"
	chunks = chatChunks(long, discordChunk)
	if len(chunks) < 3 {
		t.Fatalf("hard cuts: got %d chunks", len(chunks))
	}
	for i, c := range chunks {
		if len(c) > discordChunk {
			t.Errorf("hard cuts: chunk %d is %d bytes", i, len(c))
		}
		if strings.Count(c, "```")%2 != 0 {
			t.Errorf("hard cuts: chunk %d is unbalanced: %q", i, c)
		}
		if i > 0 && !strings.HasPrefix(c, "```\n") {
			t.Errorf("hard cuts: chunk %d does not reopen the fence: %q", i, c[:min(len(c), 20)])
		}
	}
}
