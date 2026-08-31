package gateway

import "testing"

func TestIsStatusQuery(t *testing.T) {
	yes := []string{
		"what is it doing", "What's it doing?", "status", "Status?",
		"what is the agent doing", // the live miss that created this test
		"what is kage doing", "how's it going", "whats going on",
		"any updates?", "progress", "where are we",
	}
	no := []string{
		"also check the memory limits",
		"actually, focus on the kube-system namespace instead",
		"stop",
		"what is the memory limit on the nats pod and can you also check its restarts", // long compound: steer
		"delete the deployment",
	}
	for _, s := range yes {
		if !isStatusQuery(s) {
			t.Errorf("expected status query: %q", s)
		}
	}
	for _, s := range no {
		if isStatusQuery(s) {
			t.Errorf("expected NOT a status query: %q", s)
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
