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
