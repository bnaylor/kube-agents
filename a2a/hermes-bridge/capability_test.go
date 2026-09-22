package hermesbridge

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/gke-labs/kube-agents/a2a/capability"
	"github.com/gke-labs/kube-agents/a2a/lib"
)

// The default install's executor, from the attacker's side.
//
// The session executor's suite (a2a/worker-adapter/capability_test.go) covers
// the same ground for the `delegate:` route. Both exist because both run: the
// operator renders no A2A_DEFAULT_ADDRESSEE, so an unqualified task reaches
// this bridge and nothing else. A refusal proven only on the route nobody
// takes by default is not a control.
//
// Every test here runs the real verifier against the real bucket (startServer
// arms both) and gives the bridge a command that cannot exist, so a task that
// gets past the check fails as something other than rejected. That is how
// these tests tell "refused" from "ran and then failed".

// noCommand is a hermes path that cannot exist.
var noCommand = []string{"/nonexistent/hermes"}

// submitWithAuthority publishes a submission carrying an authority block the
// caller chose, rather than the honest one submit() mints.
func submitWithAuthority(t *testing.T, c *lib.Client, taskID, prompt string, authority json.RawMessage) {
	t.Helper()
	contextID := "ctx-" + taskID
	opts := []lib.EnvelopeOption{lib.WithTo(lib.Party{Session: "platform"})}
	if authority != nil {
		opts = append(opts, lib.WithAuthority(authority))
	}
	env, err := lib.NewMessageEnvelope(gatewayParty, taskID, contextID, "corr-"+taskID,
		messagePayload(t, taskID, contextID, prompt), opts...)
	if err != nil {
		t.Fatalf("submission envelope: %v", err)
	}
	if err := c.Publish(testCtx(t), lib.TaskInSubject("platform", taskID), env); err != nil {
		t.Fatalf("submission publish: %v", err)
	}
}

// terminalText pulls the message text off the final event, which is where a
// supervisor and a human both read the reason.
func terminalText(t *testing.T, url, taskID string) string {
	t.Helper()
	var last string
	for _, env := range replayEvents(t, url, taskID) {
		var upd lib.StatusUpdate
		if err := json.Unmarshal(env.Payload, &upd); err != nil {
			continue
		}
		if upd.Status.Message == nil {
			continue
		}
		for _, p := range upd.Status.Message.Parts {
			if p.Kind == "text" {
				last = p.Text
			}
		}
	}
	return last
}

// refuseAndFold asserts the task ended terminal rejected on the stream with a
// reason naming the capability, and that no subprocess ever started.
func refuseAndFold(t *testing.T, url string, c *lib.Client, taskID string) {
	t.Helper()
	task := waitTerminal(t, c, taskID)
	if task.State != lib.StateRejected {
		t.Fatalf("state = %s, want rejected; the task was not refused", task.State)
	}
	text := terminalText(t, url, taskID)
	if !strings.Contains(text, "capability-refused") {
		t.Fatalf("the terminal event does not say why: %q", text)
	}
	for _, env := range replayEvents(t, url, taskID) {
		if state, _ := statusState(t, env); state == lib.StateWorking {
			t.Fatalf("the bridge published working for a task it refused")
		}
	}
}

// A capability that exists, is well-formed, and names somebody else as its
// delegate. Possession of the reference is not the test: it travels on a bus
// other principals read.
func TestTheBridgeRefusesACapabilityMintedForAnotherDelegate(t *testing.T) {
	_, url := startServer(t)
	startBridge(t, url, noCommand)
	c := gatewayClient(t, url)

	const taskID = "task-bridge-cap-otherdelegate"
	ref := mintFor(t, c, taskID, "chat-vole-somebody-else")
	submitWithAuthority(t, c, taskID, "do the thing", authorityFor(t, ref))
	refuseAndFold(t, url, c, taskID)
}

// A forged reference: a key the gateway never wrote, at a revision invented to
// look plausible.
func TestTheBridgeRefusesAForgedCapabilityReference(t *testing.T) {
	_, url := startServer(t)
	startBridge(t, url, noCommand)
	c := gatewayClient(t, url)

	const taskID = "task-bridge-cap-forged"
	submitWithAuthority(t, c, taskID, "do the thing",
		authorityFor(t, capability.Ref{Key: "root." + taskID + "-forged", Revision: 1}))
	refuseAndFold(t, url, c, taskID)
}

// A reference to a real, resolvable capability at the wrong revision. The pin
// is what makes an overwritten entry stop resolving, and the bridge inherits
// that only if it passes the revision through rather than looking the key up
// live.
func TestTheBridgeRefusesAReferenceAtTheWrongRevision(t *testing.T) {
	_, url := startServer(t)
	startBridge(t, url, noCommand)
	c := gatewayClient(t, url)

	const taskID = "task-bridge-cap-badrev"
	ref := mintFor(t, c, taskID, "platform")
	ref.Revision++
	submitWithAuthority(t, c, taskID, "do the thing", authorityFor(t, ref))
	refuseAndFold(t, url, c, taskID)
}

// An unpinned reference. A gateway that shipped `revision: 0` would be handing
// the verifier a key to look up live, which is the whole overwrite hole.
func TestTheBridgeRefusesAnUnpinnedReference(t *testing.T) {
	_, url := startServer(t)
	startBridge(t, url, noCommand)
	c := gatewayClient(t, url)

	const taskID = "task-bridge-cap-unpinned"
	ref := mintFor(t, c, taskID, "platform")
	ref.Revision = 0
	submitWithAuthority(t, c, taskID, "do the thing", authorityFor(t, ref))
	refuseAndFold(t, url, c, taskID)
}

// No capability at all, which is the pre-A3b gateway's envelope. This is the
// one that matters most here: the bridge is a sidecar the operator renders no
// environment for, so its default has to be the enforcing one with nothing
// configured.
func TestTheBridgeRefusesASubmissionWithNoCapabilityByDefault(t *testing.T) {
	_, url := startServer(t)
	startBridge(t, url, noCommand)
	c := gatewayClient(t, url)

	const taskID = "task-bridge-cap-null"
	submitWithAuthority(t, c, taskID, "do the thing", json.RawMessage(`{"grants":null}`))
	refuseAndFold(t, url, c, taskID)
}

// The zero value of the knob is the enforcing one, asserted on the Config the
// binary actually builds rather than on the tests' own.
func TestTheBridgesDefaultIsToRequireACapability(t *testing.T) {
	if (Config{}).CapabilityOptional {
		t.Fatal("the zero value must be the enforcing one: an unconfigured sidecar must refuse, not execute")
	}
}

// ...and the mixed-version window, which is the only thing the knob buys: a
// gateway that predates the mint keeps working against a new bridge.
func TestTheMixedVersionKnobLetsAnUncapabledSubmissionThroughTheBridge(t *testing.T) {
	_, url := startServer(t)
	startBridgeOptional(t, url, script(t, `echo '{"type":"result","subtype":"success","result":"done"}'`))
	c := gatewayClient(t, url)

	const taskID = "task-bridge-cap-optional"
	submitWithAuthority(t, c, taskID, "do the thing", nil)
	if task := waitTerminal(t, c, taskID); task.State != lib.StateCompleted {
		t.Fatalf("state = %s, want completed; the rollout window does not work", task.State)
	}
}

// The knob relaxes a MISSING capability and nothing else. An attacker who can
// set one environment variable still cannot run a refused task.
func TestTheMixedVersionKnobDoesNotRelaxAPresentCapabilityAtTheBridge(t *testing.T) {
	_, url := startServer(t)
	startBridgeOptional(t, url, noCommand)
	c := gatewayClient(t, url)

	const taskID = "task-bridge-cap-optional-present"
	submitWithAuthority(t, c, taskID, "do the thing",
		authorityFor(t, capability.Ref{Key: "root." + taskID + "-forged", Revision: 1}))
	refuseAndFold(t, url, c, taskID)
}

// An authority block that does not parse. Not a rollout state - a block is
// either absent or well-formed - so it is a refusal rather than a relaxation,
// and it stays one with the knob set.
func TestAMalformedAuthorityBlockIsRefusedByTheBridgeEvenWithTheKnobSet(t *testing.T) {
	_, url := startServer(t)
	startBridgeOptional(t, url, noCommand)
	c := gatewayClient(t, url)

	const taskID = "task-bridge-cap-malformed"
	submitWithAuthority(t, c, taskID, "do the thing",
		json.RawMessage(`{"grants":{"capability":"not-an-object"}}`))
	refuseAndFold(t, url, c, taskID)
}

// The refusal names the rule and never anything the caller supplied. A bridge
// that echoed the key back would confirm which task ids are live to anyone who
// can name one.
func TestTheBridgesRefusalQuotesNothingTheCallerSupplied(t *testing.T) {
	_, url := startServer(t)
	startBridge(t, url, noCommand)
	c := gatewayClient(t, url)

	const taskID = "task-bridge-cap-oracle"
	const marker = "root.task-bridge-cap-oracle-forged"
	submitWithAuthority(t, c, taskID, "do the thing",
		authorityFor(t, capability.Ref{Key: marker, Revision: 77}))
	refuseAndFold(t, url, c, taskID)
	if text := terminalText(t, url, taskID); strings.Contains(text, marker) || strings.Contains(text, "77") {
		t.Fatalf("the refusal quoted the caller's own reference back: %q", text)
	}
}

// A capability that exists but names somebody else, and one that does not
// exist at all, must be indistinguishable.
func TestAtTheBridgeAMissingCapabilityAndSomebodyElsesAreIndistinguishable(t *testing.T) {
	_, url := startServer(t)
	startBridge(t, url, noCommand)
	c := gatewayClient(t, url)

	const taskA = "task-bridge-cap-absent"
	submitWithAuthority(t, c, taskA, "go",
		authorityFor(t, capability.Ref{Key: "root." + taskA + "-nope", Revision: 1}))
	refuseAndFold(t, url, c, taskA)

	const taskB = "task-bridge-cap-present"
	submitWithAuthority(t, c, taskB, "go",
		authorityFor(t, mintFor(t, c, taskB, "chat-vole-somebody-else")))
	refuseAndFold(t, url, c, taskB)

	if a, b := terminalText(t, url, taskA), terminalText(t, url, taskB); a != b {
		t.Fatalf("the two refusals differ, which tells a caller whether the key existed:\n  absent  %q\n  present %q", a, b)
	}
}

// A verifier that is not there refuses the task. The alternative - execute
// when the thing that says yes cannot be reached - makes the verifier's
// availability the attacker's target rather than the operator's. "Fails
// closed" is a claim about behaviour under an outage, and nothing but an
// outage demonstrates it.
func TestAVerifierThatCannotBeReachedRefusesTheBridgesTask(t *testing.T) {
	_, url := startServerNoVerifier(t)
	startBridge(t, url, noCommand)
	c := gatewayClient(t, url)

	const taskID = "task-bridge-cap-outage"
	submitWithAuthority(t, c, taskID, "do the thing",
		authorityFor(t, capability.Ref{Key: "root." + taskID, Revision: 1}))
	refuseAndFold(t, url, c, taskID)
}
