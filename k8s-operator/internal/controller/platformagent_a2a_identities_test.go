/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

func identityTestAgent() *agentv1alpha1.PlatformAgent {
	return &agentv1alpha1.PlatformAgent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "kubeagents-system"},
	}
}

// The inbox trap, pinned. Push delivery and every JetStream API request come
// back on an inbox subject, each principal is granted only its own prefix, and
// the client sets that prefix from its user name. A principal whose subscribe
// list does not contain its own prefix authenticates, publishes, and then hangs
// on the first reply — found live twice in W6 (the provision Job could never
// succeed; no consumer could ever ack). This asserts the property rather than
// the spelling of any one grant.
func TestEveryPrincipalMaySubscribeToItsOwnInbox(t *testing.T) {
	for _, id := range a2aIdentities(identityTestAgent()) {
		if id.account == a2aAccountSys {
			// $SYS holds no application inbox grants; its user is a
			// human with the system account's own privileges.
			continue
		}
		want := "_INBOX." + id.user + ".>"
		if !slices.Contains(id.subscribe, want) {
			t.Errorf("%s: subscribe list lacks %q; every reply it waits on would time out", id.user, want)
		}
		if !slices.Contains(id.publish, want) {
			t.Errorf("%s: publish list lacks %q; it could not answer its own requests", id.user, want)
		}
	}
}

// No principal may hold another's inbox prefix. Without this the whole
// connect-time property leaks through the reply path: an agent that can
// subscribe to another's inbox reads what that principal's subject grants
// withheld.
func TestNoPrincipalHoldsAnotherPrincipalsInbox(t *testing.T) {
	ids := a2aIdentities(identityTestAgent())
	for _, id := range ids {
		for _, other := range ids {
			if other.user == id.user {
				continue
			}
			foreign := "_INBOX." + other.user + ".>"
			if slices.Contains(id.subscribe, foreign) {
				t.Errorf("%s may subscribe to %s's inbox (%q)", id.user, other.user, foreign)
			}
		}
	}
}

func TestPrincipalsAreDistinct(t *testing.T) {
	seenUser := map[string]bool{}
	seenSA := map[string]bool{}
	for _, id := range a2aIdentities(identityTestAgent()) {
		if seenUser[id.user] {
			t.Errorf("two principals share the NATS user %q", id.user)
		}
		seenUser[id.user] = true

		if id.auth != a2aAuthCallout {
			continue
		}
		// Two ServiceAccounts mapping to one user would re-create the
		// shared credential the callout exists to end, and the callout
		// refuses such a map outright — better to fail here than to
		// render a map the callout will reject at startup.
		if seenSA[id.serviceAccount] {
			t.Errorf("two principals share the ServiceAccount %q", id.serviceAccount)
		}
		seenSA[id.serviceAccount] = true
	}
}

// Each auth mode owes different fields, and a principal carrying the wrong set
// renders into the wrong place: a callout principal with no ServiceAccount
// cannot be resolved, and a static one with no creds key renders a password of
// "" — a user anyone can log in as, which is W6 finding #9 in a new costume.
func TestEachPrincipalCarriesWhatItsAuthModeNeeds(t *testing.T) {
	for _, id := range a2aIdentities(identityTestAgent()) {
		switch id.auth {
		case a2aAuthCallout:
			if id.serviceAccount == "" {
				t.Errorf("%s authenticates by callout but names no ServiceAccount", id.user)
			}
			if !strings.HasPrefix(id.serviceAccount, "system:serviceaccount:") {
				t.Errorf("%s: ServiceAccount %q is not in the form TokenReview reports", id.user, id.serviceAccount)
			}
			if id.credsKey != "" {
				t.Errorf("%s authenticates by callout but also carries the creds key %q; it must have no shared secret at all", id.user, id.credsKey)
			}
		case a2aAuthStatic:
			if id.credsKey == "" {
				t.Errorf("%s authenticates statically but names no creds key; it would render an empty password", id.user)
			}
			if id.serviceAccount != "" {
				t.Errorf("%s authenticates statically but names a ServiceAccount", id.user)
			}
		}
		if id.account == "" {
			t.Errorf("%s names no account", id.user)
		}
	}
}

// Every static principal must be exempted from the callout, and the exemption
// list is built from exactly this set. A static user missing from auth_users is
// refused at connect by a callout that has never heard of it.
func TestStaticAndCalloutPrincipalsPartitionTheSet(t *testing.T) {
	agent := identityTestAgent()
	all := a2aIdentities(agent)
	static := staticIdentities(agent)
	callout := calloutIdentities(agent)

	if len(static)+len(callout) != len(all) {
		t.Fatalf("static (%d) + callout (%d) != all (%d); a principal is in neither render or both",
			len(static), len(callout), len(all))
	}
	for _, id := range static {
		if id.auth != a2aAuthStatic {
			t.Errorf("%s is in the static set with auth mode %v", id.user, id.auth)
		}
	}
	for _, id := range callout {
		if id.auth != a2aAuthCallout {
			t.Errorf("%s is in the callout set with auth mode %v", id.user, id.auth)
		}
	}
}

// The residue, asserted so it cannot grow quietly. Each of these has a reason
// recorded at its definition, and the reasons are not the same kind of thing:
// web can never present a ServiceAccount token because a browser has none;
// worker has no identity to present because session pods are spawned without
// one; sys is a human; gateway could move today but its client program lands
// separately from this render, so moving the identity first would refuse it at
// connect on every install; and seed is applied rather than rendered, so
// dropping its user would break an object already running on installs today. A
// sixth name here means someone added a principal without asking whether it
// could have an identity.
func TestTheStaticResidueIsExactlyTheOnesWithReasons(t *testing.T) {
	var got []string
	for _, id := range staticIdentities(identityTestAgent()) {
		got = append(got, id.user)
	}
	want := []string{"gateway", "worker", "seed", "web", "sys"}
	if !slices.Equal(got, want) {
		t.Errorf("static principals = %v, want %v.\nA new static principal needs a recorded reason it cannot present a ServiceAccount token, and a card that closes it if it can.", got, want)
	}
}

// The agent pod's narrowing is the security outcome of this change, so it is
// asserted rather than left to the comment. As `worker` it could publish task
// events for any addressee — impersonate any executor, terminate any task in
// flight. Its own principal has no task-plane publish at all.
func TestTheAgentPrincipalCannotReachTheTaskPlane(t *testing.T) {
	var agentID a2aIdentity
	for _, id := range a2aIdentities(identityTestAgent()) {
		if id.user == "agent" {
			agentID = id
		}
	}
	if agentID.user == "" {
		t.Fatal("no agent principal")
	}
	for _, subject := range agentID.publish {
		if strings.HasPrefix(subject, "a2a.tasks.") {
			t.Errorf("the agent principal may publish %q; it has no reason to reach the task plane", subject)
		}
	}
}

// a2aTestCalloutKeys generates a real keypair set for render tests. Real rather
// than a fixture string: the server validates both key types and refuses to
// start on either being wrong, so a test rendering a placeholder would assert
// against a config the server would reject.
func a2aTestCalloutKeys(t *testing.T) *a2aCalloutKeys {
	t.Helper()
	keys, _, err := generateA2ACalloutKeys()
	if err != nil {
		t.Fatalf("generateA2ACalloutKeys: %v", err)
	}
	return keys
}
