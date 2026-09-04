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
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

// calloutFixturePath is the rendered map the a2a module's callout parses in its
// own test. The two modules cannot import each other, so this file is the
// contract between them: the operator writes it, the callout reads it, and a
// field renamed on one side fails on the other.
//
// Regenerate with: go test ./internal/controller/ -run TestRenderedAuthMap -update
const calloutFixturePath = "../../../a2a/authcallout/testdata/rendered-identity-map.json"

var updateAuthMapFixture = flag.Bool("update", false, "rewrite the callout's identity-map fixture from the current render")

func authMapTestAgent() *agentv1alpha1.PlatformAgent {
	return &agentv1alpha1.PlatformAgent{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "kubeagents-system"},
	}
}

func TestRenderedAuthMapCarriesEveryCalloutPrincipalAndNoStaticOne(t *testing.T) {
	agent := authMapTestAgent()
	cm, version, err := buildA2AAuthMapConfigMap(agent)
	if err != nil {
		t.Fatalf("buildA2AAuthMapConfigMap: %v", err)
	}

	var doc a2aAuthMapDocument
	if err := json.Unmarshal([]byte(cm.Data[a2aAuthMapKey]), &doc); err != nil {
		t.Fatalf("rendered map does not parse: %v", err)
	}

	got := map[string]bool{}
	for _, id := range doc.Identities {
		got[id.User] = true
	}
	for _, id := range calloutIdentities(agent) {
		if !got[id.user] {
			t.Errorf("callout principal %q is missing from the map; it would be refused at connect", id.user)
		}
	}
	// A static principal in the map would be served by BOTH the callout and
	// the config's auth_users exemption, and which one answered would depend
	// on how the client happened to connect.
	for _, id := range staticIdentities(agent) {
		if got[id.user] {
			t.Errorf("static principal %q is in the callout map; it is authenticated by nats.conf and must not be in both", id.user)
		}
	}

	if doc.Version != version {
		t.Errorf("version in the document (%q) differs from the one returned (%q)", doc.Version, version)
	}
	if cm.Annotations[a2aAuthMapVersionAnnotation] != version {
		t.Errorf("version annotation = %q, want %q", cm.Annotations[a2aAuthMapVersionAnnotation], version)
	}
}

// The version has to be a function of the grants and nothing else: stable
// across renders of an unchanged deployment, and different the moment a grant
// moves. A version that churns makes BusCredentialsReady flap; one that does
// not move on a real change makes it lie.
func TestTheMapVersionNamesTheContent(t *testing.T) {
	first, err := renderA2AAuthMap(authMapTestAgent())
	if err != nil {
		t.Fatalf("renderA2AAuthMap: %v", err)
	}
	second, err := renderA2AAuthMap(authMapTestAgent())
	if err != nil {
		t.Fatalf("renderA2AAuthMap: %v", err)
	}
	if first.Version != second.Version {
		t.Errorf("two renders of one deployment gave versions %q and %q", first.Version, second.Version)
	}

	// A different namespace means different ServiceAccount names, which is
	// a real change to who the map authenticates.
	other := authMapTestAgent()
	other.Namespace = "somewhere-else"
	moved, err := renderA2AAuthMap(other)
	if err != nil {
		t.Fatalf("renderA2AAuthMap: %v", err)
	}
	if moved.Version == first.Version {
		t.Error("moving every ServiceAccount to another namespace did not change the version")
	}
}

// The map keys on whatever ServiceAccount the pod actually runs as, including a
// CR override. Rendering the default name while the pod runs as the override
// would refuse the agent at connect with a valid token — the failure that looks
// like the callout is broken when it is doing exactly what it was told.
func TestTheMapFollowsAServiceAccountOverride(t *testing.T) {
	agent := authMapTestAgent()
	agent.Spec.Security = &agentv1alpha1.SecuritySpec{ServiceAccountName: "custom-agent-sa"}

	doc, err := renderA2AAuthMap(agent)
	if err != nil {
		t.Fatalf("renderA2AAuthMap: %v", err)
	}
	want := "system:serviceaccount:kubeagents-system:custom-agent-sa"
	for _, id := range doc.Identities {
		if id.User == "agent" {
			if id.ServiceAccount != want {
				t.Errorf("agent principal keys on %q, want %q", id.ServiceAccount, want)
			}
			return
		}
	}
	t.Fatal("no agent principal in the rendered map")
}

// The wildcard readability check, asserted because the default JSON encoder
// silently undoes it. Every subject list here is full of > wildcards and the
// escaped form is what an operator would be reading at 3 AM.
func TestTheRenderedMapIsReadable(t *testing.T) {
	cm, _, err := buildA2AAuthMapConfigMap(authMapTestAgent())
	if err != nil {
		t.Fatalf("buildA2AAuthMapConfigMap: %v", err)
	}
	body := cm.Data[a2aAuthMapKey]

	// The escape Go's default encoder would emit for the NATS wildcard,
	// spelled as its six literal characters rather than written out, so that
	// nothing between here and the file can quietly turn it back into the
	// character it is standing in for.
	escapedWildcard := `\u` + `003e`
	if strings.Contains(body, escapedWildcard) {
		t.Errorf("rendered map contains %s escapes; SetEscapeHTML(false) was lost", escapedWildcard)
	}
	if !strings.Contains(body, "_INBOX.gateway.>") {
		t.Error("rendered map does not carry the gateway inbox grant as a plain wildcard")
	}
}

// The cross-module contract. The a2a module's callout parses this exact file in
// its own test suite, with unknown fields refused, so a field renamed here
// without being renamed there fails on the other side of the repo.
func TestRenderedAuthMapMatchesTheCalloutFixture(t *testing.T) {
	cm, _, err := buildA2AAuthMapConfigMap(authMapTestAgent())
	if err != nil {
		t.Fatalf("buildA2AAuthMapConfigMap: %v", err)
	}
	rendered := cm.Data[a2aAuthMapKey]

	path := filepath.Clean(calloutFixturePath)
	if *updateAuthMapFixture {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("creating fixture directory: %v", err)
		}
		if err := os.WriteFile(path, []byte(rendered), 0o644); err != nil {
			t.Fatalf("writing fixture: %v", err)
		}
		t.Logf("wrote %s", path)
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the callout fixture: %v\nRegenerate with: go test ./internal/controller/ -run TestRenderedAuthMap -update", err)
	}
	if string(want) != rendered {
		t.Errorf("the rendered map and the fixture the callout parses have diverged.\nRegenerate with: go test ./internal/controller/ -run TestRenderedAuthMap -update\n--- fixture\n%s\n--- rendered\n%s", want, rendered)
	}
}
