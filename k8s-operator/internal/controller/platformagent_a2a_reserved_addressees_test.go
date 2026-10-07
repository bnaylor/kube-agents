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

// The callout's reserved addressees, held to the render and to the a2a module.
//
// A narrowed pod named after an addressee is handed that addressee's task
// subjects. The callout refuses such a pod by name, from A2A_RESERVED_ADDRESSEES,
// so that list has to be every fixed-name addressee the install routes to. The
// addressees themselves are configured in two places the operator does not
// render: the gateway's default (A2A_DEFAULT_ADDRESSEE, left at its default) and
// the bridge's profile (BRIDGE_PROFILE, left at its default). Those defaults
// live in the a2a module, which this module cannot import, so the test reads
// them out of the source. Every extraction fails the test rather than falling
// back, so a moved or reshaped default reds here instead of passing vacuously.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

const (
	// reservedAddresseesFixturePath is the rendered list the a2a module's
	// callout tests refuse against. The operator writes it, the callout reads
	// it. Regenerate with:
	// go test ./internal/controller/ -run TestRenderedReservedAddresseesMatchTheCalloutFixture -update
	reservedAddresseesFixturePath = "../../../a2a/authcallout/testdata/rendered-reserved-addressees.txt"

	a2aGatewayConfigSource = "../../../a2a/gateway/config.go"

	// The env names whose defaults the a2a module holds.
	gatewayDefaultAddresseeEnv = "A2A_DEFAULT_ADDRESSEE"
	bridgeProfileEnv           = "BRIDGE_PROFILE"
)

// envDefaultInSource finds the one `envOr("<env>", <default>)` call in a Go
// file and returns the default. A string literal is returned as is; an
// identifier is resolved to the string constant of that name in the same file.
// Anything else, zero calls or more than one, fails the test.
func envDefaultInSource(t *testing.T, path, env string) string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v (if it moved, this test's path must move with it)", path, err)
	}
	consts := map[string]string{}
	var defaults []ast.Expr
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.ValueSpec:
			for i, name := range n.Names {
				if i >= len(n.Values) {
					continue
				}
				if lit, ok := n.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if v, err := strconv.Unquote(lit.Value); err == nil {
						consts[name.Name] = v
					}
				}
			}
		case *ast.CallExpr:
			fn, ok := n.Fun.(*ast.Ident)
			if !ok || fn.Name != "envOr" || len(n.Args) != 2 {
				return true
			}
			key, ok := n.Args[0].(*ast.BasicLit)
			if !ok || key.Kind != token.STRING || key.Value != strconv.Quote(env) {
				return true
			}
			defaults = append(defaults, n.Args[1])
		}
		return true
	})
	if len(defaults) != 1 {
		t.Fatalf("found %d `envOr(%q, ...)` calls in %s, want exactly 1; the reserved addressees are held to that default", len(defaults), env, path)
	}
	switch d := defaults[0].(type) {
	case *ast.BasicLit:
		v, err := strconv.Unquote(d.Value)
		if err != nil || d.Kind != token.STRING {
			t.Fatalf("the %s default in %s is %s, not a string literal", env, path, d.Value)
		}
		return v
	case *ast.Ident:
		v, ok := consts[d.Name]
		if !ok {
			t.Fatalf("the %s default in %s is %s, which is not a string constant in that file", env, path, d.Name)
		}
		return v
	default:
		t.Fatalf("the %s default in %s is neither a string literal nor a constant", env, path)
	}
	return ""
}

func renderedCalloutReservedAddressees(t *testing.T, agent *agentv1alpha1.PlatformAgent) []string {
	t.Helper()
	dep := buildA2ACalloutDeployment(agent)
	if len(dep.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("the callout Deployment has %d containers, want 1", len(dep.Spec.Template.Spec.Containers))
	}
	raw, ok := envValue(dep.Spec.Template.Spec.Containers[0], a2aCalloutReservedAddresseesEnvVar)
	if !ok {
		t.Fatalf("the callout Deployment does not render %s; the callout refuses to start without it", a2aCalloutReservedAddresseesEnvVar)
	}
	if raw == "" {
		t.Fatalf("%s renders empty; every refusal test would pass vacuously", a2aCalloutReservedAddresseesEnvVar)
	}
	return strings.Split(raw, a2aReservedAddresseesSeparator)
}

// The rendered list is exactly the addressees the gateway and the bridge are
// configured with on this render: the gateway's default addressee, the
// bridge's profile, and the addressee the bridge's grants name. Each is read
// from what the operator renders when it renders the variable, and from the
// a2a module's default when it does not.
func TestTheCalloutReservesTheConfiguredAddressees(t *testing.T) {
	agent := a2aTestAgent()

	gateway := buildA2AGatewayDeployment(agent).Spec.Template.Spec.Containers[0]
	gatewayAddressee, rendered := envValue(gateway, gatewayDefaultAddresseeEnv)
	if !rendered {
		gatewayAddressee = envDefaultInSource(t, a2aGatewayConfigSource, gatewayDefaultAddresseeEnv)
	}

	agent.Spec.Deployment = &agentv1alpha1.DeploymentSpec{
		Sidecars: []corev1.Container{{Name: "hermes-bridge", Image: "example.com/bridge:v1"}},
	}
	pod := buildPodTemplateSpec(agent, "h", "h", "h", "h", nil, renderOptions{})
	var bridge *corev1.Container
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "hermes-bridge" {
			bridge = &pod.Spec.Containers[i]
		}
	}
	if bridge == nil {
		t.Fatal("the rendered pod has no hermes-bridge container, so the bridge's env cannot be read")
	}
	bridgeProfile, rendered := envValue(*bridge, bridgeProfileEnv)
	if !rendered {
		bridgeProfile = envDefaultInSource(t, a2aBridgeMainSource, bridgeProfileEnv)
	}

	want := []string{gatewayAddressee, bridgeProfile, a2aBridgeAddressee}
	slices.Sort(want)
	want = slices.Compact(want)

	got := renderedCalloutReservedAddressees(t, agent)
	sorted := slices.Clone(got)
	slices.Sort(sorted)
	if !slices.Equal(sorted, want) {
		t.Errorf("%s = %v, but the install routes to %v (gateway default %q, bridge profile %q, bridge grants %q); "+
			"a narrowed pod named after a missing one would be handed its task subjects",
			a2aCalloutReservedAddresseesEnvVar, got, want, gatewayAddressee, bridgeProfile, a2aBridgeAddressee)
	}
}

// The a2a module's callout tests take their reserved addressees from this
// fixture, so a render change reaches them only if the fixture moves with it.
func TestRenderedReservedAddresseesMatchTheCalloutFixture(t *testing.T) {
	got := strings.Join(renderedCalloutReservedAddressees(t, a2aTestAgent()), a2aReservedAddresseesSeparator) + "\n"
	path := filepath.Clean(reservedAddresseesFixturePath)
	if *updateAuthMapFixture {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("writing fixture: %v", err)
		}
		t.Logf("wrote %s", path)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the reserved addressees fixture: %v\nRegenerate with: go test ./internal/controller/ -run TestRenderedReservedAddresseesMatchTheCalloutFixture -update", err)
	}
	if string(want) != got {
		t.Errorf("the rendered %s is %q and the fixture the callout suite refuses against is %q.\n"+
			"Regenerate with: go test ./internal/controller/ -run TestRenderedReservedAddresseesMatchTheCalloutFixture -update",
			a2aCalloutReservedAddresseesEnvVar, got, want)
	}
}
