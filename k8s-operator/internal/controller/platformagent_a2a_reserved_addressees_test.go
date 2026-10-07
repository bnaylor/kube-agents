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
// so that list has to be every fixed-name addressee the install routes to. Those
// are read from three places: the subjects the bridge identity's grants name,
// which the operator renders, and two defaults it does not render, the
// gateway's (A2A_DEFAULT_ADDRESSEE) and the bridge's profile (BRIDGE_PROFILE).
// The defaults live in the a2a module, which this module cannot import, so the
// test reads them out of the source. Every extraction fails the test rather than falling
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

// bridgeGrantAddressees is every addressee the bridge identity's grants name,
// read off the subjects themselves rather than off a constant: the `<x>` in
// `a2a.tasks.<x>.*.in` and `a2a.tasks.<x>.*.events`, in `a2a.cap.verify.<x>`
// and in `a2a.cap.reply.<x>.>`. Widening the grant to a second addressee adds
// it here with no test edit. A wildcard in the addressee position fails the
// test, because no name list can reserve it; so does finding no addressee at
// all, which would make every check below pass vacuously.
func bridgeGrantAddressees(t *testing.T) []string {
	t.Helper()
	id := bridgeIdentity()
	var out []string
	add := func(subject, addressee string) {
		if addressee == "" || strings.ContainsAny(addressee, "*>") {
			t.Fatalf("the bridge grant %q names addressee %q; a wildcard addressee cannot be reserved by name", subject, addressee)
		}
		out = append(out, addressee)
	}
	for _, subject := range slices.Concat(id.publish, id.subscribe) {
		tokens := strings.Split(subject, ".")
		switch {
		case len(tokens) == 5 && tokens[0] == "a2a" && tokens[1] == "tasks" &&
			(tokens[4] == "in" || tokens[4] == "events"):
			add(subject, tokens[2])
		case len(tokens) == 4 && tokens[0] == "a2a" && tokens[1] == "cap" && tokens[2] == "verify":
			add(subject, tokens[3])
		case len(tokens) == 5 && tokens[0] == "a2a" && tokens[1] == "cap" && tokens[2] == "reply" && tokens[4] == ">":
			add(subject, tokens[3])
		}
	}
	if len(out) == 0 {
		t.Fatalf("found no addressee in the bridge's grants (publish %v, subscribe %v); the subject shapes this test reads have moved", id.publish, id.subscribe)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// Every fixed-name addressee the install routes to is reserved: each one the
// bridge's grants name, the gateway's default addressee, and the bridge's
// profile default. The check is containment, not equality, so the edit
// a2a/docs/hermes-bridge.md prescribes for an install that overrides
// BRIDGE_PROFILE (widen bridgeIdentity() and add the addressee to
// a2aReservedAddressees() in the same change) passes without a test edit, and
// widening the grant without the reservation reds here.
//
// BRIDGE_PROFILE is read from the a2a module's default only. The operator never
// renders it on the bridge container (it is a CR-declared sidecar env), so
// there is no rendered value to prefer. An install that overrides it is held
// through the grant instead: the override only works once the grant names the
// new addressee, and then the grant check above covers it.
func TestTheCalloutReservesTheConfiguredAddressees(t *testing.T) {
	agent := a2aTestAgent()
	got := renderedCalloutReservedAddressees(t, agent)

	// Today's value, pinned so the derivation below cannot drift to a set
	// that no longer includes the addressee every stock turn goes to.
	if !slices.Contains(got, a2aBridgeAddressee) {
		t.Errorf("%s = %v does not contain %q, the addressee every stock turn is routed to", a2aCalloutReservedAddresseesEnvVar, got, a2aBridgeAddressee)
	}

	gateway := buildA2AGatewayDeployment(agent).Spec.Template.Spec.Containers[0]
	gatewayAddressee, rendered := envValue(gateway, gatewayDefaultAddresseeEnv)
	if !rendered {
		gatewayAddressee = envDefaultInSource(t, a2aGatewayConfigSource, gatewayDefaultAddresseeEnv)
	}
	bridgeProfile := envDefaultInSource(t, a2aBridgeMainSource, bridgeProfileEnv)

	type source struct{ addressee, from string }
	var want []source
	for _, a := range bridgeGrantAddressees(t) {
		want = append(want, source{a, "named by the bridge's grants"})
	}
	want = append(want,
		source{gatewayAddressee, "the gateway's " + gatewayDefaultAddresseeEnv},
		source{bridgeProfile, "the bridge's " + bridgeProfileEnv + " default"},
	)
	for _, w := range want {
		if !slices.Contains(got, w.addressee) {
			t.Errorf("%s = %v does not reserve %q (%s); a narrowed pod named %q would be handed its task subjects. "+
				"Add it to a2aReservedAddressees() in the same change.",
				a2aCalloutReservedAddresseesEnvVar, got, w.addressee, w.from, w.addressee)
		}
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
