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
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

// slackBotSecret is the admin-made a2a-slack-bot Secret with the keys given,
// so a test can hand the gate a whole pair or half of one.
func slackBotSecret(agent *agentv1alpha1.PlatformAgent, keys ...string) *corev1.Secret {
	data := map[string][]byte{}
	for _, k := range keys {
		data[k] = []byte("x" + k)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: a2aSlackBotSecretName, Namespace: agent.Namespace},
		Data:       data,
	}
}

// TestTheSlackSecretRendersTheGateway: the a2a-slack-bot Secret with both
// tokens is a backend. Dark before it exists, rendered once it does, with
// no discord-bot Secret and no door.
func TestTheSlackSecretRendersTheGateway(t *testing.T) {
	t.Setenv(a2aInjectBackendEnvVar, "")
	agent := a2aTestAgent()
	r, cl, _ := a2aGateTestReconcilerWithoutABackend(t, agent)
	ctx := context.Background()
	theCalloutIsServing(t, ctx, cl, r, agent)
	if state, err := r.reconcileA2A(ctx, agent); err != nil || !state.gatewayDark {
		t.Fatalf("precondition: want a dark gateway before the Secret exists (state=%+v err=%v)", state, err)
	}

	if err := cl.Create(ctx, slackBotSecret(agent, a2aSlackBotTokenKey, a2aSlackAppTokenKey)); err != nil {
		t.Fatal(err)
	}
	state, err := r.reconcileA2A(ctx, agent)
	if err != nil {
		t.Fatalf("reconcileA2A after the Secret: %v", err)
	}
	if state.gatewayDark {
		t.Fatalf("the gateway is still reported dark with the %s Secret present: %q", a2aSlackBotSecretName, state.gatewayDarkReason)
	}
	dep := &appsv1.Deployment{}
	if err := cl.Get(ctx, types.NamespacedName{Name: a2aGatewayName(agent), Namespace: agent.Namespace}, dep); err != nil {
		t.Fatalf("the gateway Deployment was not rendered once the Slack pair existed: %v", err)
	}
	env := envMapOf(dep.Spec.Template.Spec.Containers[0].Env)
	for name, key := range map[string]string{a2aSlackBotTokenEnvVar: a2aSlackBotTokenKey, a2aSlackAppTokenEnvVar: a2aSlackAppTokenKey} {
		e, ok := env[name]
		if !ok || e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
			t.Fatalf("%s is not a Secret reference on the rendered gateway: %+v", name, e)
		}
		if ref := e.ValueFrom.SecretKeyRef; ref.Name != a2aSlackBotSecretName || ref.Key != key {
			t.Errorf("%s reads %s/%s, want %s/%s: both halves of the pair come from one Secret", name, ref.Name, ref.Key, a2aSlackBotSecretName, key)
		}
	}
}

// TestASlackSecretWithHalfAPairIsNotABackend: the gateway refuses half a
// pair (SLACK_BOT_TOKEN and SLACK_APP_TOKEN arm Slack together), so a Secret
// carrying one key would render a gateway that exits at boot. Withheld, and
// the reason names the key that is missing.
func TestASlackSecretWithHalfAPairIsNotABackend(t *testing.T) {
	t.Setenv(a2aInjectBackendEnvVar, "")
	agent := a2aTestAgent()
	r, cl, _ := a2aGateTestReconcilerWithoutABackend(t, agent)
	ctx := context.Background()
	if err := cl.Create(ctx, slackBotSecret(agent, a2aSlackBotTokenKey)); err != nil {
		t.Fatal(err)
	}
	theCalloutIsServing(t, ctx, cl, r, agent)
	state, err := r.reconcileA2A(ctx, agent)
	if err != nil {
		t.Fatalf("reconcileA2A: %v", err)
	}
	if !state.gatewayDark {
		t.Fatalf("an %s Secret with only %s counted as a backend; the gateway would render and refuse the half pair", a2aSlackBotSecretName, a2aSlackBotTokenKey)
	}
	if !strings.Contains(state.gatewayDarkReason, a2aSlackAppTokenKey) {
		t.Errorf("the reason does not name the missing key %s: %q", a2aSlackAppTokenKey, state.gatewayDarkReason)
	}
	if strings.Contains(state.gatewayDarkReason, "create the "+a2aSlackBotSecretName) {
		t.Errorf("the reason tells the admin to create a Secret that exists: %q", state.gatewayDarkReason)
	}
	err = cl.Get(ctx, types.NamespacedName{Name: a2aGatewayName(agent), Namespace: agent.Namespace}, &appsv1.Deployment{})
	if err == nil {
		t.Fatal("a gateway Deployment was rendered on half a Slack pair")
	}
}

// TestACompleteSlackPairBesideAHalfDiscordSecretIsABackend: the table's
// order must not decide the verdict. A discord-bot Secret missing its key
// beside a complete a2a-slack-bot Secret is a Slack gateway, not a withheld
// one.
func TestACompleteSlackPairBesideAHalfDiscordSecretIsABackend(t *testing.T) {
	t.Setenv(a2aInjectBackendEnvVar, "")
	agent := a2aTestAgent()
	r, cl, _ := a2aGateTestReconcilerWithoutABackend(t, agent)
	ctx := context.Background()
	wrong := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: a2aDiscordBotSecretName, Namespace: agent.Namespace},
		Data:       map[string][]byte{"DISCORD_TOKEN": []byte("misnamed")},
	}
	for _, s := range []*corev1.Secret{wrong, slackBotSecret(agent, a2aSlackBotTokenKey, a2aSlackAppTokenKey)} {
		if err := cl.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	theCalloutIsServing(t, ctx, cl, r, agent)
	state, err := r.reconcileA2A(ctx, agent)
	if err != nil {
		t.Fatalf("reconcileA2A: %v", err)
	}
	if state.gatewayDark {
		t.Fatalf("a complete Slack pair was withheld because an earlier Secret in the table is half-populated: %q", state.gatewayDarkReason)
	}
	if err := cl.Get(ctx, types.NamespacedName{Name: a2aGatewayName(agent), Namespace: agent.Namespace}, &appsv1.Deployment{}); err != nil {
		t.Fatalf("the gateway Deployment was not rendered: %v", err)
	}
}

// TestAHalfSlackPairBesideACompleteDiscordSecretIsWithheld: the gateway
// refuses half a pair before it looks at any other backend, so a complete
// discord-bot Secret does not rescue a half-populated a2a-slack-bot one;
// rendered, that gateway would crash-loop on the half-pair refusal.
func TestAHalfSlackPairBesideACompleteDiscordSecretIsWithheld(t *testing.T) {
	t.Setenv(a2aInjectBackendEnvVar, "")
	agent := a2aTestAgent()
	r, cl, _ := a2aGateTestReconcilerWithoutABackend(t, agent)
	ctx := context.Background()
	for _, s := range []*corev1.Secret{discordBotSecret(agent), slackBotSecret(agent, a2aSlackBotTokenKey)} {
		if err := cl.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	theCalloutIsServing(t, ctx, cl, r, agent)
	state, err := r.reconcileA2A(ctx, agent)
	if err != nil {
		t.Fatalf("reconcileA2A: %v", err)
	}
	if !state.gatewayDark {
		t.Fatal("a half Slack pair beside a complete Discord Secret rendered the gateway; it would exit on the half-pair refusal")
	}
	if !strings.Contains(state.gatewayDarkReason, a2aSlackAppTokenKey) {
		t.Errorf("the reason does not name the missing key %s: %q", a2aSlackAppTokenKey, state.gatewayDarkReason)
	}
}

// TestTwoCompleteSecretsAreWithheld: both references would resolve and the
// gateway refuses to start on two backends, so the gate withholds and names
// both Secrets rather than rendering the refusal.
func TestTwoCompleteSecretsAreWithheld(t *testing.T) {
	t.Setenv(a2aInjectBackendEnvVar, "")
	agent := a2aTestAgent()
	r, cl, _ := a2aGateTestReconcilerWithoutABackend(t, agent)
	ctx := context.Background()
	for _, s := range []*corev1.Secret{discordBotSecret(agent), slackBotSecret(agent, a2aSlackBotTokenKey, a2aSlackAppTokenKey)} {
		if err := cl.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	theCalloutIsServing(t, ctx, cl, r, agent)
	state, err := r.reconcileA2A(ctx, agent)
	if err != nil {
		t.Fatalf("reconcileA2A: %v", err)
	}
	if !state.gatewayDark {
		t.Fatal("two complete backend Secrets rendered the gateway; it would exit on the two-backend refusal")
	}
	for _, want := range []string{a2aDiscordBotSecretName, a2aSlackBotSecretName, "delete one"} {
		if !strings.Contains(state.gatewayDarkReason, want) {
			t.Errorf("the reason does not say %q: %q", want, state.gatewayDarkReason)
		}
	}
}

// TestTheDarkReasonNamesSlack: the remedy an admin reads off the A2AGateway
// condition names the Slack Secret and both of its keys beside the Chat
// field, the Discord Secret and the door, in one list built from the arming
// table rather than a sentence per backend.
func TestTheDarkReasonNamesSlack(t *testing.T) {
	t.Setenv(a2aInjectBackendEnvVar, "")
	agent := a2aTestAgent()
	r, cl, _ := a2aGateTestReconcilerWithoutABackend(t, agent)
	ctx := context.Background()
	theCalloutIsServing(t, ctx, cl, r, agent)
	state, err := r.reconcileA2A(ctx, agent)
	if err != nil || !state.gatewayDark {
		t.Fatalf("precondition: want a dark gateway (state=%+v err=%v)", state, err)
	}
	for _, want := range []string{a2aSlackBotSecretName, a2aSlackBotTokenKey, a2aSlackAppTokenKey, a2aDiscordBotSecretName, "spec.integration.googleChat", a2aInjectBackendEnvVar} {
		if !strings.Contains(state.gatewayDarkReason, want) {
			t.Errorf("the reason does not name %q: %q", want, state.gatewayDarkReason)
		}
	}
}

// TestTheGatewayReadsOneMapFromBothTables: the principal map is one volume
// at the gateway's one path, projected from the Discord ConfigMap and the
// Slack Secret, both optional - the gateway's own rule for a missing table -
// with A2A_PRINCIPAL_MAP rendered at that path so the two are one fact. No
// DefaultMode: the pod runs as uid 1000 with no fsGroup, so a 0400 Secret
// file would be root's and unreadable.
func TestTheGatewayReadsOneMapFromBothTables(t *testing.T) {
	t.Setenv(a2aInjectBackendEnvVar, "")
	dep := buildA2AGatewayDeployment(a2aTestAgent())
	c := dep.Spec.Template.Spec.Containers[0]
	if got := envMapOf(c.Env)[a2aPrincipalMapEnvVar].Value; got != a2aPrincipalMapDir {
		t.Errorf("%s = %q, want %q", a2aPrincipalMapEnvVar, got, a2aPrincipalMapDir)
	}
	var mount *corev1.VolumeMount
	for i := range c.VolumeMounts {
		if c.VolumeMounts[i].Name == a2aPrincipalMapVolume {
			mount = &c.VolumeMounts[i]
		}
	}
	if mount == nil || mount.MountPath != a2aPrincipalMapDir || !mount.ReadOnly {
		t.Fatalf("principal map mount = %+v, want read-only at %s", mount, a2aPrincipalMapDir)
	}
	vol := podVolume(dep.Spec.Template, a2aPrincipalMapVolume)
	if vol == nil || vol.Projected == nil {
		t.Fatalf("volume %s is not projected: %+v", a2aPrincipalMapVolume, vol)
	}
	if vol.Projected.DefaultMode != nil {
		t.Errorf("the projected map carries DefaultMode %o; without an fsGroup a non-default mode leaves the Secret's files root-owned", *vol.Projected.DefaultMode)
	}
	var cm, secret bool
	for _, src := range vol.Projected.Sources {
		switch {
		case src.ConfigMap != nil && src.ConfigMap.Name == a2aPrincipalMapConfigMapName:
			cm = true
			if src.ConfigMap.Optional == nil || !*src.ConfigMap.Optional {
				t.Error("the Discord table is not optional; an install without it would not schedule the gateway")
			}
		case src.Secret != nil && src.Secret.Name == a2aSlackPrincipalMapSecretName:
			secret = true
			if src.Secret.Optional == nil || !*src.Secret.Optional {
				t.Errorf("the %s Secret is not optional; the gateway's rule for a missing map is to run and drop every sender", a2aSlackPrincipalMapSecretName)
			}
		default:
			t.Errorf("unexpected projection source in the principal map: %+v", src)
		}
	}
	if !cm || !secret {
		t.Errorf("the principal map projects ConfigMap=%v Secret=%v, want both", cm, secret)
	}
}

// TestChatArmedDropsTheSlackReferences: one real backend per gateway process.
// With Chat armed on the CR, neither Secret-armed reference is rendered, so
// a Secret left in the namespace cannot make the gateway refuse to start.
func TestChatArmedDropsTheSlackReferences(t *testing.T) {
	t.Setenv(a2aInjectBackendEnvVar, "")
	env := envMapOf(buildA2AGatewayDeployment(gchatTestAgent("next", true)).Spec.Template.Spec.Containers[0].Env)
	for _, name := range []string{a2aSlackBotTokenEnvVar, a2aSlackAppTokenEnvVar, "DISCORD_TOKEN"} {
		if _, ok := env[name]; ok {
			t.Errorf("%s is rendered beside the Chat backend", name)
		}
	}
}

// TestNoRenderedRoleReachesASecret: the identity table is an impersonation
// primitive, so no ServiceAccount the operator mints a Role for may hold a
// verb on Secrets - the a2a-slack-principal-map and a2a-slack-bot Secrets
// are reachable by the kubelet's mount alone. Every Role builder, next stack
// and legacy, is rendered and read; a wildcard resource counts as reaching
// them. What this does not bound is the gateway's `create` on pods, which can
// mount a Secret into a pod it builds; buildA2AGatewayRole's comment records
// that as the admission policy it owes.
func TestNoRenderedRoleReachesASecret(t *testing.T) {
	agent := a2aTestAgent()
	roles := map[string][]rbacv1.PolicyRule{
		"a2a gateway":        buildA2AGatewayRole(agent).Rules,
		"a2a callout":        buildA2ACalloutRole(agent).Rules,
		"broker tokenreview": buildCredentialBrokerTokenReviewRole(agent).Rules,
		"platform minimal":   buildMinimalPlatformRole(agent).Rules,
		"platform local":     buildPlatformLocalRole(agent).Rules,
		"platform leader":    buildPlatformLeaderRole(agent).Rules,
	}
	for name, rules := range roles {
		if len(rules) == 0 {
			t.Errorf("%s: no rules rendered; the assertion below would hold vacuously", name)
		}
		for _, rule := range rules {
			for _, res := range rule.Resources {
				if res == "secrets" || res == "*" {
					t.Errorf("%s grants %v on %q", name, rule.Verbs, res)
				}
			}
		}
	}
}

// TestNoEgressPolicySelectsTheGatewayPod: Slack is reached outbound (the
// Socket Mode websocket and the Web API), and the gateway pod carries no
// egress fence - its one policy is the ingress-only inject fence - so no
// rule has to admit those hosts, and none could name them: the repository's
// policies are selector and CIDR based. Pinned here so the day a deny-default
// egress fence is put on the gateway, this test is what says Slack's egress
// must come with it.
func TestNoEgressPolicySelectsTheGatewayPod(t *testing.T) {
	agent := a2aTestAgent()
	dns := []string{"10.96.0.10"}
	gatewayPod := labels.Set(buildA2AGatewayDeployment(agent).Spec.Template.Labels)
	agentEgress, _ := buildAgentEgressNetworkPolicy(agent, dns, "")
	policies := []*networkingv1.NetworkPolicy{
		buildA2AGatewayNetworkPolicy(agent),
		buildA2ASessionNetworkPolicy(agent, dns),
		buildA2ANATSNetworkPolicy(agent),
		buildA2AVerifierNetworkPolicy(agent, dns),
		buildCredentialProxyNetworkPolicy(agent),
		buildShellSandboxNetworkPolicy(agent, dns),
		agentEgress,
	}
	for _, pol := range policies {
		if pol == nil {
			continue
		}
		sel, err := metav1.LabelSelectorAsSelector(&pol.Spec.PodSelector)
		if err != nil {
			t.Fatalf("%s: %v", pol.Name, err)
		}
		if !sel.Matches(gatewayPod) {
			continue
		}
		for _, pt := range pol.Spec.PolicyTypes {
			if pt == networkingv1.PolicyTypeEgress {
				t.Errorf("%s fences the gateway pod's egress; Slack's websocket and Web API need admitting in it", pol.Name)
			}
		}
	}
}
