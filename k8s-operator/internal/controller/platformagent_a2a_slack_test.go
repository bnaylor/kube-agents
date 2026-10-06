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
	"k8s.io/utils/ptr"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

// The Slack token pair the tests configure: a Secret and keys of the
// install's own naming, so a render that reads a name of its own (the
// legacy default, or a fixed admin Secret) is told apart from one that
// reads the CR.
const (
	slackTestSecret = "team-slack" // #nosec G101 -- test Secret name, not a credential
	slackTestBotKey = "bot"
	slackTestAppKey = "app"
)

// slackTestAgent is a2aTestAgent with spec.integration.slack configured the
// way the CRD requires once it is enabled: both token refs present. mode is
// the CR's spec.mode ("" leaves it absent). The refs say nothing about
// optional, as the chart renders them.
func slackTestAgent(mode string, enabled bool) *agentv1alpha1.PlatformAgent {
	agent := a2aTestAgent()
	if mode == "" {
		agent.Spec.Mode = nil
	} else {
		agent.Spec.Mode = ptr.To(mode)
	}
	agent.Spec.Integration = &agentv1alpha1.PlatformAgentIntegrationSpec{
		Slack: &agentv1alpha1.SlackSpec{
			Enabled: ptr.To(enabled),
			BotTokenSecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: slackTestSecret}, Key: slackTestBotKey,
			},
			AppTokenSecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: slackTestSecret}, Key: slackTestAppKey,
			},
			AllowedUsers: []string{"U123"},
		},
	}
	return agent
}

// chatAndSlackTestAgent enables both integrations on one CR.
func chatAndSlackTestAgent(mode string) *agentv1alpha1.PlatformAgent {
	agent := gchatTestAgent(mode, true)
	agent.Spec.Integration.Slack = slackTestAgent(mode, true).Spec.Integration.Slack
	return agent
}

// TestSlackConsumerIsChosenByMode: one Slack app, one Socket Mode consumer.
// The two predicates are exact complements whenever Slack is enabled and
// both false when it is not. Chat holds the gateway first when both are
// enabled under next (the gateway runs one backend per process), and Slack
// then stays on the legacy consumer rather than reaching nobody.
func TestSlackConsumerIsChosenByMode(t *testing.T) {
	for _, tc := range []struct {
		name          string
		agent         *agentv1alpha1.PlatformAgent
		armed, legacy bool
	}{
		{"today with slack", slackTestAgent("", true), false, true},
		{"today explicit with slack", slackTestAgent("today", true), false, true},
		{"next with slack", slackTestAgent("next", true), true, false},
		{"next with slack disabled", slackTestAgent("next", false), false, false},
		{"today with slack disabled", slackTestAgent("", false), false, false},
		{"next with no integration", a2aTestAgent(), false, false},
		{"next with chat only", gchatTestAgent("next", true), false, false},
		{"next with chat and slack", chatAndSlackTestAgent("next"), false, true},
		{"today with chat and slack", chatAndSlackTestAgent(""), false, true},
		// Version skew fails closed to today, as a2aChatArmed does.
		{"skew with slack", slackTestAgent("later", true), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := a2aSlackArmed(tc.agent); got != tc.armed {
				t.Errorf("a2aSlackArmed = %v, want %v", got, tc.armed)
			}
			if got := legacySlackConsumer(tc.agent); got != tc.legacy {
				t.Errorf("legacySlackConsumer = %v, want %v", got, tc.legacy)
			}
			if a2aSlackArmed(tc.agent) && a2aChatArmed(tc.agent) {
				t.Error("Slack and Chat both armed on the gateway; it refuses two real backends")
			}
		})
	}
}

// TestAnArmedGatewayCarriesTheSlackBackend: under next with Slack enabled,
// the gateway's pair is read through the CR's own refs, required whatever
// the ref says, and the Discord reference is not rendered beside it.
func TestAnArmedGatewayCarriesTheSlackBackend(t *testing.T) {
	t.Setenv(a2aInjectBackendEnvVar, "")
	agent := slackTestAgent("next", true)
	// An admin who marked a ref optional would get a gateway that starts on
	// half a pair and exits; the gateway's copy is required regardless.
	agent.Spec.Integration.Slack.AppTokenSecretRef.Optional = ptr.To(true)
	env := envMapOf(buildA2AGatewayDeployment(agent).Spec.Template.Spec.Containers[0].Env)
	for name, key := range map[string]string{a2aSlackBotTokenEnvVar: slackTestBotKey, a2aSlackAppTokenEnvVar: slackTestAppKey} {
		e, ok := env[name]
		if !ok || e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
			t.Fatalf("%s is not a Secret reference on the armed gateway: %+v", name, e)
		}
		ref := e.ValueFrom.SecretKeyRef
		if ref.Name != slackTestSecret || ref.Key != key {
			t.Errorf("%s reads %s/%s, want the CR's %s/%s", name, ref.Name, ref.Key, slackTestSecret, key)
		}
		if ptr.Deref(ref.Optional, false) {
			t.Errorf("%s is an optional reference: a missing key would start the gateway on half a pair, which it refuses at boot", name)
		}
	}
	if !ptr.Deref(agent.Spec.Integration.Slack.AppTokenSecretRef.Optional, false) {
		t.Error("the render mutated the CR's own ref")
	}
	if _, ok := env["DISCORD_TOKEN"]; ok {
		t.Error("DISCORD_TOKEN is rendered beside the Slack backend; a discord-bot Secret left in the namespace would stop the gateway on two backends")
	}
}

// TestAnUnarmedGatewayCarriesNoSlackPair: off next, or with Slack disabled,
// the gateway carries no Slack env at all; the Discord reference is what it
// was on main.
func TestAnUnarmedGatewayCarriesNoSlackPair(t *testing.T) {
	t.Setenv(a2aInjectBackendEnvVar, "")
	for _, agent := range []*agentv1alpha1.PlatformAgent{a2aTestAgent(), slackTestAgent("next", false)} {
		env := envMapOf(buildA2AGatewayDeployment(agent).Spec.Template.Spec.Containers[0].Env)
		for _, name := range []string{a2aSlackBotTokenEnvVar, a2aSlackAppTokenEnvVar} {
			if _, ok := env[name]; ok {
				t.Errorf("%s is rendered on a gateway Slack does not arm", name)
			}
		}
		if e, ok := env["DISCORD_TOKEN"]; !ok || e.ValueFrom.SecretKeyRef.Name != a2aDiscordBotSecretName {
			t.Errorf("DISCORD_TOKEN = %+v, want the optional discord-bot reference main renders", e)
		}
	}
}

// TestTheSlackIntegrationRendersTheGateway: Slack enabled under next is a
// backend on the CR alone, so the gate renders the gateway with no Secret
// in the namespace - the same no-read answer Chat gets. A missing token
// Secret is the kubelet's to report on the pod (the refs are required).
func TestTheSlackIntegrationRendersTheGateway(t *testing.T) {
	t.Setenv(a2aInjectBackendEnvVar, "")
	agent := slackTestAgent("next", true)
	r, cl, _ := a2aGateTestReconcilerWithoutABackend(t, agent)
	ctx := context.Background()
	theCalloutIsServing(t, ctx, cl, r, agent)
	state, err := r.reconcileA2A(ctx, agent)
	if err != nil {
		t.Fatalf("reconcileA2A: %v", err)
	}
	if state.gatewayDark {
		t.Fatalf("Slack is enabled under next and the gateway is reported dark: %q", state.gatewayDarkReason)
	}
	dep := &appsv1.Deployment{}
	if err := cl.Get(ctx, types.NamespacedName{Name: a2aGatewayName(agent), Namespace: agent.Namespace}, dep); err != nil {
		t.Fatalf("the gateway Deployment was not rendered for a Slack install: %v", err)
	}
	if _, ok := envMapOf(dep.Spec.Template.Spec.Containers[0].Env)[a2aSlackAppTokenEnvVar]; !ok {
		t.Errorf("the rendered gateway carries no %s", a2aSlackAppTokenEnvVar)
	}
}

// TestTheDarkReasonNamesSlack: the remedy an admin reads off the A2AGateway
// condition names the Slack integration beside the Chat field, the Discord
// Secret and the door.
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
	for _, want := range []string{"spec.integration.slack", "spec.integration.googleChat", a2aDiscordBotSecretName, a2aInjectBackendEnvVar} {
		if !strings.Contains(state.gatewayDarkReason, want) {
			t.Errorf("the reason does not name %q: %q", want, state.gatewayDarkReason)
		}
	}
}

// slackLegacyBrokerNames are what arm the broker's own Socket Mode
// connection (credential_proxy.py, serve: SlackRelay is built when both are
// set), and slackLegacyAgentNames what point Hermes's slack platform at it.
var (
	slackLegacyBrokerNames = []string{"SLACK_BOT_TOKEN", "SLACK_APP_TOKEN"}
	slackLegacyAgentNames  = []string{"SLACK_RELAY_URL", "SLACK_ALLOWED_USERS", "SLACK_ALLOW_ALL_USERS"}
)

// TestTheLegacySlackConsumerIsNotRenderedUnderNext: the broker's Slack relay
// pair, the Hermes slack platform, its relay env on the agent container and
// its pins in the managed .env all go with the mode, as Chat's do. Under
// today and skew they are what main renders; with Chat holding the next
// gateway they stay, because the gateway has no room for Slack.
func TestTheLegacySlackConsumerIsNotRenderedUnderNext(t *testing.T) {
	for _, tc := range []struct {
		name   string
		agent  *agentv1alpha1.PlatformAgent
		legacy bool
	}{
		{"today with slack", slackTestAgent("", true), true},
		{"skew with slack", slackTestAgent("later", true), true},
		{"next with slack", slackTestAgent("next", true), false},
		{"next with chat and slack", chatAndSlackTestAgent("next"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker := envMapOf(buildCredentialProxyDeployment(tc.agent, "h").Spec.Template.Spec.Containers[0].Env)
			for _, name := range slackLegacyBrokerNames {
				if _, ok := broker[name]; ok != tc.legacy {
					t.Errorf("%s on the broker = %v, want %v", name, ok, tc.legacy)
				}
			}
			pod := buildPodTemplateSpec(tc.agent, "h", "h", "h", "h", nil, renderOptions{})
			env := envMapOf(brokerContainerNamed(pod.Spec.Containers, "platform-agent").Env)
			managed := renderManagedEnv(tc.agent)
			for _, name := range slackLegacyAgentNames {
				if _, ok := env[name]; ok != tc.legacy {
					t.Errorf("%s in the agent container env = %v, want %v", name, ok, tc.legacy)
				}
				if got := strings.Contains(managed, name+"="); got != tc.legacy {
					t.Errorf("%s in the managed .env = %v, want %v", name, got, tc.legacy)
				}
			}
			config := renderConfigYAML(tc.agent, nil)
			if enabled := strings.Contains(config, "slack:\n    enabled: true"); enabled != tc.legacy {
				t.Errorf("platforms.slack.enabled rendered %v, want %v; config:\n%s", enabled, tc.legacy, config)
			}
		})
	}
}

// TestNoRenderCarriesTwoSlackSocketModeConsumers walks every container the
// operator renders for one CR and counts the ones handed the app token,
// which is what opens a Socket Mode connection (the broker's SlackRelay, the
// gateway's Slack adapter). Slack spreads one app's events across every open
// connection, so two would split the workspace's messages; none would drop
// them. Exactly one, in every mode and beside Chat.
func TestNoRenderCarriesTwoSlackSocketModeConsumers(t *testing.T) {
	t.Setenv(a2aInjectBackendEnvVar, "")
	for name, agent := range map[string]*agentv1alpha1.PlatformAgent{
		"today":             slackTestAgent("", true),
		"next":              slackTestAgent("next", true),
		"skew":              slackTestAgent("later", true),
		"next with chat":    chatAndSlackTestAgent("next"),
		"today with chat":   chatAndSlackTestAgent(""),
		"next, inject door": slackTestAgent("next", true),
	} {
		t.Run(name, func(t *testing.T) {
			if strings.HasSuffix(name, "inject door") {
				t.Setenv(a2aInjectBackendEnvVar, "true")
			}
			containers := buildCredentialProxyDeployment(agent, "h").Spec.Template.Spec.Containers
			containers = append(containers, buildA2AGatewayDeployment(agent).Spec.Template.Spec.Containers...)
			containers = append(containers, buildPodTemplateSpec(agent, "h", "h", "h", "h", nil, renderOptions{}).Spec.Containers...)
			var holders []string
			for _, c := range containers {
				if _, ok := envMapOf(c.Env)["SLACK_APP_TOKEN"]; ok {
					holders = append(holders, c.Name)
				}
			}
			if len(holders) != 1 {
				t.Errorf("containers handed SLACK_APP_TOKEN: %v, want exactly one", holders)
			}
		})
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
// With Chat armed on the CR the gateway carries Chat alone - no Slack pair
// even with Slack enabled, and no Discord reference - so neither a Slack
// integration nor a Secret left in the namespace can make it refuse to start.
func TestChatArmedDropsTheSlackReferences(t *testing.T) {
	t.Setenv(a2aInjectBackendEnvVar, "")
	env := envMapOf(buildA2AGatewayDeployment(chatAndSlackTestAgent("next")).Spec.Template.Spec.Containers[0].Env)
	for _, name := range []string{a2aSlackBotTokenEnvVar, a2aSlackAppTokenEnvVar, "DISCORD_TOKEN"} {
		if _, ok := env[name]; ok {
			t.Errorf("%s is rendered beside the Chat backend", name)
		}
	}
	if _, ok := env[a2aGchatRelayURLEnvVar]; !ok {
		t.Errorf("%s is missing: Chat is armed and holds the gateway", a2aGchatRelayURLEnvVar)
	}
}

// TestNoRenderedRoleReachesASecret: the identity table is an impersonation
// primitive, so no ServiceAccount the operator mints a Role for may hold a
// verb on Secrets - the a2a-slack-principal-map Secret is reachable by the
// kubelet's mount alone. Every Role builder, next stack
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
