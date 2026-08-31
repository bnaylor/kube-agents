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

// The A2A stack the operator renders under `mode: next` and nothing else:
// the NATS/JetStream component, its stream/KV/topic provisioning, and the A2A
// gateway Deployment. Dark by construction — no call site outside the
// renderMode gate in Reconcile reaches this file.
//
// The deployment spec (docs/designs/spec-nats-deployment.md) is the law for
// streams, retention, and the account layout; subjects come from the payload
// spec (docs/designs/spec-a2a-payloads.md).
//
// PLAYGROUND POSTURE (stage 1): static per-component NATS users instead of
// the auth callout, single-node R1 JetStream (production: 3-node R3), no
// audit exporter, no breaker, gateway sweep as the only janitor. Each has a
// decided design in the specs; none gates "Adam can play." Static creds are
// the playground, not the product.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

const (
	// a2aPartOf marks every object of the next stack, per the stage 1 launch
	// constants, so `kubectl get -l app.kubernetes.io/part-of=a2a-next` is the
	// whole venue.
	a2aPartOf = "a2a-next"

	// a2aComponentLabel distinguishes the pieces for targeted cleanup — the
	// provision Job's name carries a content hash, so deletion goes by label.
	a2aComponentLabel = "kubeagents.x-k8s.io/a2a-component"

	a2aNATSImageEnvVar      = "A2A_NATS_IMAGE"
	defaultA2ANATSImage     = "nats:2.10-alpine"
	a2aProvisionImageEnvVar = "A2A_PROVISION_IMAGE"
	// nats-box carries the nats CLI the provisioning script drives.
	defaultA2AProvisionImage = "natsio/nats-box:0.14.5"
	a2aGatewayImageEnvVar    = "A2A_GATEWAY_IMAGE"
	// The stage 1 dev registry, per the launch-card constants. A dev toggle's
	// default may name a dev registry; graduation (stage 4) moves this to the
	// release pipeline alongside the other first-party images.
	defaultA2AGatewayImage = "northamerica-northeast1-docker.pkg.dev/bnaylor-kagents-dev/a2a-demo/gateway:latest"

	// a2aPostureComment travels on every rendered config and script so the
	// posture cannot be mistaken for the product when read on the cluster.
	a2aPostureComment = `# PLAYGROUND POSTURE (stage 1): static per-component NATS users instead of
# the auth callout, single-node R1 JetStream (production: 3-node R3), no
# audit exporter, no breaker, gateway sweep as the only janitor. Each has a
# decided design in the specs (spec-nats-deployment.md); none gates "Adam
# can play." Static creds are the playground, not the product.`
)

func a2aNATSImage() string {
	if override := os.Getenv(a2aNATSImageEnvVar); override != "" {
		return override
	}
	return defaultA2ANATSImage
}

func a2aProvisionImage() string {
	if override := os.Getenv(a2aProvisionImageEnvVar); override != "" {
		return override
	}
	return defaultA2AProvisionImage
}

func a2aGatewayImage() string {
	if override := os.Getenv(a2aGatewayImageEnvVar); override != "" {
		return override
	}
	return defaultA2AGatewayImage
}

func a2aNATSName(agent *agentv1alpha1.PlatformAgent) string    { return agent.Name + "-a2a-nats" }
func a2aGatewayName(agent *agentv1alpha1.PlatformAgent) string { return agent.Name + "-a2a-gateway" }

// a2aLabels returns the common labels with part-of overridden to a2a-next and
// the component named. withCommonLabels leaves pre-set keys alone, so these
// survive applyManaged.
func a2aLabels(agent *agentv1alpha1.PlatformAgent, component string) map[string]string {
	labels := commonLabels(agent)
	labels[labelPartOf] = a2aPartOf
	labels[a2aComponentLabel] = component
	return labels
}

// randomA2APassword returns a 32-hex-char credential. Playground: the value
// only ever lives in the two Secrets this file renders and is never a
// substitute for the auth callout.
func randomA2APassword() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generating NATS credential: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// a2aCredsKeys is every key the creds Secret must carry; an absent or empty
// key would render `password: ""` into nats.conf — a user anyone can log in
// as — so ensureA2ACredsSecret repairs the shape rather than trusting it.
var a2aCredsKeys = []string{"gateway-password", "worker-password", "seed-password", "web-password", "sys-password"}

// a2aReader returns the reader for A2A bookkeeping objects. Straight from the
// API server on purpose: the cached client's first Get against a kind starts
// a cluster-wide informer for it, and this path runs on every reconcile of
// every agent — including today-mode installs that will never render the A2A
// stack. Caching every Secret and Job in the cluster to serve that is the
// same trade APIReader already refuses for collector discovery.
func (r *PlatformAgentReconciler) a2aReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// ensureA2ACredsSecret creates the per-user credential Secret once and then
// leaves it alone: regenerating on reconcile would invalidate every connected
// client every few seconds. It survives a flip back to `today` on purpose —
// it is inert data, and re-enabling `next` must not re-roll credentials the
// gateway image may have cached in a still-running pod. The one thing it
// changes on an existing Secret is a missing or empty key, which it fills.
func (r *PlatformAgentReconciler) ensureA2ACredsSecret(ctx context.Context, agent *agentv1alpha1.PlatformAgent) (*corev1.Secret, error) {
	name := types.NamespacedName{Name: a2aNATSName(agent) + "-creds", Namespace: agent.Namespace}
	existing := &corev1.Secret{}
	err := r.a2aReader().Get(ctx, name, existing)
	if err == nil {
		repaired := false
		if existing.Data == nil {
			existing.Data = map[string][]byte{}
		}
		for _, key := range a2aCredsKeys {
			if len(existing.Data[key]) > 0 {
				continue
			}
			pw, err := randomA2APassword()
			if err != nil {
				return nil, err
			}
			existing.Data[key] = []byte(pw)
			repaired = true
		}
		if repaired {
			if err := r.Update(ctx, existing); err != nil {
				return nil, err
			}
		}
		return existing, nil
	}
	if !errors.IsNotFound(err) {
		return nil, err
	}

	data := map[string][]byte{}
	for _, key := range a2aCredsKeys {
		pw, err := randomA2APassword()
		if err != nil {
			return nil, err
		}
		data[key] = []byte(pw)
	}
	secret := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace, Labels: a2aLabels(agent, "nats-creds")},
		Data:       data,
	}
	if err := ctrl.SetControllerReference(agent, secret, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, secret); err != nil {
		return nil, err
	}
	return secret, nil
}

// buildA2ANATSConfigSecret renders nats.conf with the static account layout.
//
// The property being preserved, verbatim from the deployment spec: the bus
// decides who may say what before a message is read. Deny-by-default — a
// permissions block with allow lists denies everything else — with per-user
// _INBOX prefixes so the reply path cannot leak what the subject grants
// withheld. $JS.API.> on every app user is playground posture; production
// narrows it to the per-stream API subjects when the callout arms.
func buildA2ANATSConfigSecret(agent *agentv1alpha1.PlatformAgent, creds *corev1.Secret) *corev1.Secret {
	pw := func(key string) string { return string(creds.Data[key]) }

	conf := a2aPostureComment + `

server_name: ` + a2aNATSName(agent) + `
port: 4222
http: 8222

# Websocket listener for the web user (W8's UI reads the bus over this).
#
# Plain ws IS the playground posture, stated rather than implied, and stated
# accurately: the CONNECT frame carries the web password in cleartext across
# the pod network. The Service is ClusterIP, so nothing OUTSIDE the cluster
# reaches this listener — and since W6.2 an ingress NetworkPolicy fences the
# pod network too: 4222 from the enumerated bus clients only, and NO
# pod-network peer for 8222 or 9222. The port-forward the demo uses and the
# kubelet's readiness probe both enter from the node, which the policy does
# not govern, so the ws surface is reachable through kubectl and through
# nothing else. Production still terminates TLS in front of the bus, which is
# not a toggle that exists yet.
#
# The origin allow-list is the one thing here that is not posture. WebSockets
# are exempt from CORS, and the demo transport is a kubectl port-forward to
# 9222 on a workstation — for as long as that runs, every page the operator's
# browser visits can open a socket to localhost:9222, with a credential that
# lives in browser JS by construction.
#
# allowed_origins, NOT same_origin: same_origin compares the browser's Origin
# against this listener's own host:port, and the UI is always a page on a
# different port than the bus (vite on 5173, or an nginx port), so it can never
# match. Measured by W8 in a real browser: same_origin gives every W8
# deployment a 403 at the handshake. A CLI or Node client sends no Origin
# header at all, which both settings permit — which is exactly why this needed
# a browser to find.
#
# This is defense in depth and not a boundary: Origin is browser-asserted, so
# anything that is not a browser simply omits it. The boundary is the web
# user's grant list below.
websocket {
  port: 9222
  no_tls: true
  allowed_origins: ["http://localhost:5173", "http://127.0.0.1:5173"]
}

jetstream {
  store_dir: /data
  # Under the 40Gi PV; the stream max_bytes caps (20+5+1+1 GiB) plus KV live
  # inside this.
  max_file_store: 34359738368
}

accounts {
  APP {
    jetstream: enabled
    users [
      {
        # gateway: task requester, chat-session supervisor, session-registry
        # owner. Production scopes supervisor publish to sessions the gateway
        # spawned; statically that collapses to the task-events wildcard.
        user: gateway
        password: "` + pw("gateway-password") + `"
        permissions {
          # $JS.ACK.> / $JS.FC.> are the delivery path's reply subjects: an
          # explicit ack is a publish to $JS.ACK.<stream>.<consumer>..., and
          # push flow control answers on $JS.FC.>. Without them a consumer
          # redelivers forever while TCP health stays green (NR-5's incident).
          publish { allow = [
            "a2a.tasks.*.*.in",
            "a2a.tasks.*.*.events",
            "$KV.session-state.>",
            "$JS.API.>",
            "$JS.ACK.>",
            "$JS.FC.>",
            "_INBOX.gateway.>"
          ] }
          subscribe { allow = [
            "a2a.tasks.*.*.events",
            "a2a.agents.>",
            "agents.hb.>",
            "$KV.session-state.>",
            "_INBOX.gateway.>"
          ] }
        }
      }
      {
        # worker: executor for any addressee (production: per-identity users
        # minted by the callout; the shared static user is the playground).
        user: worker
        password: "` + pw("worker-password") + `"
        permissions {
          # Topic grants name the provisioned registry exactly (payload spec:
          # topics are provisioned-only). A wildcard here would let a publish
          # to an unprovisioned topic vanish into core NATS; the exact list
          # turns that into a connect-time refusal instead of silent loss.
          publish { allow = [
            "a2a.tasks.*.*.events",
            "a2a.topics.agent.platform.upgrade-readiness",
            "a2a.topics.shared.blueprint",
            "a2a.topics.shared.annotations",
            "a2a.agents.>",
            "agents.hb.>",
            "$KV.runtime-state.>",
            "$JS.API.>",
            "$JS.ACK.>",
            "$JS.FC.>",
            "_INBOX.worker.>"
          ] }
          subscribe { allow = [
            "a2a.tasks.>",
            "a2a.topics.>",
            "$KV.runtime-state.>",
            "_INBOX.worker.>"
          ] }
        }
      }
      {
        # seed: provisions the streams and buckets (the $JS.API grant is what
        # the provision Job runs under) and writes the starter topic entries
        # (W5's seed Job). Nothing on the task plane — a seed that can publish
        # tasks is a seed that can impersonate the fabric.
        user: seed
        password: "` + pw("seed-password") + `"
        permissions {
          publish { allow = [
            "a2a.topics.agent.platform.upgrade-readiness",
            "a2a.topics.shared.blueprint",
            "a2a.topics.shared.annotations",
            "$JS.API.>",
            "$JS.ACK.>",
            "_INBOX.seed.>"
          ] }
          subscribe { allow = [
            "a2a.topics.>",
            "_INBOX.seed.>"
          ] }
        }
      }
      {
        # web: W8's read surface, the one user meant to face a browser, and
        # the only user whose credential is published to one by design.
        #
        # "Read-only" is not expressible as a subject list, and the first
        # version of this user proved it the hard way. Subject permissions
        # cannot see a request BODY, and JetStream puts the reach there: a
        # consumer's target stream, its durability, and its delivery subject
        # are all fields, not subjects. Every grant below is therefore
        # enumerated per stream rather than wildcarded, because the wildcard
        # is what turned "may read a2a.>" into the three findings the W6.1
        # review reproduced live:
        #
        #   $JS.API.CONSUMER.CREATE.>  — a push consumer on KV_session-state
        #     with deliver_subject set to web's OWN inbox read the session
        #     registry out of a bucket web has no $KV grant for, on either
        #     side. Subscribe permissions are not consulted when a consumer
        #     is created; the deliver subject is.
        #   $JS.ACK.>                  — an ack subject names a stream and a
        #     CONSUMER, not the caller, so web could publish +TERM onto the
        #     gateway's in-flight delivery and destroy it. W8 uses ack-none
        #     ordered consumers, so the grant is simply gone.
        #   CONSUMER.MSG.NEXT.*.*      — web could pull messages off the
        #     gateway's own durable and retune its config through
        #     CONSUMER.CREATE, which is create-OR-UPDATE by name.
        #
        # The list is now exactly what W8 needs, confirmed against its live
        # conformance suite: the four a2a message streams, no KV buckets, no
        # enumeration (NAMES/LIST), no ACK, no FC, no DELETE.
        #
        # Residues that remain, because a static permission map cannot hold
        # them, both closed by the auth callout when it arms:
        #  - Durability is a body field. Withholding the legacy
        #    DURABLE.CREATE subject does NOT prevent a durable; the modern
        #    CREATE carries durable_name. max_consumers on each stream bounds
        #    what that can cost.
        #  - Within these four streams, consumer names are the caller's
        #    choice, so web can still address another principal's consumer.
        #    Dropping ACK removed the destructive half; what is left is
        #    stealing a delivery of data web may already read.
        user: web
        password: "` + pw("web-password") + `"
        permissions {
          publish { allow = [
            "$JS.API.INFO",
            "$JS.API.STREAM.INFO.TASKS",
            "$JS.API.STREAM.INFO.DIRECTORY",
            "$JS.API.STREAM.INFO.TOPICS-STATE",
            "$JS.API.STREAM.INFO.TOPICS-JOURNAL",
            "$JS.API.CONSUMER.CREATE.TASKS.>",
            "$JS.API.CONSUMER.CREATE.DIRECTORY.>",
            "$JS.API.CONSUMER.CREATE.TOPICS-STATE.>",
            "$JS.API.CONSUMER.CREATE.TOPICS-JOURNAL.>",
            "$JS.API.CONSUMER.INFO.TASKS.*",
            "$JS.API.CONSUMER.INFO.DIRECTORY.*",
            "$JS.API.CONSUMER.INFO.TOPICS-STATE.*",
            "$JS.API.CONSUMER.INFO.TOPICS-JOURNAL.*",
            "$JS.API.CONSUMER.MSG.NEXT.TASKS.*",
            "$JS.API.CONSUMER.MSG.NEXT.DIRECTORY.*",
            "$JS.API.CONSUMER.MSG.NEXT.TOPICS-STATE.*",
            "$JS.API.CONSUMER.MSG.NEXT.TOPICS-JOURNAL.*",
            "_INBOX.web.>"
          ] }
          subscribe { allow = [
            "a2a.>",
            "_INBOX.web.>"
          ] }
        }
      }
    ]
  }
  # $SYS: human operators and monitoring only; no agent authenticates here.
  SYS {
    users [ { user: sys, password: "` + pw("sys-password") + `" } ]
  }
}
system_account: SYS
`

	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      a2aNATSName(agent) + "-config",
			Namespace: agent.Namespace,
			Labels:    a2aLabels(agent, "nats-config"),
		},
		Data: map[string][]byte{"nats.conf": []byte(conf)},
	}
}

// buildA2ANATSStatefulSet renders the bus. confHash is a digest of the
// rendered nats.conf: the config Secret updates in place but the nats
// container only reads it at boot, so the hash rides the pod template — the
// agent Deployment's config-hash mechanism — and a changed render rolls the
// server instead of silently diverging from it.
func buildA2ANATSStatefulSet(agent *agentv1alpha1.PlatformAgent, confHash string) *appsv1.StatefulSet {
	name := a2aNATSName(agent)
	labels := a2aLabels(agent, "nats")
	selector := map[string]string{"app": name}
	podLabels := map[string]string{"app": name}
	for k, v := range labels {
		podLabels[k] = v
	}

	return &appsv1.StatefulSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: agent.Namespace, Labels: labels},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			// Single node, R1, per the dev posture in the deployment spec;
			// production guidance is a 3-node cluster with stream replicas R3.
			Replicas: ptr.To(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: map[string]string{"kubeagents.x-k8s.io/a2a-config-hash": confHash},
				},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: ptr.To(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr.To(true),
						RunAsUser:    ptr.To(int64(1000)),
						FSGroup:      ptr.To(int64(1000)),
					},
					Containers: []corev1.Container{{
						Name:  "nats",
						Image: a2aNATSImage(),
						Args:  []string{"-c", "/etc/nats/nats.conf"},
						Ports: []corev1.ContainerPort{
							{Name: "client", ContainerPort: 4222},
							{Name: "monitor", ContainerPort: 8222},
							{Name: "websocket", ContainerPort: 9222},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "config", MountPath: "/etc/nats", ReadOnly: true},
							{Name: "data", MountPath: "/data"},
						},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("monitor")},
							},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "config",
						VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: a2aNATSName(agent) + "-config"},
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("40Gi")},
					},
				},
			}},
		},
	}
}

func buildA2ANATSService(agent *agentv1alpha1.PlatformAgent) *corev1.Service {
	name := a2aNATSName(agent)
	return &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: agent.Namespace, Labels: a2aLabels(agent, "nats")},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": name},
			Ports: []corev1.ServicePort{
				{Name: "client", Port: 4222},
				{Name: "monitor", Port: 8222},
				// The web user's transport (W8). ClusterIP on purpose: the
				// demo reaches it with kubectl port-forward, and plain ws
				// must not be reachable any other way.
				{Name: "websocket", Port: 9222},
			},
		},
	}
}

// a2aSessionComponentLabel is the label pair the gateway spawner stamps on
// every session pod (a2a/gateway/spawn.go, the source of truth): part-of is
// a2aPartOf, and app.kubernetes.io/component carries this value.
const a2aSessionComponent = "a2a-session"

func a2aSessionNetpolName(agent *agentv1alpha1.PlatformAgent) string {
	return agent.Name + "-a2a-session-netpol"
}
func a2aNATSNetpolName(agent *agentv1alpha1.PlatformAgent) string {
	return agent.Name + "-a2a-nats-netpol"
}

// buildA2ASessionNetworkPolicy fences the pods the gateway spawns (W6.2,
// closing W4 finding #6: the agent pod was fenced while the workers it
// delegates to had open egress). Deny-by-default with exactly three
// destinations, which is everything a worker's job description needs:
//
//	DNS      — name resolution for the two peers below.
//	NATS 4222 — the bus, by pod label, never CIDR (a pod IP does not survive
//	            a restart, and a policy pinned to one silently stops matching).
//	LiteLLM  — the model path. Session pods carry no ServiceAccount and no
//	            Workload Identity (W4's model-auth decision), so there is no
//	            metadata-server rule, no API-server rule, and no 443 — a
//	            worker that needs the internet is a design change, not a
//	            policy widening.
//
// PolicyTypes includes Ingress with no rules on purpose: nothing dials a
// session pod — a listener in a worker would be an accident, and an accident
// should be unreachable. kubectl exec/logs ride the kubelet API, not the pod
// network, so debugging is unaffected.
func buildA2ASessionNetworkPolicy(agent *agentv1alpha1.PlatformAgent, profile netpolProfile) *networkingv1.NetworkPolicy {
	udp := corev1.ProtocolUDP
	tcp := corev1.ProtocolTCP

	return &networkingv1.NetworkPolicy{
		TypeMeta: metav1.TypeMeta{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      a2aSessionNetpolName(agent),
			Namespace: agent.Namespace,
			Labels:    a2aLabels(agent, "session-netpol"),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					labelPartOf:                   a2aPartOf,
					"app.kubernetes.io/component": a2aSessionComponent,
				},
			},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeIngress,
				networkingv1.PolicyTypeEgress,
			},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{
					Ports: []networkingv1.NetworkPolicyPort{
						{Protocol: &udp, Port: ptr.To(intstr.FromInt32(53))},
						{Protocol: &tcp, Port: ptr.To(intstr.FromInt32(53))},
					},
					To: dnsEgressPeers(profile),
				},
				{
					Ports: []networkingv1.NetworkPolicyPort{
						{Protocol: &tcp, Port: ptr.To(intstr.FromInt32(4222))},
					},
					To: []networkingv1.NetworkPolicyPeer{{
						PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
							labelPartOf:       a2aPartOf,
							a2aComponentLabel: "nats",
						}},
					}},
				},
				{
					// The shared port set (litellmEgressPorts owns why each
					// port is granted — the short version: policy matches
					// the POST-DNAT container port, which is 8080 everywhere
					// this repo renders LiteLLM, measured live after an
					// 80/4000 first cut hung every worker model call).
					Ports: litellmEgressPorts(),
					To: []networkingv1.NetworkPolicyPeer{{
						PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "litellm"}},
					}},
				},
			},
		},
	}
}

// buildA2ANATSNetworkPolicy governs ingress to the NATS pod (W6.2, closing
// the W6.1 review's finding W6: every pod in the cluster could reach
// 4222/8222/9222 while the deny-by-default bus grants did the real refusing).
// The network layer now agrees with the grants: 4222 from exactly the
// enumerated bus clients, nothing else.
//
// 8222 (monitor) and 9222 (ws) get no pod-network peer at all, decided rather
// than forgotten. Both surfaces are node-path consumers: the kubelet's
// readiness probe on 8222 and the demo's kubectl port-forward on 9222 enter
// from the node, which NetworkPolicy does not govern (Dataplane V2 exempts
// host-local traffic), so denying every pod costs neither. An in-cluster ws
// client would be W8's UI deployed into the cluster — a peer to add to this
// list when it exists, not a reason to leave the port open to every pod now.
func buildA2ANATSNetworkPolicy(agent *agentv1alpha1.PlatformAgent) *networkingv1.NetworkPolicy {
	tcp := corev1.ProtocolTCP

	return &networkingv1.NetworkPolicy{
		TypeMeta: metav1.TypeMeta{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      a2aNATSNetpolName(agent),
			Namespace: agent.Namespace,
			Labels:    a2aLabels(agent, "nats-netpol"),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{"app": a2aNATSName(agent)},
			},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				Ports: []networkingv1.NetworkPolicyPort{
					{Protocol: &tcp, Port: ptr.To(intstr.FromInt32(4222))},
				},
				From: []networkingv1.NetworkPolicyPeer{
					// The agent pod — W7's bridge is a sidecar in it, so the
					// bridge rides this selector.
					{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
						"app": agent.Name + "-gateway",
					}}},
					// The A2A gateway.
					{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
						"app": a2aGatewayName(agent),
					}}},
					// Session pods, by the spawner's labels.
					{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
						labelPartOf:                   a2aPartOf,
						"app.kubernetes.io/component": a2aSessionComponent,
					}}},
					// The provision Job's pods.
					{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
						labelPartOf:       a2aPartOf,
						a2aComponentLabel: "provision",
					}}},
					// W5's seed Job (a2a/deploy/seed.yaml). Hand-applied, not
					// a render, but a legitimate bus client whose re-run must
					// refuse at auth if anything, not hang at the dial.
					{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
						labelPartOf:       a2aPartOf,
						a2aComponentLabel: "seed",
					}}},
				},
			}},
		},
	}
}

// a2aProvisionScript is what was W2's provisioning payload: the four streams,
// three KV buckets, and three starter topics from the deployment spec, created
// idempotently with the nats CLI. Topics are provisioned-only (payload spec):
// which topics exist is exactly the subject lists rendered here.
func a2aProvisionScript(agent *agentv1alpha1.PlatformAgent) string {
	server := fmt.Sprintf("nats://seed:${SEED_PASSWORD}@%s.%s.svc:4222", a2aNATSName(agent), agent.Namespace)
	return a2aPostureComment + `
set -euo pipefail
# --inbox-prefix: every stream/kv call here is a $JS.API request whose reply
# lands on an inbox, and seed may only subscribe under _INBOX.seed.> — the
# CLI's default _INBOX.<nuid> would be refused and every call would time out.
NATS="nats --server ` + server + ` --inbox-prefix=_INBOX.seed"

# max_consumers caps each stream at 64. Consumer durability is a request-body
# field, so no permission list can hold web to ephemeral ones (see the web user
# in nats.conf); the cap is what stops an unreapable durable per page-load from
# growing the file store without bound. The failure it converts to is loud — a
# refused create — rather than silent disk growth. Note the trade: a client that
# burns the cap can also deny a legitimate consumer, which is the right way
# round for a playground and the wrong one for production, where the callout
# mints per-identity users and this becomes a per-user limit instead.

# Retention rule (deployment spec): acknowledgement must not delete — all
# message streams are limits-based with an age window; replay is a read.
# Every stream carries a hard max_bytes with discard old so a flood degrades
# replay oldest-first instead of filling the PV and stalling JetStream.

# TASKS: a2a.tasks.>, 72h dev window, 20GiB cap.
$NATS stream info TASKS >/dev/null 2>&1 || $NATS stream add TASKS \
  --subjects='a2a.tasks.>' --storage=file --retention=limits \
  --max-age=72h --max-bytes=21474836480 --discard=old --replicas=1 --max-consumers=64 --defaults

# DIRECTORY: last-value — the tombstone replaces the card. 1GiB cap.
$NATS stream info DIRECTORY >/dev/null 2>&1 || $NATS stream add DIRECTORY \
  --subjects='a2a.agents.>' --storage=file --retention=limits \
  --max-msgs-per-subject=1 --max-bytes=1073741824 --discard=old --replicas=1 --max-consumers=64 --defaults

# TOPICS-STATE: current answer plus short history, no age limit. 1GiB cap.
# State-class topics (provisioned registry): upgrade-readiness, blueprint, probe.
#
# The probe subject is the one here with NO writer, deliberately, and it is the
# single exception to the rule that a topic's subject list and its writer's
# grant travel together. It exists so that an authorization probe has a real
# provisioned subject to be refused ON: a refusal against an unprovisioned
# subject proves only that the subject does not exist, while a refusal here
# proves the grant. W8's UI ships that probe as a button, so it is pressed in
# front of an audience rather than living in a test file.
#
# It was aimed at the blueprint topic first. That works right up until the
# grant is wrong, at which point the probe writes junk into a state-class
# topic the fleet actually reads - and "it cannot happen while the grants
# hold" is the assumption the web user already broke once. A writerless
# subject makes the failure mode land nowhere.
$NATS stream info TOPICS-STATE >/dev/null 2>&1 || $NATS stream add TOPICS-STATE \
  --subjects='a2a.topics.agent.platform.upgrade-readiness,a2a.topics.shared.blueprint,a2a.topics.shared.probe' \
  --storage=file --retention=limits \
  --max-msgs-per-subject=8 --max-bytes=1073741824 --discard=old --replicas=1 --max-consumers=64 --defaults

# TOPICS-JOURNAL: append-only, ages out at 30d. 5GiB cap.
# Journal-class topics: annotations.
$NATS stream info TOPICS-JOURNAL >/dev/null 2>&1 || $NATS stream add TOPICS-JOURNAL \
  --subjects='a2a.topics.shared.annotations' --storage=file --retention=limits \
  --max-age=720h --max-bytes=5368709120 --discard=old --replicas=1 --max-consumers=64 --defaults

# Heartbeats (agents.hb.>) are core NATS, outside JetStream — no stream.

# KV buckets: runtime-state (who is alive), session-state (the gateway's
# registry; its user is the only writer), cap (reserved for capability
# entries per docs/architecture/09-capability-envelope.md — arms with the
# authority work). Capped at 256MiB each: the streams' max_bytes discipline
# applies to KV too, or unbounded bucket growth eats the file store's
# headroom and stalls every JetStream write.
$NATS kv info runtime-state >/dev/null 2>&1 || $NATS kv add runtime-state --history=1 --replicas=1 --storage=file --max-bucket-size=268435456
$NATS kv info session-state >/dev/null 2>&1 || $NATS kv add session-state --history=1 --replicas=1 --storage=file --max-bucket-size=268435456
$NATS kv info cap           >/dev/null 2>&1 || $NATS kv add cap --history=1 --replicas=1 --storage=file --max-bucket-size=268435456

echo "a2a provisioning complete"
`
}

// buildA2AProvisionJob runs the provisioning script against the rendered NATS.
// The name carries a hash of the script so a changed payload is a new Job —
// Jobs are immutable — and completed runs clean themselves up via TTL.
//
// Creation is create-only convergence: the script's `info || add` lines make
// re-runs clean but do NOT edit a stream that already exists, so a retention
// or subject change in a later payload reaches fresh installs only. Migrating
// an existing install is a manual `nats stream edit` — stage 1 accepts that
// and says it here rather than implying the hash-rename re-provisions.
func buildA2AProvisionJob(agent *agentv1alpha1.PlatformAgent) *batchv1.Job {
	script := a2aProvisionScript(agent)
	sum := sha256.Sum256([]byte(script))
	name := fmt.Sprintf("%s-a2a-provision-%s", agent.Name, hex.EncodeToString(sum[:])[:8])

	return &batchv1.Job{
		TypeMeta:   metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: agent.Namespace, Labels: a2aLabels(agent, "provision")},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To(int32(20)),
			TTLSecondsAfterFinished: ptr.To(int32(86400)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: a2aLabels(agent, "provision")},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyOnFailure,
					AutomountServiceAccountToken: ptr.To(false),
					Containers: []corev1.Container{{
						Name:    "provision",
						Image:   a2aProvisionImage(),
						Command: []string{"sh", "-c", script},
						Env: []corev1.EnvVar{{
							Name: "SEED_PASSWORD",
							ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: a2aNATSName(agent) + "-creds"},
								Key:                  "seed-password",
							}},
						}},
					}},
				},
			},
		},
	}
}

// buildA2AGatewayDeployment renders the A2A gateway (W3's Go binary; the
// Discord adapter and session manager). It is expected to crash-loop until
// the gateway image exists and W0's discord-bot Secret is created — both are
// optional references so the render never blocks the rest of the stack.
// The gateway's session-pod RBAC (W4): spawning a worker per delegated task
// is the gateway's job, so its ServiceAccount carries exactly the pod verbs
// the spawner and its sweep use, namespace-scoped, and nothing else.
// defaultA2AMaxSessions is spec.harness.tuning.maxSessions when unset; the
// CRD field's comment carries the sizing rationale. Keep it in step with
// defaultMaxSessions in a2a/gateway/config.go - the operator renders the
// value explicitly onto A2A_MAX_SESSIONS, so the gateway's own constant only
// governs runs outside the operator (the playground path).
const defaultA2AMaxSessions = 10

// a2aQuotaHeadroom is what the namespace pod quota adds above the gateway's
// cap. The quota is namespace-wide because that is the only shape a hostile
// pod-creator cannot dodge (ResourceQuota scopes select on fields the
// creator writes), so it must leave room for everything else that
// legitimately runs here: the rendered stack and its neighbors (operator,
// agent pod, gateway, NATS, LiteLLM, dashboard), Job pods (provision, seed),
// rollout surge doubling a Deployment for a moment, and the gateway's
// count-then-create overshoot. Fifteen covers roughly ten standing pods plus
// surge; if the base install grows past that, raise this before anything
// user-visible starts failing admission.
const a2aQuotaHeadroom = 15

func resolveA2AMaxSessions(agent *agentv1alpha1.PlatformAgent) int {
	if limits := agentTuning(agent); limits != nil && limits.MaxSessions != nil {
		return *limits.MaxSessions
	}
	return defaultA2AMaxSessions
}

func a2aSessionQuotaName(agent *agentv1alpha1.PlatformAgent) string {
	return agent.Name + "-a2a-session-quota"
}

// buildA2ASessionQuota is the enforcement half of the session-pod bound; the
// gateway's A2A_MAX_SESSIONS cap is the usability half. The distinction is
// the point: the gateway counts and refuses so users get an honest chat
// reply, but the thing being bounded is the gateway itself - a compromised
// or buggy gateway ignores its own cap and cannot ignore this quota, whose
// admission check the API server runs and whose object the gateway's
// pods-only Role cannot touch. Sized above the cap so the gateway hits its
// own limit first and nobody legitimate ever sees the admission failure.
//
// `pods` (not count/pods) is deliberate: it counts non-terminal pods only,
// matching the gateway's LiveSessions denominator, so a finished worker
// awaiting sweep does not hold a slot. It is also the only key - a
// compute-resource key (requests.*) would force resource requests onto
// every pod in the namespace, which is not this bound's mandate.
func buildA2ASessionQuota(agent *agentv1alpha1.PlatformAgent) *corev1.ResourceQuota {
	limit := int64(resolveA2AMaxSessions(agent) + a2aQuotaHeadroom)
	return &corev1.ResourceQuota{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ResourceQuota"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      a2aSessionQuotaName(agent),
			Namespace: agent.Namespace,
			Labels:    a2aLabels(agent, "session-quota"),
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourcePods: *resource.NewQuantity(limit, resource.DecimalSI),
			},
		},
	}
}

func buildA2AGatewayServiceAccount(agent *agentv1alpha1.PlatformAgent) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{Name: a2aGatewayName(agent), Namespace: agent.Namespace, Labels: a2aLabels(agent, "gateway")},
	}
}

func buildA2AGatewayRole(agent *agentv1alpha1.PlatformAgent) *rbacv1.Role {
	return &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Name: a2aGatewayName(agent), Namespace: agent.Namespace, Labels: a2aLabels(agent, "gateway")},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Resources: []string{"pods"},
			Verbs:     []string{"create", "get", "list", "watch", "delete"},
		}},
	}
}

func buildA2AGatewayRoleBinding(agent *agentv1alpha1.PlatformAgent) *rbacv1.RoleBinding {
	name := a2aGatewayName(agent)
	return &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: agent.Namespace, Labels: a2aLabels(agent, "gateway")},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: name},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: name, Namespace: agent.Namespace}},
	}
}

func buildA2AGatewayDeployment(agent *agentv1alpha1.PlatformAgent) *appsv1.Deployment {
	name := a2aGatewayName(agent)
	labels := a2aLabels(agent, "gateway")
	selector := map[string]string{"app": name}
	podLabels := map[string]string{"app": name}
	for k, v := range labels {
		podLabels[k] = v
	}

	return &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: agent.Namespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					// W4 armed the session-pod path (the Delegate flow), so
					// the gateway now runs as its own ServiceAccount with the
					// pod-lifecycle Role above - the token this automounts is
					// exactly that grant, nothing ambient.
					ServiceAccountName:           a2aGatewayName(agent),
					AutomountServiceAccountToken: ptr.To(true),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr.To(true),
						RunAsUser:    ptr.To(int64(1000)),
					},
					Containers: []corev1.Container{{
						Name:  "gateway",
						Image: a2aGatewayImage(),
						Env: []corev1.EnvVar{
							{Name: "NATS_URL", Value: fmt.Sprintf("nats://%s.%s.svc:4222", a2aNATSName(agent), agent.Namespace)},
							{Name: "NATS_USER", Value: "gateway"},
							{Name: "NATS_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: a2aNATSName(agent) + "-creds"},
								Key:                  "gateway-password",
							}}},
							// W0 creates this Secret by hand (launch card); the
							// reference is optional so the pod schedules before it.
							{Name: "DISCORD_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "discord-bot"},
								Key:                  "token",
								Optional:             ptr.To(true),
							}}},
							// The W4 switch, flipped: session-pod spawning is
							// live for the Delegate flow. The default route
							// stays the fixed addressee; only a "delegate"
							// turn spawns a worker.
							{Name: "A2A_SPAWN_SESSIONS", Value: "true"},
							// Rendered explicitly even when the CR is silent:
							// the number a `kubectl describe` reader sees is
							// the same one the session quota was sized above,
							// so the two halves cannot drift apart silently.
							{Name: "A2A_MAX_SESSIONS", Value: strconv.Itoa(resolveA2AMaxSessions(agent))},
							// W3's ask: the namespace from the downward API,
							// not a baked default.
							{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: "metadata.namespace",
							}}},
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name: "principal-map", MountPath: "/etc/a2a/principal-map", ReadOnly: true,
						}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "principal-map",
						VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "principal-map"},
							Optional:             ptr.To(true),
						}},
					}},
				},
			},
		},
	}
}

// a2aProvisionState reports where the provision Job stands, because nothing
// watches Jobs (a Job watch would mean a cluster-wide informer every install
// pays for; see a2aReader). Pending drives a requeue so completion — or the
// TTL removing a finished Job — is noticed without an unrelated event; failed
// drives a Degraded status so a dead bus is visible in `kubectl describe`
// rather than sitting behind a Ready phase.
type a2aProvisionState struct {
	done    bool
	failed  bool
	message string
}

// reconcileA2A renders the next stack. Callers gate on renderMode; this
// function assumes the answer was ModeNext.
func (r *PlatformAgentReconciler) reconcileA2A(ctx context.Context, agent *agentv1alpha1.PlatformAgent) (a2aProvisionState, error) {
	state := a2aProvisionState{}

	creds, err := r.ensureA2ACredsSecret(ctx, agent)
	if err != nil {
		return state, fmt.Errorf("failed to ensure A2A NATS creds: %w", err)
	}

	config := buildA2ANATSConfigSecret(agent, creds)
	if err := ctrl.SetControllerReference(agent, config, r.Scheme); err != nil {
		return state, err
	}
	if err := r.applyManaged(ctx, agent, config); err != nil {
		return state, fmt.Errorf("failed to apply A2A NATS config: %w", err)
	}

	confSum := sha256.Sum256(config.Data["nats.conf"])
	sts := buildA2ANATSStatefulSet(agent, hex.EncodeToString(confSum[:])[:16])
	if err := ctrl.SetControllerReference(agent, sts, r.Scheme); err != nil {
		return state, err
	}
	if err := r.applyManaged(ctx, agent, sts); err != nil {
		return state, fmt.Errorf("failed to apply A2A NATS StatefulSet: %w", err)
	}

	svc := buildA2ANATSService(agent)
	if err := ctrl.SetControllerReference(agent, svc, r.Scheme); err != nil {
		return state, err
	}
	if err := r.applyManaged(ctx, agent, svc); err != nil {
		return state, fmt.Errorf("failed to apply A2A NATS Service: %w", err)
	}

	// The W6.2 fences: ingress to the bus and egress from session pods. Both
	// ride this function so they appear and disappear with the stack they
	// fence — including the skew freeze, where tearing down the ingress
	// policy would not break the bus but tearing down the session policy
	// mid-task would unfence a running worker.
	for _, np := range []*networkingv1.NetworkPolicy{
		buildA2ANATSNetworkPolicy(agent),
		buildA2ASessionNetworkPolicy(agent, r.resolveNetpolProfile(ctx, agent)),
	} {
		if err := ctrl.SetControllerReference(agent, np, r.Scheme); err != nil {
			return state, err
		}
		if err := r.applyManaged(ctx, agent, np); err != nil {
			return state, fmt.Errorf("failed to apply A2A NetworkPolicy %s: %w", np.Name, err)
		}
	}

	// The session-pod quota, the enforcement half of the bound whose
	// usability half is the gateway's own cap (see buildA2ASessionQuota for
	// why both exist and why the quota sits above the cap).
	quota := buildA2ASessionQuota(agent)
	if err := ctrl.SetControllerReference(agent, quota, r.Scheme); err != nil {
		return state, err
	}
	if err := r.applyManaged(ctx, agent, quota); err != nil {
		return state, fmt.Errorf("failed to apply A2A session ResourceQuota: %w", err)
	}

	// Jobs are immutable, so the provision Job is create-if-absent under its
	// content-hashed name; a payload change is a new name and a fresh run.
	job := buildA2AProvisionJob(agent)
	if err := ctrl.SetControllerReference(agent, job, r.Scheme); err != nil {
		return state, err
	}
	withCommonLabels(job, agent)
	existing := &batchv1.Job{}
	if err := r.a2aReader().Get(ctx, client.ObjectKeyFromObject(job), existing); err != nil {
		if !errors.IsNotFound(err) {
			return state, err
		}
		if err := r.Create(ctx, job); err != nil {
			return state, fmt.Errorf("failed to create A2A provision Job: %w", err)
		}
	} else {
		for _, cond := range existing.Status.Conditions {
			if cond.Status != corev1.ConditionTrue {
				continue
			}
			switch cond.Type {
			case batchv1.JobComplete:
				state.done = true
			case batchv1.JobFailed:
				state.failed = true
				state.message = fmt.Sprintf(
					"A2A provision Job %s failed (%s: %s); the bus has no streams until it succeeds. Inspect its pod logs; deleting the Job retries.",
					existing.Name, cond.Reason, cond.Message)
			}
		}
	}

	// Identity before workload: the gateway pod must not start before the
	// ServiceAccount its pod spec names exists.
	for _, obj := range []client.Object{
		buildA2AGatewayServiceAccount(agent),
		buildA2AGatewayRole(agent),
		buildA2AGatewayRoleBinding(agent),
	} {
		if err := ctrl.SetControllerReference(agent, obj, r.Scheme); err != nil {
			return state, err
		}
		if err := r.applyManaged(ctx, agent, obj); err != nil {
			return state, fmt.Errorf("failed to apply A2A gateway %T: %w", obj, err)
		}
	}

	dep := buildA2AGatewayDeployment(agent)
	if err := ctrl.SetControllerReference(agent, dep, r.Scheme); err != nil {
		return state, err
	}
	if err := r.applyManaged(ctx, agent, dep); err != nil {
		return state, fmt.Errorf("failed to apply A2A gateway Deployment: %w", err)
	}

	return state, nil
}

// cleanupA2A returns the dark stack to dark when the mode is not next. The
// creds Secret stays (inert data; re-enabling must not re-roll credentials)
// and so does the StatefulSet's PVC (JetStream's file store is the audit
// substrate — flipping a mode is not license to destroy evidence).
//
// Session pods are the gateway's, not the operator's, so a flip mid-task
// leaves a running worker behind after its egress fence is deleted here —
// unfenced with the worker credential in its env, but bounded: the adapter
// self-terminates at its task deadline (default 1800s), the bus it would
// dial is being torn down in the same pass, and pre-W6.2 these pods were
// never fenced at all. Reaping unowned pods from the operator would need a
// deliberate exception to the IsControlledBy refusal below; until someone
// makes that call, the window is documented rather than closed.
func (r *PlatformAgentReconciler) cleanupA2A(ctx context.Context, agent *agentv1alpha1.PlatformAgent) error {
	// Deployment/StatefulSet/Service reads come from the cache — those kinds
	// are already watched (Owns) so the reads are free. Secret and Job reads
	// go through a2aReader: a cached read would start a cluster-wide informer
	// for a kind this controller otherwise never watches, on every install.
	named := []struct {
		obj    client.Object
		reader client.Reader
	}{
		{&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: a2aGatewayName(agent), Namespace: agent.Namespace}}, r.Client},
		{&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: a2aGatewayName(agent), Namespace: agent.Namespace}}, r.a2aReader()},
		{&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: a2aGatewayName(agent), Namespace: agent.Namespace}}, r.a2aReader()},
		{&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: a2aGatewayName(agent), Namespace: agent.Namespace}}, r.a2aReader()},
		{&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: a2aNATSName(agent), Namespace: agent.Namespace}}, r.Client},
		{&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: a2aNATSName(agent), Namespace: agent.Namespace}}, r.Client},
		// NetworkPolicy is an Owns() kind (the agent's own policy), so the
		// cached reads are free.
		{&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: a2aNATSNetpolName(agent), Namespace: agent.Namespace}}, r.Client},
		{&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: a2aSessionNetpolName(agent), Namespace: agent.Namespace}}, r.Client},
		{&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: a2aNATSName(agent) + "-config", Namespace: agent.Namespace}}, r.a2aReader()},
		// ResourceQuota is not a watched kind, so the read goes through
		// a2aReader like the Secrets. Deleting it here is safe even with
		// session pods still draining (see the function comment): a quota
		// only gates admission, never running pods.
		{&corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: a2aSessionQuotaName(agent), Namespace: agent.Namespace}}, r.a2aReader()},
	}
	for _, entry := range named {
		obj := entry.obj
		if err := entry.reader.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			if client.IgnoreNotFound(err) != nil {
				return err
			}
			continue
		}
		if !metav1.IsControlledBy(obj, agent) {
			return fmt.Errorf("refusing to delete unowned A2A %T %s/%s", obj, obj.GetNamespace(), obj.GetName())
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, obj)); err != nil {
			return err
		}
	}

	// Provision Jobs carry a content hash in the name; find them by label.
	var jobs batchv1.JobList
	if err := r.a2aReader().List(ctx, &jobs, client.InNamespace(agent.Namespace), client.MatchingLabels{
		a2aComponentLabel: "provision",
		labelInstance:     instanceLabel(agent.Namespace, agent.Name),
	}); err != nil {
		return err
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if !metav1.IsControlledBy(job, agent) {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))); err != nil {
			return err
		}
	}
	return nil
}
