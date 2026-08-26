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

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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
var a2aCredsKeys = []string{"gateway-password", "worker-password", "seed-password", "sys-password"}

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

func buildA2ANATSStatefulSet(agent *agentv1alpha1.PlatformAgent) *appsv1.StatefulSet {
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
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
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
			},
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

# Retention rule (deployment spec): acknowledgement must not delete — all
# message streams are limits-based with an age window; replay is a read.
# Every stream carries a hard max_bytes with discard old so a flood degrades
# replay oldest-first instead of filling the PV and stalling JetStream.

# TASKS: a2a.tasks.>, 72h dev window, 20GiB cap.
$NATS stream info TASKS >/dev/null 2>&1 || $NATS stream add TASKS \
  --subjects='a2a.tasks.>' --storage=file --retention=limits \
  --max-age=72h --max-bytes=21474836480 --discard=old --replicas=1 --defaults

# DIRECTORY: last-value — the tombstone replaces the card. 1GiB cap.
$NATS stream info DIRECTORY >/dev/null 2>&1 || $NATS stream add DIRECTORY \
  --subjects='a2a.agents.>' --storage=file --retention=limits \
  --max-msgs-per-subject=1 --max-bytes=1073741824 --discard=old --replicas=1 --defaults

# TOPICS-STATE: current answer plus short history, no age limit. 1GiB cap.
# State-class topics (provisioned registry): upgrade-readiness, blueprint.
$NATS stream info TOPICS-STATE >/dev/null 2>&1 || $NATS stream add TOPICS-STATE \
  --subjects='a2a.topics.agent.platform.upgrade-readiness,a2a.topics.shared.blueprint' \
  --storage=file --retention=limits \
  --max-msgs-per-subject=8 --max-bytes=1073741824 --discard=old --replicas=1 --defaults

# TOPICS-JOURNAL: append-only, ages out at 30d. 5GiB cap.
# Journal-class topics: annotations.
$NATS stream info TOPICS-JOURNAL >/dev/null 2>&1 || $NATS stream add TOPICS-JOURNAL \
  --subjects='a2a.topics.shared.annotations' --storage=file --retention=limits \
  --max-age=720h --max-bytes=5368709120 --discard=old --replicas=1 --defaults

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
					// No k8s API use until W3 arms the session-pod spawn path
					// (which is behind its per-conversation addressee config);
					// the token arrives with that change, not before.
					AutomountServiceAccountToken: ptr.To(false),
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

	sts := buildA2ANATSStatefulSet(agent)
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
		{&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: a2aNATSName(agent), Namespace: agent.Namespace}}, r.Client},
		{&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: a2aNATSName(agent), Namespace: agent.Namespace}}, r.Client},
		{&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: a2aNATSName(agent) + "-config", Namespace: agent.Namespace}}, r.a2aReader()},
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
