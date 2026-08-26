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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

func a2aTestAgent() *agentv1alpha1.PlatformAgent {
	return &agentv1alpha1.PlatformAgent{
		ObjectMeta: metav1.ObjectMeta{Name: "test-agent", Namespace: "test-ns"},
		Spec:       agentv1alpha1.PlatformAgentSpec{Mode: ptr.To("next")},
	}
}

func a2aTestCreds() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "test-agent-a2a-nats-creds", Namespace: "test-ns"},
		Data: map[string][]byte{
			"gateway-password": []byte("pw-gateway"),
			"worker-password":  []byte("pw-worker"),
			"seed-password":    []byte("pw-seed"),
		},
	}
}

// The deployment spec's connect-time property: the bus decides who may say
// what before a message is read. Static users are the playground stand-in for
// the auth callout, but the deny-by-default subject lists are the real shape.
func TestBuildA2ANATSConfig(t *testing.T) {
	agent := a2aTestAgent()
	secret := buildA2ANATSConfigSecret(agent, a2aTestCreds())

	if secret.Name != "test-agent-a2a-nats-config" {
		t.Errorf("config secret name = %q", secret.Name)
	}
	if got := secret.Labels["app.kubernetes.io/part-of"]; got != a2aPartOf {
		t.Errorf("part-of label = %q, want %q", got, a2aPartOf)
	}

	conf := string(secret.Data["nats.conf"])
	if !strings.Contains(conf, "PLAYGROUND POSTURE") {
		t.Error("nats.conf is missing the playground-posture comment block")
	}
	if !strings.Contains(conf, "jetstream") {
		t.Error("nats.conf does not enable jetstream")
	}

	// Per-user inbox prefixes: without them any agent can subscribe to any
	// inbox and the connect-time property leaks through the reply path.
	for _, user := range []string{"gateway", "worker", "seed"} {
		if !strings.Contains(conf, "user: "+user) {
			t.Errorf("nats.conf missing user %q", user)
		}
		if !strings.Contains(conf, "_INBOX."+user+".>") {
			t.Errorf("nats.conf missing the _INBOX prefix for %q", user)
		}
	}

	// Passwords come from the creds Secret, not from literals invented here.
	for _, pw := range []string{"pw-gateway", "pw-worker", "pw-seed"} {
		if !strings.Contains(conf, pw) {
			t.Errorf("nats.conf does not carry the generated password %q", pw)
		}
	}

	// Spot the load-bearing grants: the gateway owns the session registry and
	// the task plane; the seed writes exactly the three starter topics.
	for _, grant := range []string{
		"a2a.tasks.*.*.in",
		"a2a.tasks.*.*.events",
		"$KV.session-state.>",
		"a2a.topics.agent.platform.upgrade-readiness",
		"a2a.topics.shared.blueprint",
		"a2a.topics.shared.annotations",
		// The delivery path's reply subjects: without $JS.ACK.> an explicit
		// ack is a permissions violation and every consumer redelivers
		// forever while TCP health stays green (the NR-5 incident class).
		"$JS.ACK.>",
		"$JS.FC.>",
	} {
		if !strings.Contains(conf, grant) {
			t.Errorf("nats.conf missing grant %q", grant)
		}
	}

	// No app user authenticates into $SYS.
	if strings.Contains(conf, "account: SYS") && !strings.Contains(conf, "system_account") {
		t.Error("nats.conf wires an app user into $SYS")
	}
}

func TestBuildA2ANATSStatefulSet(t *testing.T) {
	agent := a2aTestAgent()
	sts := buildA2ANATSStatefulSet(agent)

	if sts.Name != "test-agent-a2a-nats" {
		t.Errorf("statefulset name = %q", sts.Name)
	}
	if got := *sts.Spec.Replicas; got != 1 {
		t.Errorf("replicas = %d, want 1 (single node R1 is the dev posture)", got)
	}
	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("expected one volumeClaimTemplate (JetStream file store on a PV), got %d", len(sts.Spec.VolumeClaimTemplates))
	}
	if got := sts.Spec.Template.Spec.Containers[0].Image; got != defaultA2ANATSImage {
		t.Errorf("image = %q, want %q", got, defaultA2ANATSImage)
	}
	if got := sts.Labels["app.kubernetes.io/part-of"]; got != a2aPartOf {
		t.Errorf("part-of label = %q, want %q", got, a2aPartOf)
	}

	t.Setenv(a2aNATSImageEnvVar, "example.com/nats:pinned")
	if got := buildA2ANATSStatefulSet(agent).Spec.Template.Spec.Containers[0].Image; got != "example.com/nats:pinned" {
		t.Errorf("env override ignored, image = %q", got)
	}
}

// The provisioning payload is what was W2: four streams, three KV buckets,
// three starter topics, the deployment spec's retention numbers verbatim.
func TestBuildA2AProvisionJob(t *testing.T) {
	agent := a2aTestAgent()
	job := buildA2AProvisionJob(agent)

	if !strings.HasPrefix(job.Name, "test-agent-a2a-provision") {
		t.Errorf("job name = %q", job.Name)
	}
	script := ""
	for _, c := range job.Spec.Template.Spec.Containers {
		for _, e := range c.Args {
			script += e + "\n"
		}
		for _, e := range c.Command {
			script += e + "\n"
		}
	}

	for _, want := range []string{
		// TASKS: a2a.tasks.>, 72h, 20GiB
		"TASKS", "a2a.tasks.>", "--max-age=72h", "21474836480",
		// DIRECTORY: last-value, 1GiB
		"DIRECTORY", "a2a.agents.>", "--max-msgs-per-subject=1",
		// TOPICS-STATE: 8-deep, no age, the two state topics
		"TOPICS-STATE", "--max-msgs-per-subject=8",
		"a2a.topics.agent.platform.upgrade-readiness", "a2a.topics.shared.blueprint",
		// TOPICS-JOURNAL: 30d, 5GiB, the journal topic
		"TOPICS-JOURNAL", "--max-age=720h", "5368709120", "a2a.topics.shared.annotations",
		// max_bytes discipline
		"1073741824", "--discard=old",
		// KV buckets, capped like the streams
		"runtime-state", "session-state", "--max-bucket-size",
		// Every stream/kv call is a $JS.API request answered on an inbox, and
		// seed may only subscribe under _INBOX.seed.> — without the prefix
		// override every CLI call times out and the Job can never succeed.
		"--inbox-prefix=_INBOX.seed",
		// posture
		"PLAYGROUND POSTURE",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("provision script missing %q", want)
		}
	}
	// The reserved capability bucket ("cap", capability envelope design) —
	// checked as a distinct word so "cap" inside another token cannot satisfy it.
	if !strings.Contains(script, "kv add cap") {
		t.Error("provision script missing the reserved capability bucket")
	}
}

// mode: next renders the A2A stack; flipping back to today removes it. This is
// the reconciler-level gate — builders are covered above.
func TestReconcileA2AGatedByMode(t *testing.T) {
	scheme := setupScheme()
	agent := a2aTestAgent()

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(agent).
		WithStatusSubresource(&agentv1alpha1.PlatformAgent{}).
		WithInterceptorFuncs(fakeServerSideApplyInterceptors()).
		Build()

	r := &PlatformAgentReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-agent", Namespace: "test-ns"}}
	ctx := context.Background()

	// finalizer pass, then the real one
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile 1 failed: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile 2 failed: %v", err)
	}

	sts := &appsv1.StatefulSet{}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-nats", Namespace: "test-ns"}, sts); err != nil {
		t.Errorf("NATS StatefulSet not rendered under next: %v", err)
	}
	svc := &corev1.Service{}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-nats", Namespace: "test-ns"}, svc); err != nil {
		t.Errorf("NATS Service not rendered under next: %v", err)
	}
	creds := &corev1.Secret{}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-nats-creds", Namespace: "test-ns"}, creds); err != nil {
		t.Fatalf("creds Secret not rendered under next: %v", err)
	}
	for _, key := range []string{"gateway-password", "worker-password", "seed-password"} {
		if len(creds.Data[key]) < 24 {
			t.Errorf("creds key %q missing or too short", key)
		}
	}
	gen1 := string(creds.Data["gateway-password"])

	jobs := &batchv1.JobList{}
	if err := cl.List(ctx, jobs); err != nil || len(jobs.Items) == 0 {
		t.Errorf("provision Job not rendered under next (err=%v, n=%d)", err, len(jobs.Items))
	}
	dep := &appsv1.Deployment{}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-gateway", Namespace: "test-ns"}, dep); err != nil {
		t.Errorf("A2A gateway Deployment not rendered under next: %v", err)
	}

	// Reconcile again: the creds Secret must be generated once and kept, not
	// re-rolled — re-rolling would invalidate every connected client on every
	// reconcile.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile 3 failed: %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-nats-creds", Namespace: "test-ns"}, creds); err != nil {
		t.Fatalf("creds Secret vanished on re-reconcile: %v", err)
	}
	if string(creds.Data["gateway-password"]) != gen1 {
		t.Error("creds Secret was regenerated on re-reconcile")
	}

	// Flip to today: the dark stack goes back to dark.
	fresh := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("failed to get agent: %v", err)
	}
	fresh.Spec.Mode = nil
	if err := cl.Update(ctx, fresh); err != nil {
		t.Fatalf("failed to update agent: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile 4 failed: %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-nats", Namespace: "test-ns"}, sts); !errors.IsNotFound(err) {
		t.Errorf("NATS StatefulSet still present under today (err=%v)", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-gateway", Namespace: "test-ns"}, dep); !errors.IsNotFound(err) {
		t.Errorf("A2A gateway Deployment still present under today (err=%v)", err)
	}
	// Today's own stack is untouched by the cleanup.
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-gateway", Namespace: "test-ns"}, &appsv1.Deployment{}); err != nil {
		t.Errorf("today's Deployment missing after A2A cleanup: %v", err)
	}
}

// Version skew must not touch the A2A branch in either direction: renderMode
// fails closed to today, and letting that reach cleanupA2A would have a
// one-version operator rollback tear down a live bus a newer CRD legitimately
// rendered. Skew is a status problem, not a rendering instruction.
func TestUnrecognizedModePreservesRunningNextStack(t *testing.T) {
	scheme := setupScheme()
	agent := a2aTestAgent()

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(agent).
		WithStatusSubresource(&agentv1alpha1.PlatformAgent{}).
		WithInterceptorFuncs(fakeServerSideApplyInterceptors()).
		Build()

	r := &PlatformAgentReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-agent", Namespace: "test-ns"}}
	ctx := context.Background()

	// Render the next stack first.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile 1 failed: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile 2 failed: %v", err)
	}
	sts := &appsv1.StatefulSet{}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-nats", Namespace: "test-ns"}, sts); err != nil {
		t.Fatalf("next stack did not render: %v", err)
	}

	// Now the skew: a mode this binary does not know.
	fresh := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("failed to get agent: %v", err)
	}
	fresh.Spec.Mode = ptr.To("next2")
	if err := cl.Update(ctx, fresh); err != nil {
		t.Fatalf("failed to update agent: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile 3 failed: %v", err)
	}

	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-nats", Namespace: "test-ns"}, sts); err != nil {
		t.Errorf("skew tore down the running NATS StatefulSet: %v", err)
	}
	dep := &appsv1.Deployment{}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-gateway", Namespace: "test-ns"}, dep); err != nil {
		t.Errorf("skew tore down the running A2A gateway: %v", err)
	}
	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("failed to get agent: %v", err)
	}
	if fresh.Status.Phase != "Degraded" {
		t.Errorf("skew must still be reported: phase %q, want Degraded", fresh.Status.Phase)
	}
}

// A creds Secret missing a key would render `password: ""` into nats.conf — a
// user anyone can log in as — so the shape is repaired, while intact keys are
// never re-rolled.
func TestEnsureA2ACredsSecretRepairsMissingKeys(t *testing.T) {
	scheme := setupScheme()
	agent := a2aTestAgent()
	partial := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "test-agent-a2a-nats-creds", Namespace: "test-ns"},
		Data:       map[string][]byte{"gateway-password": []byte("keep-this-value-intact-1234")},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent, partial).Build()
	r := &PlatformAgentReconciler{Client: cl, Scheme: scheme}

	got, err := r.ensureA2ACredsSecret(context.Background(), agent)
	if err != nil {
		t.Fatalf("ensureA2ACredsSecret failed: %v", err)
	}
	if string(got.Data["gateway-password"]) != "keep-this-value-intact-1234" {
		t.Error("an intact key was re-rolled")
	}
	for _, key := range a2aCredsKeys {
		if len(got.Data[key]) == 0 {
			t.Errorf("key %q was not repaired", key)
		}
	}
}
