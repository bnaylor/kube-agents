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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

// The ordering property, at the granularity A1 can honestly assert it.
//
// A workload must not be dispatched against a bus that cannot yet authenticate
// it, and the condition is how anything downstream asks. False while the
// callout is absent or not fully ready; true, naming the rendered map version,
// once every replica is serving.
// sandboxKeysSecret satisfies the reconcile step that reports a missing shell
// sandbox keypair, so that the tests using it exercise a Ready install rather
// than a Degraded one. It no longer has to exist for BusCredentialsReady to be
// written — that was the defect the four tests at the bottom of this file pin —
// but Ready=True is the state most of these assertions are about.
func sandboxKeysSecret(agent *agentv1alpha1.PlatformAgent) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shellSandboxAuthorizedKeysSecretName(agent),
			Namespace: agent.Namespace,
		},
	}
}

func TestBusCredentialsReadyTracksTheCallout(t *testing.T) {
	scheme := setupScheme()
	agent := a2aTestAgent()

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(agent, sandboxKeysSecret(agent)).
		WithStatusSubresource(&agentv1alpha1.PlatformAgent{}).
		WithInterceptorFuncs(fakeServerSideApplyInterceptors()).
		Build()
	r := &PlatformAgentReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-agent", Namespace: "test-ns"}}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile %d: %v", i+1, err)
		}
	}

	// The fake client does not run the Deployment controller, so no replica
	// is ready: the condition must be false rather than optimistic. This is
	// the state a real install spends its first seconds in, and dispatching
	// into it is exactly what the condition exists to prevent.
	fresh := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	cond := meta.FindStatusCondition(fresh.Status.Conditions, busCredentialsReadyCondition)
	if cond == nil {
		t.Fatal("no BusCredentialsReady condition under next")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("condition = %s with no ready callout replica, want False", cond.Status)
	}

	// Now report the callout fully ready, as its readiness probe would once
	// it is serving a map.
	dep := &appsv1.Deployment{}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-callout", Namespace: "test-ns"}, dep); err != nil {
		t.Fatalf("get callout Deployment: %v", err)
	}
	dep.Status.Replicas = 2
	dep.Status.ReadyReplicas = 2
	if err := cl.Status().Update(ctx, dep); err != nil {
		t.Fatalf("update callout status: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile after callout ready: %v", err)
	}

	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	cond = meta.FindStatusCondition(fresh.Status.Conditions, busCredentialsReadyCondition)
	if cond.Status != metav1.ConditionTrue {
		t.Fatalf("condition = %s with every callout replica ready, want True (message: %s)", cond.Status, cond.Message)
	}
	// The rendered version travels in the message so an operator can compare
	// it against what the callout reports at runtime.
	authMapVersion, err := renderA2AAuthMap(a2aTestAgent())
	if err != nil {
		t.Fatalf("renderA2AAuthMap: %v", err)
	}
	if !strings.Contains(cond.Message, authMapVersion.Version) {
		t.Errorf("condition message %q does not name the rendered map version %q", cond.Message, authMapVersion.Version)
	}
}

// The darkness property reaches status. A today install must not carry a
// condition describing a component it does not have — a reviewer reading
// `kubectl describe` on a normal install would see the feature named.
func TestBusCredentialsReadyIsAbsentUnderToday(t *testing.T) {
	scheme := setupScheme()
	agent := a2aTestAgent()

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(agent, sandboxKeysSecret(agent)).
		WithStatusSubresource(&agentv1alpha1.PlatformAgent{}).
		WithInterceptorFuncs(fakeServerSideApplyInterceptors()).
		Build()
	r := &PlatformAgentReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-agent", Namespace: "test-ns"}}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile %d: %v", i+1, err)
		}
	}

	fresh := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	fresh.Spec.Mode = nil
	if err := cl.Update(ctx, fresh); err != nil {
		t.Fatalf("flip to today: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile after flip: %v", err)
	}

	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	if cond := meta.FindStatusCondition(fresh.Status.Conditions, busCredentialsReadyCondition); cond != nil {
		t.Errorf("BusCredentialsReady survives a flip to today: %+v", cond)
	}
}

// Both tests below are the same defect from its two ends: the condition was
// written at the very bottom of Reconcile, under every early return, so a
// reconcile that parked Degraded above it neither wrote it nor cleared it.
//
// The workaround is visible in this file's own history: sandboxKeysSecret
// exists so the tests above reach the write at all. A helper that exists to
// step over an early return is evidence about the production path, not just
// about the fixture -- an install with no sandbox keypair is an ordinary
// install, not a broken one.

// A missing shell sandbox keypair parks the reconcile Degraded. It must not
// also decide whether anything can be dispatched onto the bus: those are
// unrelated components, and the condition is the only thing that says whether
// the callout can authenticate. Reported here even while Ready is False --
// especially then, since that is when someone is reading conditions.
func TestBusCredentialsReadyIsWrittenEvenWhenAnEarlierStepParksDegraded(t *testing.T) {
	scheme := setupScheme()
	agent := a2aTestAgent()

	// Deliberately no sandboxKeysSecret: this is the install the helper above
	// exists to avoid, and it is a supported one.
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(agent).
		WithStatusSubresource(&agentv1alpha1.PlatformAgent{}).
		WithInterceptorFuncs(fakeServerSideApplyInterceptors()).
		Build()
	r := &PlatformAgentReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-agent", Namespace: "test-ns"}}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile %d: %v", i+1, err)
		}
	}

	fresh := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	if ready := meta.FindStatusCondition(fresh.Status.Conditions, "Ready"); ready == nil ||
		ready.Status != metav1.ConditionFalse {
		t.Fatalf("precondition: want Ready=False from the missing keypair, got %+v", ready)
	}
	if meta.FindStatusCondition(fresh.Status.Conditions, busCredentialsReadyCondition) == nil {
		t.Error("no BusCredentialsReady condition on a next install parked Degraded by an " +
			"unrelated step; nothing downstream can tell whether the bus can authenticate")
	}
}

// The other end: a condition already written must not survive the component it
// describes. A flip back to today tears the callout down, and if the same
// reconcile parks Degraded above the clear, the CR goes on reporting that a
// callout which no longer exists is serving a map -- which reads as healthy.
func TestBusCredentialsReadyIsClearedOnAFlipToTodayThatAlsoParksDegraded(t *testing.T) {
	scheme := setupScheme()
	agent := a2aTestAgent()
	keys := sandboxKeysSecret(agent)

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(agent, keys).
		WithStatusSubresource(&agentv1alpha1.PlatformAgent{}).
		WithInterceptorFuncs(fakeServerSideApplyInterceptors()).
		Build()
	r := &PlatformAgentReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-agent", Namespace: "test-ns"}}
	ctx := context.Background()

	// Twice: the first pass creates the callout objects, the second observes
	// them, which is when the condition is written.
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile %d under next: %v", i+1, err)
		}
	}
	fresh := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	if meta.FindStatusCondition(fresh.Status.Conditions, busCredentialsReadyCondition) == nil {
		t.Fatal("precondition: no BusCredentialsReady under next")
	}

	// Flip to today and remove the keypair in the same step, so the reconcile
	// that tears the callout down is also one that parks Degraded.
	fresh.Spec.Mode = ptr.To("today")
	if err := cl.Update(ctx, fresh); err != nil {
		t.Fatalf("flip mode: %v", err)
	}
	if err := cl.Delete(ctx, keys); err != nil {
		t.Fatalf("delete keypair: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile after flip: %v", err)
	}

	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	if cond := meta.FindStatusCondition(fresh.Status.Conditions, busCredentialsReadyCondition); cond != nil {
		t.Errorf("BusCredentialsReady survived the flip to today as %s/%s (%q); "+
			"the CR describes a callout it no longer has",
			cond.Status, cond.Reason, cond.Message)
	}
}

// The two above were only two of the parks, and moving the write up the
// sequence fixed those two rather than the class. Four refusals return above
// the bus step and cannot be moved below it -- they are refusals of today's
// stack, and the bus step is at the end because it renders on top of what they
// withhold. So the condition cannot be written in sequence at all: it has to be
// written on the way out, whichever exit the reconcile takes.
//
// ShellSandboxCannotBeDisabled stands for all four here. It is a hard refusal:
// no requeue, no rendering, and every step after it withheld -- including both
// the bus step and the teardown.

// First direction: a next CR refused before the bus step ever runs. The callout
// does not exist because nothing rendered it, and saying so is the whole point
// -- a CR with no BusCredentialsReady at all is indistinguishable from a today
// install to anything reading conditions.
func TestBusCredentialsReadyIsWrittenWhenARefusalReturnsAboveTheBusStep(t *testing.T) {
	scheme := setupScheme()
	agent := a2aTestAgent()
	agent.Spec.Harness = &agentv1alpha1.HarnessSpec{
		Experimental: &agentv1alpha1.ExperimentalSpec{
			ShellSandbox: &agentv1alpha1.ShellSandboxSpec{Enabled: ptr.To(false)},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(agent, sandboxKeysSecret(agent)).
		WithStatusSubresource(&agentv1alpha1.PlatformAgent{}).
		WithInterceptorFuncs(fakeServerSideApplyInterceptors()).
		Build()
	r := &PlatformAgentReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-agent", Namespace: "test-ns"}}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile %d: %v", i+1, err)
		}
	}

	fresh := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	if ready := meta.FindStatusCondition(fresh.Status.Conditions, "Ready"); ready == nil ||
		ready.Reason != reasonShellSandboxCannotBeDisabled {
		t.Fatalf("precondition: want Ready parked by the refusal, got %+v", ready)
	}
	cond := meta.FindStatusCondition(fresh.Status.Conditions, busCredentialsReadyCondition)
	if cond == nil {
		t.Fatal("no BusCredentialsReady on a next install refused above the bus step; " +
			"nothing downstream can tell the bus is not there")
	}
	if cond.Status != metav1.ConditionFalse || cond.Reason != busCredsReasonAbsent {
		t.Errorf("condition = %s/%s, want False/%s: the refusal withheld the callout, so it is absent",
			cond.Status, cond.Reason, busCredsReasonAbsent)
	}
}

// Second direction, and the worse one: a condition that was true stops being
// re-derived. updateStatusDegraded writes Ready alone and preserves the rest,
// so the last value stands unchallenged for as long as the refusal does -- the
// CR reports a callout serving a named map version while every replica of it
// has gone. Absent is a gap somebody notices; this reads as healthy.
func TestBusCredentialsReadyIsNotLeftStaleByARefusalAboveTheBusStep(t *testing.T) {
	scheme := setupScheme()
	agent := a2aTestAgent()

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(agent, sandboxKeysSecret(agent)).
		WithStatusSubresource(&agentv1alpha1.PlatformAgent{}).
		WithInterceptorFuncs(fakeServerSideApplyInterceptors()).
		Build()
	r := &PlatformAgentReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-agent", Namespace: "test-ns"}}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile %d under next: %v", i+1, err)
		}
	}
	dep := &appsv1.Deployment{}
	depKey := types.NamespacedName{Name: "test-agent-a2a-callout", Namespace: "test-ns"}
	if err := cl.Get(ctx, depKey, dep); err != nil {
		t.Fatalf("get callout Deployment: %v", err)
	}
	dep.Status.Replicas = 2
	dep.Status.ReadyReplicas = 2
	if err := cl.Status().Update(ctx, dep); err != nil {
		t.Fatalf("update callout status: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile with the callout ready: %v", err)
	}
	fresh := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	if cond := meta.FindStatusCondition(fresh.Status.Conditions, busCredentialsReadyCondition); cond == nil ||
		cond.Status != metav1.ConditionTrue {
		t.Fatalf("precondition: want BusCredentialsReady=True before the refusal, got %+v", cond)
	}

	// Now the two events that make the condition a lie: the callout loses
	// every replica, and the CR picks up a refusal that returns above the step
	// which would have noticed.
	if err := cl.Get(ctx, depKey, dep); err != nil {
		t.Fatalf("re-get callout Deployment: %v", err)
	}
	dep.Status.ReadyReplicas = 0
	if err := cl.Status().Update(ctx, dep); err != nil {
		t.Fatalf("zero the ready replicas: %v", err)
	}
	fresh.Spec.Harness = &agentv1alpha1.HarnessSpec{
		Experimental: &agentv1alpha1.ExperimentalSpec{
			ShellSandbox: &agentv1alpha1.ShellSandboxSpec{Enabled: ptr.To(false)},
		},
	}
	if err := cl.Update(ctx, fresh); err != nil {
		t.Fatalf("introduce the refusal: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile under the refusal: %v", err)
	}

	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	cond := meta.FindStatusCondition(fresh.Status.Conditions, busCredentialsReadyCondition)
	if cond == nil {
		t.Fatal("BusCredentialsReady removed by a refusal; the callout is still deployed")
	}
	if cond.Status != metav1.ConditionFalse || cond.Reason != busCredsReasonUnavailable {
		t.Errorf("condition = %s/%s (%q) with no callout replica ready; the refusal above the "+
			"bus step left the last value standing", cond.Status, cond.Reason, cond.Message)
	}
}
