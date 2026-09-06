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
// sandbox keypair. Without it the reconcile parks Degraded and returns before
// any status condition below that point is written — including this one — so a
// status test that omits it is testing the early return rather than the
// condition. On a working install the Secret exists.
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
