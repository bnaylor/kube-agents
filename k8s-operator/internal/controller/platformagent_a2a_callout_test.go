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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

// The darkness property for everything the callout adds: rendered under next,
// gone under today, including the cluster-scoped binding that nothing else
// reclaims.
func TestA2ACalloutIsGatedByMode(t *testing.T) {
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

	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile %d: %v", i+1, err)
		}
	}

	// Under next: every callout object exists.
	dep := &appsv1.Deployment{}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-callout", Namespace: "test-ns"}, dep); err != nil {
		t.Errorf("callout Deployment not rendered under next: %v", err)
	}
	if got := *dep.Spec.Replicas; got != 2 {
		t.Errorf("callout replicas = %d, want 2: one is a single point of failure in front of every new bus connection", got)
	}
	if got := dep.Spec.Strategy.RollingUpdate.MaxUnavailable.IntValue(); got != 0 {
		t.Errorf("callout maxUnavailable = %d, want 0: a moment with no ready callout is a moment nothing can connect", got)
	}

	sa := &corev1.ServiceAccount{}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-callout", Namespace: "test-ns"}, sa); err != nil {
		t.Errorf("callout ServiceAccount not rendered under next: %v", err)
	}
	authMap := &corev1.ConfigMap{}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-authmap", Namespace: "test-ns"}, authMap); err != nil {
		t.Errorf("identity map not rendered under next: %v", err)
	}
	crb := &rbacv1.ClusterRoleBinding{}
	if err := cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-callout-tokenreview"}, crb); err != nil {
		t.Errorf("callout ClusterRoleBinding not rendered under next: %v", err)
	}
	if crb.RoleRef.Name != a2aAuthDelegatorRole {
		t.Errorf("callout binds %q, want the built-in %q", crb.RoleRef.Name, a2aAuthDelegatorRole)
	}

	// Flip to today.
	fresh := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	fresh.Spec.Mode = nil
	if err := cl.Update(ctx, fresh); err != nil {
		t.Fatalf("update agent: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile after flip: %v", err)
	}

	gone := []struct {
		what string
		get  func() error
	}{
		{"Deployment", func() error {
			return cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-callout", Namespace: "test-ns"}, &appsv1.Deployment{})
		}},
		{"Service", func() error {
			return cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-callout", Namespace: "test-ns"}, &corev1.Service{})
		}},
		{"ServiceAccount", func() error {
			return cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-callout", Namespace: "test-ns"}, &corev1.ServiceAccount{})
		}},
		{"Role", func() error {
			return cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-callout", Namespace: "test-ns"}, &rbacv1.Role{})
		}},
		{"RoleBinding", func() error {
			return cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-callout", Namespace: "test-ns"}, &rbacv1.RoleBinding{})
		}},
		{"keys Secret", func() error {
			return cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-callout-keys", Namespace: "test-ns"}, &corev1.Secret{})
		}},
		{"identity map", func() error {
			return cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-authmap", Namespace: "test-ns"}, &corev1.ConfigMap{})
		}},
		// The one nothing else would reclaim: cluster-scoped, so it carries
		// no owner reference, and a security reviewer diffing a "normal"
		// install lists cluster-scoped objects first.
		{"ClusterRoleBinding", func() error {
			return cl.Get(ctx, types.NamespacedName{Name: "test-agent-a2a-callout-tokenreview"}, &rbacv1.ClusterRoleBinding{})
		}},
	}
	for _, g := range gone {
		if err := g.get(); !errors.IsNotFound(err) {
			t.Errorf("callout %s survives a flip to today (err=%v)", g.what, err)
		}
	}
}
