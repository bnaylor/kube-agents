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

// The mode switch's whole-surface transition test (S7, the darkness audit).
//
// The per-object gating tests in platformagent_a2a_manifests_test.go each pin
// one render to the mode. This one asks the property the spec states —
// "a normal install cannot tell this feature exists" — across everything the
// reconciler writes: a today install that visits next and comes back must be
// indistinguishable from one that never left, except for the residue
// cleanupA2A's comment documents on purpose (the creds Secret; the JetStream
// PVC would be its sibling on a real cluster, below).
//
// The enumeration is deliberately label-blind. The A2A label keys are
// asymmetric by design (the spawner stamps app.kubernetes.io/component, the
// operator stamps kubeagents.x-k8s.io/a2a-component), so an audit that sweeps
// by label misses whichever half it did not know about. Sweeping every kind
// in the namespace and diffing sets cannot be fooled that way.

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

// snapshotNamespace lists every object of every kind the reconciler can write
// and returns them keyed by Kind/name, content included. PlatformAgent itself
// is excluded: the CR necessarily differs across the flip (it carries the mode).
//
// What a fake client cannot show, so this sweep cannot either: the PVC a real
// StatefulSet controller materializes from the NATS volumeClaimTemplate (the
// second documented survivor — deleted by handleDeletion, kept across flips),
// and pods spawned at runtime by the A2A gateway (the transition-window
// finding W6.2 #4 documents in cleanupA2A's comment).
func snapshotNamespace(t *testing.T, ctx context.Context, cl client.Client, namespace string) map[string]client.Object {
	t.Helper()

	lists := []client.ObjectList{
		&appsv1.DeploymentList{},
		&appsv1.StatefulSetList{},
		&appsv1.DaemonSetList{},
		&corev1.ServiceList{},
		&corev1.ServiceAccountList{},
		&corev1.ConfigMapList{},
		&corev1.SecretList{},
		&corev1.PersistentVolumeClaimList{},
		&corev1.PodList{},
		&batchv1.JobList{},
		&batchv1.CronJobList{},
		&rbacv1.RoleList{},
		&rbacv1.RoleBindingList{},
		&rbacv1.ClusterRoleList{},
		&rbacv1.ClusterRoleBindingList{},
		&networkingv1.NetworkPolicyList{},
		&policyv1.PodDisruptionBudgetList{},
	}

	snapshot := map[string]client.Object{}
	for _, list := range lists {
		// Cluster-scoped kinds ignore the namespace option, which is what we
		// want: the reconciler's ClusterRole/ClusterRoleBinding writes are part
		// of the surface too.
		if err := cl.List(ctx, list, client.InNamespace(namespace)); err != nil {
			t.Fatalf("listing %T: %v", list, err)
		}
		items, err := extractItems(list)
		if err != nil {
			t.Fatalf("extracting items from %T: %v", list, err)
		}
		for _, obj := range items {
			key := fmt.Sprintf("%T", obj) + "/" + obj.GetName()
			// Normalize volatile bookkeeping so the content diff is semantic.
			clean := obj.DeepCopyObject().(client.Object)
			clean.SetResourceVersion("")
			clean.SetUID("")
			clean.SetGeneration(0)
			clean.SetCreationTimestamp(metav1.Time{})
			clean.SetManagedFields(nil)
			snapshot[key] = clean
		}
	}
	return snapshot
}

func extractItems(list client.ObjectList) ([]client.Object, error) {
	switch l := list.(type) {
	case *appsv1.DeploymentList:
		return collect(l.Items, func(o appsv1.Deployment) client.Object { return &o })
	case *appsv1.StatefulSetList:
		return collect(l.Items, func(o appsv1.StatefulSet) client.Object { return &o })
	case *appsv1.DaemonSetList:
		return collect(l.Items, func(o appsv1.DaemonSet) client.Object { return &o })
	case *corev1.ServiceList:
		return collect(l.Items, func(o corev1.Service) client.Object { return &o })
	case *corev1.ServiceAccountList:
		return collect(l.Items, func(o corev1.ServiceAccount) client.Object { return &o })
	case *corev1.ConfigMapList:
		return collect(l.Items, func(o corev1.ConfigMap) client.Object { return &o })
	case *corev1.SecretList:
		return collect(l.Items, func(o corev1.Secret) client.Object { return &o })
	case *corev1.PersistentVolumeClaimList:
		return collect(l.Items, func(o corev1.PersistentVolumeClaim) client.Object { return &o })
	case *corev1.PodList:
		return collect(l.Items, func(o corev1.Pod) client.Object { return &o })
	case *batchv1.JobList:
		return collect(l.Items, func(o batchv1.Job) client.Object { return &o })
	case *batchv1.CronJobList:
		return collect(l.Items, func(o batchv1.CronJob) client.Object { return &o })
	case *rbacv1.RoleList:
		return collect(l.Items, func(o rbacv1.Role) client.Object { return &o })
	case *rbacv1.RoleBindingList:
		return collect(l.Items, func(o rbacv1.RoleBinding) client.Object { return &o })
	case *rbacv1.ClusterRoleList:
		return collect(l.Items, func(o rbacv1.ClusterRole) client.Object { return &o })
	case *rbacv1.ClusterRoleBindingList:
		return collect(l.Items, func(o rbacv1.ClusterRoleBinding) client.Object { return &o })
	case *networkingv1.NetworkPolicyList:
		return collect(l.Items, func(o networkingv1.NetworkPolicy) client.Object { return &o })
	case *policyv1.PodDisruptionBudgetList:
		return collect(l.Items, func(o policyv1.PodDisruptionBudget) client.Object { return &o })
	default:
		return nil, fmt.Errorf("unhandled list type %T", list)
	}
}

func collect[T any](items []T, conv func(T) client.Object) ([]client.Object, error) {
	out := make([]client.Object, 0, len(items))
	for i := range items {
		out = append(out, conv(items[i]))
	}
	return out, nil
}

func sortedObjectKeys(m map[string]client.Object) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func reconcileToStable(t *testing.T, r *PlatformAgentReconciler, req ctrl.Request) {
	t.Helper()
	ctx := context.Background()
	// The next-mode reconcile legitimately requeues while the provision Job is
	// pending, so "stable" here means the rendered object set stops changing,
	// which a handful of passes guarantees for the fake client.
	for i := 0; i < 5; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile pass %d failed: %v", i+1, err)
		}
	}
}

func TestModeRoundTripLeavesOnlyTheDocumentedResidue(t *testing.T) {
	scheme := setupScheme()
	agent := &agentv1alpha1.PlatformAgent{
		ObjectMeta: metav1.ObjectMeta{Name: "test-agent", Namespace: "test-ns"},
		// Mode absent: the "normal install" of the spec's property.
		Spec: agentv1alpha1.PlatformAgentSpec{},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(agent).
		WithStatusSubresource(&agentv1alpha1.PlatformAgent{}).
		WithInterceptorFuncs(fakeServerSideApplyInterceptors()).
		Build()

	r := &PlatformAgentReconciler{Client: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-agent", Namespace: "test-ns"}}
	ctx := context.Background()

	// 1. The install that never heard of A2A.
	reconcileToStable(t, r, req)
	before := snapshotNamespace(t, ctx, cl, "test-ns")

	// 2. Visit next. Sanity-check the stack actually rose, so the round-trip
	// below is proving cleanup and not a render that never happened.
	setMode := func(mode *string) {
		fresh := &agentv1alpha1.PlatformAgent{}
		if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
			t.Fatalf("get agent: %v", err)
		}
		fresh.Spec.Mode = mode
		if err := cl.Update(ctx, fresh); err != nil {
			t.Fatalf("update agent: %v", err)
		}
	}
	setMode(ptr.To("next"))
	reconcileToStable(t, r, req)
	during := snapshotNamespace(t, ctx, cl, "test-ns")
	if len(during) <= len(before) {
		t.Fatalf("next rendered nothing: %d objects before, %d during", len(before), len(during))
	}
	for _, want := range []string{
		"*v1.StatefulSet/test-agent-a2a-nats",
		"*v1.Deployment/test-agent-a2a-gateway",
		"*v1.NetworkPolicy/test-agent-a2a-nats-netpol",
		"*v1.NetworkPolicy/test-agent-a2a-session-netpol",
		"*v1.Secret/test-agent-a2a-nats-creds",
		"*v1.Secret/test-agent-a2a-nats-config",
		"*v1.ServiceAccount/test-agent-a2a-gateway",
		"*v1.Role/test-agent-a2a-gateway",
		"*v1.RoleBinding/test-agent-a2a-gateway",
		"*v1.Service/test-agent-a2a-nats",
	} {
		if _, ok := during[want]; !ok {
			t.Errorf("next stack missing %s (keys: %v)", want, sortedObjectKeys(during))
		}
	}

	// 3. Come back to a normal install.
	setMode(nil)
	reconcileToStable(t, r, req)
	after := snapshotNamespace(t, ctx, cl, "test-ns")

	// The documented residue, and nothing else: the creds Secret survives a
	// flip on purpose (re-enabling next must not re-roll credentials a running
	// gateway may hold; see ensureA2ACredsSecret). Its sibling on a real
	// cluster, the JetStream PVC, is created by the StatefulSet controller —
	// which the fake client does not run — so it cannot appear here; it is
	// documented in cleanupA2A and deleted by handleDeletion.
	wantResidue := map[string]bool{
		"*v1.Secret/test-agent-a2a-nats-creds": true,
	}
	for key := range after {
		if _, existed := before[key]; existed {
			continue
		}
		if wantResidue[key] {
			delete(wantResidue, key)
			continue
		}
		t.Errorf("undocumented residue after next->today: %s", key)
	}
	for key := range wantResidue {
		t.Errorf("documented residue missing (did cleanup grow teeth?): %s", key)
	}
	for key := range before {
		if _, still := after[key]; !still {
			t.Errorf("cleanupA2A deleted a today object: %s", key)
		}
	}

	// 4. Content, not just names: every object both installs have must be
	// byte-identical, or the flip left a trace inside something that stayed.
	// (This is what catches a NATS_URL still on the agent Deployment or a
	// managed.env still saying next — the whole point of the audit.)
	for key, was := range before {
		is, ok := after[key]
		if !ok {
			continue // already reported above
		}
		if diff := cmp.Diff(was, is); diff != "" {
			t.Errorf("object %s differs after the round trip (-before +after):\n%s", key, diff)
		}
	}
}
