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
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	if err := cl.Get(ctx, types.NamespacedName{Name: "kubeagents:a2a-callout-tokenreview:test-ns:test-agent"}, crb); err != nil {
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
			return cl.Get(ctx, types.NamespacedName{Name: "kubeagents:a2a-callout-tokenreview:test-ns:test-agent"}, &rbacv1.ClusterRoleBinding{})
		}},
	}
	for _, g := range gone {
		if err := g.get(); !errors.IsNotFound(err) {
			t.Errorf("callout %s survives a flip to today (err=%v)", g.what, err)
		}
	}
}

// Label-blind teardown sweep: the check that catches an object nobody
// remembered to delete.
//
// TestA2ACalloutIsGatedByMode asserts each object it knows about by name, which
// is exactly the wrong shape for the failure that actually happens — someone
// adds a ninth object to the render and not to cleanupA2A, and every
// named-object test still passes. This enumerates the namespace by kind instead
// and asks whether anything at all is still labelled as part of the next stack.
//
// The documented residue is deliberate and small: the per-user creds Secret
// (re-enabling must not re-roll credentials a running pod may have cached) and
// the JetStream PVC (the file store is the audit substrate). Everything else
// must be gone. The PVC does not appear here because the fake client does not
// run the StatefulSet controller, so no PVC is ever created.
// The callout container declares a working directory its UID can enter.
//
// This is #1259's shape on a fourth A2A container. The image is built on
// distroless static nonroot, whose WORKDIR is /home/nonroot, 0700 owned by
// 65532; the Deployment imposes runAsUser 1000. Measured with `crane config`
// rather than assumed, on both the built image and the base:
//
//	WorkingDir: "/home/nonroot"  User: "nonroot"
//
// Latent rather than broken - the binary never stats "." and this Deployment
// has been observed 2/2 on a cluster - which is exactly why it wants a test
// rather than a shrug. "Latent" describes today's code, and the change that
// ends it would not announce itself.
//
// PROVISIONAL. #1272 generalises this into a table over every A2A container,
// each row carrying that image's measured WORKDIR and the directories usable
// under the UID the pod imposes, and it fails on absence - a fourth container
// with no row fails it. When that lands and this branch merges main, delete
// this test and add the row: imageWorkDir "/home/nonroot", usable ["/"],
// wantWritable false. Do not keep both.
func TestTheCalloutLandsInAWorkingDirectoryItsUserCanEnter(t *testing.T) {
	dep := buildA2ACalloutDeployment(a2aTestAgent())

	spec := dep.Spec.Template.Spec
	if spec.SecurityContext == nil || spec.SecurityContext.RunAsUser == nil {
		t.Fatal("no pod-level RunAsUser, so this test cannot say which UID has to enter the cwd")
	}
	uid := *spec.SecurityContext.RunAsUser

	for _, c := range spec.Containers {
		// "/" is the only directory measured usable by UID 1000 on this
		// image. Asserting non-empty would pass against /home/nonroot spelled
		// out, which is the bug rather than the fix.
		if c.WorkingDir != "/" {
			t.Errorf("container %s: WorkingDir = %q, want \"/\" - the image ships WORKDIR "+
				"/home/nonroot, 0700 owned by 65532, and this pod runs as UID %d, which "+
				"cannot chdir into it", c.Name, c.WorkingDir, uid)
		}
	}
}

func TestNothingA2ALabelledSurvivesAFlipToToday(t *testing.T) {
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

	// Sanity: the sweep is only meaningful if it saw a populated namespace
	// first. A sweep that passes because nothing was ever rendered is the
	// vacuous version of this test.
	if n := countA2ALabelled(ctx, t, cl); n == 0 {
		t.Fatal("no A2A-labelled objects under next; the sweep would pass vacuously")
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

	var leftovers []string
	sweepA2ALabelled(ctx, t, cl, func(kind, name string) {
		// The one documented survivor.
		if kind == "Secret" && name == "test-agent-a2a-nats-creds" {
			return
		}
		leftovers = append(leftovers, kind+"/"+name)
	})
	if len(leftovers) > 0 {
		t.Errorf("these A2A-labelled objects survive a flip to today: %v\n"+
			"Either add them to cleanupA2A, or add them to this test's documented-residue list with a reason.", leftovers)
	}
}

func countA2ALabelled(ctx context.Context, t *testing.T, cl client.Client) int {
	t.Helper()
	n := 0
	sweepA2ALabelled(ctx, t, cl, func(string, string) { n++ })
	return n
}

// sweepA2ALabelled visits every object of every kind this change can render
// that carries the next stack's part-of label. Listed by kind rather than by
// name, so an object added to the render without being added to the teardown is
// found here rather than on a cluster.
func sweepA2ALabelled(ctx context.Context, t *testing.T, cl client.Client, visit func(kind, name string)) {
	t.Helper()
	inNS := client.InNamespace("test-ns")
	hasLabel := client.MatchingLabels{labelPartOf: a2aPartOf}

	var secrets corev1.SecretList
	if err := cl.List(ctx, &secrets, inNS, hasLabel); err != nil {
		t.Fatalf("list secrets: %v", err)
	}
	for i := range secrets.Items {
		visit("Secret", secrets.Items[i].Name)
	}

	var cms corev1.ConfigMapList
	if err := cl.List(ctx, &cms, inNS, hasLabel); err != nil {
		t.Fatalf("list configmaps: %v", err)
	}
	for i := range cms.Items {
		visit("ConfigMap", cms.Items[i].Name)
	}

	var sas corev1.ServiceAccountList
	if err := cl.List(ctx, &sas, inNS, hasLabel); err != nil {
		t.Fatalf("list serviceaccounts: %v", err)
	}
	for i := range sas.Items {
		visit("ServiceAccount", sas.Items[i].Name)
	}

	var svcs corev1.ServiceList
	if err := cl.List(ctx, &svcs, inNS, hasLabel); err != nil {
		t.Fatalf("list services: %v", err)
	}
	for i := range svcs.Items {
		visit("Service", svcs.Items[i].Name)
	}

	var deps appsv1.DeploymentList
	if err := cl.List(ctx, &deps, inNS, hasLabel); err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	for i := range deps.Items {
		visit("Deployment", deps.Items[i].Name)
	}

	var sts appsv1.StatefulSetList
	if err := cl.List(ctx, &sts, inNS, hasLabel); err != nil {
		t.Fatalf("list statefulsets: %v", err)
	}
	for i := range sts.Items {
		visit("StatefulSet", sts.Items[i].Name)
	}

	var roles rbacv1.RoleList
	if err := cl.List(ctx, &roles, inNS, hasLabel); err != nil {
		t.Fatalf("list roles: %v", err)
	}
	for i := range roles.Items {
		visit("Role", roles.Items[i].Name)
	}

	var rbs rbacv1.RoleBindingList
	if err := cl.List(ctx, &rbs, inNS, hasLabel); err != nil {
		t.Fatalf("list rolebindings: %v", err)
	}
	for i := range rbs.Items {
		visit("RoleBinding", rbs.Items[i].Name)
	}

	// Cluster-scoped, and therefore the one that cannot be reclaimed by an
	// owner reference — the residue most likely to be left behind and the
	// most visible to anyone auditing a "normal" install.
	var crbs rbacv1.ClusterRoleBindingList
	if err := cl.List(ctx, &crbs, hasLabel); err != nil {
		t.Fatalf("list clusterrolebindings: %v", err)
	}
	for i := range crbs.Items {
		visit("ClusterRoleBinding", crbs.Items[i].Name)
	}
}

// Deleting the CR must reclaim the cluster-scoped grant too.
//
// There are two ways the next stack goes away and they run different code: a
// flip to today goes through cleanupA2A, deletion goes through handleDeletion.
// The ClusterRoleBinding can carry no owner reference, so nothing reclaims it
// implicitly, and the generic RBAC sweep does not select it — its labels and
// its name both fall outside what that sweep matches. Left behind it is a
// standing grant of tokenreviews/create to a ServiceAccount name in a
// namespace, outliving the workload it was minted for.
func TestDeletingTheCRReclaimsTheCalloutClusterRoleBinding(t *testing.T) {
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
	crbName := a2aCalloutClusterRoleBindingName(agent)
	if err := cl.Get(ctx, types.NamespacedName{Name: crbName}, &rbacv1.ClusterRoleBinding{}); err != nil {
		t.Fatalf("the ClusterRoleBinding was not rendered under next: %v", err)
	}

	fresh := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	if err := cl.Delete(ctx, fresh); err != nil {
		t.Fatalf("delete agent: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile after delete: %v", err)
	}

	if err := cl.Get(ctx, types.NamespacedName{Name: crbName}, &rbacv1.ClusterRoleBinding{}); !errors.IsNotFound(err) {
		t.Errorf("the callout ClusterRoleBinding survives deletion of the CR (err=%v).\n"+
			"Nothing else reclaims it: it is cluster-scoped so it carries no owner reference, and the generic RBAC sweep does not select it.", err)
	}
}

// The cluster-scoped name must be unambiguous between two agents of the same
// name in different namespaces. A collision is not benign — each reconcile
// would rewrite the other's Subjects, so one namespace's callout silently loses
// TokenReview and refuses every connection.
func TestTheCalloutClusterRoleBindingNameIsNamespaceQualified(t *testing.T) {
	a := &agentv1alpha1.PlatformAgent{ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "team-a"}}
	b := &agentv1alpha1.PlatformAgent{ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "team-b"}}

	if a2aCalloutClusterRoleBindingName(a) == a2aCalloutClusterRoleBindingName(b) {
		t.Errorf("two agents named %q in different namespaces share the cluster-scoped binding name %q",
			a.Name, a2aCalloutClusterRoleBindingName(a))
	}
	for _, agent := range []*agentv1alpha1.PlatformAgent{a, b} {
		name := a2aCalloutClusterRoleBindingName(agent)
		if !strings.Contains(name, agent.Namespace) {
			t.Errorf("%q does not carry the namespace", name)
		}
	}
}
