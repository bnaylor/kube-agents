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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

const (
	// hostPathEnvtestNamespace is the CR under test's namespace; the name is
	// envtestAgentName, shared with the observed-generation envtest.
	hostPathEnvtestNamespace = "hostpath-strip"
)

// TestAHostPathVolumeStaysOutOfTheDeploymentWithoutTheWebhookEnvtest is the
// chart-default scenario against a real API server: envtest runs the operator's
// CRDs and no admission webhook, which is exactly the install #1671 describes.
// The CR carries a hostPath on each volume list with a mount naming each, the
// API server admits it unchallenged, and the assertion is on what the
// controller then wrote: a gateway Deployment the API server accepted, with
// neither the hostPath volumes nor the mounts that named them, and a
// VolumesDropped condition on the CR saying so. Against a controller without
// the fix the same Deployment carries both hostPaths, which is the live
// equivalent the issue asked to have confirmed.
func TestAHostPathVolumeStaysOutOfTheDeploymentWithoutTheWebhookEnvtest(t *testing.T) {
	cl, scheme := startEnvtest(t)
	ctx := context.Background()

	if err := cl.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: hostPathEnvtestNamespace}}); err != nil {
		t.Fatalf("creating namespace: %v", err)
	}
	// The CR the observed-generation envtest uses (the CRD requires
	// spec.harness and nothing here contacts the project it names), carrying
	// the hostPath fixture's spec.deployment.
	agent := &agentv1alpha1.PlatformAgent{
		ObjectMeta: metav1.ObjectMeta{Name: envtestAgentName, Namespace: hostPathEnvtestNamespace},
		Spec: agentv1alpha1.PlatformAgentSpec{
			Harness: &agentv1alpha1.HarnessSpec{
				ProjectID:   envtestHarnessProject,
				Location:    envtestHarnessLocation,
				ClusterName: envtestHarnessCluster,
			},
			AgentSpec: agentv1alpha1.AgentSpec{Deployment: hostPathDeploymentSpec()},
		},
	}
	if err := cl.Create(ctx, agent); err != nil {
		t.Fatalf("creating PlatformAgent with hostPath volumes (the API server should admit it; envtest runs no webhook): %v", err)
	}
	if err := cl.Create(ctx, shellSandboxKeysSecret(agent)); err != nil {
		t.Fatalf("creating sandbox keys Secret: %v", err)
	}

	r := &PlatformAgentReconciler{Client: cl, APIReader: cl, Scheme: scheme}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(agent)}
	// The first pass adds the finalizer and returns; the second renders.
	for _, pass := range []string{"finalizer", "render"} {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile (%s) failed: %v", pass, err)
		}
	}

	dep := &appsv1.Deployment{}
	if err := cl.Get(ctx, client.ObjectKey{Name: agent.Name + "-gateway", Namespace: agent.Namespace}, dep); err != nil {
		t.Fatalf("the API server holds no gateway Deployment after the render pass: %v", err)
	}
	pod := dep.Spec.Template.Spec
	for _, v := range pod.Volumes {
		if v.HostPath != nil {
			t.Errorf("the accepted Deployment carries volume %q with hostPath %s", v.Name, v.HostPath.Path)
		}
	}
	for _, name := range []string{hostPathFixtureExtraVolume, hostPathFixtureSidecarVolume} {
		if hasVolume(pod.Volumes, name) {
			t.Errorf("the accepted Deployment declares %q", name)
		}
		for _, c := range append(append([]corev1.Container{}, pod.InitContainers...), pod.Containers...) {
			if hasMount(c.VolumeMounts, name) {
				t.Errorf("container %q in the accepted Deployment mounts %q", c.Name, name)
			}
		}
	}
	if !hasVolume(pod.Volumes, hostPathFixtureEmptyDirExtra) || !hasVolume(pod.Volumes, hostPathFixtureEmptyDirSide) {
		t.Errorf("the emptyDir volumes beside the hostPath entries did not reach the Deployment: %v", pod.Volumes)
	}
	assertNoDanglingMounts(t, pod)

	got := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatalf("reading the PlatformAgent back: %v", err)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, hostPathConditionType)
	if cond == nil {
		t.Errorf("no %s condition on the CR; conditions: %+v", hostPathConditionType, got.Status.Conditions)
	} else {
		if cond.Status != metav1.ConditionTrue || cond.Reason != hostPathConditionReason {
			t.Errorf("%s condition = %s/%s, want True/%s", hostPathConditionType, cond.Status, cond.Reason, hostPathConditionReason)
		}
		t.Logf("%s: %s/%s: %s", cond.Type, cond.Status, cond.Reason, cond.Message)
	}
	t.Logf("phase=%s volumes=%d hostPath volumes in Deployment=%d", got.Status.Phase, len(pod.Volumes), countHostPath(pod.Volumes))
}

func countHostPath(volumes []corev1.Volume) int {
	n := 0
	for _, v := range volumes {
		if v.HostPath != nil {
			n++
		}
	}
	return n
}
