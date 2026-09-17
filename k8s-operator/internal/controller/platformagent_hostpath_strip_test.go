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
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

// The tests in this file are for #1671: a hostPath on spec.deployment.extraVolumes
// or .sidecarVolumes was refused by the admission webhook and nowhere else, and
// the chart ships the webhook off. They assert the render-side layer: the volume
// and every mount naming it are out of the Pod, the CR's own slices are not
// edited, the drop is reported on status, and a non-hostPath volume of the same
// shape is untouched. The condition type and reason are spelled as literals so
// the file also compiles against a controller without the fix, which is how the
// failing run against main was produced.

const (
	// Fixture names. The paths are chosen so a test that finds one in the
	// rendered Pod is unambiguous about which entry leaked.
	hostPathFixtureExtraVolume   = "host-root"
	hostPathFixtureExtraPath     = "/"
	hostPathFixtureSidecarVolume = "host-sock"
	hostPathFixtureSidecarPath   = "/var/run/docker.sock"
	hostPathFixtureSidecarName   = "user-sidecar"
	hostPathFixtureInitName      = "user-init"
	hostPathFixtureEmptyDirExtra = "extra-scratch"
	hostPathFixtureEmptyDirSide  = "sidecar-scratch"
	hostPathFixtureMountPath     = "/mnt/host"
	hostPathFixtureScratchPath   = "/mnt/scratch"
	// The condition the fix writes, as literals (see the file comment).
	hostPathConditionType   = "VolumesDropped"
	hostPathConditionReason = "HostPathVolumeDropped"
)

// hostPathAgent is a CR carrying one hostPath on each list, mounted from the
// agent container (extraVolumeMounts), a user sidecar and a user init container,
// beside an emptyDir of the same shape on each list so the tests can tell a
// filter that drops hostPath from one that drops everything.
func hostPathAgent() *agentv1alpha1.PlatformAgent {
	agent := brokerPodAgent()
	agent.Spec.Deployment = hostPathDeploymentSpec()
	return agent
}

// hostPathDeploymentSpec is the spec.deployment the fixture carries, on its own
// so the envtest case can put it on a CR the CRD's own validation admits.
func hostPathDeploymentSpec() *agentv1alpha1.DeploymentSpec {
	return &agentv1alpha1.DeploymentSpec{
		ExtraVolumes: []corev1.Volume{
			{Name: hostPathFixtureExtraVolume, VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: hostPathFixtureExtraPath}}},
			{Name: hostPathFixtureEmptyDirExtra, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		},
		ExtraVolumeMounts: []corev1.VolumeMount{
			{Name: hostPathFixtureExtraVolume, MountPath: hostPathFixtureMountPath},
			{Name: hostPathFixtureEmptyDirExtra, MountPath: hostPathFixtureScratchPath},
		},
		SidecarVolumes: []corev1.Volume{
			{Name: hostPathFixtureSidecarVolume, VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: hostPathFixtureSidecarPath}}},
			{Name: hostPathFixtureEmptyDirSide, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		},
		Sidecars: []corev1.Container{{
			Name:  hostPathFixtureSidecarName,
			Image: "sidecar:test",
			VolumeMounts: []corev1.VolumeMount{
				{Name: hostPathFixtureSidecarVolume, MountPath: hostPathFixtureMountPath},
				{Name: hostPathFixtureEmptyDirSide, MountPath: hostPathFixtureScratchPath},
			},
		}},
		InitContainers: []corev1.Container{{
			Name:  hostPathFixtureInitName,
			Image: "init:test",
			VolumeMounts: []corev1.VolumeMount{
				{Name: hostPathFixtureExtraVolume, MountPath: hostPathFixtureMountPath},
			},
		}},
	}
}

func renderHostPathPod(t *testing.T, agent *agentv1alpha1.PlatformAgent) corev1.PodSpec {
	t.Helper()
	return buildDeployment(agent, "h1", "h2", "h3", "h4", nil, renderOptions{imageVolumeSupported: true}).Spec.Template.Spec
}

// mustFindContainer is findContainer (platformagent_manifests_test.go) with
// the miss turned into a failure; hasVolume is the broker split test's.
func mustFindContainer(t *testing.T, pod corev1.PodSpec, name string) corev1.Container {
	t.Helper()
	c, ok := findContainer(pod, name)
	if !ok {
		t.Fatalf("no container named %q in the rendered Pod", name)
	}
	return c
}

func hasMount(mounts []corev1.VolumeMount, name string) bool {
	for _, m := range mounts {
		if m.Name == name {
			return true
		}
	}
	return false
}

// assertNoDanglingMounts is the API-server rule the render has to satisfy:
// every volumeMount on every container names a volume the Pod declares.
func assertNoDanglingMounts(t *testing.T, pod corev1.PodSpec) {
	t.Helper()
	declared := make(map[string]bool, len(pod.Volumes))
	for _, v := range pod.Volumes {
		declared[v.Name] = true
	}
	for _, c := range append(append([]corev1.Container{}, pod.InitContainers...), pod.Containers...) {
		for _, m := range c.VolumeMounts {
			if !declared[m.Name] {
				t.Errorf("container %q mounts %q at %s, and the Pod declares no such volume: the API server rejects this Deployment", c.Name, m.Name, m.MountPath)
			}
		}
	}
}

func assertNoHostPathVolumes(t *testing.T, volumes []corev1.Volume) {
	t.Helper()
	for _, v := range volumes {
		if v.HostPath != nil {
			t.Errorf("volume %q reached the Pod with hostPath %s", v.Name, v.HostPath.Path)
		}
	}
}

func TestRenderDropsAHostPathExtraVolumeAndItsMounts(t *testing.T) {
	pod := renderHostPathPod(t, hostPathAgent())

	assertNoHostPathVolumes(t, pod.Volumes)
	if hasVolume(pod.Volumes, hostPathFixtureExtraVolume) {
		t.Errorf("extraVolumes entry %q is in the Pod", hostPathFixtureExtraVolume)
	}
	agentContainer := mustFindContainer(t, pod, "platform-agent")
	if hasMount(agentContainer.VolumeMounts, hostPathFixtureExtraVolume) {
		t.Errorf("the agent container still mounts %q", hostPathFixtureExtraVolume)
	}
	if !hasMount(agentContainer.VolumeMounts, hostPathFixtureEmptyDirExtra) {
		t.Errorf("the agent container lost its emptyDir mount %q; only the hostPath's mount should go", hostPathFixtureEmptyDirExtra)
	}
	// The dashboard container takes extraVolumeMounts too, and on main it was
	// the third place the hostPath mount landed.
	dashboard := mustFindContainer(t, pod, "platform-agent-dashboard")
	if hasMount(dashboard.VolumeMounts, hostPathFixtureExtraVolume) {
		t.Errorf("the dashboard container still mounts %q", hostPathFixtureExtraVolume)
	}
	initContainer := mustFindContainer(t, pod, hostPathFixtureInitName)
	if hasMount(initContainer.VolumeMounts, hostPathFixtureExtraVolume) {
		t.Errorf("the user init container still mounts %q", hostPathFixtureExtraVolume)
	}
	assertNoDanglingMounts(t, pod)
}

func TestRenderDropsAHostPathSidecarVolumeAndItsMounts(t *testing.T) {
	pod := renderHostPathPod(t, hostPathAgent())

	if hasVolume(pod.Volumes, hostPathFixtureSidecarVolume) {
		t.Errorf("sidecarVolumes entry %q is in the Pod", hostPathFixtureSidecarVolume)
	}
	sidecar := mustFindContainer(t, pod, hostPathFixtureSidecarName)
	if hasMount(sidecar.VolumeMounts, hostPathFixtureSidecarVolume) {
		t.Errorf("the user sidecar still mounts %q", hostPathFixtureSidecarVolume)
	}
	if !hasMount(sidecar.VolumeMounts, hostPathFixtureEmptyDirSide) {
		t.Errorf("the user sidecar lost its emptyDir mount %q; only the hostPath's mount should go", hostPathFixtureEmptyDirSide)
	}
	assertNoDanglingMounts(t, pod)
}

// The control: the same lists with no hostPath render exactly as before, and
// the emptyDir beside each dropped entry survives with its mounts.
func TestRenderKeepsANonHostPathVolumeOfTheSameShape(t *testing.T) {
	agent := hostPathAgent()
	pod := renderHostPathPod(t, agent)
	for _, name := range []string{hostPathFixtureEmptyDirExtra, hostPathFixtureEmptyDirSide} {
		if !hasVolume(pod.Volumes, name) {
			t.Errorf("emptyDir volume %q was dropped with the hostPath entries", name)
		}
	}

	agent.Spec.Deployment.ExtraVolumes = agent.Spec.Deployment.ExtraVolumes[1:]
	agent.Spec.Deployment.ExtraVolumeMounts = agent.Spec.Deployment.ExtraVolumeMounts[1:]
	agent.Spec.Deployment.SidecarVolumes = agent.Spec.Deployment.SidecarVolumes[1:]
	agent.Spec.Deployment.Sidecars[0].VolumeMounts = agent.Spec.Deployment.Sidecars[0].VolumeMounts[1:]
	agent.Spec.Deployment.InitContainers[0].VolumeMounts = nil
	clean := renderHostPathPod(t, agent)
	if !hasVolume(clean.Volumes, hostPathFixtureEmptyDirExtra) || !hasVolume(clean.Volumes, hostPathFixtureEmptyDirSide) {
		t.Errorf("a CR with no hostPath lost a volume: %v", clean.Volumes)
	}
	if !hasMount(mustFindContainer(t, clean, "platform-agent").VolumeMounts, hostPathFixtureEmptyDirExtra) {
		t.Errorf("a CR with no hostPath lost the agent container's extraVolumeMounts entry")
	}
	if !hasMount(mustFindContainer(t, clean, hostPathFixtureSidecarName).VolumeMounts, hostPathFixtureEmptyDirSide) {
		t.Errorf("a CR with no hostPath lost the sidecar's mount")
	}
	assertNoDanglingMounts(t, clean)
}

func TestRenderDropsAnUnmountedHostPathVolumeWithoutADanglingMount(t *testing.T) {
	agent := brokerPodAgent()
	agent.Spec.Deployment = &agentv1alpha1.DeploymentSpec{
		ExtraVolumes: []corev1.Volume{
			{Name: hostPathFixtureExtraVolume, VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: hostPathFixtureExtraPath}}},
		},
	}
	pod := renderHostPathPod(t, agent)
	assertNoHostPathVolumes(t, pod.Volumes)
	if hasVolume(pod.Volumes, hostPathFixtureExtraVolume) {
		t.Errorf("unmounted hostPath volume %q is in the Pod", hostPathFixtureExtraVolume)
	}
	for _, c := range append(append([]corev1.Container{}, pod.InitContainers...), pod.Containers...) {
		if hasMount(c.VolumeMounts, hostPathFixtureExtraVolume) {
			t.Errorf("container %q mounts %q, which nothing in the spec asked for", c.Name, hostPathFixtureExtraVolume)
		}
	}
	assertNoDanglingMounts(t, pod)
}

// The containers and mount lists come off the manager's cached copy of the CR.
// A filter that edits them in place would make the second reconcile see a
// spec the author did not write, and the condition below would have nothing
// to report.
func TestRenderLeavesTheCRsOwnSlicesUntouched(t *testing.T) {
	agent := hostPathAgent()
	before := agent.DeepCopy()
	renderHostPathPod(t, agent)
	if !hasMount(agent.Spec.Deployment.Sidecars[0].VolumeMounts, hostPathFixtureSidecarVolume) {
		t.Errorf("render removed the mount from the CR's own sidecar container")
	}
	if !hasMount(agent.Spec.Deployment.InitContainers[0].VolumeMounts, hostPathFixtureExtraVolume) {
		t.Errorf("render removed the mount from the CR's own init container")
	}
	if len(agent.Spec.Deployment.ExtraVolumes) != len(before.Spec.Deployment.ExtraVolumes) ||
		len(agent.Spec.Deployment.SidecarVolumes) != len(before.Spec.Deployment.SidecarVolumes) ||
		len(agent.Spec.Deployment.ExtraVolumeMounts) != len(before.Spec.Deployment.ExtraVolumeMounts) {
		t.Errorf("render shortened one of the CR's own volume lists")
	}
}

func TestReconcileReportsADroppedHostPathVolumeAndClearsItWhenRemoved(t *testing.T) {
	agent := hostPathAgent()
	r, cl := newSplitReconciler(t, agent)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: agent.Name, Namespace: agent.Namespace}}
	reconcileTwice := func() {
		t.Helper()
		for pass := 0; pass < 2; pass++ {
			if _, err := r.Reconcile(ctx, req); err != nil {
				t.Fatalf("Reconcile pass %d failed: %v", pass, err)
			}
		}
	}
	reconcileTwice()

	dep := &appsv1.Deployment{}
	if err := cl.Get(ctx, types.NamespacedName{Name: agent.Name + "-gateway", Namespace: agent.Namespace}, dep); err != nil {
		t.Fatalf("reading the gateway Deployment: %v", err)
	}
	assertNoHostPathVolumes(t, dep.Spec.Template.Spec.Volumes)
	assertNoDanglingMounts(t, dep.Spec.Template.Spec)

	got := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(agent), got); err != nil {
		t.Fatalf("reading the PlatformAgent back: %v", err)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, hostPathConditionType)
	if cond == nil {
		t.Fatalf("no %s condition after rendering around a hostPath; conditions: %+v", hostPathConditionType, got.Status.Conditions)
	}
	if cond.Status != metav1.ConditionTrue || cond.Reason != hostPathConditionReason {
		t.Errorf("%s condition = %s/%s, want True/%s", hostPathConditionType, cond.Status, cond.Reason, hostPathConditionReason)
	}
	for _, want := range []string{
		"spec.deployment.extraVolumes[0]", hostPathFixtureExtraVolume, hostPathFixtureExtraPath,
		"spec.deployment.sidecarVolumes[0]", hostPathFixtureSidecarVolume, hostPathFixtureSidecarPath,
	} {
		if !strings.Contains(cond.Message, want) {
			t.Errorf("%s message does not name %q: %s", hostPathConditionType, want, cond.Message)
		}
	}
	if got.Status.Phase == "Degraded" {
		t.Errorf("a dropped hostPath parked the CR on Degraded; the issue asks for drop-and-continue")
	}
	t.Logf("%s: %s/%s: %s", cond.Type, cond.Status, cond.Reason, cond.Message)

	// Removing the entries clears the condition on the next pass.
	got.Spec.Deployment.ExtraVolumes = got.Spec.Deployment.ExtraVolumes[1:]
	got.Spec.Deployment.ExtraVolumeMounts = got.Spec.Deployment.ExtraVolumeMounts[1:]
	got.Spec.Deployment.SidecarVolumes = got.Spec.Deployment.SidecarVolumes[1:]
	got.Spec.Deployment.Sidecars[0].VolumeMounts = got.Spec.Deployment.Sidecars[0].VolumeMounts[1:]
	got.Spec.Deployment.InitContainers[0].VolumeMounts = nil
	if err := cl.Update(ctx, got); err != nil {
		t.Fatalf("removing the hostPath entries: %v", err)
	}
	reconcileTwice()
	if err := cl.Get(ctx, client.ObjectKeyFromObject(agent), got); err != nil {
		t.Fatalf("reading the PlatformAgent back: %v", err)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, hostPathConditionType); cond != nil {
		t.Errorf("%s condition survived the removal of every hostPath entry: %+v", hostPathConditionType, cond)
	}
}

func TestReconcileWritesNoVolumesDroppedConditionWithoutAHostPath(t *testing.T) {
	agent := brokerPodAgent()
	r, cl := newSplitReconciler(t, agent)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: agent.Name, Namespace: agent.Namespace}}
	for pass := 0; pass < 2; pass++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile pass %d failed: %v", pass, err)
		}
	}
	got := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(agent), got); err != nil {
		t.Fatalf("reading the PlatformAgent back: %v", err)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, hostPathConditionType); cond != nil {
		t.Errorf("%s condition written for a CR with no hostPath: %+v", hostPathConditionType, cond)
	}
}
