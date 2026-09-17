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
	"regexp"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

// The tests in this file are for #1671: a hostPath on spec.deployment.extraVolumes
// or .sidecarVolumes was refused by the admission webhook and nowhere else, and
// the chart ships the webhook off. They assert the render-side layer: the volume
// and every mount naming it are out of the Pod, the CR's own slices are not
// edited, the drop is reported on status and only on the passes where it is
// true of the Pod that is running, and a non-hostPath volume of the same shape
// is untouched.
//
// The condition type and reason are spelled as literals below because the
// render cases were first run against a controller without the fix, to watch
// them fail. The file as a whole no longer compiles there: the message-budget
// and condition-gate cases reach hostPathDroppedMessage,
// hostPathExtraVolumesField and hostPathDroppedEntryEllipsis, none of which
// exist without it. Reproducing that failing run now means taking the render
// cases over on their own.

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
	// conditionMessageMaxLength is the cap the CRD schema puts on a condition
	// message (`maxLength: 32768` under status.conditions[].message in
	// config/crd/bases/kubeagents.x-k8s.io_platformagents.yaml, from
	// metav1.Condition's own marker). A message over it does not fail this
	// condition alone: the API server refuses the whole status subresource
	// write, so Ready, the phase and every other condition go with it.
	conditionMessageMaxLength = 32768
	// The flood fixture: enough author-chosen characters that listing every
	// entry would run past that cap.
	floodedHostPathCount   = 64
	floodedHostPathNameLen = 1024
)

// hostPathOverflowCountPattern reads the count back out of the message's
// overflow clause, so the test does not hard-code how many entries the budget
// happens to fit.
var hostPathOverflowCountPattern = regexp.MustCompile(`and (\d+) more`)

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

	// A pass that changes nothing writes nothing. The condition is in
	// updateStatusReady's unchanged comparison for the same reason the others
	// are: a status write per pass re-enqueues the CR through the unfiltered
	// watch and reconciles it continuously.
	settledVersion := got.ResourceVersion
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile (settled) failed: %v", err)
	}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(agent), got); err != nil {
		t.Fatalf("reading the PlatformAgent back: %v", err)
	}
	if got.ResourceVersion != settledVersion {
		t.Errorf("a reconcile with nothing to change wrote the CR (resourceVersion %s -> %s); the %s condition is causing a write per pass", settledVersion, got.ResourceVersion, hostPathConditionType)
	}

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

// TestADroppedHostPathIsReportedOnAReconcileThatParksDegraded is the gap the
// Ready-only condition write left. Three refusals render the workload in full
// and then park the CR on Degraded (ModeNotRecognized, A2AProvisionFailed,
// ShellSandboxKeysMissing), and on those passes updateStatusReady never runs.
// The one used here is the one a chart-default install sits on indefinitely:
// the chart renders the sandbox's authorized-keys Secret only when a public
// key is supplied, and `credentials.create` is false by default — which is the
// same install the webhook is off on, so it is also the install where a
// hostPath reaches the reconcile at all. The Deployment is written without the
// volume either way; what this asserts is that the CR says so.
func TestADroppedHostPathIsReportedOnAReconcileThatParksDegraded(t *testing.T) {
	agent := hostPathAgent()
	r, cl := newSplitReconciler(t, agent)
	ctx := context.Background()
	if err := cl.Delete(ctx, shellSandboxKeysSecret(agent)); err != nil {
		t.Fatalf("removing the sandbox keys Secret the fixture creates: %v", err)
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: agent.Name, Namespace: agent.Namespace}}
	for pass := 0; pass < 2; pass++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile pass %d failed: %v", pass, err)
		}
	}

	// The render happened: without it there is no drop to report and the test
	// would pass against a controller that never writes the condition at all.
	dep := &appsv1.Deployment{}
	if err := cl.Get(ctx, types.NamespacedName{Name: agent.Name + "-gateway", Namespace: agent.Namespace}, dep); err != nil {
		t.Fatalf("the Degraded path did not render the gateway Deployment, so this is no longer the case under test: %v", err)
	}
	assertNoHostPathVolumes(t, dep.Spec.Template.Spec.Volumes)
	assertNoDanglingMounts(t, dep.Spec.Template.Spec)

	got := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(agent), got); err != nil {
		t.Fatalf("reading the PlatformAgent back: %v", err)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if got.Status.Phase != "Degraded" || ready == nil || ready.Reason != reasonShellSandboxKeysMissing {
		t.Fatalf("phase=%q Ready=%+v, want Degraded/%s; the pass under test is the one that parks there", got.Status.Phase, ready, reasonShellSandboxKeysMissing)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, hostPathConditionType)
	if cond == nil {
		t.Fatalf("no %s condition on a CR parked Degraded after the render dropped a hostPath; conditions: %+v", hostPathConditionType, got.Status.Conditions)
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

	// Still one write per change, not one per pass: the Degraded writer's
	// unchanged comparison has to cover the condition it now carries, or the
	// parked CR writes status on every requeue tick (#1392).
	settledVersion := got.ResourceVersion
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile (settled) failed: %v", err)
	}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(agent), got); err != nil {
		t.Fatalf("reading the PlatformAgent back: %v", err)
	}
	if got.ResourceVersion != settledVersion {
		t.Errorf("a parked pass with nothing to change wrote the CR (resourceVersion %s -> %s)", settledVersion, got.ResourceVersion)
	}

	// And it clears on the Degraded path too, rather than standing until the
	// CR happens to reach Ready.
	got.Spec.Deployment.ExtraVolumes = got.Spec.Deployment.ExtraVolumes[1:]
	got.Spec.Deployment.ExtraVolumeMounts = got.Spec.Deployment.ExtraVolumeMounts[1:]
	got.Spec.Deployment.SidecarVolumes = got.Spec.Deployment.SidecarVolumes[1:]
	got.Spec.Deployment.Sidecars[0].VolumeMounts = got.Spec.Deployment.Sidecars[0].VolumeMounts[1:]
	got.Spec.Deployment.InitContainers[0].VolumeMounts = nil
	if err := cl.Update(ctx, got); err != nil {
		t.Fatalf("removing the hostPath entries: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile (after removal) failed: %v", err)
	}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(agent), got); err != nil {
		t.Fatalf("reading the PlatformAgent back: %v", err)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, hostPathConditionType); cond != nil {
		t.Errorf("%s condition survived the removal of every hostPath entry on the Degraded path: %+v", hostPathConditionType, cond)
	}
}

// hostPathFloodDeploymentSpec is a spec.deployment carrying count hostPath
// volumes whose names and paths are long enough that listing them all would
// run past the 32768 characters the CRD schema allows a condition message.
// Author-chosen strings, both of them, and nothing bounds either.
func hostPathFloodDeploymentSpec(count, nameLen int) *agentv1alpha1.DeploymentSpec {
	spec := &agentv1alpha1.DeploymentSpec{}
	for i := 0; i < count; i++ {
		// Distinct names: extraVolumes is a list-map keyed on name, so the API
		// server refuses a CR that repeats one.
		suffix := "-" + strconv.Itoa(i)
		spec.ExtraVolumes = append(spec.ExtraVolumes, corev1.Volume{
			Name: strings.Repeat("v", nameLen) + suffix,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/" + strings.Repeat("p", nameLen) + suffix},
			},
		})
	}
	return spec
}

func TestTheDroppedVolumeMessageStaysUnderTheConditionCap(t *testing.T) {
	agent := brokerPodAgent()
	agent.Spec.Deployment = hostPathFloodDeploymentSpec(floodedHostPathCount, floodedHostPathNameLen)
	msg := hostPathDroppedMessage(agent)

	if len(msg) > conditionMessageMaxLength {
		t.Errorf("message is %d characters, over the %d the CRD schema allows: every status write on this CR fails, not just this condition", len(msg), conditionMessageMaxLength)
	}
	if !strings.Contains(msg, "spec.deployment.extraVolumes[0]") {
		t.Errorf("message does not name the first entry, which is the one the author has to find: %s", msg)
	}
	// Every entry it did not list has to be accounted for, or the message is
	// a shorter lie rather than a shorter report.
	listed := strings.Count(msg, hostPathExtraVolumesField+"[")
	match := hostPathOverflowCountPattern.FindStringSubmatch(msg)
	if match == nil {
		t.Fatalf("message lists %d of %d entries and does not say how many it left out: %s", listed, floodedHostPathCount, msg)
	}
	rest, err := strconv.Atoi(match[1])
	if err != nil {
		t.Fatalf("unreadable overflow count %q: %v", match[1], err)
	}
	if listed+rest != floodedHostPathCount {
		t.Errorf("message lists %d entries and counts %d more, which is %d of %d", listed, rest, listed+rest, floodedHostPathCount)
	}
	t.Logf("%d entries of %d characters each rendered a %d-character message listing %d of them", floodedHostPathCount, floodedHostPathNameLen, len(msg), listed)
}

// One entry can be longer on its own than the message may be, so the budget
// has to cut inside an entry rather than only between entries.
func TestASingleOversizedHostPathEntryIsTruncated(t *testing.T) {
	agent := brokerPodAgent()
	agent.Spec.Deployment = hostPathFloodDeploymentSpec(1, 64*1024)
	msg := hostPathDroppedMessage(agent)

	if len(msg) > conditionMessageMaxLength {
		t.Errorf("message is %d characters, over the %d the CRD schema allows", len(msg), conditionMessageMaxLength)
	}
	if !strings.Contains(msg, "spec.deployment.extraVolumes[0]") {
		t.Errorf("a truncated message still has to name the field the entry is on: %s", msg)
	}
	if !strings.Contains(msg, hostPathDroppedEntryEllipsis) {
		t.Errorf("a truncated entry is not marked as truncated: %s", msg)
	}
}

// TestAPreRenderRefusalWritesNoVolumesDroppedCondition is the other side of
// the Degraded-path write. Four refusals return before reconcileWorkload —
// ForbiddenVolumeMount, ShellSandboxCannotBeDisabled, RuntimeClassNotFound and
// EgressAllowlistRefused — and on those passes no Pod is rendered, so the
// running workload is whatever the previous pass left. Writing the condition
// there would report a security property of a Pod this operator never wrote:
// on an install rolled forward over a CR whose Deployment a pre-fix render
// gave real hostPath mounts, `kubectl describe` would say the mounts are gone
// while they are still mounted, on every 30s requeue for as long as the
// refusal stands.
//
// The refusal used here is the one that needs no cluster state to provoke.
func TestAPreRenderRefusalWritesNoVolumesDroppedCondition(t *testing.T) {
	agent := hostPathAgent()
	agent.Spec.Harness.Experimental = &agentv1alpha1.ExperimentalSpec{
		ShellSandbox: &agentv1alpha1.ShellSandboxSpec{Enabled: ptr.To(false)},
	}
	r, cl := newSplitReconciler(t, agent)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: agent.Name, Namespace: agent.Namespace}}
	for pass := 0; pass < 2; pass++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("a refusal is a Degraded status, not a reconcile error (pass %d): %v", pass, err)
		}
	}

	// Nothing rendered, which is what makes this the case under test rather
	// than a variant of the Degraded case above.
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: agent.Name + "-gateway", Namespace: agent.Namespace}}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(dep), dep); !apierrors.IsNotFound(err) {
		t.Fatalf("the refusal rendered the gateway Deployment (err %v), so this is no longer a pre-render pass", err)
	}

	got := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(agent), got); err != nil {
		t.Fatalf("reading the PlatformAgent back: %v", err)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if got.Status.Phase != "Degraded" || ready == nil || ready.Reason != reasonShellSandboxCannotBeDisabled {
		t.Fatalf("phase=%q Ready=%+v, want Degraded/%s; the pass under test is the one that parks there", got.Status.Phase, ready, reasonShellSandboxCannotBeDisabled)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, hostPathConditionType); cond != nil {
		t.Errorf("%s written on a pass that rendered no Pod: the CR now asserts the hostPath entries are out of a workload this operator never wrote: %+v", hostPathConditionType, cond)
	}
}

// TestAPreRenderRefusalLeavesAnAlreadyPresentVolumesDroppedInPlace pins the
// other half of the gate, which is not symmetric with it. A condition already
// on the CR was written by a pass that did render, and the Pod that pass wrote
// is still the one running — so a pre-render refusal leaves it exactly as it
// stands rather than clearing or refreshing it.
//
// The discriminator is a spec edit that takes the hostPath entries away at the
// same time as it provokes the refusal. Recomputing the condition there would
// remove it, and the CR would stop reporting volumes that are genuinely absent
// from the running Pod. Left in place it is stale in its wording — it names
// entries the spec no longer carries — and correct in what it asserts, which
// is the direction this condition has to err in: over-reporting a drop that
// happened, never claiming one that did not. The next pass to reach the render
// refreshes or removes it.
func TestAPreRenderRefusalLeavesAnAlreadyPresentVolumesDroppedInPlace(t *testing.T) {
	agent := hostPathAgent()
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
	rendered := meta.FindStatusCondition(got.Status.Conditions, hostPathConditionType)
	if rendered == nil {
		t.Fatalf("the rendering passes wrote no %s condition, so there is nothing for the refusal to preserve", hostPathConditionType)
	}
	renderedMessage := rendered.Message

	// Switch the sandbox off and take the hostPath entries out in one edit.
	got.Spec.Harness.Experimental = &agentv1alpha1.ExperimentalSpec{
		ShellSandbox: &agentv1alpha1.ShellSandboxSpec{Enabled: ptr.To(false)},
	}
	got.Spec.Deployment.ExtraVolumes = got.Spec.Deployment.ExtraVolumes[1:]
	got.Spec.Deployment.ExtraVolumeMounts = got.Spec.Deployment.ExtraVolumeMounts[1:]
	got.Spec.Deployment.SidecarVolumes = got.Spec.Deployment.SidecarVolumes[1:]
	got.Spec.Deployment.Sidecars[0].VolumeMounts = got.Spec.Deployment.Sidecars[0].VolumeMounts[1:]
	got.Spec.Deployment.InitContainers[0].VolumeMounts = nil
	if err := cl.Update(ctx, got); err != nil {
		t.Fatalf("switching the sandbox off and removing the hostPath entries: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("a refusal is a Degraded status, not a reconcile error: %v", err)
	}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(agent), got); err != nil {
		t.Fatalf("reading the PlatformAgent back: %v", err)
	}
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if got.Status.Phase != "Degraded" || ready == nil || ready.Reason != reasonShellSandboxCannotBeDisabled {
		t.Fatalf("phase=%q Ready=%+v, want Degraded/%s", got.Status.Phase, ready, reasonShellSandboxCannotBeDisabled)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, hostPathConditionType)
	if cond == nil {
		t.Fatalf("the refusal cleared a %s condition a rendering pass had written; the Pod that pass rendered is still the one running without those volumes", hostPathConditionType)
	}
	if cond.Status != metav1.ConditionTrue || cond.Reason != hostPathConditionReason || cond.Message != renderedMessage {
		t.Errorf("the refusal rewrote the condition instead of leaving it: got %s/%s %q, want it untouched at %s/%s %q", cond.Status, cond.Reason, cond.Message, metav1.ConditionTrue, hostPathConditionReason, renderedMessage)
	}

	// And a parked CR still writes once per change, not once per pass. The
	// condition is out of the Degraded writer's comparison on a pre-render
	// pass, because a term no write can satisfy would make every requeue tick
	// a status write (#1392).
	settledVersion := got.ResourceVersion
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile (settled) failed: %v", err)
	}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(agent), got); err != nil {
		t.Fatalf("reading the PlatformAgent back: %v", err)
	}
	if got.ResourceVersion != settledVersion {
		t.Errorf("a parked pre-render pass with nothing to change wrote the CR (resourceVersion %s -> %s)", settledVersion, got.ResourceVersion)
	}
}
