package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

// The verifier crash-loops on purpose until the provision Job creates the cap
// bucket -- reconcileA2A applies it before the Job and its comment says so. The
// pod scan has to tell that wait from a fault, because the arm of
// updateStatusReady that calls the scan is live for exactly that window (notReady
// holds "bus provisioning" until the same Job completes). Reporting it would turn
// every fresh `next` install's ordinary Provisioning into a Degraded an operator
// acts on, for a workload behaving as designed.
//
// The suppression is narrow in three directions and each gets a case here: the
// container, the reason, and the bucket not yet existing.

func bucketWaitAgent() *agentv1alpha1.PlatformAgent {
	agent := &agentv1alpha1.PlatformAgent{
		ObjectMeta: metav1.ObjectMeta{Name: "test-agent", Namespace: "test-ns"},
	}
	agent.Spec.Mode = ptr.To(string(ModeNext))
	return agent
}

func verifierPod(agent *agentv1alpha1.PlatformAgent, container, waitingReason string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      a2aVerifierName(agent) + "-abc123",
			Namespace: agent.Namespace,
			Labels:    map[string]string{"app": a2aVerifierName(agent)},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  container,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: waitingReason}},
			}},
			Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}},
		},
	}
}

func TestTheVerifiersBucketWaitIsNotReportedAsADegradedInstall(t *testing.T) {
	cases := []struct {
		name string
		// capBucketProvisioned is the provision Job having completed.
		provisioned bool
		container   string
		reason      string
		wantPhase   string
		wantReason  string
	}{
		{
			// The case the whole change exists for: every fresh next install.
			name:        "a crash loop before the bucket exists is the design, not a fault",
			provisioned: false,
			container:   a2aVerifierContainerName,
			reason:      reasonCrashLoopBackOff,
			wantPhase:   "Provisioning",
			wantReason:  "Provisioning",
		},
		{
			// Narrow on the bucket: once the Job is done the wait is over, so a
			// crash loop is the verifier failing at something else.
			name:        "the same crash loop after the bucket exists is a fault",
			provisioned: true,
			container:   a2aVerifierContainerName,
			reason:      reasonCrashLoopBackOff,
			wantPhase:   "Degraded",
			wantReason:  reasonCrashLoopBackOff,
		},
		{
			// Narrow on the reason, and the reason this selector was added at
			// all: a2aReleaseImage can name something unpullable at any of its
			// three rungs, and a verifier that cannot start refuses every task.
			name:        "an unpullable image is a fault even before the bucket exists",
			provisioned: false,
			container:   a2aVerifierContainerName,
			reason:      "ImagePullBackOff",
			wantPhase:   "Degraded",
			wantReason:  "ImagePullBackOff",
		},
		{
			// Narrow on the container: the suppression must not swallow a fault
			// on something else sharing the verifier's pod.
			name:        "another container crash looping in the verifier pod is a fault",
			provisioned: false,
			container:   "sidecar",
			reason:      reasonCrashLoopBackOff,
			wantPhase:   "Degraded",
			wantReason:  reasonCrashLoopBackOff,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := setupScheme()
			agent := bucketWaitAgent()
			if !a2aStackRendering(agent) {
				t.Fatalf("the fixture does not render the A2A stack, so the verifier selector is never appended and every case below would pass vacuously")
			}
			pod := verifierPod(agent, tc.container, tc.reason)
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent, pod).Build()
			r := &PlatformAgentReconciler{Client: cl, Scheme: scheme}

			phase, reason, message := r.getDeploymentStatusDetails(context.Background(), agent, tc.provisioned)
			if phase != tc.wantPhase {
				t.Errorf("phase = %q, want %q (message %q)", phase, tc.wantPhase, message)
			}
			if reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
			if tc.wantPhase == "Degraded" && !strings.Contains(message, tc.container) {
				t.Errorf("the Degraded message does not name the faulting container %q: %q", tc.container, message)
			}
		})
	}
}

// The suppression is keyed on the verifier's own label, so a crash loop
// somewhere else in the install stays reportable during the same window. Without
// this the fix would trade a false Degraded for a missing one.
//
// The label term and the container-name term are separately load-bearing, and it
// took a mutation to see it: dropping the label alone survived the first version
// of this file, because every other pod the scan reaches happens to have no
// container called "verifier". That is a fact about today's renders, not a
// property of the guard, so the second test below pins the label term against a
// pod that does.
func TestTheBucketWaitSuppressionIsKeyedOnTheVerifierAlone(t *testing.T) {
	scheme := setupScheme()
	agent := bucketWaitAgent()

	gateway := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      agent.Name + "-gateway-xyz",
			Namespace: agent.Namespace,
			Labels:    map[string]string{"app": agent.Name + "-gateway"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "platform-agent",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reasonCrashLoopBackOff}},
			}},
			Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}},
		},
	}
	// The verifier is in its by-design wait at the same time, which is the
	// ordinary shape of a fresh install that is also broken.
	verifier := verifierPod(agent, a2aVerifierContainerName, reasonCrashLoopBackOff)

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent, gateway, verifier).Build()
	r := &PlatformAgentReconciler{Client: cl, Scheme: scheme}

	phase, reason, message := r.getDeploymentStatusDetails(context.Background(), agent, false)
	if phase != "Degraded" || reason != reasonCrashLoopBackOff {
		t.Fatalf("phase/reason = %q/%q, want Degraded/%s", phase, reason, reasonCrashLoopBackOff)
	}
	if !strings.Contains(message, "platform-agent") {
		t.Errorf("the Degraded names the wrong container; want the gateway's: %q", message)
	}
	if strings.Contains(message, a2aVerifierName(agent)) {
		t.Errorf("the Degraded names the verifier, whose crash loop is the designed bucket wait: %q", message)
	}
}

// A container named "verifier" in some other workload's pod is not the verifier,
// and its crash loop is not the bucket wait. Nothing renders such a pod today --
// which is exactly why this is pinned: the guard must hold on the label, not on
// the coincidence that the name is unique across the install's renders.
func TestAContainerNamedVerifierElsewhereIsStillAFault(t *testing.T) {
	scheme := setupScheme()
	agent := bucketWaitAgent()

	impostor := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      agent.Name + "-gateway-xyz",
			Namespace: agent.Namespace,
			Labels:    map[string]string{"app": agent.Name + "-gateway"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  a2aVerifierContainerName,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reasonCrashLoopBackOff}},
			}},
			Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent, impostor).Build()
	r := &PlatformAgentReconciler{Client: cl, Scheme: scheme}

	phase, reason, message := r.getDeploymentStatusDetails(context.Background(), agent, false)
	if phase != "Degraded" || reason != reasonCrashLoopBackOff {
		t.Fatalf("phase/reason = %q/%q, want Degraded/%s (message %q)", phase, reason, reasonCrashLoopBackOff, message)
	}
	if !strings.Contains(message, impostor.Name) {
		t.Errorf("the Degraded does not name the faulting pod: %q", message)
	}
}
