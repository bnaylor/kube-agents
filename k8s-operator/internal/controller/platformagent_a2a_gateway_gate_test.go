package controller

// BusCredentialsReady, from the side that reads it.
//
// The condition reports that the auth callout is serving the identity map that
// every bus principal authenticates against. Until A6 nothing consumed it: the
// CRD reference said so in as many words, and a claim with no reader is a claim
// nobody notices going wrong. What reads it now is the gateway render, and only
// its CREATE - a gateway that already exists keeps reconciling through a
// callout outage, because withholding its updates would freeze its image and
// env at whatever the outage interrupted, and because the sessions it already
// spawned hang off its Deployment UID.

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ctrl "sigs.k8s.io/controller-runtime"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

// reportCalloutServing puts the callout Deployment into the state its readiness
// probe reaches once it is answering: every replica ready, and ready on the
// CURRENT pod template. The fake client runs no Deployment controller, so
// without this a test install never gets past the gate.
func reportCalloutServing(t *testing.T, ctx context.Context, cl client.Client, agent *agentv1alpha1.PlatformAgent) {
	t.Helper()
	dep := &appsv1.Deployment{}
	key := types.NamespacedName{Name: a2aCalloutName(agent), Namespace: agent.Namespace}
	if err := cl.Get(ctx, key, dep); err != nil {
		t.Fatalf("get callout Deployment: %v", err)
	}
	replicas := int32(1)
	if dep.Spec.Replicas != nil {
		replicas = *dep.Spec.Replicas
	}
	dep.Status.Replicas = replicas
	dep.Status.ReadyReplicas = replicas
	dep.Status.UpdatedReplicas = replicas
	if err := cl.Status().Update(ctx, dep); err != nil {
		t.Fatalf("update callout status: %v", err)
	}
}

// letTheGatewayThrough is reportCalloutServing plus the two passes the gate
// costs: syncBusCredentialsReady is deferred, so the reconcile that observes a
// serving callout is the one that publishes the condition, and the NEXT one is
// the first to read it True. Tests that are about something other than the gate
// call this to get past it.
func letTheGatewayThrough(t *testing.T, ctx context.Context, cl client.Client, r *PlatformAgentReconciler, req ctrl.Request, agent *agentv1alpha1.PlatformAgent) {
	t.Helper()
	reportCalloutServing(t, ctx, cl, agent)
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile %d after the callout came up: %v", i+1, err)
		}
	}
}

// completeTheProvisionJob reports the A2A provision Job complete, which is what
// sets a2aProvisionState.done. The fake client runs no Job controller, so
// without this every A2A reconcile looks unprovisioned and requeues for that
// reason alone.
func completeTheProvisionJob(t *testing.T, ctx context.Context, cl client.Client, agent *agentv1alpha1.PlatformAgent) {
	t.Helper()
	jobs := &batchv1.JobList{}
	if err := cl.List(ctx, jobs, client.InNamespace(agent.Namespace)); err != nil {
		t.Fatalf("list Jobs: %v", err)
	}
	var job *batchv1.Job
	for i := range jobs.Items {
		if strings.Contains(jobs.Items[i].Name, "-a2a-provision-") {
			job = &jobs.Items[i]
		}
	}
	if job == nil {
		t.Fatalf("no A2A provision Job among %d Jobs; this helper would silently do nothing", len(jobs.Items))
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := cl.Status().Update(ctx, job); err != nil {
		t.Fatalf("update provision Job status: %v", err)
	}
}

func a2aGateTestReconciler(t *testing.T, agent *agentv1alpha1.PlatformAgent) (*PlatformAgentReconciler, client.Client, ctrl.Request) {
	t.Helper()
	scheme := setupScheme()
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(agent, sandboxKeysSecret(agent)).
		WithStatusSubresource(&agentv1alpha1.PlatformAgent{}).
		WithInterceptorFuncs(fakeServerSideApplyInterceptors()).
		Build()
	return &PlatformAgentReconciler{Client: cl, Scheme: scheme}, cl,
		ctrl.Request{NamespacedName: types.NamespacedName{Name: agent.Name, Namespace: agent.Namespace}}
}

// The gate itself: no gateway until the callout serves.
//
// The rest of the bus stack must still render - the gate is on the one
// component that dispatches work onto the bus, not on provisioning it. A gate
// that also withheld NATS would deadlock: the callout cannot become ready
// without a bus to attach to.
func TestTheGatewayIsWithheldUntilTheCalloutServes(t *testing.T) {
	agent := a2aTestAgent()
	r, cl, req := a2aGateTestReconciler(t, agent)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile %d: %v", i+1, err)
		}
	}

	gwKey := types.NamespacedName{Name: a2aGatewayName(agent), Namespace: agent.Namespace}
	if err := cl.Get(ctx, gwKey, &appsv1.Deployment{}); !errors.IsNotFound(err) {
		t.Fatalf("gateway rendered while BusCredentialsReady is False (err=%v); it would dispatch onto a bus that refuses every session's connect", err)
	}

	// Everything the callout needs in order to become ready is already
	// there, or the gate is a deadlock rather than an ordering.
	for _, name := range []string{a2aNATSName(agent), a2aCalloutName(agent)} {
		if err := cl.Get(ctx, types.NamespacedName{Name: name, Namespace: agent.Namespace}, &appsv1.Deployment{}); err != nil {
			if err := cl.Get(ctx, types.NamespacedName{Name: name, Namespace: agent.Namespace}, &appsv1.StatefulSet{}); err != nil {
				t.Errorf("%s did not render while the gateway waits: %v", name, err)
			}
		}
	}

	// And the wait is reported rather than silent: the reconcile requeues,
	// so the gate converges on its own instead of waiting for an unrelated
	// event to wake the controller.
	//
	// Measured with the provision Job reported complete, which is the state
	// that makes this a real question. An incomplete Job requeues on its own
	// account, so a gate tested against one would requeue whether or not
	// anything watched it — and the install where the gate actually has to
	// carry the requeue is exactly the provisioned one, where the bus is up
	// and the callout is the only thing not serving yet.
	completeTheProvisionJob(t, ctx, cl, agent)
	res, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile while held: %v", err)
	}
	// The interval, not merely non-zero. A provisioned, Ready install still
	// requeues for unrelated reasons — the telemetry re-probe is 15 minutes —
	// so `!= 0` would pass with the gate's own requeue deleted, and the gate
	// would converge a quarter of an hour late while the assertion stayed
	// green.
	if res.RequeueAfter != 30*time.Second {
		t.Errorf("a held gateway on a fully provisioned bus requeued after %s, want 30s; nothing else is watching the callout's readiness at that cadence", res.RequeueAfter)
	}

	letTheGatewayThrough(t, ctx, cl, r, req, agent)

	if err := cl.Get(ctx, gwKey, &appsv1.Deployment{}); err != nil {
		t.Fatalf("gateway still withheld after the callout reported serving: %v", err)
	}
}

// The other half of "creation only": once the gateway exists, an unready
// callout must not stop it being reconciled.
//
// A callout that crash-loops after the gateway is up is an outage, and the
// operator's job during an outage is to keep converging the spec - not to pin
// the running gateway to whatever image and env the outage interrupted. This
// pins the distinction by moving a value the CR owns while the callout is down
// and requiring it to reach the live Deployment.
func TestARunningGatewayKeepsReconcilingThroughACalloutOutage(t *testing.T) {
	agent := a2aTestAgent()
	r, cl, req := a2aGateTestReconciler(t, agent)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile %d: %v", i+1, err)
		}
	}
	letTheGatewayThrough(t, ctx, cl, r, req, agent)

	gwKey := types.NamespacedName{Name: a2aGatewayName(agent), Namespace: agent.Namespace}
	if err := cl.Get(ctx, gwKey, &appsv1.Deployment{}); err != nil {
		t.Fatalf("gateway did not render: %v", err)
	}

	// The outage: no replica serving, on the current template or any other.
	dep := &appsv1.Deployment{}
	if err := cl.Get(ctx, types.NamespacedName{Name: a2aCalloutName(agent), Namespace: agent.Namespace}, dep); err != nil {
		t.Fatalf("get callout: %v", err)
	}
	dep.Status.ReadyReplicas = 0
	dep.Status.UpdatedReplicas = 0
	if err := cl.Status().Update(ctx, dep); err != nil {
		t.Fatalf("update callout status: %v", err)
	}
	// Two passes to land it, for the same deferred-write reason as
	// letTheGatewayThrough: the condition is published on the way out. The
	// spec change below must arrive when the gate is genuinely reading False,
	// or this test passes on staleness rather than on the rule it is about.
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile %d after the callout went unready: %v", i+1, err)
		}
	}
	fresh := &agentv1alpha1.PlatformAgent{}
	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	if meta.IsStatusConditionTrue(fresh.Status.Conditions, busCredentialsReadyCondition) {
		t.Fatal("BusCredentialsReady is still true after the callout went unready; the rest of this test would prove nothing")
	}

	// A spec change lands during it.
	if err := cl.Get(ctx, req.NamespacedName, fresh); err != nil {
		t.Fatalf("get agent: %v", err)
	}
	sessions := 7
	fresh.Spec.Harness = &agentv1alpha1.HarnessSpec{Tuning: &agentv1alpha1.TuningSpec{MaxSessions: &sessions}}
	if err := cl.Update(ctx, fresh); err != nil {
		t.Fatalf("update agent: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile %d during the outage: %v", i+1, err)
		}
	}

	live := &appsv1.Deployment{}
	if err := cl.Get(ctx, gwKey, live); err != nil {
		t.Fatalf("a callout outage deleted the running gateway: %v", err)
	}
	var got string
	for _, e := range live.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "A2A_MAX_SESSIONS" {
			got = e.Value
		}
	}
	if got != "7" {
		t.Errorf("A2A_MAX_SESSIONS = %q on the live gateway, want \"7\"; the gate froze a running gateway's spec instead of only withholding its creation", got)
	}
}
