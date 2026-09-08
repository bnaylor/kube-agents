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
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

// BusCredentialsReady: whether the bus can authenticate the identities the
// operator has rendered.
//
// The race it exists to remove is the deployment spec's: an identity's entry
// lands, a workload that needs it is spawned seconds later, and the callout has
// not caught up — so a legitimate client holding a perfectly good token is
// refused, and the refusal is indistinguishable from a bad credential. The
// answer is not to hope the propagation wins; it is to have a condition that
// says whether it has, and for the thing that dispatches work to wait on it.
//
// **What this asserts, exactly.** The callout Deployment is Available with all
// replicas ready. Since the callout's readiness probe answers 503 until it is
// serving a map, that means every replica is serving one. Combined with the
// map being rendered before the callout in the same reconcile, this is "the
// bus can authenticate what the operator has rendered".
//
// **What it does not assert**, so nobody reads more into it than it carries: it
// does not confirm that a named replica has observed a named map version. The
// message names the version this reconcile RENDERED, not one any replica
// reported serving, and nothing here reads the callout's /status. That is
// acceptable while the identity set changes only when the operator re-renders
// it — which is A1's situation, where the identities are fixed at install. It
// stops being acceptable when profiles arrive at runtime and identities become
// dynamic: at that point this has to become a per-replica check of the served
// version against the rendered one, and the callout's status endpoint already
// exposes exactly what such a check would read.
//
// Two failure shapes fit in that gap, and they are not the same size:
//
//   - The benign one, and the only one this comment used to name: the
//     sub-second window after a re-render in which a ready replica is still
//     serving the previous map. It closes by itself on the next informer event.
//   - The one that does NOT close by itself: a map the callout REFUSES at
//     parse. It keeps serving the previous map on purpose, so its probe stays
//     green, so the Deployment stays Ready, so this sets True and names a
//     version that was never served — permanently, and while silently dropping
//     every other identity change in the same map. renderA2AAuthMap now runs
//     the callout's own validation before writing the ConfigMap, so the
//     reconcile fails with the offending entry named instead of reaching this
//     function at all. That closes the shape the operator can see. A map the
//     callout refuses for a reason the operator's copy does not know about
//     would still land here, which is the residual argument for the
//     per-replica check above.
//
// Rolling the callout pods on every map change would close the gap and was
// rejected: the callout is on the connection path, so a rolling restart is a
// window in which new connections fail, which is the thing the informer exists
// to avoid. A stale condition for a moment is cheaper than a refused connection.
const busCredentialsReadyCondition = "BusCredentialsReady"

const (
	busCredsReasonServing     = "CalloutServing"
	busCredsReasonUnavailable = "CalloutUnavailable"
	busCredsReasonAbsent      = "CalloutAbsent"
)

// setBusCredentialsReady writes the condition from the callout Deployment's
// state. mapVersion is what this reconcile rendered, carried into the message
// so an operator can compare it against what the callout reports.
func (r *PlatformAgentReconciler) setBusCredentialsReady(ctx context.Context, agent *agentv1alpha1.PlatformAgent, mapVersion string) error {
	dep := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: a2aCalloutName(agent), Namespace: agent.Namespace}, dep)

	condition := metav1.Condition{Type: busCredentialsReadyCondition, LastTransitionTime: metav1.Now()}
	switch {
	case client.IgnoreNotFound(err) != nil:
		return err
	case err != nil:
		condition.Status = metav1.ConditionFalse
		condition.Reason = busCredsReasonAbsent
		condition.Message = "the auth callout is not deployed; nothing can authenticate to the bus"
	case dep.Status.ReadyReplicas > 0 && dep.Status.ReadyReplicas == dep.Status.Replicas:
		condition.Status = metav1.ConditionTrue
		condition.Reason = busCredsReasonServing
		condition.Message = "the auth callout is serving identity map " + mapVersion
	default:
		condition.Status = metav1.ConditionFalse
		condition.Reason = busCredsReasonUnavailable
		// With the counts, because "not ready" on a component that gates
		// every new connection is the first thing someone will want to size:
		// one replica of two is a degraded rollout, zero of two is the bus
		// accepting no new client at all.
		condition.Message = fmt.Sprintf(
			"the auth callout has %d of %d replicas ready; new connections to the bus may be refused",
			dep.Status.ReadyReplicas, dep.Status.Replicas)
	}

	meta.SetStatusCondition(&agent.Status.Conditions, condition)
	return r.Status().Update(ctx, agent)
}

// clearBusCredentialsReady removes the condition. A today install must not
// carry a condition that describes a component it does not have — the darkness
// property reaches status, not just objects.
func (r *PlatformAgentReconciler) clearBusCredentialsReady(agent *agentv1alpha1.PlatformAgent) bool {
	return meta.RemoveStatusCondition(&agent.Status.Conditions, busCredentialsReadyCondition)
}
