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
	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

// The bus principals, in one place, as data.
//
// This list is the single source for three renders that MUST agree: the
// nats.conf user blocks for the principals that still authenticate statically,
// the auth callout's identity-to-permissions map for the ones that authenticate
// with a Kubernetes ServiceAccount token, and the NATS_USER a client is handed
// so it can set the inbox prefix its own grants require. Before the callout,
// those three lived in a config string, a Secret and a container env block, and
// nothing but review connected them. A grant list that disagrees with the user
// name in any of the three produces a client that authenticates, publishes, and
// then hangs forever on a reply its subscribe grant does not cover — the
// hardest failure in this deployment to read from the outside, and the one W6
// found twice.
//
// Deny-by-default is unchanged and so are the subject lists: arming the callout
// changes who vouches for an identity, not what that identity may say. The
// exception is deliberate and is the point of the exercise — see agentIdentity
// below, where a principal that had the shared worker grants gets the narrower
// set its actual job needs, because it now has a name of its own to hang them
// on.

// a2aAuthMode says how a principal proves who it is.
type a2aAuthMode int

const (
	// a2aAuthCallout: the principal presents a projected ServiceAccount
	// token and the callout resolves it against the cluster. No shared
	// secret exists for it anywhere.
	a2aAuthCallout a2aAuthMode = iota

	// a2aAuthStatic: the principal stays in nats.conf with a rendered
	// password and is listed in auth_users, which exempts it from the
	// callout. Every static principal carries a reason below, and the
	// reason is either "no Kubernetes identity exists to present" or "it
	// cannot have one".
	a2aAuthStatic
)

// a2aIdentity is one bus principal: what it is called on the bus, what it may
// say, and how it proves it is itself.
type a2aIdentity struct {
	// user is the NATS user name. It is also the principal's inbox prefix
	// (_INBOX.<user>.>) and therefore appears in its own subscribe list;
	// a2aIdentities() is what keeps those two in step.
	user string

	// account is the NATS account the principal lands in. The account is
	// the tenant boundary and the blast-radius container.
	account string

	auth a2aAuthMode

	// credsKey names this principal's entry in the creds Secret. Set only
	// for a2aAuthStatic.
	credsKey string

	// serviceAccount is the KSA whose token authenticates this principal,
	// as TokenReview spells it. Set only for a2aAuthCallout.
	serviceAccount string

	// comment is rendered above this principal's block in nats.conf, for the
	// static ones, or beside its entry in the map. The rationale belongs
	// where the operator reading the live config will find it, not only
	// here.
	comment string

	publish   []string
	subscribe []string
}

// Account names. One application account per scope; $SYS for operators and
// monitoring, which no agent ever authenticates into.
const (
	a2aAccountApp = "APP"
	a2aAccountSys = "SYS"
)

// a2aServiceAccountName spells a KSA the way the Kubernetes TokenReview API
// reports it, which is how the callout's map is keyed. Built here rather than
// in the map renderer so the operator and the callout cannot disagree about the
// format of the thing they are matching on.
func a2aServiceAccountName(namespace, name string) string {
	return "system:serviceaccount:" + namespace + ":" + name
}

// a2aIdentities returns every bus principal for this agent.
//
// Ordering is stable and meaningful: it is the order the map and the config are
// rendered in, so a diff of either is a diff of intent rather than of map
// iteration.
func a2aIdentities(agent *agentv1alpha1.PlatformAgent) []a2aIdentity {
	ns := agent.Namespace
	return []a2aIdentity{
		gatewayIdentity(agent, ns),
		agentIdentity(agent, ns),
		provisionIdentity(agent, ns),
		workerIdentity(),
		webIdentity(),
		sysIdentity(),
	}
}

// gateway: task requester, chat-session supervisor, session-registry owner.
// Production scopes supervisor publish to sessions the gateway spawned;
// statically that collapses to the task-events wildcard.
func gatewayIdentity(agent *agentv1alpha1.PlatformAgent, ns string) a2aIdentity {
	return a2aIdentity{
		user:           "gateway",
		account:        a2aAccountApp,
		comment:        "task requester, chat-session supervisor, session-registry owner",
		auth:           a2aAuthCallout,
		serviceAccount: a2aServiceAccountName(ns, a2aGatewayName(agent)),
		// $JS.ACK / $JS.FC.> are the delivery path's reply subjects: an
		// explicit ack is a publish to $JS.ACK.<stream>.<consumer>...,
		// and push flow control answers on $JS.FC.>. Without them a
		// consumer redelivers forever while TCP health stays green.
		//
		// The ack grant is scoped to the streams this user consumes with
		// explicit ack (the gateway-relay durable on TASKS; everything
		// else it reads is ordered/ack-none). An ack subject names a
		// stream and a CONSUMER, never the caller, so unscoped
		// $JS.ACK.> would let this user +TERM another principal's
		// in-flight delivery on ANY stream.
		publish: []string{
			"a2a.tasks.*.*.in",
			"a2a.tasks.*.*.events",
			"$KV.session-state.>",
			"$JS.API.>",
			"$JS.ACK.TASKS.>",
			"$JS.FC.>",
			"_INBOX.gateway.>",
		},
		subscribe: []string{
			"a2a.tasks.*.*.events",
			"a2a.agents.>",
			"agents.hb.>",
			"$KV.session-state.>",
			"_INBOX.gateway.>",
		},
	}
}

// agent: the platform agent's own container and the Hermes bridge sidecar that
// shares its pod.
//
// This principal is new, and it is the first narrowing the callout buys. It ran
// as `worker` — the same credential every spawned session pod holds — because
// before the callout there was no way to tell the two apart: one password, one
// grant list, three workloads. The agent pod has a ServiceAccount of its own, so
// now it gets the grants its job actually needs. Its job is reading topics and
// the directory, which is what the env comment in platformagent_manifests.go
// already said it was ("grants already fit an agent-side reader").
//
// What it loses by being named: the task plane entirely. As `worker` it could
// publish task events for ANY addressee — impersonate any executor on the bus,
// and emit a terminal event on any task in flight. Nothing it does needs that,
// and it is the single widest thing a prompt-injected agent could have reached.
func agentIdentity(agent *agentv1alpha1.PlatformAgent, ns string) a2aIdentity {
	return a2aIdentity{
		user:           "agent",
		account:        a2aAccountApp,
		comment:        "the platform agent's own container and the Hermes bridge sidecar beside it: a topic and directory reader, with no reach onto the task plane",
		auth:           a2aAuthCallout,
		serviceAccount: a2aServiceAccountName(ns, agentServiceAccountName(agent)),
		// Topic grants name the provisioned registry exactly (payload
		// spec: topics are provisioned-only). A wildcard here would let
		// a publish to an unprovisioned topic vanish into core NATS; the
		// exact list turns that into a connect-time refusal instead of
		// silent loss. Adding a topic is still two edits that travel
		// together — the stream's subject list and this grant.
		//
		// No ack grant: every read this principal makes is ordered or
		// ack-none, so an ack grant would be unused capability to +TERM
		// another principal's delivery. The bridge sidecar's durable
		// task consumer is the one thing here that acked, and it is not
		// this principal's — see the note on workerIdentity.
		publish: []string{
			"a2a.topics.agent.platform.upgrade-readiness",
			"a2a.topics.shared.blueprint",
			"a2a.topics.shared.annotations",
			"agents.hb.>",
			"$JS.API.>",
			"_INBOX.agent.>",
		},
		subscribe: []string{
			"a2a.topics.>",
			"a2a.agents.>",
			"_INBOX.agent.>",
		},
	}
}

// provision: the operator-rendered Job that creates the streams, buckets and
// starter topics.
//
// Nothing on the task plane — a provisioner that can publish tasks is a
// provisioner that can impersonate the fabric. No ack grant at all: it creates
// no consumers, so provisioning is $JS.API requests and the starter topics are
// publishes, and nothing here ever acks.
func provisionIdentity(agent *agentv1alpha1.PlatformAgent, ns string) a2aIdentity {
	return a2aIdentity{
		user:           "provision",
		account:        a2aAccountApp,
		comment:        "creates the streams, buckets and starter topics; nothing on the task plane",
		auth:           a2aAuthCallout,
		serviceAccount: a2aServiceAccountName(ns, a2aProvisionServiceAccountName(agent)),
		publish: []string{
			"a2a.topics.agent.platform.upgrade-readiness",
			"a2a.topics.shared.blueprint",
			"a2a.topics.shared.annotations",
			"$JS.API.>",
			"_INBOX.provision.>",
		},
		subscribe: []string{
			"a2a.topics.>",
			"_INBOX.provision.>",
		},
	}
}

// worker: executor for any addressee, shared by every spawned session pod.
//
// STATIC, and this is the residue A2 exists to close. A session pod carries no
// Kubernetes identity at all — the spawner sets AutomountServiceAccountToken
// false, names no ServiceAccountName, and mounts nothing but scratch — so there
// is no token to present and nothing for the callout to resolve. Giving every
// session pod one shared ServiceAccount would move the shared credential rather
// than end it, which is why A1 leaves this alone: A2 gives each session its own
// principal, scoped to its own addressee prefix, and this entry goes away.
//
// The seed Job (hand-applied, `a2a/deploy/seed.yaml`) also authenticates here
// for the same reason; it is the artifact nothing owns.
func workerIdentity() a2aIdentity {
	return a2aIdentity{
		user:    "worker",
		account: a2aAccountApp,
		comment: "executor for any addressee, shared by every spawned session pod.\n" +
			"STATIC because a session pod carries no Kubernetes identity at all -\n" +
			"no ServiceAccount, no projected token, nothing to present. Giving them\n" +
			"all one shared ServiceAccount would move the shared credential rather\n" +
			"than end it, so this closes when each session gets its own principal.",
		auth:     a2aAuthStatic,
		credsKey: "worker-password",
		publish: []string{
			"a2a.tasks.*.*.events",
			"a2a.topics.agent.platform.upgrade-readiness",
			"a2a.topics.shared.blueprint",
			"a2a.topics.shared.annotations",
			"a2a.agents.>",
			"agents.hb.>",
			"$KV.runtime-state.>",
			"$JS.API.>",
			"$JS.ACK.TASKS.>",
			"$JS.FC.>",
			"_INBOX.worker.>",
		},
		subscribe: []string{
			"a2a.tasks.>",
			"a2a.topics.>",
			"$KV.runtime-state.>",
			"_INBOX.worker.>",
		},
	}
}

// web: the read surface, the one user meant to face a browser, and the only
// user whose credential is published to one by design.
//
// STATIC, permanently. A browser holds no Kubernetes ServiceAccount token and
// there is no mechanism by which it could, so this principal can never move to
// the callout. It is not a residue awaiting a card; it is the shape of the
// thing. What the callout does change is that this is now the ONLY credential
// in the deployment a browser is ever handed.
//
// "Read-only" is not expressible as a subject list — subject permissions cannot
// see a request body, and JetStream puts the reach there — so the JS API grants
// are enumerated per stream rather than given as $JS.API.>, and there is no ack
// grant. The residues that enumeration cannot close (durability, ack policy and
// consumer names are body fields) are recorded in the deployment spec, and they
// are the ones the callout was expected to close for this user. It does not:
// they close with a separate account and an export/import, which stays open.
func webIdentity() a2aIdentity {
	return a2aIdentity{
		user:    "web",
		account: a2aAccountApp,
		comment: "the read surface, and the only credential published to a browser by\n" +
			"design. STATIC permanently: a browser holds no ServiceAccount token and\n" +
			"there is no mechanism by which it could. Read-only is not expressible as\n" +
			"a subject list - JetStream puts the reach in the request BODY - so the JS\n" +
			"API grants are enumerated per stream and there is no ack grant.",
		auth:     a2aAuthStatic,
		credsKey: "web-password",
		publish: []string{
			"$JS.API.INFO",
			"$JS.API.STREAM.INFO.TASKS",
			"$JS.API.STREAM.INFO.DIRECTORY",
			"$JS.API.STREAM.INFO.TOPICS-STATE",
			"$JS.API.STREAM.INFO.TOPICS-JOURNAL",
			"$JS.API.CONSUMER.CREATE.TASKS.>",
			"$JS.API.CONSUMER.CREATE.DIRECTORY.>",
			"$JS.API.CONSUMER.CREATE.TOPICS-STATE.>",
			"$JS.API.CONSUMER.CREATE.TOPICS-JOURNAL.>",
			"$JS.API.CONSUMER.INFO.TASKS.*",
			"$JS.API.CONSUMER.INFO.DIRECTORY.*",
			"$JS.API.CONSUMER.INFO.TOPICS-STATE.*",
			"$JS.API.CONSUMER.INFO.TOPICS-JOURNAL.*",
			"$JS.API.CONSUMER.MSG.NEXT.TASKS.*",
			"$JS.API.CONSUMER.MSG.NEXT.DIRECTORY.*",
			"$JS.API.CONSUMER.MSG.NEXT.TOPICS-STATE.*",
			"$JS.API.CONSUMER.MSG.NEXT.TOPICS-JOURNAL.*",
			"_INBOX.web.>",
		},
		subscribe: []string{
			"a2a.>",
			"_INBOX.web.>",
		},
	}
}

// sys: human operators and monitoring in $SYS. No agent ever authenticates
// here.
//
// STATIC: the holder is a person with a port-forward or a scrape config, not a
// workload with a projected token. The callout service's own connection is a
// separate matter and is not this principal — see a2aCalloutServiceUser.
func sysIdentity() a2aIdentity {
	return a2aIdentity{
		user:    "sys",
		account: a2aAccountSys,
		comment: "human operators and monitoring. No agent ever authenticates here.\n" +
			"STATIC: the holder is a person with a port-forward or a scrape config.",
		auth:     a2aAuthStatic,
		credsKey: "sys-password",
	}
}

// calloutIdentities returns the principals the callout serves, in render order.
func calloutIdentities(agent *agentv1alpha1.PlatformAgent) []a2aIdentity {
	var out []a2aIdentity
	for _, id := range a2aIdentities(agent) {
		if id.auth == a2aAuthCallout {
			out = append(out, id)
		}
	}
	return out
}

// staticIdentities returns the principals that stay in nats.conf, in render
// order. Every one of them is listed in auth_users, which is what exempts it
// from the callout; a static user NOT in that list would be refused at connect
// by a callout that has never heard of it.
func staticIdentities(agent *agentv1alpha1.PlatformAgent) []a2aIdentity {
	var out []a2aIdentity
	for _, id := range a2aIdentities(agent) {
		if id.auth == a2aAuthStatic {
			out = append(out, id)
		}
	}
	return out
}
