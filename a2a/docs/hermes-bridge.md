# The Hermes bridge

- **Author:** [@bnaylor]
- **Date:** 2026-08-26
- **Status:** draft for review
- **Companions:** the A2A payload spec (task lifecycle, steering), the NATS deployment
  spec (accounts), the subagent profiles spec (supervision, CAS)

## Purpose

The retargeted first wave routes every gateway task to the addressee `platform`, and
nothing answers on that subject yet - the worker adapter (W4) fast-follows, and the
dispatcher is stage 3.  The bridge is the stand-in executor: a small Go daemon on
`a2a/lib` that consumes tasks addressed to `platform`, runs
`hermes -p platform chat -q <prompt>` per task, and publishes the lifecycle events with
the output as the `result` artifact.  It is scaffolding with a planned demolition date:
when the dispatcher and worker adapter land, the bridge retires.  Nothing here is
protocol - the wire contract is the payload spec's, unchanged.

## Where it runs

**Sidecar in the platform-agent pod, declared via the CR's `spec.sidecars` field.**  The
bridge needs two things that only exist in that pod: the `hermes` CLI (it lives in the
platform-agent image, so the bridge image builds FROM it and adds one static binary) and
the persona state - `$HERMES_HOME` is the agent's data PVC, RWO, holding the platform
profile's config, memory, and skills.  A separate Deployment would need that PVC mounted
cross-pod, which RWO only allows with same-node scheduling games.  Not worth it for a
component we intend to delete.

The `sidecars` field takes ordinary `corev1.Container` entries, so the operator renders
the bridge without any operator code change and reconcile never fights us.  The sidecar
mounts the same data volume, runs as the pod's KSA (model auth via Workload Identity for
free), and gets `NATS_URL` plus creds from the a2a creds Secret.

Concurrent hermes processes under one `$HERMES_HOME` is the kanban dispatcher's existing
posture (`deploy/docker/patches/kanban_result_required.py` documents `_default_spawn`
doing exactly this invocation), so the bridge inherits a known-working concurrency
story.  Cap is 2, matching the platform profile's `concurrency` in the profiles spec.

## Bus user and grants

The `…in` subject has two reader roles by design - the dispatcher for new tasks, the
executor for everything after the submission.  The bridge is both, collapsed into one
process: it holds the one durable consumer on `a2a.tasks.platform.*.in` (the dispatcher
role, `durable: bridge-platform`) and handles follow-ups and cancels for tasks it is
running (the executor role).  That collapse is exactly what makes it a stand-in - when
the real dispatcher arrives, the roles separate again and the bridge has nothing left to
do.

On the W6 install the bridge connects as the static `worker` user, whose grants already
cover it: subscribe `a2a.tasks.>`, publish `a2a.tasks.*.*.events`, plus the JetStream
tax (`$JS.API.>`, `$JS.ACK.>`, `$JS.FC.>`, `_INBOX.worker.>`) and `$KV.runtime-state.>`
for the in-flight registry below.  Playground posture - the shared static user is the
playground, per the deployment spec.  The production shape, recorded for when the
callout arms: a dedicated `bridge` identity with subscribe `a2a.tasks.platform.*.in`,
publish `a2a.tasks.platform.*.events`, its own inbox prefix, and the KV grant.  Nothing
wider - a bridge that can publish submissions is a bridge that can impersonate the
gateway.

## Lifecycle, steering, cancel

Per task: `submitted` on accept (before the consumer ack, so a bridge death before the
ack just redelivers), `working` when the subprocess spawns, the stdout as a `result`
artifact (chunked if large), one terminal `status-update` with `final: true`.  A nonzero
exit is terminal `failed` with the exit code and a stderr tail in the status message.  A
submission with no text parts is terminal `rejected`.  New-task detection is the
dispatcher's rule: empty `…events` subject means new; anything on `…in` for a task with
a terminal event is acked with a warning and nothing else.

**Steering:** `hermes chat -q` is one-shot - there is no stdin to inject into.  A
follow-up message to a running task is acked and answered with a non-final `working`
status whose message says the input cannot be absorbed mid-run and cancel is available.
Honest, never silent.  This does not change task state (payload spec assertion 12).

**Cancel:** SIGTERM to the subprocess's process group, SIGKILL after a grace period,
then terminal `canceled`.  A task racing to completion may land `completed` first - both
orders are legal and the terminal event wins.  A per-task deadline (default 7200s,
matching the profile's `activeDeadlineSeconds`) takes the same kill path and lands
`failed`.

## Supervision

The bridge is its own janitor, per the ratified split - every task's supervisor is the
component that spawned its execution.  Two failure classes:

- **The subprocess dies under a live bridge.**  The runner sees the exit and publishes
  terminal `failed` with the evidence.  Ordinary executor path, nothing special.
- **The bridge dies mid-task.**  The submission was already acked, so nobody redelivers
  it, and no terminal event exists.  The bridge keeps an in-flight registry in the
  `runtime-state` KV bucket (key per accepted taskId, written before `submitted`,
  deleted after the terminal publish).  On startup, before consuming, it sweeps: fold
  each registered task's events, and for any non-final one publish terminal `failed`
  (`reason: bridge-died-without-terminal-event`).

The sweep's publish is a compare-and-swap per the profiles spec, not a read-then-write:
expected-last-subject-sequence pinned to the last event the fold saw.  A dying
subprocess's flush racing the sweep wins cleanly, the CAS is rejected, and the sweep
re-reads instead of double-finalizing.  Whichever writer loses lands in the
warn-and-drop path like any other post-final event.

The sweep assumes incarnations are serial.  That assumption is real on this install -
the kubelet restarts the sidecar container in place, and the operator renders the agent
Deployment with strategy `Recreate`, so two bridges never run at once.  It is an
assumption, not a mechanism: an overlapping incarnation could sweep-fail a task its
predecessor is still running.  Real executor fencing belongs to the stage-3 dispatcher,
not to scaffolding with a demolition date.

Honest gaps, accepted for the playground: no queue-staleness guard (the lib's subscribe
path doesn't expose server ingest timestamps, and `queueTimeoutSeconds` is the
dispatcher's job when it exists), no heartbeats on `agents.hb.>`, and a submission whose
events lookup fails transiently is dropped with a log line rather than redelivered (the
lib acks unconditionally after the handler; a nak path is a lib delta if it ever bites).
All retire with the bridge.  Zombie reaping is the pod's problem, not ours - the agent
pod shares its process namespace, so the pause container reaps what group kills orphan.

## Definition of done

A task published to `a2a.tasks.platform.<id>.in` on the W6 install returns the platform
agent's real answer as a `result` artifact.  Cancel works.  The event sequence passes
the lifecycle conformance assertions (9, 10, 12, 13, 14, 15, 18), table-driven like the
W1 suite.
