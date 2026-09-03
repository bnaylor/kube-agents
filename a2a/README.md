# The A2A stage-1 tree, integrated

This branch (`a2a/something-working`) is the first merge of every stage-1
workstream onto a current `upstream/main`.  It exists so that work building on
A2A - a TUI, direct chat-to-agent routing, anything that needs the whole stack
in one checkout - has a single branch to build from while the upstream PR
series lands piece by piece.  It is a merge, not a rebase; every commit SHA
cited in the stage-1 findings records resolves here.

Read this file first, then the design set in `docs/designs/spec-*.md`
(upstream design of record).  The install bring-up procedure lives in the
sprint workspace repo (`kube-agents-vamp`, `round_2/demo-runbook.md`) and is
deliberately not duplicated here.

## What runs where

| Component | Code | Runs as |
| --- | --- | --- |
| Client library (`a2a-jetstream/0.4`) | `a2a/lib` | imported by everything below |
| `a2a` CLI (topics read/write, probes) | `a2a/cmd/a2a` | in the agent image; the `a2a-topics` skill drives it |
| Chat gateway + Discord adapter | `a2a/gateway`, `a2a/cmd/gateway` | Deployment, operator-rendered under `mode: next` |
| Worker adapter | `a2a/worker-adapter`, `a2a/cmd/worker-adapter` | session pods the gateway spawns per `Delegate:` |
| Hermes bridge | `a2a/hermes-bridge`, `a2a/cmd/hermes-bridge` | sidecar on the agent pod, via `spec.deployment.sidecars` - user-set, `mode: next` installs only |
| Subagent profiles | `a2a/profiles` | config loaded by the worker adapter |
| NATS + streams + users + fences | `k8s-operator/internal/controller/platformagent_a2a_manifests.go` | StatefulSet, provision Job, NetworkPolicies, quota - all operator-rendered under `mode: next` |
| Web rail (read-only observer) | `a2a/web` | dev machine, `kubectl port-forward` to the ws listener; see `a2a/web/README.md` |

The mode switch itself (`spec.mode`, `today`/`next`) is upstream since #1129.
Everything A2A renders only under `next`; a `today` install shows exactly one
visible difference (`KUBEAGENTS_MODE=today` in the managed .env).  The
darkness audit in the sprint workspace (`round_2/darkness-audit.md`) is the
receipt.

## Which seams are stable

Build on these; they have conformance tests or merged spec behind them:

- The 0.4 envelope and the payload rules (`docs/designs/spec-a2a-payloads.md`,
  assertions 1-20 covered in `a2a/lib`'s suite; 21's dispatcher half waits for
  stage 3).
- The gateway's `Adapter` interface (`a2a/gateway/adapter.go`).  Discord is
  the test backend; a new chat backend implements this and nothing else.
- The four provisioned streams and the deny-by-default user grants, including
  the scoped `$JS.ACK.<stream>.>` shape.
- The topic namespace and the `a2a` CLI's read/write contract.

## Which seams are still moving

Anything named in a findings record's "owed" list is not settled.  The ones
most likely to bite a consumer of this branch:

- **Ordered-consumer leaks against `max_consumers`.**  The `tasks/get` half
  is fixed on this branch (`a2a/lib/fold.go` deletes its replay consumer;
  merged 2026-09-02).  The worker-adapter half is not fixed anywhere yet:
  `fetchOrigin` creates a new ordered consumer inside its retry loop, and
  `consumeIn`'s cleanup stops delivery without deleting the consumer.  A
  fresh install caps TASKS at 64 consumers, and those slots are shared with
  the gateway relay and the bridge - the failure presents as a worker unable
  to open its input, not as replay failing.  Escalated to bnaylor; details
  in the S10 findings addendum (2026-09-02).
- The static bus users (`gateway`/`worker`/`web`/`seed`) are the playground
  posture.  The auth callout replaces them at stage 2 and will change how
  credentials reach every component.
- `progress` artifacts are model narration today, not a tool contract
  (recorded deviation in the payload spec).
- Per-consumer-name scoping inside a granted stream is not expressible in
  NATS wildcards (measured); cross-principal `+TERM` within TASKS survives
  until the callout.
- Two low-severity supervisor races are recorded, not fixed: the reap/sweep
  TOCTOU on the terminal publish, and the `Canceled` mark riding the
  end-of-turn KV write.

Details and dispositions live in the `round_2/*-findings.md` records in the
sprint workspace repo, one per workstream.

## Validation on this branch

`go test -race ./...` in `a2a/` and the operator suite in `k8s-operator/` are
both green on the merged tree, and `make docs-check` passes.  The live
environment for end-to-end runs is the `a2a-next-dev` install; the runbook
above brings it up from nothing.
