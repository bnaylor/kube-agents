# AgentProfiles and dispatch, off kanban

- **Author:** [@bnaylor]
- **Date:** 2026-10-22
- **Status:** decided; build tracked in #3654 (#3655 dispatcher, #3656 persona format, #3657
  gateway delegation, #3658 cluster-agent profile, #3659 tiers).
- **Companions:** [`spec-subagent-profiles.md`](spec-subagent-profiles.md) - the `AgentProfile`
  CRD and what the operator renders from it (this spec supersedes its execution model); the
  A2A payload spec ([`spec-a2a-payloads.md`](spec-a2a-payloads.md)) - envelope, subjects,
  authority; the NATS deployment spec ([`spec-nats-deployment.md`](spec-nats-deployment.md)) -
  streams and accounts.

## Purpose

This spec moves delegation off Hermes kanban and onto AgentProfiles, with real skills and
prompts per profile. Kanban is going away entirely. Its execution bandwidth is limited, and
the integration has grown a pile of patches (the kanban-only image patches, the `chat.notify`
stopgap from #2734, and guards such as #2932, #3255 and #3359).

What exists today: the `AgentProfile` CRD and what the operator renders per profile (#2469:
a ServiceAccount, a narrowed callout identity, a published agent card). There is no
dispatcher and no running profile. Delegation is one hop to `platform`, and from there it
runs on kanban.

## Where dispatch lives

Clients address a profile, and the operator runs it. A client may be the gateway's chat
front door, a desktop console client (the Antigravity MCP client), cron or the event watcher.
Server-side execution has to exist anyway, for events, cron and chat users without a desktop
client.

Desktop clients keep their own client-side agent definitions, fully supported, and gain the
**option** of routing to AgentProfile agents. That needs one gateway console verb or an
addressee field on the console frame. The gateway stays the single entry point, which is
where tiers and grants apply.

## What a profile is

A profile is a **persona** (instructions, its own skills, its own configuration) mounted into
a **harness**, with the model chosen through LiteLLM (`harness.model`). Models go through
LiteLLM only (#2922).

**Harness-agnostic.** Nothing may require one harness's specific functionality. Claude Code
is the design center. Antigravity is the alternative and has to keep working.

### Persona format

The persona is built into the profile's `persona.image` and mounted read-only at `/persona`:

```
persona.yaml            apiVersion, name, description, default tier, model hint,
                        tool allowlist, harness hints
instructions.md         the profile's instructions; plain markdown, AGENTS.md-compatible
skills/<name>/SKILL.md  one directory per skill, Agent Skills format
                        (frontmatter name + description, body, optional scripts/)
mcp.yaml                MCP servers the persona needs, by reference to brokered tools;
                        no credentials
```

A thin **adapter** per harness runs at task start, in the worker, and writes that harness's
own configuration into the task's fresh workspace. Claude Code and Antigravity get the first
two adapters. An adapter only maps: anything a harness can't express is dropped with a
warning, never a hard failure, so no persona can depend on one harness's feature.

Each profile's own skills are baked into its persona image. Shared skill packs mount as
plugins (#1726), so fixing a shared skill doesn't mean rebuilding every persona. A small
`persona build` step produces persona images, in CI and for customers' own profiles.

Because the persona is read-only and the harness configuration is regenerated into a fresh
workspace for every task, an agent that edits its own skills or configuration (#3444) changes
nothing that outlives the task.

### Command-line tools

`harness.image` is the worker image, set per profile, so a profile can point at a base image
built on the standard harness. But tools baked into a harness image have to be rebuilt for
every harness. So `AgentProfile` gains a **`tools`** list of OCI images, each mounted
read-only and put on `PATH`, in the same way `AgentPlugin` mounts image volumes. One tools
image then serves every harness, and `harness.image` stays the escape hatch.

```yaml
spec:
  persona:
    image: registry/kube-agents/personas/gitops-helper:1.2
  harness:
    image: registry/kube-agents/harness-claude-code:0.17
    model: claude-sonnet
  tools:
    - image: registry/kube-agents/tools/terraform:1.9
    - image: registry/kube-agents/tools/helm-kustomize:3.16
```

Tools that need credentials (`kubectl` or `gcloud` against the fleet, `gh` against a forge)
get them through the broker and credential proxy, scoped per task and per tier. Credentials
are never in a tools image. Tools that need none (`helm template`, `jq`, `kustomize`) run
locally.

## One profile per role, not per cluster

There is **one generic cluster-agent profile**, and the target cluster is a parameter on each
task. There are no per-cluster CRs, identities or cards. The same design runs 10 clusters and
10,000, with no mode flip on the way. A flip would mean two code paths, and a migration that
lands while a customer is growing fast.

What a one-per-cluster design would have bought, and where it lives instead:

- **Least privilege per cluster:** at task start the credential proxy issues credentials
  scoped to the target cluster only.
- **Per-cluster context and memory:** a store keyed by cluster, which each task reads at
  start. It is data, not objects, so it scales.
- **A different persona or tier for one cluster:** a **pinned profile** with `clusterRef` set
  (already in the CRD), for example a production cluster on a stricter tier. That is a
  handful of CRs, not one per cluster.

## Scale rules

A profile is a template, not one agent. Concurrent instances are bounded by the dispatcher's
cap, so the profile itself never becomes the bottleneck. Two design rules keep the real
limits in check:

- **Breadth by collectors, depth by agents.** A fleet-wide question runs a deterministic
  collector sweep (as #3135 moved the audit streams onto one collector run). Agents start only
  for the few clusters the sweep flags. Fan-out to agents is narrow by design, and the cap is
  a backstop.
- **Correlate before you dispatch.** Unattended triggers dedupe and group across clusters
  first, so one incident gets one investigation, not one per affected cluster. Each trigger
  has its own rate cap.

Very wide work fans out in layers (group coordinators per project or region, each fanning out
to its clusters).

The model-quota ceiling, and replicas or sharding for the gateway, NATS, LiteLLM and the
credential proxy, belong to the sizing workstream. Profiles take their concurrency caps from
its budget function; until it exists, a fixed conservative default behind a knob.

## Replacing kanban's plumbing

1. **Results back to chat.** The gateway relays results from any profile and any number of
   children to the asking conversation. This retires `chat.notify` and the #2734 stopgap.
2. **Progress.** Profile workers publish progress on the task's event stream using the
   `kube-agents.step/v1` format, with a `subtask` kind. Desktop clients and the chat adapters
   both render it.
3. **Fan-out with a wait.** A profile can start several child tasks and wait on their
   results. Width is bounded by the dispatcher's cap.
4. **Stop.** One stop cancels the conversation's whole task tree, enforced by the dispatcher,
   keeping the user-facing behaviour of #3263 and #3379.
5. **No "run after" dependencies.** Kanban's `kanban_link` is dropped on purpose. Fan-out plus
   a wait covers every use we know of. Dependencies get added only when a profile actually
   needs them.

**The parent waits by staying open** (with a time cap) while its children run, rather than
finishing and being woken when they are done. This is the simple path, chosen deliberately.
It gets revisited only if workers held during long fan-outs become a real cost.

## Tiers and identity

- **Two tiers to start:**
  - **Read:** cluster state plus contents and logs. No Secret contents. Writes are proposed
    through GitOps.
  - **Unattended:** the system principal of a trigger (the event watcher, a named cron). It
    reads cluster state plus logs, may only propose (a ledger entry, a pull request, a
    notice), and has a cap per trigger.
- **Tiers are data, not code.** The tier set is defined in a CR, so tiers can be added without
  a release, by us or by customers. A tier names its Kubernetes RBAC set, its GCP role set,
  its write mode (propose or none), its caps, and whether it is interactive or unattended (so
  the budget knows which side of the interactive share a tier falls on). A tier's caps never
  lift the install ceiling.
- **No tier reads Secret contents.** That keeps cluster state apart from secrets with only two
  tiers. A secrets-reading tier would be added explicitly.
- **No direct writes** in this round.
- **The carrier is the #1884 capability grant,** issued at ingress and resolved through the
  verifier. `authority.requester` stays advisory, as the payload spec requires. The verifier
  answers yes or no by design, so the dispatcher **asks** it "does this grant permit profile
  P at tier T?" rather than reading a tier out of the grant.
- **Membership:** which tier a human is in comes from a mapping on the PlatformAgent (groups or
  user ids to tier), defaulting to Read. Unattended is never assignable to a human.

Gaps on main that the build closes (#3659):

- The gateway mints every grant with the install-wide `AuthorityTier`, so the membership
  mapping has to feed the mint.
- The capability library ranks a hard-coded tier list and refuses unknown tiers. That becomes
  explicit per-tier permission sets, since Read and Unattended are not a rank.
- Per-tier Kubernetes ServiceAccounts don't change cloud authority. GCP service accounts come
  from Terraform and fleet reads run as the broker's account, so each tier needs a
  Terraform-provided GCP role set, with the broker choosing by tier.

## How tasks are routed

### Addressing

1. Every profile publishes a card on `a2a.agents.<profile>`. That is how clients discover what
   they can address.
2. A client names a profile and, for cluster work, a target cluster.
3. The gateway resolves the address:
   1. it checks that the requester's grant permits the profile and tier;
   2. for cluster work, a pinned profile whose `clusterRef` matches the target gets the task,
      otherwise the generic cluster agent does;
   3. it checks that the target is in scope (`spec.scope`, including `exclude.clusters`);
   4. it publishes the task on `a2a.tasks.<profile>.<task>.in`, with the target cluster as a
      task parameter rather than a subject token, so there is one queue per profile, not per
      cluster.

### Admission and queueing

- All of a profile's workers share **one durable consumer** on `a2a.tasks.<profile>.*.in`, with
  `max_ack_pending` set to `spec.concurrency`. `max_ack_pending` is per consumer, so it caps
  the profile's total in flight. A durable per worker would deliver every task to every
  worker, because the TASKS stream is limits-based.
- A worker holds a task unacknowledged for the task's whole life (an ack wait longer than
  `lifecycle.activeDeadline`, or periodic in-progress acks), or the cap does not gate. The
  shared client library acknowledges on handler return today, so this is library work.
  Redelivery after a worker dies mid-task is accepted; the library's dedup window covers it.
- A task beyond the cap waits in the queue. The dispatcher fails a task that has waited longer
  than `spec.queueTimeoutSeconds` (default 3600) with a visible notice. It checks the
  message's age itself, because the stream's age limit is stream-wide.
- An install-wide ceiling on the PlatformAgent caps the total across profiles, with a share
  reserved for interactive work (about `max(1, ceil(ceiling / 3))`), so unattended work cannot
  starve users. Caps derive from the sizing budget and must be dynamic, from 10 clusters to
  10,000.
- Each profile's consumer counts against the TASKS consumer budget (#2077), so that formula
  includes profiles.

### Execution

- **Interactive profiles (the cluster agent, chat) run as warm pools:** a small always-on set
  of worker pods per profile, pulling from the profile's durable. The replica count comes from
  the budget. Between tasks each worker gets a fresh workspace and freshly issued scoped
  credentials, so one task's state can't reach the next. A warm pool is a fixed, predictable
  footprint, which also suits Autopilot.
- **The unattended tier runs a Job per task:** it is bursty and latency-insensitive, and each
  run gets a clean pod.
- Profile workloads run in **their own namespace** with their own quota, so they can never take
  `/session` slots (the session quota counts every pod in its namespace). The operator renders
  the profile ServiceAccounts, bus credentials, tier RBAC and NetworkPolicy there.

### Results and stop

- Results and progress go back through the gateway relay to the asking conversation, whichever
  door it came in on.
- Stop on a conversation cancels its task tree: the dispatcher tells warm workers to abandon
  the task (and resets their workspace) or deletes the Job.

## Order of work

1. **Cluster agent** with tools through the broker (#1699). It replaces the per-cluster Hermes
   profiles.
2. **Chat:** the gateway reads its own `chat` AgentProfile.
3. **One unattended investigation profile**, to exercise the unattended tier early.
4. Rewrite `platform`'s instructions and skills off `kanban_*`.

Each step moves its share of delegation off kanban as it lands. The kanban patches and guards
are deleted once nothing files cards. There is no big-bang switch.
