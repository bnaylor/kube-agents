# Persona content for the A2A bus

What lives here: the platform agent's `a2a-topics` skill, which is how a
running agent reads and writes the topic blackboard. It sits under `a2a/`
rather than in `agents/platform/skills/` on purpose — everything in this
workspace is dark by construction, and a skill copied into the shipped persona
tree would appear in the agent's skill list on every install, including the
ones where the bus does not exist. "A normal install cannot tell this feature
exists" is the mode switch's promise, and a skill file is the easiest way to
break it.

## The three pieces, and where each one belongs

The skill needs three things present in the agent pod. Today all three are
placed by hand; each has a product home, and none of the product homes is W5's
to build.

| Piece | Playground placement | Where it belongs |
| ----- | -------------------- | ---------------- |
| The `a2a` client binary | copied to `/opt/data/scripts/a2a` on the data PVC | built into the agent image from `a2a/cmd/a2a`, the way `k8s-event-watcher` is built from `k8s-operator/cmd` — one builder stage and one `COPY`, the pattern is already in `deploy/docker/Dockerfile` |
| `SKILL.md` | copied to `/opt/data/profiles/platform/skills/a2a-topics/`, and **gone at the next pod start** — see below | shipped in the image outside `/opt/platform-template`, overlaid into the profile only when the mode is `next`. The gate reads `KUBEAGENTS_MODE` through `agents/platform/scripts/runtime_mode.py`, which lives on the W6 branch — W5 does not fork it |
| `NATS_URL` / `NATS_USER` / `NATS_PASSWORD` | exported into the shell that invokes the agent | rendered by the operator into the agent Deployment's env under `next`, from the `<agent>-a2a-nats-creds` Secret the gateway already reads. W7's bridge needs the same env in the same pod, so this is one seam, not two |

`/opt/data/scripts` is the right playground home for the binary rather than an
arbitrary path: it is the shared executable directory every profile reaches,
`profiles/<name>/scripts` is a symlink to it, and `profile_scaffold.py` states
outright that executables are not part of a profile template. The skill spells
its path from `$HERMES_HOME` for the reason `github-issue-resolver` does — a
delegated task starts the agent in the task workspace, not the profile
directory.

## The skill copy does not survive a restart, and that is by design

Observed on the venue 8/26 and then found in the code: the entrypoint's step
2.6a (`sync_profile_skills` in `deploy/shared/docker-entrypoint.sh`) rebuilds
each specialist profile's `skills/` from the image into `skills.new` and
renames it over the live directory on **every** pod start. It is a replace, not
a merge — deliberately, so that an image roll cannot leave a profile running
stale skills. A skill that exists only on the PVC is therefore deleted by the
next restart, silently, and the agent simply stops knowing how to read a topic.

The binary is unaffected: `scripts/` is synced additively and keeps what is
already there.

So there is no supported way to add a skill to a running agent without going
through the image. The copy above is good for a demonstration and nothing
longer, and the mode-gated overlay is not a nicety — it is the only route.

## Installing it on a running install

Reading the bus needs a bus user. `worker` is the right role: its grants cover
subscribing to every topic and publishing the ones a worker owns. Its password
is in the `<agent>-a2a-nats-creds` Secret alongside the gateway's.

```bash
ctx=gke_bnaylor-kagents-dev_northamerica-northeast1_a2a-next-dev
ns=kubeagents-system
pod=$(kubectl --context $ctx -n $ns get pods -o name |
        grep platform-agent-gateway | head -1 | cut -d/ -f2)

# 1. the client, cross-compiled for the amd64 nodes
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/a2a ./cmd/a2a
kubectl --context $ctx -n $ns cp /tmp/a2a $pod:/opt/data/scripts/a2a -c platform-agent
kubectl --context $ctx -n $ns exec $pod -c platform-agent -- chmod 0755 /opt/data/scripts/a2a

# 2. the skill
kubectl --context $ctx -n $ns exec $pod -c platform-agent -- \
  mkdir -p /opt/data/profiles/platform/skills/a2a-topics
kubectl --context $ctx -n $ns cp persona/platform/skills/a2a-topics/SKILL.md \
  $pod:/opt/data/profiles/platform/skills/a2a-topics/SKILL.md -c platform-agent
```

Then supply the three environment variables to whatever invokes the agent,
taking `NATS_PASSWORD` from the `worker-password` key of the creds Secret. Do
not write the password onto the data volume: the operator injecting it into the
pod's env is a small change on W6's branch, and it is the answer, so a copy on
the PVC would be a second place to rotate and a first place to leak.
