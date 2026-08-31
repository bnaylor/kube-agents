# Persona content for the A2A bus

What lives here: the platform agent's `a2a-topics` skill, which is how a
running agent reads and writes the topic blackboard. It sits under `a2a/`
rather than in `agents/platform/skills/` on purpose — everything in this
workspace is dark by construction, and a skill copied into the shipped persona
tree would appear in the agent's skill list on every install, including the
ones where the bus does not exist. "A normal install cannot tell this feature
exists" is the mode switch's promise, and a skill file is the easiest way to
break it.

This directory is the SOURCE the agent image builds from: the Dockerfile
copies `platform/skills/` to `/opt/a2a-template/skills/`, deliberately outside
`/opt/platform-template`, and the entrypoint overlays it into the platform
profile only on a `next` install (below).

## The three pieces, and where each one lives

W5 placed all three by hand to prove the reader; W6.1 landed the product home
for each. Nothing is hand-placed anymore.

| Piece | Playground placement (W5, retired) | Product home (W6.1) |
| ----- | ---------------------------------- | ------------------- |
| The `a2a` client binary | copied to `/opt/data/scripts/a2a` on the data PVC | built from `a2a/cmd/a2a` in the image's `a2a-builder` stage and `COPY`d to `/usr/local/bin/a2a` — the `k8s-event-watcher` pattern. Ungated: a binary is inert until something invokes it, and the thing that invokes it is what ships dark |
| `SKILL.md` | copied to `/opt/data/profiles/platform/skills/a2a-topics/`, and **gone at the next pod start** — see below | shipped at `/opt/a2a-template/skills/`, overlaid into the profile by `docker-entrypoint.sh` step 2.6a-bis only when `runtime_mode.is_next()`. The overlay runs after 2.6a's replace-from-image, which is what makes it stick — and what cleans it off on the first boot after a flip back to `today` |
| `NATS_URL` / `NATS_USER` / `NATS_PASSWORD` | exported into the shell that invoked the agent | rendered by the operator into the agent container's env under `next`, as the `worker` user, from the `<agent>-a2a-nats-creds` Secret the gateway already reads. W7's bridge sidecar shares the pod, so this is one seam, not two |

The skill invokes the client as plain `a2a` — `/usr/local/bin` is on the
agent's rendered `PATH` from a chat turn and from a delegated task alike, so
there is no profile-relative path to spell and nothing for a task-workspace
dispatch to miss.

## The skill copy does not survive a restart, and that is by design

Observed on the venue 8/26 and then found in the code: the entrypoint's step
2.6a (`sync_profile_skills` in `deploy/shared/docker-entrypoint.sh`) rebuilds
each specialist profile's `skills/` from the image into `skills.new` and
renames it over the live directory on **every** pod start. It is a replace, not
a merge — deliberately, so that an image roll cannot leave a profile running
stale skills. A skill that exists only on the PVC is therefore deleted by the
next restart, silently, and the agent simply stops knowing how to read a topic.

So there is no supported way to add a skill to a running agent without going
through the image. That constraint is why the mode-gated overlay exists: it is
not a nicety, it is the only route — and it is also what makes the gate honest,
since the same replace that would delete a hand-copied skill is what removes
the overlaid one the first boot after the mode leaves `next`.

## Nothing to install by hand

On a `next` install the operator and the image do all three placements; a
fresh pod roll is the whole procedure. The old hand-install recipe that used
to live here (kubectl cp of the binary and the skill, env on the exec line)
was the W5 demo path and died with W6.1 — if you find yourself copying
anything onto the PVC to make the reader work, the install is not actually
running `mode: next`, and that is the thing to fix.
