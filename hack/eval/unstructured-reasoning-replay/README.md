# Replay: a leaked `thought` reply is copied (#3206)

The eval case for #3785. On Gemini, Hermes sometimes stores a reply whose content opens with
`thought`: the model's reasoning written as plain text. When that reply is the latest assistant
turn before a tool result, Gemini copies the format on its next call. #3785's image patch
sends such a reply to the model as the bare marker `[omitted]` instead.

The bench can't make this go red. Every graded turn on its inject path starts with a new user
message, and a new user message breaks the pattern (0 of 3 imitated, unpatched). So the case
is a replay at the model call, which the eval rule's owner accepted for this fix.

This is tied to the Hermes runtime (v2026.9.14's message builders). It goes away when the
Hermes replacement does.

## Files

- `build_fixture.py` builds the request from this repository and the pinned Hermes source:
  the platform agent's system prompt (`agents/platform/SOUL.md` plus Hermes's Gemini guidance),
  the `fleet-consistency-drift` cron prompt, a clean tool call, then a synthetic leaked reply
  with its tool call and the SOP as its result. Nothing comes from a cluster.
- `replay.py` sends it to an install's inference gateway. `--arm main` sends it as built.
  `--arm patched` first runs each message through `replace_unstructured_reasoning` from
  `deploy/docker/patches/unstructured_reasoning.py`, the call the image patch inserts. It exits 1
  unless main is red on every run and patched on none.
- `tests/test_unstructured_reasoning_replay.py` covers both arms' request bodies and the exit
  code without a gateway.

## Running it

On a lane running Gemini (this is a model run: take the lease and announce it):

```
git clone --depth 1 --branch v2026.9.14 https://github.com/NousResearch/hermes-agent /tmp/hermes-agent
python3 -I hack/eval/unstructured-reasoning-replay/build_fixture.py . /tmp/hermes-agent /tmp/urr-fixture.json
kubectl -n kubeagents-system port-forward svc/inference-gateway 8080:80 &
python3 hack/eval/unstructured-reasoning-replay/replay.py /tmp/urr-fixture.json --arm main --runs 3
python3 hack/eval/unstructured-reasoning-replay/replay.py /tmp/urr-fixture.json --arm patched --runs 3
```

## Record

2026-10-23 00:32Z, a next-mode lane on Gemini 3.5 Flash (Vertex) at `main`'s gateway config,
fixture built from this branch and Hermes v2026.9.14:

| arm     | replies opening with `thought` | exit |
| ------- | ------------------------------ | ---- |
| main    | 3 of 3                         | 0    |
| patched | 0 of 3, each a plain tool call | 0    |

No-history controls (the same system prompt and cron prompt with no planted turns) were 0 of 6
when the case was being shaped, so the red comes from the planted reply.
