---
name: a2a-topics
description: >-
  Read the agent blackboard - the durable, named topics on the A2A bus where
  agents publish what they currently know (upgrade readiness, the environment
  blueprint, dated annotations). Use when asked about fleet or environment
  state that another agent may already have assessed, before starting a fresh
  diagnosis. Do not use for task status (that is stream replay, not a topic),
  for anything not in the topics_list tool's answer, or on an install where
  the topics tools are absent.
metadata:
  category: Platform
---

# A2A topics: the standing state other agents left you

A topic is a durable, named place on the bus where one agent publishes what it
currently knows, so the next question starts from standing state instead of a
cold diagnosis. Tasks are conversations; topics are the blackboard.

Two things this skill exists to prevent. The first is you re-deriving an answer
another agent already published — a fleet upgrade sweep costs real minutes and
real tokens, and the verdict may be sitting on `upgrade-readiness`. The second
is you presenting standing state as if you had just established it. Every entry
carries who wrote it and when, and you relay both.

## The mechanism: MCP tools, not shell commands

The reader is the `topics_list` and `topics_read` MCP tools. They run in the
agent pod, next to the bus credential; your shell runs in a sandbox that has
neither. Do not shell out to `a2a` — the sandbox cannot reach the bus, and
that is a design property, not a gap.

If the tools are not available, this install is not running the A2A bus. Say
so plainly and answer the question another way. Do not attempt to guess a bus
address.

## Workflows

### 1. Find out what exists

Call `topics_list`. Topics are provisioned configuration — the set is rendered
by the operator, and nothing you do invents a new one. The list is the
authority on what is readable; a name that is not in it does not exist, and
asking for it is an error rather than an empty answer.

The `CLASS` column matters when you read:

- **state** — the current answer plus a short history. The read returns the
  newest entry. This is what you want almost always.
- **journal** — an append-only record of dated observations. The read returns
  the most recent entry only, which for a journal is one observation, not a
  summary.

### 2. Read a topic

Call `topics_read` with the topic name (`upgrade-readiness`, or scope-qualified
like `shared.blueprint` when a bare name is ambiguous).

The output leads with provenance — who wrote it, when, and under what
correlation id — then the summary, then the structured data.

Three rules for using what comes back:

1. **Relay the provenance.** "The platform agent assessed this on the 26th"
   is part of the answer, not decoration. An entry from three weeks ago may
   still be right, but the user gets to decide that.
2. **Do not launder it.** If the entry says two of three clusters are ready,
   say the entry says that. You did not check the clusters.
3. **"Provisioned but has no entries yet" is a real answer.** Nobody has
   written it. That means "no one has assessed this" — it is not a failure
   and not a reason to retry.

Pass `envelope: true` when you need the raw envelope, including the task id
the write happened under. Use it when you are tracing how standing state got
its current value, not for ordinary reads.

## Writing topics

Not available from this profile right now, deliberately. The shell sandbox
cannot reach the bus, and a write tool is on hold until the authority work
decides which principal a model-driven write acts as — a write recorded under
a shared identity would be worse than no write. Until then, put a finding
worth other agents' attention in your task result, where the caller can see
it. Writers outside the agent pod — provisioning and seed tooling with their
own containers and clients — are unaffected by any of this.

## What this skill is not

- **Not task status.** "What is that task doing?" is answered by replaying the
  task's event stream, not by a topic.
- **Not memory.** Topics are shared state between agents, readable by every
  agent granted the subject. Nothing private goes here.
- **Not a scratchpad.** The topic set is provisioned. If you want a place to
  keep working notes, that is not this.
