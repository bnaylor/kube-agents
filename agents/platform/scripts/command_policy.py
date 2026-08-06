#!/usr/bin/env python3
"""Read-only enforcement for the two CLIs that reach customer infrastructure.

The credential proxy already refuses commands that disclose or replace a
credential. It has never refused one that changes a cluster: every rule in
policy.json matches a credential pattern, so `kubectl delete ns prod` reaches
the sidecar and runs. What stopped that until now was the Platform Agent's
persona, which is not a permission boundary.

This is the backup layer under Kubernetes impersonation, not the boundary
itself (see the F10 agent permission model). The ordering is what makes an
allowlist acceptable here: under impersonation the API server authorizes as the
requesting user, so a normalizer that misreads an argv loses a redundant check.
The same mistake in a check-then-act design would let the command through
unauthorized.

An allowlist, which is the opposite of the choice GIT_MUTATING_SUBCOMMANDS
makes in credential_proxy.py. The asymmetry differs. Over-blocking git inside a
lease breaks a skill and someone files a bug; under-blocking kubectl against a
customer's production cluster is the thing this model exists to prevent. So git
keeps its denylist and defaults to permitting, and this defaults to refusing.

`git` and `gh` are out of scope on purpose. Writing to the artifact plane is how
the agent is meant to act -- it opens a pull request and CI applies it -- and
the git workspace lease already governs those verbs.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Decision:
    allowed: bool
    rule_id: str
    message: str


_ALLOWED = Decision(allowed=True, rule_id="", message="")

# Only these two reach a cluster or a cloud project. Everything else the proxy
# executes is governed elsewhere.
_GOVERNED_TOOLS = frozenset({"kubectl", "gcloud"})

# Verb sequences that only read. Tuples rather than bare strings so a verb whose
# effect depends on its subcommand can say which subcommand it meant: `rollout
# status` reports on a rollout, `rollout restart` reschedules every pod behind
# a Deployment.
#
# `diff` is absent deliberately. It is non-mutating, but it works by issuing a
# server-side dry-run write, so it needs write RBAC to succeed at all. Allowing
# it under a read-only grant would buy a confusing failure rather than a
# capability.
KUBECTL_READ_VERBS: frozenset[tuple[str, ...]] = frozenset(
    {
        ("api-resources",),
        ("api-versions",),
        ("cluster-info",),
        ("describe",),
        ("events",),
        ("explain",),
        ("get",),
        ("logs",),
        ("top",),
        ("version",),
        ("wait",),
        ("auth", "can-i"),
        ("auth", "whoami"),
        ("config", "current-context"),
        ("config", "get-contexts"),
        ("config", "view"),
        ("rollout", "history"),
        ("rollout", "status"),
    }
)

# kubectl's global options that consume the following argument. This is
# enumerated exhaustively (via `kubectl options` on v1.36.3) rather than
# enumerating unknowns: the next release adds a new flag, and under an allowlist
# that would cause a silent bypass nobody sees. Under this denylist of
# value-taking flags, an unknown flag that takes a value becomes unreadable --
# someone reports it, and we update this set.
_KUBECTL_FLAGS_WITH_VALUE = frozenset(
    {
        "-n", "--namespace", "--context", "--cluster", "--kubeconfig",
        "-s", "--server", "--user", "--token", "--request-timeout",
        "--cache-dir", "--certificate-authority", "--client-certificate",
        "--client-key", "--tls-server-name", "--username", "--password",
        "--kuberc", "--profile", "--profile-output", "--log-flush-frequency",
        "-v", "--v", "--vmodule",
    }
)

# kubectl's boolean global flags. An unrecognized flag (whether boolean or
# value-taking) is treated as making the verb unreadable, which refuses the
# command. This is the inverse of the allowlist for verbs: we enumerate what we
# know, and anything else is assumed to be a hostile or novel flag that could
# hide a verb.
_KUBECTL_BOOLEAN_FLAGS = frozenset(
    {
        "-h", "--help",
        "--insecure-skip-tls-verify", "--disable-compression",
        "--match-server-version", "--warnings-as-errors",
    }
)

# Impersonation belongs to the broker. An agent that supplies its own `--as`
# chooses its own principal, which inverts the model, so these are refused
# before the verb is even read. Checked by exact membership on the flag name
# (before the `=` separator) so `--as=x` and `--as x` are both caught.
_IMPERSONATION_FLAGS = frozenset(
    {"--as", "--as-group", "--as-uid", "--as-user-extra", "--impersonate-service-account"}
)


def _kubectl_verb(argv: list[str]) -> tuple[str, ...] | None:
    """The leading verb sequence in a kubectl argv, or None if unreadable.

    None is a refusal rather than a shrug: an argv whose verb cannot be found
    is an argv whose effect is unknown, and the caller denies on it.

    This function applies the strict unknown-flag rule only to global flags
    (which come before the verb). Command-specific flags (which come after)
    cannot hide the verb, so we stop at the first one rather than skipping
    over it. This avoids false refusals for common commands like `kubectl logs -f`.
    """
    # Phase 1: Skip global flags until we find the first bare word (the verb).
    # Unknown flags are rejected; they could hide the verb.
    index = 1
    while index < len(argv):
        token = argv[index]
        if token.startswith("-"):
            name, separator, _ = token.partition("=")
            # Unknown flags are rejected so that a new kubectl release doesn't
            # silently bypass this gate. A flag we don't recognize could be
            # anything; claim the verb is unreadable.
            if name not in _KUBECTL_FLAGS_WITH_VALUE and name not in _KUBECTL_BOOLEAN_FLAGS:
                return None
            if name in _KUBECTL_FLAGS_WITH_VALUE and not separator:
                index += 1
            index += 1
            continue
        # Found the first bare word (the verb).
        word1 = token
        index += 1
        break
    else:
        # Reached end of argv without finding a bare word.
        return None

    # Phase 2: Look for a second bare word. Once we have the verb, a flag
    # cannot hide it, so stop if the next token is a flag. Do not skip over
    # flags hunting for word two -- that reopens the hole one level down.
    # An unknown command-specific flag that takes a value could otherwise make
    # `rollout --someflag status restart x` read as `("rollout","status")` and
    # allow a restart. Better to false-refuse `rollout --unknown status x` as
    # unreadable by stopping at the flag, rare, and the safe direction.
    if index < len(argv) and not argv[index].startswith("-"):
        word2 = argv[index]
        return (word1, word2)

    return (word1,)


def _refuses_impersonation(argv: list[str]) -> bool:
    for token in argv[1:]:
        name, _, _ = token.partition("=")
        if name in _IMPERSONATION_FLAGS:
            return True
    return False


def evaluate(argv: list[str]) -> Decision:
    """Allow or refuse a command on read-only grounds.

    Never raises. Anything unrecognised is refused.
    """
    if not argv or argv[0] not in _GOVERNED_TOOLS:
        return _ALLOWED

    if _refuses_impersonation(argv):
        return Decision(
            allowed=False,
            rule_id="identity.caller-supplied-impersonation",
            message=(
                "Impersonation is set by the credential proxy, not by the "
                "caller. Remove --as/--as-group/--impersonate-service-account."
            ),
        )

    if argv[0] == "kubectl":
        verb = _kubectl_verb(argv)
        if verb is None:
            return Decision(
                allowed=False,
                rule_id="kubernetes.unreadable-command",
                message="Could not identify a kubectl verb, so the command was refused.",
            )
        if verb in KUBECTL_READ_VERBS or verb[:1] in KUBECTL_READ_VERBS:
            return _ALLOWED
        return Decision(
            allowed=False,
            rule_id="kubernetes.read-only",
            message=(
                "Agents hold read-only access to Kubernetes. Propose this change "
                "as a pull request instead."
            ),
        )

    return _ALLOWED
