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

## Structural limitations

This module cannot cover `CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT` environment
variable or impersonation set in the default gcloud configuration file. Both
arrive without appearing in argv and cannot be detected here. The control is
the credential proxy owning `CLOUDSDK_CONFIG` and the process environment.
This is a gate on argv, not the impersonation boundary.
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

# Impersonation and identity-changing flags belong to the broker, not the caller.
# An agent that supplies its own principal chooses its own identity, which inverts
# the model, so these are refused before the verb is even read. Checked by exact
# membership on the flag name (before the `=` separator) so both `--flag=value`
# and `--flag value` forms are caught.
#
# kubectl impersonation flags:
_IMPERSONATION_FLAGS = frozenset(
    {"--as", "--as-group", "--as-uid", "--as-user-extra", "--impersonate-service-account"}
)

# gcloud identity-changing flags (in addition to --impersonate-service-account above):
# - --access-token-file: authenticates with the file's contents instead of the
#   active account (agent controls the filesystem, so this is caller-supplied
#   credentials through a different door)
# - --configuration: names a gcloud config file which can carry impersonate settings
# - --account: selects a different already-credentialed principal
_GCLOUD_IDENTITY_FLAGS = frozenset(
    {"--access-token-file", "--configuration", "--account"}
)

# gcloud's grammar is `gcloud GROUP... VERB [POSITIONAL...]`, so the verb is
# neither first nor last: `gcloud container clusters get-credentials prod` ends
# in a cluster name. Finding it by position would mean encoding gcloud's whole
# command tree, so the allowed paths are listed instead and everything else is
# refused. The list is meant to grow, and growing it should be a reviewable act
# rather than a regex someone widens in a hurry.
GCLOUD_READ_COMMANDS: frozenset[tuple[str, ...]] = frozenset(
    {
        ("auth", "list"),
        ("config", "get"),
        ("config", "list"),
        ("container", "clusters", "describe"),
        ("container", "clusters", "list"),
        # Writes a kubeconfig in the sidecar and nothing in the cloud. It is
        # also how a Cluster Agent points itself at its target cluster, so
        # refusing it would break the read path this module is protecting.
        ("container", "clusters", "get-credentials"),
        ("container", "get-server-config"),
        ("container", "node-pools", "describe"),
        ("container", "node-pools", "list"),
        ("info",),
        ("logging", "read"),
        ("projects", "describe"),
        ("projects", "get-iam-policy"),
        ("projects", "list"),
        ("version",),
    }
)

# gcloud flags that consume the following argument. Without these,
# `gcloud --project my-proj container clusters list` reads `my-proj` as the
# first word of the command path and matches nothing. This is enumerated from
# gcloud help, so new flags in future releases that take values will not be
# recognized and will cause a refusal (fail-closed). An unknown flag means the
# command is unreadable and is refused.
_GCLOUD_FLAGS_WITH_VALUE = frozenset(
    {
        "--project", "--format", "--filter", "--region", "--zone",
        "--location", "--account", "--configuration", "--verbosity",
        "--billing-project", "--sort-by", "--limit", "--trace-token",
        "--flatten", "--access-token-file", "-z", "--page-size",
    }
)

# gcloud boolean global flags that do not consume the following argument.
# These are enumerated from gcloud help and are boolean **at the global parser
# level only**. -v and --version are value-taking in some subcommands like
# `gcloud app` or `gcloud firebase test`, so if those trees are ever added to
# the allowlist, the boolean assumption reopens the hole. An unknown flag is
# still rejected as unreadable, but known boolean flags do not hide the
# command path.
_GCLOUD_BOOLEAN_FLAGS = frozenset(
    {
        "--quiet", "-q", "--version", "-v", "--help", "-h",
    }
)


def _gcloud_has_flags_file(argv: list[str]) -> bool:
    """Check if argv contains --flags-file without reading its contents.

    --flags-file reads flags from a YAML file, so flags in that file (like
    --impersonate-service-account) never appear in argv and cannot be checked
    by _refuses_impersonation. The file itself is under the agent's control,
    so we cannot safely scan it: the agent could rewrite it between our check
    and gcloud's read, a race we cannot win. Refusing it outright is the only
    safe option, consistent with how credential_proxy.py handles kubeconfigs.
    """
    for token in argv[1:]:
        name, _, _ = token.partition("=")
        if name == "--flags-file":
            return True
    return False


def _gcloud_words(argv: list[str]) -> list[str] | None:
    """The bare words of a gcloud argv, with flags and their values removed.

    Returns None if the command is unreadable (e.g., contains an unknown flag
    that could hide the command path). A flag we do not recognize could have
    arbitrary arity; claim the command is unreadable, fail-closed.
    """
    words: list[str] = []
    index = 1
    while index < len(argv):
        token = argv[index]
        if token.startswith("-"):
            name, separator, _ = token.partition("=")
            # Check if it's a known boolean flag (doesn't consume next token).
            if name in _GCLOUD_BOOLEAN_FLAGS:
                index += 1
                continue
            # Check if it's a known flag that consumes a value.
            if name in _GCLOUD_FLAGS_WITH_VALUE:
                # If it has =, the value is in this token. If not, skip next token.
                if not separator:
                    index += 1
                index += 1
                continue
            # Unknown flags are rejected so that a new gcloud release with a
            # flag we don't know the arity of does not silently bypass this
            # gate. The flag could take a value and hide the command path.
            return None
        words.append(token)
        index += 1
    return words


def _gcloud_is_read_only(words: list[str] | None) -> bool:
    """Is the command a listed read-only gcloud command?

    Args:
        words: The result of _gcloud_words(), which may be None if the command
               was unreadable. Returns False if words is None.

    The command path must match exactly a tuple in GCLOUD_READ_COMMANDS.
    Positional arguments after the verb are allowed: `container clusters
    get-credentials my-cluster` matches the path `(container, clusters,
    get-credentials)` and ignores the cluster name.
    """
    if words is None:
        return False
    # A prefix of the words must exactly match a listed command. This allows
    # positional arguments after the command: get-credentials my-cluster matches
    # (container, clusters, get-credentials).
    return any(tuple(words[:length]) in GCLOUD_READ_COMMANDS
               for length in range(1, len(words) + 1))


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

    # Phase 2: Look for a second bare word. Skip flags of known arity (we can
    # correctly consume their values), but stop dead on anything unrecognized.
    # This allows `kubectl rollout -n prod status x` to work (known flag, safe
    # to skip) while refusing `kubectl rollout --unknown status x` (could hide
    # the subcommand). An unknown command-specific flag that takes a value could
    # otherwise make `rollout --someflag status restart x` read as
    # `("rollout","status")` and allow a restart.
    while index < len(argv):
        token = argv[index]
        if token.startswith("-"):
            name, separator, _ = token.partition("=")
            # Stop on unknown flags (arity unknown, could hide the subcommand).
            if name not in _KUBECTL_FLAGS_WITH_VALUE and name not in _KUBECTL_BOOLEAN_FLAGS:
                break
            # Skip known flags, consuming their value if needed.
            if name in _KUBECTL_FLAGS_WITH_VALUE and not separator:
                index += 1
            index += 1
            continue
        # Found a bare word (the subcommand).
        word2 = token
        return (word1, word2)

    return (word1,)


def _refuses_impersonation(argv: list[str]) -> bool:
    for token in argv[1:]:
        name, _, _ = token.partition("=")
        if name in _IMPERSONATION_FLAGS:
            return True
    return False


def _gcloud_refuses_identity_change(argv: list[str]) -> bool:
    """Check if gcloud argv contains identity-changing flags.

    --access-token-file, --configuration, and --account all change the
    identity that gcloud uses, which inverts the model. Checked by exact
    membership on the flag name (before the `=` separator).
    """
    for token in argv[1:]:
        name, _, _ = token.partition("=")
        if name in _GCLOUD_IDENTITY_FLAGS:
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

    if argv[0] == "gcloud":
        if _gcloud_has_flags_file(argv):
            return Decision(
                allowed=False,
                rule_id="gcp.flags-file-forbidden",
                message=(
                    "--flags-file reads from a file under the agent's control. "
                    "We cannot read that file without a race condition, so we refuse "
                    "it outright. Expand flags manually instead of using a file."
                ),
            )

        if _gcloud_refuses_identity_change(argv):
            return Decision(
                allowed=False,
                rule_id="gcp.identity-change-forbidden",
                message=(
                    "Identity belongs to the broker. Remove --access-token-file, "
                    "--configuration, and --account to use the default identity."
                ),
            )

        words = _gcloud_words(argv)
        if words is None:
            return Decision(
                allowed=False,
                rule_id="gcp.unreadable-command",
                message=(
                    "gcloud used a flag whose arity is unknown to this module, so the "
                    "command path cannot be read. Report a new gcloud global flag to "
                    "your infrastructure team."
                ),
            )

        if not _gcloud_is_read_only(words):
            return Decision(
                allowed=False,
                rule_id="gcp.read-only",
                message=(
                    "Agents hold read-only access to Google Cloud. Propose this "
                    "change as a pull request instead."
                ),
            )
        return _ALLOWED

    return _ALLOWED
