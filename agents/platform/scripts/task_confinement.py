#!/usr/bin/env python3
"""Confine a profile worker's kubectl and gcloud to its task's target (#3889).

A profile worker's tools are the credential-proxy shim, so every credentialed
call it makes arrives at the shared broker as an argv. The dispatcher's
``proxyToken`` (a credential-proxy task credential, task_credential.py) names
the one cluster the task was admitted against, and this module holds each argv
to it before the broker runs anything:

  * **kubectl**: every ``--context``, every ``--kubeconfig`` (a context name by
    the time it arrives; the shim resolves the path in its own pod) and the
    request's ``kubeconfigContext`` must name the target. A call naming none of
    them is given the target's context; it never falls through to the host
    cluster the broker's own kubeconfig names. ``--cluster``, ``--server``,
    ``--user``, ``--insecure-skip-tls-verify`` and the other flags that
    redirect a credential or supply one are refused by name here, as well as by
    the session flag allowlist that runs first.
  * **gcloud**: ``--project`` must be the target project, and a call without
    one gets ``--project=<target>`` (never the broker's default project).
    ``--cluster``, ``--location``, ``--region`` and ``--zone``, where present,
    must be the target's. ``container clusters get-credentials`` must name the
    target cluster, location and project exactly, with no "run as given"
    fallback when the target can't be read. ``--impersonate-service-account``,
    ``--account`` and the other identity flags are refused, as are reads with
    no project scope (the PROJECTLESS lists below). A ``projects/<id>/...``
    resource name in any argument must name the target project.
  * **Local-only commands** (``version``, ``--help``, ``gcloud config get`` and
    ``list``, ``gcloud auth list``, ``kubectl version --client``, ``kubectl
    config view``) are allowed with or without a target. A command that
    changes what a later bare command targets is not local-only and is refused
    with or without one: ``gcloud config set``/``unset``/``configurations``, and
    every ``kubectl config`` write (``use-context``, ``set-context``,
    ``set-cluster``, ``set-credentials``, ...).
  * **Injection is unconditional.** Every credentialed kubectl call carries
    ``--context=<target>`` and runs on the target's kubeconfig; every
    credentialed gcloud call carries ``--project=<target>``, and a GKE call
    ``--location`` (and a node-pool call ``--cluster``) for the target. So a
    stored default changed by some path this module missed (a gcloud property,
    a kubeconfig's ``current-context``) still can't redirect a call: an
    explicit flag beats both.
  * **No target** (no valid ``proxyToken``): local-only commands and nothing
    else, run against no kubeconfig at all.

This is a confinement layer over IAM, not a sandbox. What it cannot parse it
lists as a known limit rather than pretending to cover:

  * a resource named by project *number* (``projects/123456789``) is refused,
    not resolved, since the target is a project id;
  * a project, cluster or location named inside a free-text value (a
    ``--filter`` or ``--format`` expression such as
    ``resource.labels.project_id=other``) is not parsed;
  * resource forms that aren't ``projects/...`` paths: a BigQuery
    ``project:dataset.table``, an Artifact Registry
    ``LOCATION-docker.pkg.dev/PROJECT/...`` image, a Pub/Sub short name with
    ``--project`` elsewhere;
  * a zonal read under a regional target (``--zone us-central1-a`` for a
    ``us-central1`` cluster) is refused, since the location must match
    exactly;
  * what a permitted read returns is bounded by the broker identity's IAM, as
    it always was; this narrows which project and cluster a profile names, and
    the IAM grant is still the outer bound.

``storage`` is refused outright for the same reason ``projects list`` is: a
bucket name is global, so a ``gs://`` path names no project to hold it to.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import command_policy
from credential_proxy_client import (
    CONTEXT_FLAG,
    END_OF_FLAGS,
    KUBECONFIG_FLAG,
    ClusterTarget,
    parse_gke_context,
)

# The refusal rule id the broker reports for every confinement refusal, and
# the one for a profile call with no usable task credential.
RULE_TASK_TARGET = "caller.task-target"

# kubectl flags that redirect the credential (to another server or cluster
# entry) or supply one. All are already outside the session flag allowlist,
# which runs first; they are refused here by name as well so the confinement
# does not rest on an allowlist edit someone makes for another reason.
KUBECTL_REDIRECT_FLAGS = frozenset({
    "--cluster", "--server", "-s", "--user", "--insecure-skip-tls-verify",
    "--token", "--username", "--password", "--client-certificate", "--client-key",
    "--certificate-authority", "--tls-server-name", "--kuberc",
    "--as", "--as-group", "--as-uid", "--as-user-extra",
})

# kubectl reads that touch no cluster: the help surface and the client's own
# version. `kubectl version` without --client asks the API server.
KUBECTL_LOCAL_VERBS = frozenset({"help", "options"})
KUBECTL_HELP_FLAGS = frozenset({"--help", "-h"})
# The `kubectl config` verbs that only read the kubeconfig. Every other one
# (use-context, set-context, set-cluster, set-credentials, set, unset,
# delete-*, rename-context, and whatever a later kubectl adds) writes it, and
# so changes what a later command naming no context reaches: refused.
KUBECTL_CONFIG_READ_VERBS = frozenset({"view", "get-contexts", "current-context", "get-clusters", "get-users"})

# gcloud flags that choose the identity a call runs as. command_policy refuses
# them too (_IMPERSONATION_FLAGS, _GCLOUD_IDENTITY_FLAGS); here a profile
# caller has no expected value for any of them, so any presence is refused.
GCLOUD_IDENTITY_FLAGS = frozenset({
    "--impersonate-service-account", "--account", "--configuration",
    "--access-token-file", "--credential-file-override",
    "--authorization-token-file", "--authority-selector",
    # Reads its flags from a file on the broker, where --project or an
    # identity flag would be out of sight.
    "--flags-file",
})

# Scopes above a project. A read naming one has no project to hold to the
# target, so it is refused rather than given --project.
GCLOUD_ORG_SCOPE_FLAGS = frozenset({"--organization", "--folder", "--billing-account"})

# The location and cluster flags a gcloud read may carry. Each value must be
# the target's.
GCLOUD_LOCATION_FLAGS = frozenset({"--location", "--region", "--zone", "-z"})
GCLOUD_CLUSTER_FLAG = "--cluster"
GCLOUD_PROJECT_FLAG = "--project"
GCLOUD_BILLING_PROJECT_FLAG = "--billing-project"
GCLOUD_SCOPE_FLAG = "--scope"

# Ruling (c): reads with no project scope. A surface here is refused whole;
# the local-only exceptions (`auth list`, `config get`) are decided before
# this list is read.
GCLOUD_PROJECTLESS_SURFACES = frozenset({
    "organizations",           # the org itself
    "resource-manager",        # folders, org policies, tags above a project
    "billing",                 # billing accounts are not project resources
    "access-context-manager",  # org-level access policies
    "scc",                     # Security Command Center is org- or folder-scoped
    "identity",                # Cloud Identity groups: the customer's directory
    "domains",
    "storage",                 # bucket names are global; gs:// names no project
    "auth",                    # print-access-token and friends: the broker's credential
    "config",                  # the broker's own configuration
    "info",                    # the broker's account, config and paths
    "components",
    "init",
    "meta",
})
# Ruling (c), command by command, within surfaces that are otherwise
# project-scoped.
GCLOUD_PROJECTLESS_COMMANDS = frozenset({
    ("projects", "list"),
    ("projects", "search"),
})
# `asset` verbs that read a --scope rather than --project: the scope must be
# given and must be the target project.
GCLOUD_ASSET_SCOPED_VERB_PREFIXES = ("search-all-", "analyze-", "query")

# gcloud commands that touch nothing remote (ruling (d)).
GCLOUD_LOCAL_WORDS = frozenset({"help", "topic", "version"})
GCLOUD_LOCAL_COMMANDS = frozenset({("config", "get"), ("config", "get-value"), ("config", "list"), ("auth", "list")})
# The `gcloud config` verbs that only read. Every other one (set, unset,
# configurations activate/create/delete, ...) writes the configuration a later
# bare command reads its project, account, region, zone or cluster from:
# refused, with or without a target.
GCLOUD_CONFIG_READ_VERBS = frozenset({"get", "get-value", "list"})
# The GKE groups whose commands take --location, and the one that takes
# --cluster; injected for the target when absent (see _confine_gcloud).
GCLOUD_CONTAINER_LOCATED_GROUPS = frozenset({"clusters", "node-pools", "operations"})
GCLOUD_CONTAINER_CLUSTER_GROUPS = frozenset({"node-pools"})
GCLOUD_LOCAL_FLAGS = frozenset({"--help", "-h", "--version"})
# Release tracks precede the surface (command_policy._GCLOUD_RELEASE_TRACKS,
# plus preview, which gcloud also ships).
GCLOUD_RELEASE_TRACKS = frozenset({"alpha", "beta", "preview"})

# A `projects/<id>` path segment, and a GKE cluster's full resource name, in
# any argument: a positional, a flag value, a URL. The id stops at the next
# separator a resource name or an argument list can carry.
_EMBEDDED_PROJECT = re.compile(r"(?:^|[^A-Za-z0-9_-])projects/([^/\s,;'\"()\]]+)")
_EMBEDDED_CLUSTER = re.compile(
    r"(?:^|[^A-Za-z0-9_-])projects/[^/\s]+/(?:locations|zones)/([^/\s]+)/clusters/([^/\s,;'\"()\]]+)"
)


class ConfinementRefusal(Exception):
    """The argv names something outside the task's target, or can't be read.

    The message is for the caller and the log: it names the flag or the rule,
    and the target where saying it helps the model correct the call. It never
    quotes the task credential.
    """


@dataclass(frozen=True)
class Confined:
    """What the broker runs for a confined call.

    ``argv`` may carry an injected ``--project``; ``kubeconfig_context`` is the
    context kubectl runs against (the target's when the call named none), and
    None for gcloud. ``isolated`` means no target: run against no kubeconfig.
    """

    argv: list[str]
    kubeconfig_context: str | None
    isolated: bool


def target_from_claims(project: str, location: str, cluster: str) -> ClusterTarget | None:
    """The claims' target as a GKE context triple, or None if it isn't one.

    The token's target fields are bounded printable ASCII; a GKE context name
    is narrower (parse_gke_context). A target that doesn't round-trip through
    that grammar can't be named to kubectl, so the caller treats it as no
    target at all.
    """
    target = ClusterTarget(project=project, location=location, cluster=cluster)
    return target if parse_gke_context(target.context_name) == target else None


# ---------------------------------------------------------------------------
# kubectl
# ---------------------------------------------------------------------------


def _kubectl_tables():
    # The session flag allowlist's tables, which every profile kubectl argv
    # has already passed (session_kubectl_flag_refusal). Imported here, not at
    # module scope, because credential_proxy imports this module.
    import credential_proxy

    return (
        credential_proxy.SESSION_KUBECTL_VALUE_FLAGS,
        credential_proxy.SESSION_KUBECTL_BOOLEAN_FLAGS,
        credential_proxy.SESSION_KUBECTL_SHORTHANDS,
    )


def _kubectl_walk(argv: list[str]) -> tuple[list[tuple[str, str | None]], list[str]]:
    """kubectl's flags (name, value) and bare words, up to `--`.

    pflag's grammar: a value flag takes `=value` or the next token; a
    single-dash token is a cluster of shorthands, each boolean consuming
    nothing and the first value-taking one swallowing the rest of the token or
    the next element. A redirect flag is reported by name wherever it sits.
    An unknown flag is reported with no value: its arity is unknown, and the
    caller refuses it.
    """
    value_flags, _, shorthands = _kubectl_tables()
    flags: list[tuple[str, str | None]] = []
    words: list[str] = []
    index = 1
    while index < len(argv):
        token = argv[index]
        index += 1
        if token == END_OF_FLAGS:
            break
        if token.startswith("--"):
            name, separator, value = token.partition("=")
            if separator:
                flags.append((name, value))
            elif name in value_flags:
                flags.append((name, argv[index] if index < len(argv) else ""))
                index += 1
            else:
                flags.append((name, None))
            continue
        if token.startswith("-") and len(token) > 1:
            cluster = token[1:]
            position = 0
            while position < len(cluster):
                letter = cluster[position]
                long_name = shorthands.get(letter)
                if long_name is None:
                    flags.append(("-" + letter, None))
                    break
                if long_name not in value_flags:
                    flags.append((long_name, None))
                    position += 1
                    continue
                attached = cluster[position + 1:]
                if attached.startswith("="):
                    attached = attached[1:]
                if attached:
                    flags.append((long_name, attached))
                else:
                    flags.append((long_name, argv[index] if index < len(argv) else ""))
                    index += 1
                break
            continue
        words.append(token)
    return flags, words


def _kubectl_is_local(flags: list[tuple[str, str | None]], words: list[str]) -> bool:
    names = {name for name, _ in flags}
    if names & KUBECTL_HELP_FLAGS:
        return True
    if words and words[0] in KUBECTL_LOCAL_VERBS:
        return True
    if words[:1] == ["config"]:
        return len(words) == 1 or words[1] in KUBECTL_CONFIG_READ_VERBS
    if words[:1] == ["version"]:
        return any(name == "--client" and value in (None, "true") for name, value in flags)
    return False


def _confine_kubectl(argv: list[str], kubeconfig_context: str | None, target: ClusterTarget | None) -> Confined:
    flags, words = _kubectl_walk(argv)
    value_flags, boolean_flags, _ = _kubectl_tables()
    for name, _ in flags:
        if name in KUBECTL_REDIRECT_FLAGS:
            raise ConfinementRefusal(
                f"{name} is refused for a profile task: the cluster and the credential are the task's, not the caller's"
            )
        # The session allowlist has refused anything else already; refused
        # here too, so the walk above never steps over a flag whose arity it
        # guessed.
        if name not in value_flags and name not in boolean_flags:
            raise ConfinementRefusal(f"{name} is not a flag the broker can read for a profile task")
    # Before the target check, so it holds with or without one, and before
    # the help check: a write is refused however it is spelled.
    if words[:1] == ["config"] and len(words) > 1 and words[1] not in KUBECTL_CONFIG_READ_VERBS:
        raise ConfinementRefusal(
            f"kubectl config {words[1][:32]} changes the kubeconfig a later command reads, so it is refused "
            "for a profile task; kubectl config view, get-contexts and current-context still run"
        )
    contexts = [value for name, value in flags if name == CONTEXT_FLAG]
    kubeconfigs = [value for name, value in flags if name == KUBECONFIG_FLAG]
    if target is None:
        if not _kubectl_is_local(flags, words):
            raise ConfinementRefusal(
                "this profile task has no target cluster (no valid task credential), so only local-only "
                "commands run: kubectl version --client, kubectl help, --help"
            )
        if contexts or kubeconfigs or kubeconfig_context:
            raise ConfinementRefusal("this profile task has no target cluster, so a command may not name one")
        return Confined(argv=list(argv), kubeconfig_context=None, isolated=True)
    for value in contexts:
        if value != target.context_name:
            raise ConfinementRefusal(
                f"--context must name this task's cluster, {target.context_name}"
            )
    for value in kubeconfigs:
        if value is None or parse_gke_context(value.strip()) != target:
            raise ConfinementRefusal(
                f"--kubeconfig must name this task's cluster, {target.context_name}"
            )
    if kubeconfig_context is not None and parse_gke_context(kubeconfig_context.strip()) != target:
        raise ConfinementRefusal(
            f"the kubeconfig's context must be this task's cluster, {target.context_name}"
        )
    # Naming nothing gets the target: without this the broker resolves the
    # host cluster the operator named for its own kubeconfig.
    context = kubeconfig_context
    if context is None and not kubeconfigs:
        context = target.context_name
    # And the context is named in argv as well, every time: the kubeconfig
    # the broker hands kubectl is the target's, and --context makes kubectl
    # select the target's entry in it whatever its current-context says, so
    # a default changed by a write this module failed to refuse can't move
    # the call.
    confined = list(argv)
    if not contexts:
        confined = [argv[0], f"{CONTEXT_FLAG}={target.context_name}", *argv[1:]]
    return Confined(argv=confined, kubeconfig_context=context, isolated=False)


# ---------------------------------------------------------------------------
# gcloud
# ---------------------------------------------------------------------------


def _gcloud_walk(argv: list[str]) -> tuple[list[tuple[str, str | None]], list[str]]:
    """gcloud's flags (name, value) and positionals, with command_policy's arity.

    The same walk command_policy._gcloud_words_and_flag makes, so a value that
    looks like a flag (`--format --help`) is a value here too. An unknown flag
    is refused rather than stepped over: its arity is unknown, so nothing after
    it can be placed. Everything after `--` is positional.
    """
    flags: list[tuple[str, str | None]] = []
    words: list[str] = []
    index = 1
    while index < len(argv):
        token = argv[index]
        index += 1
        if token == END_OF_FLAGS:
            words.extend(argv[index:])
            break
        if token.startswith("-") and len(token) > 1:
            name, separator, value = token.partition("=")
            if name in GCLOUD_IDENTITY_FLAGS or name in GCLOUD_ORG_SCOPE_FLAGS:
                flags.append((name, value if separator else None))
                continue
            if name in command_policy._GCLOUD_BOOLEAN_FLAGS:
                flags.append((name, None))
                continue
            if name in command_policy._GCLOUD_FLAGS_WITH_VALUE:
                if not separator:
                    value = argv[index] if index < len(argv) else ""
                    index += 1
                flags.append((name, value))
                continue
            raise ConfinementRefusal(
                f"{name} is not a flag the broker can read for a profile task, so the command can't be confined"
            )
        words.append(token)
    return flags, words


def _gcloud_surface_words(words: list[str]) -> list[str]:
    start = 0
    while start < len(words) and words[start] in GCLOUD_RELEASE_TRACKS:
        start += 1
    return words[start:]


def _gcloud_is_local(flags: list[tuple[str, str | None]], words: list[str]) -> bool:
    if any(name in GCLOUD_LOCAL_FLAGS for name, _ in flags):
        return True
    surface = _gcloud_surface_words(words)
    if not surface or surface[0] in GCLOUD_LOCAL_WORDS:
        return True
    return tuple(surface[:2]) in GCLOUD_LOCAL_COMMANDS


def _check_embedded(value: str, target: ClusterTarget, where: str) -> None:
    for project in _EMBEDDED_PROJECT.findall(value):
        if project != target.project:
            raise ConfinementRefusal(
                f"{where} names projects/{project[:64]}; a profile task reaches only projects/{target.project}"
            )
    for location, cluster in _EMBEDDED_CLUSTER.findall(value):
        if location != target.location or cluster != target.cluster:
            raise ConfinementRefusal(
                f"{where} names another cluster; a profile task reaches only {target.context_name}"
            )


def _confine_gcloud(argv: list[str], kubeconfig_context: str | None, target: ClusterTarget | None) -> Confined:
    flags, words = _gcloud_walk(argv)
    names = [name for name, _ in flags]
    surface = _gcloud_surface_words(words)
    # Before the local-only check and the target check: a configuration
    # write is refused with or without a target, and `--help` doesn't make
    # one local (gcloud would print help, but the refusal is cheaper to
    # reason about than the exception).
    if surface[:1] == ["config"] and len(surface) > 1 and surface[1] not in GCLOUD_CONFIG_READ_VERBS:
        raise ConfinementRefusal(
            f"gcloud config {surface[1][:32]} changes the configuration a later command reads its project, "
            "account or location from, so it is refused for a profile task; gcloud config get and list still run"
        )
    local = _gcloud_is_local(flags, words)
    if target is None:
        if not local:
            raise ConfinementRefusal(
                "this profile task has no target project (no valid task credential), so only local-only "
                "commands run: gcloud version, gcloud config get, gcloud auth list, --help"
            )
        if kubeconfig_context:
            raise ConfinementRefusal("this profile task has no target cluster, so a command may not name one")
        return Confined(argv=list(argv), kubeconfig_context=None, isolated=True)
    if kubeconfig_context is not None and parse_gke_context(kubeconfig_context.strip()) != target:
        raise ConfinementRefusal(
            f"the kubeconfig's context must be this task's cluster, {target.context_name}"
        )
    if local:
        return Confined(argv=list(argv), kubeconfig_context=None, isolated=False)

    for name in names:
        if name in GCLOUD_IDENTITY_FLAGS:
            raise ConfinementRefusal(
                f"{name} is refused for a profile task: the identity a call runs as is the broker's, not the caller's"
            )
        if name in GCLOUD_ORG_SCOPE_FLAGS:
            raise ConfinementRefusal(
                f"{name} names a scope above a project; a profile task reaches only project {target.project}"
            )
    if surface and surface[0] in GCLOUD_PROJECTLESS_SURFACES:
        raise ConfinementRefusal(
            f"gcloud {surface[0]} reads outside any one project, and a profile task reaches only project {target.project}"
        )
    if tuple(surface[:2]) in GCLOUD_PROJECTLESS_COMMANDS:
        raise ConfinementRefusal(
            f"gcloud {' '.join(surface[:2])} reads outside any one project, and a profile task reaches only "
            f"project {target.project}"
        )

    projects = [value for name, value in flags if name == GCLOUD_PROJECT_FLAG]
    for value in projects:
        if value != target.project:
            raise ConfinementRefusal(f"--project must be this task's project, {target.project}")
    for value in (value for name, value in flags if name == GCLOUD_BILLING_PROJECT_FLAG):
        if value != target.project:
            raise ConfinementRefusal(f"--billing-project must be this task's project, {target.project}")
    for name, value in flags:
        if name in GCLOUD_LOCATION_FLAGS and value != target.location:
            raise ConfinementRefusal(f"{name} must be this task's location, {target.location}")
        if name == GCLOUD_CLUSTER_FLAG and value != target.cluster:
            raise ConfinementRefusal(f"--cluster must be this task's cluster, {target.cluster}")
    scopes = [value for name, value in flags if name == GCLOUD_SCOPE_FLAG]
    for value in scopes:
        if value != f"projects/{target.project}":
            raise ConfinementRefusal(f"--scope must be this task's project, projects/{target.project}")
    if surface[:1] == ["asset"] and len(surface) > 1 and surface[1].startswith(GCLOUD_ASSET_SCOPED_VERB_PREFIXES) and not scopes:
        raise ConfinementRefusal(
            f"gcloud asset {surface[1]} needs --scope=projects/{target.project} for a profile task"
        )
    if surface[:1] == ["projects"] and len(surface) > 2 and surface[2] != target.project:
        raise ConfinementRefusal(f"gcloud projects names another project; a profile task reaches only {target.project}")
    if surface[:2] == ["container", "clusters"] and len(surface) > 2:
        verb, rest = surface[2], surface[3:]
        if verb == "get-credentials" and len(rest) != 1:
            # The parsed target must be the token's, and there is no "run as
            # given" fallback: a cluster gcloud would default is one this side
            # can't see. (A missing location is the target's, injected below.)
            raise ConfinementRefusal(
                "get-credentials for a profile task must name the cluster: "
                f"gcloud container clusters get-credentials {target.cluster}"
            )
        if verb != "list" and rest and rest[0] != target.cluster:
            _check_embedded(rest[0], target, "the cluster argument")
            if not _EMBEDDED_CLUSTER.search(rest[0]):
                raise ConfinementRefusal(f"the cluster must be this task's cluster, {target.cluster}")
    for word in words:
        _check_embedded(word, target, "an argument")
    for name, value in flags:
        if value:
            _check_embedded(value, target, name)

    # Unconditional: every credentialed call names the target's project,
    # either as the caller gave it (checked equal above) or injected, never
    # the broker's default project (the bootstrap set gcloud's to the host
    # cluster's) or whatever a stored property says. --project is global, so
    # it goes right after the executable; nothing after a `--` is gcloud's.
    confined = list(argv)
    if not projects:
        confined = [argv[0], f"{GCLOUD_PROJECT_FLAG}={target.project}", *argv[1:]]
    # The same for a GKE call's location and a node-pool call's cluster,
    # which gcloud otherwise takes from compute/region, compute/zone and
    # container/cluster. Command flags, so they go after the command, ahead
    # of any `--`.
    command_flags: list[str] = []
    if surface[:1] == ["container"] and surface[1:2] and surface[1] in GCLOUD_CONTAINER_LOCATED_GROUPS:
        if not any(name in GCLOUD_LOCATION_FLAGS for name in names):
            command_flags.append(f"--location={target.location}")
        if surface[1] in GCLOUD_CONTAINER_CLUSTER_GROUPS and GCLOUD_CLUSTER_FLAG not in names:
            command_flags.append(f"{GCLOUD_CLUSTER_FLAG}={target.cluster}")
    if command_flags:
        end = confined.index(END_OF_FLAGS) if END_OF_FLAGS in confined else len(confined)
        confined = [*confined[:end], *command_flags, *confined[end:]]
    return Confined(argv=confined, kubeconfig_context=None, isolated=False)


def confine(argv: list[str], kubeconfig_context: str | None, target: ClusterTarget | None) -> Confined:
    """Hold a profile caller's argv to ``target``, or raise ConfinementRefusal.

    ``target`` is None when the caller presented no valid task credential;
    then only local-only commands pass. ``argv[0]`` is kubectl or gcloud: the
    profile role's executables (ROLE_EXECUTABLES) are checked before this.
    """
    if not argv:
        raise ConfinementRefusal("an empty command")
    if argv[0] == "kubectl":
        return _confine_kubectl(argv, kubeconfig_context, target)
    if argv[0] == "gcloud":
        return _confine_gcloud(argv, kubeconfig_context, target)
    raise ConfinementRefusal(f"{argv[0][:32]} is not an executable a profile task may run")
