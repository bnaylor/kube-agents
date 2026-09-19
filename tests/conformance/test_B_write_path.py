"""Group B -- The write path.

B1  No agent principal holds a credential that can cause a production change.
B2  Assent is human or policy. Agents get veto only.
B3  The enforcement substrate is not agent-authorable.
B4  The executor is a governed principal.
B5  What the approver sees is what will be applied.
B6  No self-approval, and no agent satisfies a required review.

Agents are read-only against clusters and propose changes as pull requests, so
the ceiling this group tests is a ceiling on `kubectl` -- not a ceiling on
impact. An invariant set that only governs API calls governs the wrong API,
which is why most of what follows reads workflows and manifests rather than
argv.
"""

from __future__ import annotations

import re
import sys
import unittest

import yaml

from . import _harness as h
from ._harness import command_policy

_WORKFLOWS = sorted(
    # Both extensions: GitHub Actions accepts .yaml too, and a workflow added
    # as .yaml would otherwise escape every assertion over this set silently.
    (h.REPO_ROOT / ".github" / "workflows").glob("*.y*ml")
)

# A `run:` step reaches the pull request's head without the checkout action.
# `git fetch origin pull/N/head` is GitHub's own documented way to do it by
# hand, and a `ref:` assertion on `uses:` steps never sees it.
_PULL_REQUEST_REF = re.compile(
    # `[^\n]*?` rather than a tighter class: the ref is interpolated, and
    # `${{ github.event.number }}` has spaces in it.
    r"pull/[^\n]*?/(head|merge)\b"
)
# The three spellings of the pull request's head that have turned up in a
# workflow here. They are a backstop rather than the rule: what governs a
# step is the allowlist below, and these three are for the text that carries
# no `${{ ... }}` for that allowlist to read -- a JavaScript property path, a
# ref written into a shell string. Deliberately not extended.
# `github.event.before` and `github.event.pull_request.merge_commit_sha` name
# the same code, `${{ GITHUB.EVENT.AFTER }}` is a second spelling of the third
# alternative because expressions are case-insensitive, and answering each of
# those with one more alternative is the shape the allowlist replaced.
_PULL_REQUEST_HEAD = re.compile(
    r"pull_request\.head|github\.head_ref|github\.event\.after"
)

# Any `${{ ... }}` an interpolated value carries. `re.DOTALL` because `.` is
# otherwise blind to a newline and an expression is allowed to contain one: a
# block scalar keeps `${{ format('{0}',` / `github.event.after) }}` exactly as
# written, newline and all. Without the flag `findall` returned nothing for
# that ref -- so the allowlist loop below never ran, `sub` removed nothing,
# and the whole expression reached the literal scan as text, where
# `github.event.after` contains none of `pull`, `head` or `merge`. One missing
# flag, open in both directions, and the reason the ref half now also refuses
# a `$` left behind in the residue.
_EXPRESSION = re.compile(r"\$\{\{\s*(.+?)\s*\}\}", re.DOTALL)

# An index into a context, which is the other way GitHub spells a property
# access: `github.event.pull_request['head'].sha` and
# `github.event.pull_request.head.sha` are the same expression and only the
# second one looks like it. The subscript has to be a literal string. One
# expression indexed by another is left as it stands, which keeps it off every
# allowlist here, which is the answer this file gives a ref it cannot resolve.
_EXPRESSION_INDEX = re.compile(r"""\[\s*(?:'([^']*)'|"([^"]*)")\s*\]""")

# The expressions a `pull_request_target` checkout ref may name. This is an
# allowlist because the denylist it replaces could not work: it looked for
# `pull_request`, `head` and `merge`, and `${{ github.event.after }}` is the
# pull request's head SHA spelled with none of them. One line in a workflow
# and the whole test went quiet. Listing the safe refs instead means an
# indirection this file cannot resolve -- `env.X`, `steps.X.outputs.Y`, a
# value laundered through `$GITHUB_ENV` -- reads as unsafe rather than as
# innocent text, which is the direction to be wrong in.
#
# One entry, and deliberately: see the test's docstring for what each of the
# three omitted spellings costs, because the three are not one argument.
# `github.ref` and `github.sha` have been synonyms for the entry below since
# GitHub moved this trigger's ref to the default branch on 2025-12-08, so
# leaving them off is a one-spelling rule rather than a safety one.
# `github.base_ref` is the pull request's base branch, which its author picks
# from the branches that already exist here -- a stale unprotected one is not
# the default branch. No fork can write any of the three anyway; that needs
# push access here. So this list is about the author of the pull request
# rather than about the fork, and costs nothing today.
_SAFE_CHECKOUT_EXPRESSIONS = frozenset({"github.event.repository.default_branch"})

# The same treatment for `repository:`, because a ref is resolved *inside* a
# repository and `ref:` alone therefore does not say what gets checked out.
# Absent is the safe case -- the default is the workflow's own repository --
# and `github.repository` is that default spelled out. Why this half refuses a
# literal outright where the `ref:` half tolerates one is argued in the test's
# docstring, which is where the `_SAFE_CHECKOUT_EXPRESSIONS` argument lives
# too; this is the list.
_SAFE_CHECKOUT_REPOSITORIES = frozenset({"github.repository"})

# The expressions any step of a `pull_request_target` workflow may name, in
# its script or anywhere in its `env:`. The refs are the checkout list above,
# because a `git fetch` resolves a ref the same way the action does and there
# is no reason for the two halves to disagree about which refs are safe. The
# rest are not refs at all, and each is here because a carrier in this
# repository names it today. A step authenticates with a token, spelled
# either of the two ways GitHub spells it, and names a remote. A workflow
# that labels or comments on the pull request needs its number, which GitHub
# types as an integer -- the fork chooses which number, not what a number
# is, so it cannot be a ref, a path or a command. `inputs.dry_run` is a
# `workflow_dispatch` boolean, which only somebody who can already push here
# is able to set, and is not the pull request's at all.
#
# The pull request's number is a concession and should be read as one. It is
# the only entry here that is not independent of the pull request -- it
# identifies it -- and with it and the token in the same step, the head is one
# `gh api repos/O/R/pulls/$N --jq .head.sha` away. Measured: that step was
# refused before this entry existed and is not refused now, and so is `gh pr
# checkout "$PR_NUMBER"`. Both are in the test's "Not covered" list with the
# measurement. The concession is made because two carriers label and comment
# with the number and there is no way to tell that use from the other one
# without reading the program, which is the thing this file declines to do;
# and because the alternative on offer is a denylist over `gh pr`, which
# `auto-assign-milestone.yml` would be the first to trip on with `gh pr edit`.
# What the number does *not* open is the direct path: `pull/N/head` built out
# of it is refused by `_PULL_REQUEST_REF` over every step, whatever is on this
# list.
#
# Anything else -- another event field, a `steps.X.outputs.Y`, an `env.X`
# that did not resolve -- reads as unsafe, which is the direction the ref
# half is wrong in and for the same reason.
_SAFE_SCRIPT_EXPRESSIONS = _SAFE_CHECKOUT_EXPRESSIONS | {
    "github.repository",
    "github.token",
    "secrets.github_token",
    "github.event.pull_request.number",
    "inputs.dry_run",
}

# The JavaScript half of the same rule. `actions/github-script` hands its
# script the whole webhook payload as `context`, so `context.payload.after` is
# `${{ github.event.after }}` written in a language `_EXPRESSION` cannot see
# and `_SAFE_SCRIPT_EXPRESSIONS` therefore never reads. The accessor is
# optional in this pattern on purpose: a bare `context` is the payload too,
# and a script that serialises it whole and picks a field out of the result
# names no property anywhere a regex could find one.
_SCRIPT_CONTEXT = re.compile(
    r"""\bcontext\b(?:\s*\.\s*(\w+)|\s*\[\s*['"](\w+)['"]\s*\])?"""
)
# The third language the payload is written in. `GITHUB_EVENT_PATH` holds the
# whole webhook event as a file on disk, so a script can read the head SHA out
# of it without naming an expression for `_SAFE_SCRIPT_EXPRESSIONS` to read or
# a `context` property for the rule below -- `jq -r .after "$GITHUB_EVENT_PATH"`
# in a shell, `JSON.parse(fs.readFileSync(process.env.GITHUB_EVENT_PATH))` in a
# `script:` input. Both were live and green against the allowlists above, which
# is the same lesson a third time: the rule is about reaching the payload, and
# the payload has more spellings than any one of them covers.
_EVENT_PAYLOAD_FILE = re.compile(r"GITHUB_EVENT_PATH")

# `context.repo` is `{owner, repo}` for the repository the workflow lives in,
# which is `github.repository` from the list above in the other language.
# `context.payload`, `context.sha` and `context.ref` are all the pull request
# on this trigger, and are not on it.
_SAFE_SCRIPT_CONTEXTS = frozenset({"repo"})

# Enough passes to settle any chain a workflow would plausibly write. Both
# fixed-point loops that read it -- `_expand_env` and the `run:` half's
# pickup -- are capped by this rather than run to exhaustion, so a
# self-referential pair cannot spin.
_ENV_EXPANSION_LIMIT = 10

#: Every scope a `permissions:` block can name, so that `write-all` expands to
#: what GitHub means by it. Only `contents` and `id-token` are read today; a
#: shorthand expanded over a partial list would be a quieter way to be wrong
#: than not expanding it at all.
_PERMISSION_SCOPES = frozenset({
    "actions", "attestations", "checks", "contents", "deployments",
    "discussions", "id-token", "issues", "models", "packages", "pages",
    "pull-requests", "repository-projects", "security-events", "statuses",
})
_PERMISSION_SHORTHANDS = {"write-all": "write", "read-all": "read"}


def _permission_scopes(document):
    """Every `permissions:` block in a workflow, as {scope: level} mappings.

    Workflow level and each job's, because GitHub honours either and a job
    inherits the workflow's when it declares none.

    The shorthand is why this is a function rather than a list comprehension
    at each call site. `permissions:` usually takes a mapping, but it also
    takes the bare strings `write-all` and `read-all`, and `write-all` is the
    widest grant a workflow can make -- every scope, `contents` and `id-token`
    among them. Every call site filtered on `isinstance(scope, dict)`, so the
    string fell through the filter and a `pull_request_target` workflow with
    `permissions: write-all` at the top satisfied every assertion about its
    token. Job-level `write-all` was caught, but by `test_B2_...`, which is a
    different test asking a different question -- a neighbour's red is not
    this assertion working. The one site that wants a single job's effective
    grant rather than every block in the file uses `_effective_permissions`;
    the shorthand is the same there, the inheritance rule is not.

    An unknown string yields no grant, which is how `permissions: {}` reads;
    the ones that matter are the two GitHub documents.
    """
    blocks = [document.get("permissions")] + [
        (job or {}).get("permissions")
        for job in (document.get("jobs") or {}).values()
    ]
    for block in blocks:
        yield _permission_mapping(block)


def _permission_mapping(block):
    """One `permissions:` block as a {scope: level} mapping.

    A mapping is itself, the two shorthands expand, and anything else -- a
    missing block, or a string GitHub does not document -- is no grant.
    """
    if isinstance(block, dict):
        return block
    if isinstance(block, str) and block.strip().lower() in _PERMISSION_SHORTHANDS:
        level = _PERMISSION_SHORTHANDS[block.strip().lower()]
        return {scope: level for scope in _PERMISSION_SCOPES}
    return {}


def _effective_permissions(document, job):
    """What a job's token actually carries.

    `_permission_scopes` reads every block in the file and cannot answer this:
    a job inherits the workflow's block only when it declares none of its own,
    and a caller asking "is this job a deploy" needs the one that applies to
    it rather than the union of all of them.
    """
    block = (job or {}).get("permissions")
    if block is None:
        block = document.get("permissions")
    return _permission_mapping(block)


def _env_values(block):
    """The `env:` values of a workflow, job or step, as {NAME: text}.

    Every level is consulted because GitHub merges them, and a ref laundered
    through any one of them is the same ref.
    """
    env = (block or {}).get("env") or {}
    if not isinstance(env, dict):
        return {}
    return {str(k): str(v) for k, v in env.items()}


def _normalise_expression(expression):
    """One expression in the spelling the allowlists are written in.

    Membership of a set of strings answers a question about how an expression
    is spelled, and the allowlists here are asking what it names. GitHub is
    case-insensitive about both the context and the property -- `${{
    GITHUB.EVENT.AFTER }}` is `${{ github.event.after }}` -- and it reads an
    index into a context as the property access it is, so
    `github.event.pull_request['head'].sha` is the head SHA wearing another
    expression's clothes. Both reached the `run:` half as unrecognised text
    and were read as innocent.

    So an index with a literal subscript becomes a property, whitespace around
    a dot goes (the expression parser allows it), and the result is folded to
    lower case. What is still unrecognised after that stays unrecognised,
    which against an allowlist means refused.
    """
    expression = _EXPRESSION_INDEX.sub(
        lambda match: "." + (match.group(1) or match.group(2) or ""), expression
    )
    return re.sub(r"\s*\.\s*", ".", expression).strip().lower()


def _expand_env(text, env):
    """`${{ env.NAME }}` replaced by NAME's value, to a fixed point.

    Iterated rather than single-pass because one `env:` value may name
    another, and a single pass resolves such a chain only when the mapping
    happens to be ordered favourably. The same two declarations written in
    the other order would leave the ref half-expanded and looking innocent,
    which is a difference GitHub does not make. The cap stops a
    self-referential pair from spinning; text that has not settled by then
    keeps its `${{ ... }}`, and the caller refuses what it cannot resolve.

    The match is case-insensitive in both halves, which is what GitHub does:
    `${{ env.REV }}`, `${{ env.rev }}` and `${{ Env.REV }}` are one reference
    to one value. A case-sensitive substitution left the third spelling
    standing, where it was neither resolved here nor picked up by the caller
    as a shell variable, because it is not one. The `run:` half's own pickup
    is case-*sensitive* and stays that way for the opposite reason: `$rev` is
    not `$REV` to a shell, so folding in a value the script cannot be reading
    would be a false red rather than a catch.

    The substitution is total: an unknown name is left alone rather than
    blanked, so a miss cannot quietly turn a suspicious ref into an innocent
    one. The replacement goes through a lambda because `re.sub` reads a string
    replacement as a template, and it parses that template whether or not the
    pattern matches. A value holding a backslash -- a Windows path, a `sed`
    snippet -- would otherwise raise for every ref in scope of the `env:` that
    declared it, including the refs that never mention it.
    """
    for _ in range(_ENV_EXPANSION_LIMIT):
        before = text
        for name, value in env.items():
            text = re.sub(
                r"\$\{\{\s*env\." + re.escape(name) + r"\s*\}\}",
                lambda _match, replacement=value: replacement,
                text,
                flags=re.IGNORECASE,
            )
        if text == before:
            break
    return text


def _with_inputs(step, name):
    """A step's `with:` values under `name`, matched case-insensitively.

    Case matters here and it is not obvious. The runner hands an input to an
    action as `INPUT_<NAME>`, upper-casing the key, and `core.getInput("ref")`
    looks it up by upper-casing too -- so `Ref:` and `REF:` reach
    `actions/checkout` as the ref it checks out. A `with.get("ref")` reads
    none of them. This is the `uses:` bug one field along: that filter was
    case-sensitive too, `Actions/checkout` walked past it, and the mutation
    row that found it is still in the harness. A list rather than a value
    because both spellings can be present at once, and the rule has to hold
    for each.

    A `with:` that is not a mapping -- `with: ${{ fromJSON(env.CONFIG) }}` --
    is returned whole rather than skipped, so it reaches the allowlist and is
    refused as the unresolvable thing it is. Returning nothing there would
    fail open, which is the one direction this test does not go.

    A null value is `""` and not `"None"`. `ref:` with nothing after it is
    valid YAML and parses to `None`, and `str(None)` is a four-character
    truthy string that satisfies `any(refs)`, carries no expression for the
    allowlist to check, and contains none of `pull`, `head` or `merge`. So a
    bare `ref:` -- which is a checkout with no ref, the case the caller
    refuses `actions/checkout` for -- read as an ordinary literal ref and
    passed, while the honest spelling `ref: ""` reddened. That is the
    fail-open direction reached by the shortest possible diff.
    """
    inputs = (step or {}).get("with")
    if inputs is None:
        return []
    if not isinstance(inputs, dict):
        return [str(inputs)]
    return [
        "" if value is None else str(value)
        for key, value in inputs.items()
        if str(key).lower() == name
    ]


def _step_scripts(step):
    """Everything in a step that is a script, as one text.

    `run:` is the obvious one and it is not the only one.
    `actions/github-script` takes JavaScript as its `script:` input and runs
    it in the job, with the same token and the same working directory, so
    `await exec.exec('git', ['fetch', 'origin', head])` written there is the
    identical hazard in a different language -- and a scan of `run:` alone
    reads none of it.

    The rule is over the shape rather than over the action: every `with:`
    value that is a string is folded in. Naming `actions/github-script` would
    make this a rule about that vendor, which is the objection this test
    already makes to a rule about `actions/checkout`, and there are several
    actions that run a script they are handed.

    "Every string" was "every string spanning more than one line", on the
    reasoning that `ref:`, `python-version:` and `fetch-depth:` are one line
    each and nobody writes a multi-line value that is not a program. That is a
    guess about formatting rather than a fact about the value, and it was
    wrong in both directions a program can be written: a `script:` short
    enough to fit on one line is still a program, and a `>-` folded scalar is
    a program that looks multi-line in the file and arrives here as a single
    line, because folding it is what the scalar means. Both walked past
    everything downstream of this function. Reading every string costs those
    scans an ordinary input or two, and what they look for -- a fetch verb, an
    expression that is not on an allowlist, a `context` reference -- is not
    what a version number says.

    A `with:` that is not a mapping is an expression standing in for the whole
    block, and is folded in whole rather than skipped, so that the scans see
    it rather than nothing.
    """
    scripts = [str((step or {}).get("run", ""))]
    inputs = (step or {}).get("with")
    if isinstance(inputs, dict):
        scripts += [value for value in inputs.values() if isinstance(value, str)]
    elif inputs is not None:
        scripts.append(str(inputs))
    return "\n".join(scripts)


def _workflows():
    """The workflow set, which is never legitimately empty.

    Four of the five assertions reading this glob answer for an empty set on
    their own: two compare it against a named allowlist and go red when the
    expected names go missing, and the `workflow_run` deploy gate and the
    `pull_request_target` checkout test each carry a non-empty precondition
    over their own filtered subset. The fifth -- B2's "no workflow approves or
    merges a pull request" -- asserts an absence, and an absence is true of
    the empty set.

    That one is not defenceless. Moving `.github/workflows` reds tests in this
    file and elsewhere: C4's SHA-pin sweep keeps its own copy of this glob and
    guards it, and `autopush-deploy.yml` is a registered `_harness.SOURCES`
    entry, so the harness self-check goes red too. But what B2 inherits from
    that is an answer to somebody else's question, and a count of neighbours
    is not a thing to depend on. Three counts in this docstring have been
    wrong and corrected during review -- the last of them by a change in this
    same pull request, which added a precondition and so moved the number it
    had just been corrected to. That is the argument against writing a fourth,
    so this paragraph names the mechanisms and leaves the counting to whoever
    runs it. This is B2 answering its own question, in the place the set is
    built, for the same reason `_harness.text()` raises rather than returning
    an empty string.

    The name is private by convention only -- all five consumers live in this
    module and nothing stops them reading `_WORKFLOWS` directly. What the
    underscore buys is that `_workflows()` reads as the intended route, so a
    new assertion reaching past it looks wrong to a reviewer.
    """
    if not _WORKFLOWS:
        raise AssertionError(
            f"no workflows matched {h.REPO_ROOT / '.github' / 'workflows'}/*.y*ml; "
            "the glob is wrong"
        )
    return _WORKFLOWS


def _workflow_documents():
    """Every workflow, parsed, with YAML 1.1's `on:` -> True quirk normalised."""
    for path in _workflows():
        document = yaml.safe_load(path.read_text())
        if True in document:  # `on:` is the YAML 1.1 boolean `y`/`yes`/`on`
            document["on"] = document.pop(True)
        yield path, document


class B1NoAgentCredentialCausesAProductionChange(unittest.TestCase):
    """B1: not "cannot mutate a cluster" -- cannot cause a change, by any route."""

    def test_B1_kubectl_write_verbs_are_refused(self) -> None:
        """The headline question, in the form a reviewer asks it.

        Read-only is an allowlist rather than a denylist here, deliberately:
        over-blocking kubectl breaks a skill and someone files a bug, while
        under-blocking it against a customer's production cluster is the thing
        the model exists to prevent. These are the verbs a denylist author
        would have had to think of, and the point is that they are refused
        without anyone having thought of them.
        """
        writes = (
            ["kubectl", "delete", "namespace", "prod"],
            ["kubectl", "delete", "pod", "web-0"],
            ["kubectl", "apply", "-f", "manifest.yaml"],
            ["kubectl", "create", "deployment", "web", "--image=nginx"],
            ["kubectl", "patch", "deployment", "web", "-p", "{}"],
            ["kubectl", "replace", "-f", "manifest.yaml"],
            ["kubectl", "edit", "deployment", "web"],
            ["kubectl", "scale", "deployment", "web", "--replicas=0"],
            ["kubectl", "annotate", "pod", "web-0", "a=b"],
            ["kubectl", "label", "pod", "web-0", "a=b"],
            ["kubectl", "set", "image", "deployment/web", "web=nginx:2"],
            ["kubectl", "rollout", "restart", "deployment/web"],
            ["kubectl", "rollout", "undo", "deployment/web"],
            ["kubectl", "drain", "node-1"],
            ["kubectl", "cordon", "node-1"],
            ["kubectl", "uncordon", "node-1"],
            ["kubectl", "taint", "nodes", "node-1", "k=v:NoSchedule"],
            ["kubectl", "exec", "web-0", "--", "sh"],
            ["kubectl", "cp", "web-0:/etc/passwd", "/tmp/p"],
            ["kubectl", "port-forward", "web-0", "8080:80"],
            ["kubectl", "attach", "web-0"],
            ["kubectl", "proxy"],
            ["kubectl", "run", "shell", "--image=busybox"],
            ["kubectl", "debug", "web-0", "--image=busybox"],
            ["kubectl", "certificate", "approve", "csr-1"],
        )
        for argv in writes:
            with self.subTest(argv=argv):
                decision = command_policy.evaluate(argv)
                self.assertFalse(decision.allowed, f"{argv} reached the cluster")

    def test_B1_ordinary_reads_still_work(self) -> None:
        """A read-only gate nobody can work behind gets switched off.

        `CREDENTIAL_PROXY_ENFORCE_READ_ONLY` is global, unscoped and has no
        expiry, so the cost of a false refusal is not one failed command -- it
        is an operator disabling the whole posture to get through the day. The
        allowlist's coverage is therefore part of the control.
        """
        reads = (
            ["kubectl", "get", "pods", "-n", "prod"],
            ["kubectl", "describe", "node", "node-1"],
            ["kubectl", "logs", "web-0", "-f"],
            ["kubectl", "top", "pods"],
            ["kubectl", "events", "--for", "pod/web-0"],
            ["kubectl", "auth", "can-i", "delete", "pods"],
            ["kubectl", "rollout", "status", "deployment/web"],
            ["kubectl", "rollout", "-n", "prod", "history", "deployment/web"],
            ["kubectl", "api-resources"],
            ["kubectl", "explain", "pod.spec"],
            ["kubectl", "config", "current-context"],
            ["gcloud", "container", "clusters", "list"],
            ["gcloud", "--project", "p", "container", "clusters", "describe", "c"],
            ["gcloud", "container", "clusters", "get-credentials", "c"],
            ["gcloud", "logging", "read", "resource.type=k8s_cluster"],
            ["gcloud", "projects", "get-iam-policy", "p"],
        )
        for argv in reads:
            with self.subTest(argv=argv):
                decision = command_policy.evaluate(argv)
                self.assertTrue(decision.allowed, f"{argv}: {decision.message}")

    def test_B1_gcloud_write_commands_are_refused(self) -> None:
        """gcloud's grammar puts the verb neither first nor last.

        `gcloud container clusters get-credentials prod` ends in a cluster
        name, so finding the verb by position would mean encoding gcloud's
        whole command tree. The allowlist of read paths is what makes these
        refusals fall out rather than needing to be enumerated.
        """
        writes = (
            ["gcloud", "container", "clusters", "delete", "prod"],
            ["gcloud", "container", "clusters", "create", "prod"],
            ["gcloud", "container", "clusters", "update", "prod", "--enable-autoscaling"],
            ["gcloud", "projects", "add-iam-policy-binding", "p", "--member=user:x"],
            ["gcloud", "projects", "set-iam-policy", "p", "policy.json"],
            ["gcloud", "iam", "service-accounts", "keys", "create", "k.json"],
            ["gcloud", "compute", "instances", "delete", "vm-1"],
            ["gcloud", "container", "node-pools", "delete", "np-1"],
            ["gcloud", "secrets", "versions", "access", "latest", "--secret=s"],
        )
        for argv in writes:
            with self.subTest(argv=argv):
                decision = command_policy.evaluate(argv)
                self.assertFalse(decision.allowed, f"{argv} reached the project")

    def test_B1_the_sandbox_image_ships_no_credentialed_cli(self) -> None:
        """The build gate, asserted against the stage graph rather than a grep.

        A gate in a stage the agent image does not derive from is not a gate,
        and the `credential-proxy` stage deliberately reinstalls all four CLIs
        afterwards -- so "the Dockerfile contains this RUN" is not the
        assertion. This walks `FROM` back from the agent target and requires
        the gate on that path.
        """
        source = h.text("dockerfile")

        # Instruction keywords are matched case-sensitively and continuation
        # lines are skipped, because neither is optional here: `from
        # gateway.kanban_handoff_clip import …`, inside a multi-line RUN that
        # patches a plugin, reads as a stage boundary under the obvious
        # case-insensitive regex and silently splits agent-base in two. The
        # first draft of this test passed for that reason.
        stages: dict[str, str] = {}
        parents: dict[str, str] = {}
        current = None
        continued = False
        for line in source.splitlines():
            match = None if continued else re.match(r"^FROM\s+(\S+)(?:\s+AS\s+(\S+))?", line)
            continued = line.rstrip().endswith("\\")
            if match:
                parent, name = match.group(1), match.group(2)
                current = name or parent
                stages[current] = ""
                parents[current] = parent
                continue
            if current is not None:
                stages[current] += line + "\n"

        self.assertIn("platform", stages, "the agent build target is gone")

        lineage, cursor = [], "platform"
        while cursor in stages:
            lineage.append(cursor)
            parent = parents[cursor]
            if parent == cursor or parent not in stages:
                break
            cursor = parent

        # Reworded by #913, which moved the real CLIs into deploy/sandbox/: the
        # guard that matters is still the agent image refusing to carry them.
        gate = "unexpected cluster CLI in the agent image"
        gated = [stage for stage in lineage if gate in stages[stage]]
        self.assertTrue(
            gated,
            f"no stage on the agent image's lineage {lineage} asserts the "
            f"absence of credentialed CLIs",
        )
        # The gate has to cover all four, not just the one someone remembered.
        gate_stage = stages[gated[0]]
        for binary in ("gcloud", "kubectl", "gh", "git"):
            with self.subTest(binary=binary):
                self.assertRegex(
                    gate_stage,
                    rf"for binary in[^\n]*\b{binary}\b",
                    f"the build gate does not check for {binary}",
                )

    def test_B1_the_shipped_denylist_refuses_credential_disclosure(self) -> None:
        """Read out of the rendered ConfigMap, not out of the Go constant.

        The constant is what someone wrote; the ConfigMap is what the sidecar
        loads. These are the commands that hand the credential to the caller
        rather than using it, which is the one thing the denylist has always
        been for.
        """
        disclosures = (
            ["gcloud", "auth", "print-access-token"],
            ["gcloud", "auth", "print-identity-token"],
            ["gcloud", "config", "config-helper"],
            ["gh", "auth", "token"],
            ["gh", "auth", "status", "--show-token"],
            ["kubectl", "create", "token", "default"],
            ["kubectl", "config", "view", "--raw"],
            ["git", "credential", "fill"],
            ["gcloud", "auth", "login"],
            ["gcloud", "auth", "activate-service-account", "--key-file=k.json"],
            ["gh", "auth", "login"],
            ["gh", "auth", "refresh"],
            ["gcloud", "components", "install", "alpha"],
            ["gh", "extension", "install", "owner/repo"],
        )
        for argv in disclosures:
            with self.subTest(argv=argv):
                self.assertIsNotNone(
                    h.policy_blocks(argv),
                    f"{argv} is not matched by any shipped denylist rule",
                )

    def test_B1_precondition_the_denylist_governs_gh(self) -> None:
        """Guards the expected-failure below: `gh` rules must still exist."""
        rule_ids = {rule["id"] for rule in h.rendered_policy_rules()}
        self.assertIn("github.credential-replacement", rule_ids)
        self.assertIn("gh", h.credential_proxy.CommandExecutor.ALLOWED_EXECUTABLES)

    def test_B1_the_denylist_refuses_merge_and_approve(self) -> None:
        """The half of B1 that holds today, pinned so it cannot quietly stop.

        github.merge and github.assent ship in the rendered denylist and are
        the only thing standing between the agent's credential and completing
        its own pull request. The first spelling of this file recorded all of
        B1 as one known violation whose cause said "every denial rule matches
        only gh auth or gh extension" -- true when written, closed since --
        and expectedFailure absorbs any subset, so the two live rules were
        covered by nothing: deleting either from the shipped policy left the
        whole suite green.
        """
        for argv in (
            ["gh", "pr", "merge", "1", "--squash"],
            ["gh", "pr", "review", "1", "--approve"],
            ["gh", "pr", "merge", "--auto", "1"],
        ):
            with self.subTest(argv=argv):
                self.assertIsNotNone(
                    h.policy_blocks(argv),
                    f"{argv} is permitted by the shipped denylist",
                )

    @h.known_violation("B1", "04_major_requirements.md B1")
    def test_B1_the_agent_cannot_merge_or_approve(self) -> None:
        """KNOWN VIOLATION, narrowed to what still holds: force-push.

        The merge and approve halves closed -- github.merge and github.assent
        ship in the denylist now, pinned by the passing test above. What
        remains open is `git push --force`: the denylist has no rule for it
        (the git verb and lease machinery in the broker constrain *where* git
        may write, not this force flag), so the credential that can rewrite a
        branch a GitOps Application watches is still a production-change
        credential.

        B1 is not "cannot mutate a cluster", it is "cannot cause a production
        change". Rewriting a watched branch is causing one.
        """
        for argv in (
            ["git", "push", "--force", "origin", "main"],
        ):
            with self.subTest(argv=argv):
                self.assertIsNotNone(
                    h.policy_blocks(argv),
                    f"{argv} is permitted by the shipped denylist",
                )


class B2AssentIsHumanOrPolicy(unittest.TestCase):
    """B2: gatekeepers may block and may never approve."""

    def test_B2_no_workflow_approves_or_merges_a_pull_request(self) -> None:
        """The structural form of "no model verdict causes a merge".

        A veto is monotone in the safe direction -- successful injection
        against one produces a false block, a nuisance on one pull request
        rather than a breach. That property only holds while nothing in CI can
        assent, so this refuses the mechanisms rather than the intent: the
        merge and approve calls, and GitHub's auto-merge.
        """
        assenting = (
            r"gh\s+pr\s+merge",
            r"gh\s+pr\s+review[^\n]*--approve",
            r"enablePullRequestAutoMerge",
            r"pulls/\$?\{?[^\n]*\}?/reviews",
            r"peter-evans/enable-pull-request-automerge",
            r"pascalgn/automerge-action",
            r"hmarr/auto-approve-action",
        )
        offences = []
        for path in _workflows():
            text = path.read_text()
            for pattern in assenting:
                for match in re.finditer(pattern, text):
                    line = text[: match.start()].count("\n") + 1
                    offences.append(f"{path.name}:{line} {match.group(0)!r}")
        self.assertEqual(
            [],
            offences,
            "a workflow can assent to its own repository's changes",
        )

    def test_B2_no_workflow_grants_a_bot_the_ability_to_approve(self) -> None:
        """`pull-requests: write` is the permission an approval needs.

        Six workflows hold it, and none can give an approval:

        - auto_request_review, which requests reviewers and does not give them.
        - auto-assign-milestone: triggers on `pull_request_target: closed`
          gated on `merged == true`, so it runs only after the merge decision
          has been taken, and its one call is `gh pr edit --milestone`.
        - coverage-comment: `workflow_run`-triggered in the base repository,
          job-gated on `github.repository`, and its one write is posting the
          coverage comment.
        - risk_classify: `pull_request_target` with `permissions: {}` at the
          top, the grant job-scoped, checkout pinned to the default branch,
          and its one write is swapping the `risk:*` label.
        - hold-unresolved-threads: `schedule` plus `pull_request_target:
          labeled`, `permissions: {}` at the top, the grant job-scoped,
          checkout pinned to the default branch, and its writes are the
          `do-not-merge` label and one comment on pull requests with
          unresolved review threads. It withholds a merge; it cannot grant one.
        - ci-health: `schedule` plus `workflow_dispatch`, job-gated on
          `github.repository` and `refs/heads/main`, top-level permissions
          `contents: read` and `id-token: write` only, the grant job-scoped,
          checkout with `persist-credentials: false`, and its pull-request
          write is one comment on a pull request whose smoke run went red,
          edited in place on later runs. It explains a red; it cannot approve,
          label, or merge anything.

        The list is an allowlist of holders, not of intents: the permission is
        a capability, and this asserts membership rather than absence so a
        seventh holder is a red test and a conversation rather than a silent
        addition. Adding a name here means someone read the workflow.
        """
        holders = []
        for path, document in _workflow_documents():
            # `_permission_scopes`, so that `permissions: write-all` -- which
            # is a string rather than a mapping, and grants this scope along
            # with every other -- joins the list rather than slipping past it.
            for scope in _permission_scopes(document):
                if scope.get("pull-requests") == "write":
                    holders.append(path.name)
                    break
        self.assertEqual(
            [
                "auto-assign-milestone.yml",
                "auto_request_review.yml",
                "ci-health.yml",
                "coverage-comment.yml",
                "hold-unresolved-threads.yml",
                "risk_classify.yml",
            ],
            sorted(set(holders)),
            "an unexpected workflow can write to pull requests",
        )


class B3TheSubstrateIsNotAgentAuthorable(unittest.TestCase):
    """B3: the biggest thing the first draft of the invariants missed."""

    def test_B3_precondition_the_customer_gitops_template_still_exists(self) -> None:
        self.assertIn("/clusters/", h.text("codeowners_example"))

    @h.known_violation("B3", "overnight-b/findings.md 2.3")
    def test_B3_the_substrate_paths_are_enumerated_as_code(self) -> None:
        """KNOWN VIOLATION. The human-only path set exists only in prose.

        B3's own test text is "substrate paths enumerated as code. A PR
        touching them takes a different, human-only path than a PR changing a
        replica count." There is no such enumeration anywhere in this
        repository -- not a checker, not a workflow, not a data file. The
        nearest artifact is `examples/gitops-repo/CODEOWNERS.example`, which is
        a template for the *customer's* repository and is not enforced here,
        and `branch-protection.md`, which documents a `review-gate.yml`
        workflow that does not exist.

        Two consequences worth separating. Nothing gates a change to the VAP,
        the operator ClusterRole or the workflows in this repo differently from
        a change to a replica count. And path-based gating would not be enough
        even if it existed -- Kubernetes does not care what directory a
        manifest lives in, so a ClusterRoleBinding committed under
        `clusters/*/namespaces/team-x/` matches the namespace glob and gets
        approved by the wrong humans. The invariant wants the rendered object
        set gated, not the path.
        """
        candidates = [
            h.REPO_ROOT / ".github" / "CODEOWNERS",
            h.REPO_ROOT / "CODEOWNERS",
            h.REPO_ROOT / "docs" / "CODEOWNERS",
            h.REPO_ROOT / ".github" / "workflows" / "review-gate.yml",
        ]
        present = [path for path in candidates if path.is_file()]
        self.assertTrue(
            present,
            "no substrate enumeration and no gate that reads one",
        )

    def test_B3_the_agent_cannot_reach_the_admission_policy_through_kubectl(self) -> None:
        """One half of B3 that *is* enforced, and worth pinning.

        The VAP and the RBAC it guards live in the repository the agent
        proposes into, so the artifact-plane half is open. The API half is not:
        every verb that would edit an admission policy or a NetworkPolicy in
        place is refused by the read-only allowlist.
        """
        for argv in (
            ["kubectl", "delete", "validatingadmissionpolicy", "kube-agents-agent-readonly"],
            ["kubectl", "patch", "validatingadmissionpolicybinding", "b", "-p", "{}"],
            ["kubectl", "delete", "networkpolicy", "platformagent-sandbox-metadata-deny"],
            ["kubectl", "apply", "-f", "clusterrolebinding.yaml"],
            ["kubectl", "delete", "clusterrole", "kubeagents:explorer"],
        ):
            with self.subTest(argv=argv):
                self.assertFalse(command_policy.evaluate(argv).allowed)


class B4TheExecutorIsAGovernedPrincipal(unittest.TestCase):
    """B4: CI/CD holds the only production write credential, so it is in scope."""

    def test_B4_every_workflow_run_deploy_gates_on_repository_and_branch(self) -> None:
        """`workflow_run` fires from the default branch with the *triggering* run's context.

        Without all three predicates a fork's completed run, or a run from a
        non-default branch, reaches a job that mints a deployment credential.
        The three are asserted individually so that dropping one -- the
        plausible edit, made while debugging a deploy -- is red.
        """
        consumers = [
            (path, document)
            for path, document in _workflow_documents()
            if "workflow_run" in (document.get("on") or {})
        ]
        self.assertTrue(consumers, "no workflow_run consumers found; the filter is wrong")

        saw_a_deploy = False
        for path, document in consumers:
            for job_name, job in (document.get("jobs") or {}).items():
                condition = str((job or {}).get("if", ""))
                # A job with no permissions block inherits the
                # workflow-level one wholesale, so reading only the job's
                # would let id-token: write hoisted to the top of the file
                # mint the deploy credential past the strict gate. The
                # helper also expands `write-all`, which grants id-token
                # among the rest and is a string -- read raw it used to
                # raise AttributeError here rather than answer the question.
                permissions = _effective_permissions(document, job)
                with self.subTest(workflow=path.name, job=job_name):
                    # Every workflow_run job gates on the repository — the
                    # AGENTS.md fork rule, and the credential half of it.
                    self.assertIn("github.repository ==", condition)
                    # The full three-predicate gate is owed by the jobs that
                    # mint a deploy credential. When this suite was written
                    # every workflow_run consumer was a deploy; main has since
                    # grown non-deploy consumers (a coverage commenter, a
                    # broken-main notifier — which fires on *failure*, so
                    # conclusion == 'success' would be definitionally wrong
                    # there), so the strict form keys on the credential.
                    if permissions.get("id-token") == "write":
                        saw_a_deploy = True
                        chain_conditions = [condition]
                        needs = (job or {}).get("needs")
                        if isinstance(needs, str):
                            needs = [needs]
                        elif not needs:
                            needs = []
                        all_jobs = document.get("jobs") or {}
                        for needed in needs:
                            if needed in all_jobs:
                                chain_conditions.append(str((all_jobs[needed] or {}).get("if", "")))

                        self.assertTrue(
                            any("workflow_run.conclusion == 'success'" in c for c in chain_conditions),
                            f"{path.name}:{job_name} and its prerequisites must gate on workflow_run.conclusion == 'success'",
                        )
                        self.assertTrue(
                            any("workflow_run.head_branch == 'main'" in c for c in chain_conditions),
                            f"{path.name}:{job_name} and its prerequisites must gate on workflow_run.head_branch == 'main'",
                        )
        self.assertTrue(
            saw_a_deploy,
            "no workflow_run job mints a deploy credential any more; the "
            "strict half of this test has gone dead and needs re-pointing",
        )

    def test_B4_no_pull_request_target_workflow_checks_out_the_pull_request(self) -> None:
        """`pull_request_target` runs with the base repository's secrets.

        Checking out the head is how that becomes arbitrary code execution
        with write credentials. A checkout of something *else* is the
        documented-safe pattern — risk_classify pins `ref:` to the default
        branch, so the code that runs is code already merged — and the
        dangerous ingredient is specifically a ref derived from the pull
        request. So every checkout step in such a workflow must carry an
        explicit `ref:` that does not reference the pull request.

        `actions/checkout` carries one obligation more: it must say `ref:` at
        all. That rule was a security rule and is now an explicitness rule,
        and the difference is a dated fact worth getting right. A checkout
        with no `ref:` takes `GITHUB_REF`, and until 2025-12-08 `GITHUB_REF`
        on this trigger was the pull request's *base* branch -- a ref its
        author picks from the branches that already exist here, so a stale
        unprotected one would serve. GitHub's changelog of 2025-11-07
        ("Actions pull_request_target and environment branch protections
        changes") moved it: from 2025-12-08, `GITHUB_REF` for
        `pull_request_target` resolves to the default branch and `GITHUB_SHA`
        to the latest commit on it, which is what the
        events-that-trigger-workflows reference records for this event today.
        So the implicit ref is now the one expression this test allows
        explicitly, and the rule buys review rather than safety: an explicit
        ref is a line somebody can read and check against the list, and a
        default is a line that is not there. Worth keeping, and not worth
        claiming more for. (The Actions *variables* reference page still says
        this trigger takes its ref from the base branch. That page is stale
        against the changelog and is not the citation here.
        `refs/pull/N/merge` is the `pull_request` trigger's default, not this
        one; `risk_classify.yml` and `hold-unresolved-threads.yml` both carry
        the dated note at the checkout step that relies on it.)

        Both filters are asserted non-empty for the same reason B4's
        `workflow_run` gate asserts its own: this test says nothing at all
        about a repository with no `pull_request_target` workflows, or one
        where none of them checks anything out, so it cannot tell "the trigger
        is gone" from "the parse stopped seeing it". Three workflows carry the
        trigger today and two of them run a checkout. If either reaches zero
        the test should be read again, not passed by default.

        The `run:` half is two rules over every step of the workflow, and
        it is not a proof: a script can reach a ref any number of ways and
        no assertion over YAML will catch all of them. The first rule is
        literal. A `pull/N/head` refspec is refused wherever it appears,
        because it is the checkout action's own documented manual equivalent
        and needs no interpolation at all. The second is the `ref:` half's
        rule one field along: a step may name only the expressions in
        `_SAFE_SCRIPT_EXPRESSIONS`, its `env:` may carry only those, an
        `actions/github-script` body may reach only the `context` properties
        in `_SAFE_SCRIPT_CONTEXTS`, and nothing in it may reach the webhook
        payload as a file. Those are the three languages the payload is
        written in here -- an expression, a JavaScript property path, and a
        file on disk -- and the rule is the same in each.

        "Every step" was "every step that fetches" until 2026-09-19, and the
        gate that said so is gone for the reason every other denylist in
        this file went: it was a list of verbs. `fetch`, `checkout`, `clone`
        and `git pull` are four ways to put a fork's code on the runner and
        there are not four -- `git remote add` and `git remote update`,
        `curl .../tarball/... | tar xz`, `pip install "git+https://..."`,
        `docker build "https://...#<sha>"` -- and a verb laundered through
        `env:` is not a verb this file can read at all, which `${{
        env['CMD'] }}` and `os.environ["CMD"].split()` each demonstrated
        while green. The gate was asking "does this step run the fork's
        code", which is a question about programs and about a package
        manager's argument grammar, and a regex over four words cannot
        answer it. What this file can answer is "does this step reach the
        pull request", and that question needs no gate: reaching the payload
        is the thing the rules below already read for. So the condition went
        and the verdicts stayed. It is the same move the environment
        refusal below made one round earlier against the same objection, and
        it is the whole argument of this half -- a denylist of nouns loses,
        and an allowlist of what is reachable wins.

        What that costs is that every step is now read, including the ones
        that do nothing but call an API. A step that carries the pull
        request's head in its `env:` for a label or a log line is refused
        exactly as a fetching one is, because "does the program read this
        variable" is a question about a program. That is a wider cost than
        the gated version's and it is the same kind: a line of review, on a
        trigger where the job holds a writable token, on a step whose author
        can say in one line why the value is there. Measured against the
        carriers, below.

        The second rule was a denylist over three spellings until
        2026-09-19, and it failed the way the `ref:` half's denylist failed,
        which is the argument for the shape rather than a coincidence. Five
        of them, each live and each green: `${{ github.event.before }}` and
        `${{ github.event.pull_request.merge_commit_sha }}`, two more fields
        naming the same commit; `${{ github.event.pull_request['head'].sha
        }}`, which is `pull_request.head` written as an index; `${{
        GITHUB.EVENT.AFTER }}`, because GitHub resolves an expression
        case-insensitively and a Python regex does not; and
        `context.payload.after` inside a `script:` input, which is the same
        field in a language that interpolates nothing at all. Answering those
        with five more alternatives would have left the sixth.
        `_PULL_REQUEST_HEAD` and `_PULL_REQUEST_REF` stay as a backstop over
        text that carries no expression for the allowlist to read, and are
        deliberately not extended.

        What the allowlist costs is measured rather than assumed, and
        dropping the gate is what made it cost anything. Every step of all
        three carriers is read now, and two expressions had to go on the
        list for them to stay green: `github.event.pull_request.number`,
        which two of them label and comment with, and `inputs.dry_run`, a
        `workflow_dispatch` boolean. Both arguments are at
        `_SAFE_SCRIPT_EXPRESSIONS`. That is the bill in full -- two entries
        and two reasons, paid once, for a rule that no longer depends on
        recognising a verb. The next genuinely safe expression somebody
        needs is a line of review and a line on the list saying why, which
        is the same price and the same answer as the ref allowlist.

        "The step's script" is not the same thing as its `run:`.
        `actions/github-script` takes JavaScript as an input and runs it in
        the job with the same token, so the fetch can be written in the
        `with:` block instead and a scan of `run:` reads nothing at all.
        `_step_scripts` therefore folds in every `with:` value that is a
        string -- by shape rather than by action name, for the same reason
        the `ref:` rule below is not about `actions/checkout`. It folded in
        only the values spanning more than one line until 2026-09-19, on the
        reasoning that a program has newlines in it, which is a guess about
        formatting: a one-line `script:` is a program, and so is a `>-`
        folded scalar, which arrives as a single line because folding is what
        the scalar means.

        Both patterns are read over that script and the step's `env:` values
        together, not over the script alone. Passing an event field through
        `env:` is this repository's house style rather than an exotic dodge
        -- `risk_classify.yml` does exactly that with `PR_NUMBER`, on the
        stated grounds that event fields are attacker-controlled input -- so
        a scan of `run:` by itself reads `$PR_HEAD` and sees nothing.

        Which of those values get folded in is a question that stopped
        deciding anything when the gate went, and saying so is worth more
        than the loop that answers it. The pickup below folds in the values
        the script names, following a chain one hop at a time, and the two
        literal backstops read the result. Those backstops also read every
        environment value singly, because the wholesale refusal below does,
        and the folded text is a subset of the values that refusal already
        walks -- neither pattern can match across the newline the fold joins
        on, so there is no string the pickup puts in front of a pattern that
        the pattern was not going to see anyway. That is an argument, so it
        was also measured: neutering the pickup to an empty mapping leaves
        all twelve round-7 cases, all fifteen round-6 cases and all
        forty-two evasion cases at exactly the verdicts they hold with it.

        It stays because it is cheap and because the wholesale refusal is
        one edit from being narrowed again, and it stays *unpinned*, which
        is the part to write down: no mutation row covers it, and none can
        while no workflow in this repository carries a head in an `env:`
        value. A row that neutered it would be green forever and would be
        theatre. Read it as redundancy that is deliberate rather than as a
        rule -- and if the next round wants it gone, the measurement above
        is the permission slip.

        "Names" has to mean both ways of naming, and expansion has to run
        first. A script can reach an `env:` value as `$REV`, which is a
        shell variable, or as `${{ env.REV }}`, which GitHub substitutes
        before the shell ever starts -- and the second spelling is not a
        shell variable reference, so a pickup keyed on `$NAME` folds in
        nothing and the fetch reads as innocent. The substitution is
        case-insensitive because GitHub's is, which `${{ Env.REV }}` walked
        past; the pickup below is case-sensitive because a shell's variables
        are, and folding in a value `$rev` cannot be reading would be a false
        red rather than a catch. A value can also name
        another value, `REV: ${{ env.A }}`, which is text that matches
        neither the refspec nor the head pattern while `A` never gets
        followed. So the script is expanded, the pickup runs over the
        expansion, the picked-up values are expanded too, and the pickup
        repeats until it stops finding names. Both shapes were live against
        the first version of this half and both have mutation rows now, as do
        `printenv NAME` and the indirect `${!PTR}` -- two more ways to spell a
        read that a pickup keyed on `$NAME` cannot see, and two the rule
        below now refuses without reading them at all. The repeat is capped
        by `_ENV_EXPANSION_LIMIT`, the cap `_expand_env` uses.

        Past that, the answer is refusal rather than more syntax, and what
        is refused is the environment rather than the script. A step whose
        `env:` carries the pull request's head is refused, whether or not
        anything in the script appears to read that value. It has to be.
        `shell: python` makes the read `os.environ["REV"]` and no `$REV` is
        written anywhere; an inline `python3 -c` does the same under the
        default shell; and any program a script starts inherits the whole
        environment without naming a field of it. An earlier version
        enumerated the shell constructs that defeat the pickup --
        `printenv`, indirect expansion, `eval`, `env`, `declare`, `source` --
        and refused only a step that used one of them. The verdict was right
        and the condition on it was a guess: a denylist over idioms, one
        idiom behind by construction, with both interpreters above already
        past it. The condition is gone and the verdict stayed. Same answer an
        unresolvable ref expression gets, one field along: "this file cannot
        tell what this does" reads as unsafe.

        The same reasoning took the fetch gate off this rule a round later,
        which is the only change of substance: the head in a step's
        environment is refused now whatever else the step does, where before
        it was refused only if the step also said `fetch`. A step that reads
        `github.head_ref` to name a label reds today. That is the trade the
        ref allowlist already makes, and a step is a place where a comment
        fits.

        The `ref:` half, by contrast, is an allowlist, and that is the whole
        design. It began as a denylist over three nouns -- `pull_request`,
        `head`, `merge` -- and `${{ github.event.after }}` is the pull
        request's head SHA on every `synchronize`, spelled with none of them.
        One line in a workflow and this test reported `ok`. Naming the safe
        refs instead means anything this file cannot resolve reads as unsafe
        rather than as innocent text: an `env.X` chain, a
        `steps.X.outputs.Y`, a value laundered through `$GITHUB_ENV`. That is
        the direction to be wrong in. The cost is a false red the day
        somebody adds a legitimately safe expression, which is a line of
        review, and the docstring is where they will look.

        The list -- `_SAFE_CHECKOUT_EXPRESSIONS` -- is one entry, and the
        three obvious candidates are off it for two different reasons, only
        one of which is about safety. Since the 2025-12-08 change above,
        `github.ref` and `github.sha` *are* the entry that is on the list:
        the default branch, and the head commit of the default branch.
        Refusing them is a one-spelling rule, which is worth having for its
        own sake -- one way to write a thing is one thing to review -- and it
        is not a tightening; reporting it as one overstates what it bought.
        `github.base_ref` is the omission that stands on its own. It is the
        pull request's base branch, chosen by the pull request's *author*
        from the branches that already exist in this repository, and a stale
        unprotected branch here is not the default branch and is not
        necessarily code anyone has read this year. No fork can write any of
        the three -- that needs push access here -- so this entry is about
        the author of the pull request rather than about the fork the test is
        named for. Both live carriers use the default branch anyway.

        `ref:` is only half of what a checkout resolves. The action looks the
        ref up inside whatever `repository:` says, so the allowlist above
        says nothing on its own: `repository: ${{
        github.event.pull_request.head.repo.full_name }}` with `ref: ${{
        github.event.repository.default_branch }}` is the fork's copy of its
        own default branch, which the fork wrote, and every assertion on the
        ref passes. `repository:` therefore gets its own allowlist,
        `_SAFE_CHECKOUT_REPOSITORIES` -- absent, or `github.repository` --
        and unlike the ref half it refuses a literal outright, including
        this repository's own name. The ref half tolerates literals because
        `main` is an ordinary ref; there is no equivalent reason to write
        out a repository when the expression for it exists, and a literal is
        exactly where a lookalike owner would go unread.

        Both inputs are read through `_with_inputs`, case-insensitively,
        which is not decoration. The runner passes an input to an action as
        `INPUT_<NAME>`, upper-casing the key, and `core.getInput` looks it up
        the same way, so `Ref:` is the ref `actions/checkout` checks out and a
        `with.get("ref")` reads none of it. The same helper maps a YAML null
        to the empty string, because `str(None)` is a truthy `"None"` and a
        bare `ref:` is a checkout with no ref wearing a literal's clothes.
        This is the `uses:` bug one field along -- that filter was
        case-sensitive too until `Actions/checkout` was found walking past it
        -- and the pattern in both is the same: each round of review finds the
        input the last round did not read.

        The rule is over any step that takes a `ref:`, not over
        `actions/checkout`. A third-party checkout action fetches the same
        code, and a rule about one vendor is a rule about that vendor.
        `actions/checkout` keeps the one extra obligation argued at the top
        of this docstring -- it must carry a `ref:` at all -- because it is
        the action whose no-ref behaviour is documented and therefore the one
        this file can say anything about. And a job that calls a reusable
        workflow is refused
        outright: its steps live in a file keyed `workflow_call`, so it is
        not in `consumers` either, and this test would inspect nothing while
        reporting `ok`.

        Not covered: `gh pr checkout` and `gh pr diff`, which need no ref
        text at all, a local composite action, which moves the checkout into
        a file this test does not read, and the `run:` half's own laundering
        -- a `$GITHUB_ENV` write in an earlier step, which arrives in a later
        step's environment without appearing in its `env:` block. Read that
        list as examples rather than as the boundary. An earlier version of
        it was written as though it were complete and did not mention
        `github.event.after`, which turned out to be both live and shorter
        than either path it did name. Two entries have since come off it
        rather than on: `steps.X.outputs.Y` is an expression like any other
        and the allowlist refuses it for not being recognised, and a
        `script:` that builds its own client still writes the word `context`
        to reach the payload.
        """
        consumers = [
            (path, document)
            for path, document in _workflow_documents()
            if "pull_request_target" in (document.get("on") or {})
        ]
        self.assertTrue(
            consumers,
            "no pull_request_target workflows found; the filter is wrong",
        )

        saw_a_checkout = False
        for path, document in consumers:
            with self.subTest(workflow=path.name):
                workflow_env = _env_values(document)
                for job_name, job in (document.get("jobs") or {}).items():
                    # A job that calls a reusable workflow carries no `steps:`
                    # of its own, and the called file is keyed `workflow_call`
                    # rather than `pull_request_target`, so it is not in
                    # `consumers` either. Its checkout would be real and this
                    # test would inspect nothing. Refuse rather than pass: the
                    # rule this enforces is worth more than the convenience.
                    self.assertNotIn(
                        "uses",
                        job or {},
                        f"{path.name}:{job_name} calls a reusable workflow, "
                        "whose steps this test cannot see; inline it, or "
                        "teach this test to follow it",
                    )
                    scope_env = workflow_env | _env_values(job)
                    for step in (job or {}).get("steps") or []:
                        step_env = scope_env | _env_values(step)
                        # GitHub resolves `uses:` case-insensitively, so this
                        # match has to be too. `Actions/checkout` runs the same
                        # action and would otherwise walk past the filter.
                        uses = str((step or {}).get("uses", "")).lower()
                        refs = _with_inputs(step, "ref")
                        if uses.startswith("actions/checkout"):
                            saw_a_checkout = True
                            self.assertTrue(
                                any(refs),
                                "a checkout on pull_request_target with no "
                                "ref: takes GITHUB_REF, which is the default "
                                "branch on this trigger and was the pull "
                                "request's base branch until 2025-12-08. Say "
                                "which ref you mean: an explicit ref is "
                                "reviewable and a default is not",
                            )
                        # Every action that takes a `ref:`, not only
                        # `actions/checkout`. A third-party checkout action
                        # fetches the same code, and naming one vendor turns
                        # the rule into a rule about that vendor.
                        for ref in refs:
                            if not ref:
                                continue
                            resolved = _expand_env(ref, step_env)
                            for expression in _EXPRESSION.findall(resolved):
                                self.assertIn(
                                    _normalise_expression(expression),
                                    _SAFE_CHECKOUT_EXPRESSIONS,
                                    f"{path.name}: the checkout ref names "
                                    f"`{expression}`, which is not one of the "
                                    "refs known to be independent of the pull "
                                    "request",
                                )
                            # Whatever is left once the expressions are gone
                            # is literal text, where `refs/pull/N/head` needs
                            # no interpolation at all.
                            literal = _EXPRESSION.sub("", resolved).lower()
                            # Belt and braces around the loop above: literal
                            # text holding a `$` is an expression
                            # `_EXPRESSION` did not match, and an expression
                            # nothing checked against the allowlist has
                            # reached the three-word scan below as innocent
                            # prose. That is exactly what a missing
                            # `re.DOTALL` did. A regex that misses one should
                            # red here rather than fall through.
                            self.assertNotIn(
                                "$",
                                literal,
                                f"{path.name}: the checkout ref carries an "
                                "expression this test could not parse, so "
                                "nothing checked it against the allowlist",
                            )
                            for fragment in ("pull", "head", "merge"):
                                self.assertNotIn(
                                    fragment,
                                    literal,
                                    f"{path.name}: the checkout ref derives "
                                    "from the pull request",
                                )
                        # A ref is resolved inside a repository, so the
                        # ref allowlist says nothing on its own: the default
                        # branch of the *fork* is code the fork wrote. Same
                        # shape of rule, one input along.
                        for repository in _with_inputs(step, "repository"):
                            resolved = _expand_env(repository, step_env)
                            for expression in _EXPRESSION.findall(resolved):
                                self.assertIn(
                                    _normalise_expression(expression),
                                    _SAFE_CHECKOUT_REPOSITORIES,
                                    f"{path.name}: the checkout names the "
                                    f"repository `{expression}`, which is not "
                                    "known to be this repository -- a safe "
                                    "ref resolved there is somebody else's "
                                    "code",
                                )
                            self.assertEqual(
                                "",
                                _EXPRESSION.sub("", resolved).strip(),
                                f"{path.name}: the checkout names a literal "
                                "repository; say `${{ github.repository }}` "
                                "or leave it out, which is the same thing and "
                                "cannot be a lookalike",
                            )
                        # The script, plus the `env:` values it actually
                        # names -- see the docstring on why `run:` alone is
                        # not enough and why order is not required.
                        #
                        # This pickup decides nothing on its own any more.
                        # Only the two literal backstops read what it
                        # produces, the wholesale refusal further down reads
                        # every environment value whether the script names it
                        # or not, and neither backstop matches across a
                        # newline -- so every string folded in here is one
                        # that rule was going to look at singly. Measured, at
                        # the empty mapping, against all three case
                        # harnesses: not one verdict moves. It is kept as
                        # redundancy against the day that rule is narrowed,
                        # and the docstring says plainly that nothing pins
                        # it, because a mutation row over a redundancy no
                        # carrier exercises would be green forever.
                        #
                        # Everything is expanded before it is matched, and
                        # the pickup runs over what expansion produced. A
                        # script naming its value as `${{ env.REV }}` rather
                        # than as `$REV` is never a shell variable reference,
                        # so the shell-style pickup alone reads nothing; and
                        # a value that names another value -- `REV: ${{
                        # env.A }}` -- is text that matches neither pattern,
                        # so a pickup that stopped at one hop would fold in
                        # `A`'s name and not `A`. Iterating both closes the
                        # pair. The loop terminates because `named` only
                        # grows and `step_env` is finite; the limit is the
                        # same cap `_expand_env` uses, and reaching it leaves
                        # an unexpanded `${{ ... }}`, which the ref half
                        # refuses and this half simply does not match.
                        script = _expand_env(_step_scripts(step), step_env)
                        named: dict[str, str] = {}
                        for _ in range(_ENV_EXPANSION_LIMIT):
                            haystack = "\n".join([script] + list(named.values()))
                            picked = {
                                name: _expand_env(value, step_env)
                                for name, value in step_env.items()
                                if name not in named
                                and re.search(
                                    # `$NAME`, `${NAME}`, `${!NAME}` and
                                    # `printenv NAME` are four spellings of
                                    # the same read. The `!` form names the
                                    # variable holding the name rather than
                                    # the value, which this pickup follows
                                    # one hop and no further. Neither the hop
                                    # it takes nor the one it cannot decides
                                    # a verdict: what follows refuses the
                                    # environment wholesale rather than by
                                    # what the script appears to read.
                                    r"\$\{?!?" + re.escape(name) + r"\b"
                                    r"|\bprintenv\s+" + re.escape(name) + r"\b",
                                    haystack,
                                )
                            }
                            if not picked:
                                break
                            named.update(picked)
                        haystack = "\n".join([script] + list(named.values()))
                        # A `pull/N/head` refspec is refused wherever it
                        # appears: it is a literal, it needs no expression,
                        # and there is no innocent reason to write one down
                        # in a workflow that must not have the fork's code.
                        self.assertIsNone(
                            _PULL_REQUEST_REF.search(haystack),
                            f"{path.name}: a step names a pull/N/head "
                            "refspec, which is the checkout action's hazard "
                            "without the checkout action",
                        )
                        # Everything the step's environment will actually
                        # hold, resolved. Every value rather than the ones
                        # the pickup found: the rules below are about what
                        # the step can reach, and a program reaches all of
                        # its environment without naming a field of it.
                        environment = {
                            name: _expand_env(value, step_env)
                            for name, value in step_env.items()
                        }
                        # The ref half's rule, one field along. A step may
                        # name only expressions that are known not to be the
                        # pull request, in its script or in anything its
                        # environment carries, and everything else -- an
                        # event field nobody here has heard of, an `env.X`
                        # that did not resolve -- is refused for being
                        # unrecognised rather than allowed for not matching a
                        # list of nouns.
                        unsafe = sorted({
                            expression
                            for text in [script, *environment.values()]
                            for expression in _EXPRESSION.findall(text)
                            if _normalise_expression(expression)
                            not in _SAFE_SCRIPT_EXPRESSIONS
                        })
                        self.assertEqual(
                            [],
                            unsafe,
                            f"{path.name}: a step names {unsafe}, which "
                            "is not among the expressions "
                            "known to be independent of the pull request. If "
                            "one of them is, add it to "
                            "_SAFE_SCRIPT_EXPRESSIONS and say why",
                        )
                        # And the same rule in JavaScript, which the
                        # expression scan cannot read because a
                        # `github-script` body interpolates nothing: it is
                        # handed the payload as `context` and reaches the
                        # head by property access.
                        reached = sorted({
                            match.group(0).strip()
                            for match in _SCRIPT_CONTEXT.finditer(script)
                            if (match.group(1) or match.group(2) or "").lower()
                            not in _SAFE_SCRIPT_CONTEXTS
                        })
                        self.assertEqual(
                            [],
                            reached,
                            f"{path.name}: a script: input reaches "
                            f"{reached}, which is the webhook "
                            "payload -- the same fields the expression "
                            "allowlist refuses, in the language that runs",
                        )
                        # Belt and braces behind both allowlists, for text
                        # that carries no expression for them to read.
                        self.assertIsNone(
                            _PULL_REQUEST_HEAD.search(haystack),
                            f"{path.name}: a step names the pull "
                            "request's head, which is the checkout action's "
                            "hazard without the checkout action",
                        )
                        # The environment is refused wholesale rather than
                        # followed. Whether this step's script reads a value
                        # is a question about a program in a language nobody
                        # here parses -- `shell: python` reads it as
                        # `os.environ["REV"]`, an inline `python3 -c` does the
                        # same under bash, and any program a script starts
                        # inherits the lot without naming anything -- so a
                        # step that has the head in its environment at all is
                        # refused. The cost is a false red on a step that
                        # carries the head for a label or a log line, which
                        # is a line of review.
                        carried = sorted(
                            name
                            for name, value in environment.items()
                            if _PULL_REQUEST_HEAD.search(value)
                            or _PULL_REQUEST_REF.search(value)
                        )
                        self.assertEqual(
                            [],
                            carried,
                            f"{path.name}: a step's environment carries "
                            f"the pull request's head as {carried}. Whether "
                            "the script reads it is not a question this test "
                            "can answer, so it does not assume the answer",
                        )
                        # And the payload as a file, which is neither an
                        # expression nor a `context` property and so reaches
                        # neither allowlist. Refused on the read rather than
                        # on what is read out of it: every field in that file
                        # arrived from the pull request, and a step that
                        # fetches has no business in any of them.
                        self.assertIsNone(
                            _EVENT_PAYLOAD_FILE.search(
                                "\n".join([script, *environment.values()])
                            ),
                            f"{path.name}: a run: step fetches and reads "
                            "`GITHUB_EVENT_PATH`, which is the whole webhook "
                            "payload on disk -- the same fields both "
                            "allowlists refuse, in the one language neither "
                            "of them reads",
                        )
                # `_permission_scopes` rather than the blocks themselves:
                # `permissions: write-all` is a string, and a filter that
                # read only mappings granted this workflow every scope
                # without either assertion below seeing a thing.
                for scope in _permission_scopes(document):
                    self.assertNotEqual(
                        "write",
                        scope.get("contents"),
                        f"{path.name}: a pull_request_target workflow holds "
                        "contents: write",
                    )
                    self.assertNotEqual(
                        "write",
                        scope.get("id-token"),
                        f"{path.name}: a pull_request_target workflow holds "
                        "id-token: write",
                    )
        self.assertTrue(
            saw_a_checkout,
            "no pull_request_target workflow runs a checkout step any more; "
            "the ref half of this test examined nothing",
        )

    def test_B4_contents_write_is_confined_to_the_release_path(self) -> None:
        """The credential that can push to this repository, and where it lives.

        The release path is the only thing that needs it. Asserting the set
        rather than the absence keeps the grant reviewable: adding a holder is
        a decision someone makes on purpose. The two added on main since this
        suite was written are both that path and both `workflow_dispatch`-only,
        repo-gated, with the grant job-scoped: nightly-pipeline's
        promote-to-staging step, and release-publish's publish job.
        """
        holders = set()
        for path, document in _workflow_documents():
            # Through `_permission_scopes`, which expands `write-all`: the
            # widest grant GitHub offers is spelled as a string, and reading
            # only the mappings would leave the holder set below silent about
            # the one workflow that granted everything.
            for scope in _permission_scopes(document):
                if scope.get("contents") == "write":
                    holders.add(path.name)
        self.assertEqual(
            {
                "rc-create-tag.yml",
                "rc-tag-validated.yml",
                "rc-release-pipeline.yml",
                "nightly-pipeline.yml",
                "release-publish.yml",
            },
            holders,
        )


class B5WhatTheApproverSeesIsWhatWillBeApplied(unittest.TestCase):
    """B5: the invariant we never stated, on the input to the one we did."""

    def setUp(self) -> None:
        scripts = h.REPO_ROOT / "agents/platform/skills/fleet-audit/scripts"
        if str(scripts) not in sys.path:
            sys.path.insert(0, str(scripts))
        import audit_report

        self.audit_report = audit_report

    def test_B5_precondition_the_renderer_still_sanitises_its_inputs(self) -> None:
        """The markdown-injection hardening that *does* exist, pinned.

        `_ident` flattens newlines and replaces backticks because either one
        ends an inline code span and renders the rest of an attacker's value as
        markup. `_cell` escapes the pipe that would otherwise forge a table
        column. Both are real controls and neither should be lost while the
        expected failure below is being fixed.

        It also pins that all four renderers the expected failure calls still
        EXIST. That belongs in a passing test rather than in the violation: an
        expected failure records any exception, so an AttributeError from a
        renamed renderer would be counted among the twelve while the bidi
        assertions never ran -- and the violation could then never close, since
        an unexpected success cannot fire from a test that always raises.
        `audit_report.py` is a 3000-line module nobody editing it would connect
        to this suite, which is why the anchors in SOURCES back this up.
        """
        self.assertEqual("a'b", self.audit_report._ident("a`b"))
        self.assertEqual("a b", self.audit_report._ident("a\nb"))
        self.assertEqual("a\\|b", self.audit_report._cell("a|b"))
        self.assertEqual("a b", self.audit_report._cell("a\nb"))
        for name in ("_ident", "_cell", "trim_command", "trim_excerpt"):
            with self.subTest(renderer=name):
                self.assertTrue(
                    callable(getattr(self.audit_report, name, None)),
                    f"audit_report.{name} is gone; the B5 violation below calls "
                    "it and would record the AttributeError as its expected "
                    "failure",
                )

    @h.known_violation("B5", "overnight-b/findings.md 2.4")
    def test_B5_rendered_evidence_carries_no_direction_or_width_trickery(self) -> None:
        """KNOWN VIOLATION. Bidi and zero-width characters survive into the body.

        The fleet audit requires every finding to carry "the exact read-only
        command that produced it and a verbatim excerpt of the output". Those
        are attacker-controlled bytes rendered inside a block the system labels
        evidence -- anyone who can write a Pod log can write to it.

        The renderer is careful about markdown: backticks, pipes and newlines
        are all neutralised, and the fence is chosen to be longer than any
        run inside the content. What it does not handle is the class B5 names
        explicitly: `U+202E` reverses the displayed order of everything after
        it, and `U+200B`/`U+FEFF` hide inside an identifier. So a reader can be
        shown `kubectl get pods` for a value that is not that, inside the
        block the document calls evidence.

        This is B5's whole point. B2 makes the human the decision point; without
        B5 that boundary has no integrity requirement on its only input.
        """
        trickery = {
            "‮": "right-to-left override",
            "‭": "left-to-right override",
            "⁦": "left-to-right isolate",
            "​": "zero-width space",
            "﻿": "zero-width no-break space",
            "‎": "left-to-right mark",
        }
        for character, description in trickery.items():
            payload = f"kubectl get pods{character} --all-namespaces"
            for name, function in (
                ("_ident", self.audit_report._ident),
                ("_cell", self.audit_report._cell),
                ("trim_command", self.audit_report.trim_command),
                ("trim_excerpt", self.audit_report.trim_excerpt),
            ):
                with self.subTest(character=description, renderer=name):
                    self.assertNotIn(
                        character,
                        function(payload),
                        f"{name} passes {description} through to the approver",
                    )


class B6NoSelfApproval(unittest.TestCase):
    """B6: an approver must hold authority sufficient to make the change directly."""

    def test_B6_the_gitops_template_names_no_automation_identity(self) -> None:
        """The one thing GitHub can actually enforce.

        Self-approval is blocked on account identity, so two agent identities
        defeat it, and approval *count* can probably be satisfied by a GitHub
        App token -- undocumented, and Minty already mints these. The two rules
        a bot cannot satisfy are CODEOWNERS review and the ruleset
        required-reviewer team rule, because apps are not eligible code owners
        and cannot be team members. So the mechanism has to be a named human
        team containing no automation identity, and this asserts the template
        we hand customers says that.
        """
        rules = [
            line
            for line in h.text("codeowners_example").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        owners = [
            owner for line in rules for owner in re.findall(r"@[\w.-]+(?:/[\w.-]+)?", line)
        ]
        self.assertTrue(owners, "the template names no owners at all")
        for owner in owners:
            with self.subTest(owner=owner):
                self.assertNotIn("[bot]", owner)
                self.assertFalse(
                    owner.endswith("-agent"),
                    "an agent identity is named as a code owner",
                )
                self.assertIn(
                    "/",
                    owner,
                    "a code owner must be a team rather than an individual "
                    "account, so that the required review cannot be satisfied "
                    "by whoever happens to be on call",
                )

    def test_B6_every_guarded_path_in_the_template_has_an_owner(self) -> None:
        """A CODEOWNERS entry that covers nothing is the trap this avoids.

        The six path classes the branch-protection note calls guarded --
        provisioning, agents, namespaces, policy, knowledge and .kube-agents
        -- each need a rule, or the ruleset that requires code-owner review on
        them requires review from nobody. The last two are the declared-intent
        pair: a knowledge/ note can move an audit posture off the ledger, and
        .kube-agents/intent.yaml decides which paths' notes can.

        A class is matched as a whole path segment, not as a substring: the
        `/.kube-agents/` rule contains the letters `agents`, and a substring
        test would let it stand in for the deleted `/clusters/*/agents/` rule.
        """
        text = h.text("codeowners_example")
        rules = [
            line.split()[0].strip("/").split("/")
            for line in text.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        for guarded in ("provisioning", "agents", "namespaces", "policy", "knowledge", ".kube-agents"):
            with self.subTest(path=guarded):
                self.assertTrue(
                    any(guarded in segments for segments in rules),
                    f"no CODEOWNERS rule covers {guarded}",
                )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
