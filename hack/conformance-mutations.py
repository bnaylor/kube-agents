#!/usr/bin/env python3
"""Mutation-verify the conformance suite: delete a control, expect red.

If deleting a control leaves the suite green, the test does not exist. Slice 2a
shipped a whole gate that could be removed with its suite byte-identical, and
only a dedicated task caught it -- so this is the property the conformance
suite is graded on, not test count.

Each mutation names the control it removes and the test it must break. The run
applies one mutation at a time to the working tree, runs the suite, restores
the file with `git checkout`, and reports:

    KILLED   the named test failed. The test is real.
    SURVIVED the named test still passed. The test is theatre -- fix it.
    NOISY    the mutation broke something other than the named test as well.
             Not a failure, but worth reading: it usually means two assertions
             overlap, and occasionally means the mutation was blunter than
             intended.
    STALE    the file is gone, or the `old` text is not in it, so nothing was
             mutated and nothing was proved. Reads like silence -- always a
             bug in the row, usually a renamed file, a pin, or a neighbouring
             line that moved.
    OVERSHOT a `must_survive` control was caught: the suite goes red on a
             change that weakens nothing.
    SURVIVED (expected)
             a `must_survive` control was not caught, which is its pass.
             Thirty-six rows print this on a green run, and the
             SURVIVED line above is exactly the wrong reading of them: the
             suite staying green is the property they assert. `--list` is
             the authority on how many there are, because it marks each one;
             this line said twenty through the round in which there were
             twenty-one, twenty-one through the round that added two, and
             twenty-three through the round that corrected the same count on
             the field below and left this one where it was. Two places
             carrying one number is why; `--list` is the third and the only
             one that counts the rows.
    BASELINE POLLUTED
             not a per-row verdict but a line printed after the run: the
             suite is not green once every mutation has been restored, so
             every verdict after whatever caused it is untrustworthy. It
             changes the exit code. Stale bytecode is what taught this line
             to exist and is not what it catches now: `_purge_bytecode` runs
             before every suite run and the suite runs under `-B` with
             `PYTHONDONTWRITEBYTECODE` set, so no cache outlives a restore.
             What is left is whatever a mutated run leaves behind that the
             restore does not reach, the restore being a `git checkout` of
             the one path the row names and nothing else.

The summary line's `survived=` count is survivors *and* OVERSHOT controls,
because both are the same news: a row whose verdict is not what it was
written to be.

An expected failure that a mutation turns into an *unexpected success* also
counts as KILLED: the recorded gap moved, which is exactly the signal wanted.

Usage:
    python3 hack/conformance-mutations.py            # every mutation
    python3 hack/conformance-mutations.py --list     # ids, paths, controls
    python3 hack/conformance-mutations.py -k C1      # substring filter on the id

The tree must be clean. It edits tracked files in place and restores them, so a
dirty tree risks losing work -- it refuses rather than guessing.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


@dataclasses.dataclass(frozen=True)
class Mutation:
    """One removed control, and the test that has to notice."""

    id: str
    path: str
    #: (old, new), applied as a single str.replace of the first occurrence.
    edit: tuple[str, str]
    #: Substring matching the test name that must go red.
    kills: str
    #: What the mutation is pretending to be: a plausible bad change, not noise.
    pretext: str
    #: True for a mutation that must NOT be caught. A suite that goes red on a
    #: harmless change is a suite people learn to override, so a no-op edit is
    #: run as a control on the harness itself. Neither of the two verdicts a
    #: row without this field can get is printed for one that has it: the
    #: pass is `SURVIVED (expected)` and the failure is OVERSHOT, which is
    #: what the module docstring says and what `main` writes. There are
    #: thirty-six today, which `--list` is the authority on rather than
    #: this comment -- it marks each control `[control]`, and the list below
    #: named ten on 2026-09-19 when there were eleven, and one of the ten by
    #: an id no row had. Written in the order `--list` prints them so that
    #: the two can be compared by eye:
    #: A3-fastpath-redundant,
    #: B1-denylist-rule,
    #: B4-pull-request-target-run-local-ref-file,
    #: B4-pull-request-target-run-ref-endpoint-namespaced,
    #: B4-pull-request-target-run-ref-quoted-word-ordinary,
    #: B4-pull-request-target-api-gh-pr-interposed-flag-write,
    #: B4-pull-request-target-api-gh-wrapped-readable-program,
    #: B4-pull-request-target-api-gh-pipeline-readable-program,
    #: B4-pull-request-target-run-quoted-paren-ordinary,
    #: B4-pull-request-target-run-unbalanced-paren-ordinary,
    #: B4-pull-request-target-api-gh-substitution-ordinary,
    #: B4-pull-request-target-run-backtick-closed-then-prose,
    #: B4-pull-request-target-api-actions-path-ordinary,
    #: B4-pull-request-target-api-web-link-comment,
    #: B4-pull-request-target-api-gh-issue-comment-write,
    #: B4-pull-request-target-api-gh-argv-vector-write,
    #: B4-pull-request-target-run-context-repo,
    #: B4-pull-request-target-run-context-repo-env,
    #: B4-pull-request-target-run-json-glob-ordinary,
    #: B4-pull-request-target-run-environment-word,
    #: B4-pull-request-target-run-quoted-word-ordinary,
    #: B4-pull-request-target-run-spaced-quoted-word-ordinary,
    #: B4-pull-request-target-run-backtick-and-comment,
    #: B4-pull-request-target-run-eval-ordinary,
    #: B4-pull-request-target-run-eval-quoted-dollar-ordinary,
    #: B4-pull-request-target-run-proc-path-ordinary,
    #: B4-pull-request-target-pwsh-named-variable,
    #: B4-pull-request-target-pwsh-drive-neighbourhood-ordinary,
    #: B4-pull-request-target-run-array-index-walk,
    #: B4-pull-request-target-run-env-ordinary-shell,
    #: B4-pull-request-target-shell-ordinary,
    #: B4-pull-request-target-checkout-ref-env-case,
    #: B4-pull-request-target-run-clone-bare,
    #: B4-pull-request-target-run-log-m-flags,
    #: B4-pull-request-target-run-runner-repository-name, and
    #: B4-pull-request-target-container-pinned-image.
    must_survive: bool = False


MUTATIONS: list[Mutation] = [
    # ---- A. Authority ---------------------------------------------------
    Mutation(
        "A3-inject-auth",
        "agents/platform/scripts/session_kv_server.py",
        ('"/sessions/{session_id}/inject", dependencies=[Depends(verify_api_key)])',
         '"/sessions/{session_id}/inject")'),
        "test_A3_the_session_inject_endpoint_authenticates_its_caller",
        "drop the route's auth dependency, restoring the unauthenticated "
        "prompt-injection endpoint gke-labs/kube-agents#616 closed -- the exact "
        "state this assertion recorded as a known violation until main fixed it",
    ),
    Mutation(
        "A3-impersonation-flags",
        "agents/platform/scripts/command_policy.py",
        ('{"--as", "--as-group", "--as-uid", "--as-user-extra", "--impersonate-service-account"}',
         '{"--as-group", "--as-uid", "--as-user-extra", "--impersonate-service-account"}'),
        "test_A3_rejects_caller_supplied_as",
        "drop the plain --as, keeping the others -- the shape a careless "
        "refactor of a set literal takes",
    ),
    Mutation(
        "A3-kuberc",
        "agents/platform/scripts/command_policy.py",
        ('        if name == "--kuberc":\n            return "--kuberc"\n',
         '        if name == "--kuberc-disabled":\n            return "--kuberc"\n'),
        "test_A3_rejects_kuberc",
        "neuter the dedicated kuberc check; the flag stays in "
        "_KUBECTL_IDENTITY_FLAGS, so this tests whether the second guard holds",
    ),
    Mutation(
        # The fast-path `-sVALUE` test and the cluster walk are mutually
        # redundant for the attached spelling — deleting either alone weakens
        # nothing, measured by execution. The kill is therefore the walk's own
        # return, which is the only coverage the clustered spelling
        # (`-As http://host`, boolean then s) has; the corpus carries that
        # spelling so this deletion goes red.
        "A3-attached-shorthand",
        "agents/platform/scripts/command_policy.py",
        ('                if character == "s":\n                    return "-s"\n', ""),
        "test_A3_rejects_attached_shorthand_server",
        "drop the cluster walk's server detection while simplifying the loop; "
        "-As http://host then reaches the server flag unrefused",
    ),
    Mutation(
        # The other half of the redundancy, pinned as deliberate: deleting the
        # `-sVALUE` fast-path must leave the suite green, because the cluster
        # walk catches every spelling the corpus carries. If this ever goes
        # OVERSHOT, the walk lost coverage and the fast-path became the only
        # thing standing — which is worth knowing loudly.
        "A3-fastpath-redundant",
        "agents/platform/scripts/command_policy.py",
        ('        if token.startswith("-s") and token != "-s":\n            return "-s"\n', ""),
        "test_A3_rejects_attached_shorthand_server",
        "remove the redundant fast-path; the cluster walk covers it",
        must_survive=True,
    ),
    Mutation(
        "A3-kubectl-kuberc-env",
        "agents/platform/scripts/credential_proxy.py",
        # Indented to pin the sandbox environment the test reads. The
        # unindented spelling occurs first, in `_GIT_PROBE_ENVIRONMENT`,
        # and a one-shot replace aimed there proves nothing.
        ('            "KUBECTL_KUBERC": "false",',
         '            "KUBECTL_KUBERC_UNUSED": "false",'),
        "test_A3_default_path_kuberc_is_disabled",
        "rename the env var while 'tidying', leaving the default-path kuberc "
        "feature on and the protection resting on mount geometry alone",
    ),
    Mutation(
        "A3-server-flag",
        "agents/platform/scripts/command_policy.py",
        ('        "-s", "--server",\n        "--token", "--user", "--username", "--password",',
         '        "--token", "--user", "--username", "--password",'),
        "test_A3_rejects_credential_redirection",
        "drop --server from the identity set -- it is also in "
        "_KUBECTL_FLAGS_WITH_VALUE, so it still parses and looks handled",
    ),
    Mutation(
        "A1-refusal-content",
        "agents/platform/scripts/command_policy.py",
        ('                "Identity and API server address belong to the broker. Remove "',
         '                f"Identity belongs to the broker; {argv} was refused. Remove "'),
        "test_A1_a_refusal_names_no_caller_supplied_value",
        "make the refusal more helpful by naming what was refused -- the "
        "obvious improvement that turns a denial into an oracle",
    ),
        Mutation(
        # The original unpinned bind's resourceNames. #387 removed the bind
        # rule outright and this edit became "grant escalate on the RBAC rule
        # the operator holds full CRUD through"; the auth callout has since
        # brought bind back, scoped to one name, and A4-bind-unscoped below
        # is the mutation that covers the scoping. This one stays on escalate.
        "A4-operator-escalate",
        "k8s-operator/config/rbac/role.yaml",
        ("      - roles\n    verbs:\n      - create\n",
         "      - roles\n    verbs:\n      - create\n      - escalate\n"),
        "test_A4_the_operator_cannot_escalate_its_own_grants",
        "grant the operator escalate so a reconcile can widen a role in place "
        "-- the ceiling in C5 becomes advisory",
    ),
    Mutation(
        "A1-refusal-emptied",
        "agents/platform/scripts/command_policy.py",
        ('                message=(\n'
         '                    "Identity and API server address belong to the broker. Remove "\n'
         '                    "--server, --token, --user, --client-certificate, "\n'
         '                    "--insecure-skip-tls-verify and the other credential flags to "\n'
         '                    "use the cluster and identity the proxy configured."\n'
         '                ),\n',
         '                message="",\n'),
        "test_A1_a_refusal_names_the_rule_that_fired",
        "collapse a refusal onto its rule id -- the cheapest way to satisfy "
        "A1's no-caller-supplied-byte bound is to stop saying anything, and an "
        "agent handed an empty body cannot tell policy from an unreachable "
        "cluster",
    ),
    Mutation(
        "A3-gcloud-flags-file",
        "agents/platform/scripts/command_policy.py",
        ('        if name == "--flags-file":\n            return "--flags-file"\n',
         '        if name == "--flags-file-disabled":\n            return "--flags-file"\n'),
        "test_A3_rejects_gcloud_flags_file",
        "neuter the dedicated flags-file check the way A3-kuberc does. Unlike "
        "--kuberc there is no second guard: the command survives only because "
        "the flag's arity is unknown, so it is refused for the wrong reason and "
        "becomes allowed the day _GCLOUD_FLAGS_WITH_VALUE learns about it",
    ),
    Mutation(
        "A3-attached-shorthand-overreach",
        "agents/platform/scripts/command_policy.py",
        ('        if token.startswith("-s") and token != "-s":\n            return "-s"\n',
         '        if token.startswith("-") and token.lstrip("-").startswith("s") '
         'and token != "-s":\n            return "-s"\n'),
        "test_A3_the_attached_shorthand_rule_does_not_overreach",
        "the looser spelling the docstring warns against -- strip the dashes, "
        "test for a leading s. Every -sVALUE is still refused, so the shorthand "
        "test stays green while --sort-by, --since and --selector become "
        "refusals, which is how a control gets switched off in production",
    ),
        Mutation(
        # The bind grant is only bounded while it names resources. Stripping
        # resourceNames leaves a rule that lets the operator attach ANY
        # existing ClusterRole -- cluster-admin included -- to any subject it
        # can write a binding for, which is escalate without the verb.
        "A4-bind-unscoped",
        "k8s-operator/config/rbac/role.yaml",
        ("    resourceNames:\n      - system:auth-delegator\n", ""),
        "test_A4_the_operator_cannot_escalate_its_own_grants",
        "drop the resourceNames bound on the operator's bind grant so it can "
        "attach any role to any subject -- escalate without the verb",
    ),
    Mutation(
        # The generated block is gated byte-for-byte by `make chart-check`, so
        # the interesting place to hide a grant is just past its end marker:
        # chart-sync will not touch it and a parser that reads only the block
        # never sees it.
        "A4-chart-bind-outside-markers",
        "charts/kube-agents/templates/operator-rbac.yaml",
        ("  # END GENERATED RULES",
         "  # END GENERATED RULES\n  - apiGroups:\n      - rbac.authorization.k8s.io\n"
         "    resources:\n      - clusterroles\n    verbs:\n      - bind"),
        "test_A4_the_chart_grants_the_same_ceiling_as_the_kustomize_role",
        "write an unrestricted bind into the chart BELOW the generated-rules "
        "marker, where chart-sync leaves it and a block-scoped parse misses it",
    ),
    Mutation(
        # Originally unpinned the chart's bind-to-view rule. Both delivery
        # paths now carry a bind again (scoped to system:auth-delegator), so
        # the edit is an UNSCOPED bind appearing in the chart copy alone —
        # the same-ceiling drift A4 exists to catch, in the direction that
        # widens.
        "A4-chart-bind-returns",
        "charts/kube-agents/templates/operator-rbac.yaml",
        ("  # END GENERATED RULES",
         "  - apiGroups:\n      - rbac.authorization.k8s.io\n    resources:\n"
         "      - clusterroles\n    verbs:\n      - bind\n  # END GENERATED RULES"),
        "test_A4_the_chart_grants_the_same_ceiling_as_the_kustomize_role",
        "give the chart's operator role an unrestricted bind the kustomize "
        "role does not carry -- one delivery path quietly grows a ceiling",
    ),
    Mutation(
        # The chart half was three literal string scans until #1319; this is
        # the spelling that walked past them. Flow style is not exotic -- it is
        # what `helm create` scaffolds and what a hand-edit reaches for.
        "A4-chart-impersonate-flow-style",
        "charts/kube-agents/templates/operator-rbac.yaml",
        ("    resources:\n      - events\n    verbs:\n      - create\n      - patch",
         "    resources:\n      - events\n    verbs: [impersonate, create, patch]"),
        "test_A4_the_chart_grants_the_same_ceiling_as_the_kustomize_role",
        "give the chart's leader-election Role impersonate, written in flow "
        "style: the object is outside the generated block so chart-sync leaves "
        "it, and `- impersonate` as a substring does not appear",
    ),
    Mutation(
        # The kustomize half read role.yaml alone. leader_election_role.yaml is
        # listed beside it in the same kustomization and installs just as
        # readily.
        "A4-leader-election-escalate",
        "k8s-operator/config/rbac/leader_election_role.yaml",
        ("    resources:\n      - events\n    verbs:\n      - create\n      - patch",
         "    resources:\n      - events\n    verbs:\n      - escalate\n      - create\n      - patch"),
        "test_A4_the_operator_cannot_escalate_its_own_grants",
        "add escalate to the operator's OTHER Role -- the leader-election one, "
        "which the same kustomization installs and A4 did not read",
    ),
    Mutation(
        "A4-inject-assertion-renamed",
        "tests/conformance/test_A_authority.py",
        ("    def test_A3_the_session_inject_endpoint_authenticates_its_caller(self) -> None:",
         "    def test_A3_the_inject_endpoint_authenticates_its_caller(self) -> None:"),
        "test_A4_triggering_is_covered_by_the_A3_inject_finding",
        "shorten an over-long test name in a tidy-up. A4's triggering clause has "
        "no assertion of its own -- it looks its coverage up by qualname -- so a "
        "rename uncovers the invariant without deleting a line of assertion",
    ),
    Mutation(
        "A2-ceiling-test-renamed",
        "tests/conformance/test_C_enforcement.py",
        ("    def test_C5_no_minted_role_grants_a_write_verb(self) -> None:",
         "    def test_C5_no_minted_role_grants_write_verbs(self) -> None:"),
        "test_A2_the_agent_ceiling_half_of_the_intersection_is_asserted",
        "rename the minted-RBAC ceiling test. A2 has no mechanism of its own to "
        "assert, so it borrows C5's assertion by name; the borrow is what breaks "
        "first, and it has to break loudly or A2 falls off the map",
    ),
    # ---- B. The write path ----------------------------------------------
    Mutation(
        "B1-read-only-verbs",
        "agents/platform/scripts/command_policy.py",
        ('        ("get",),\n', '        ("get",),\n        ("delete",),\n'),
        "test_B1_kubectl_write_verbs_are_refused",
        "add delete to the read allowlist -- the single-line change an "
        "operator makes to unblock a skill",
    ),
    Mutation(
        "B1-image-gate",
        "deploy/docker/Dockerfile",
        ("unexpected cluster CLI in the agent image", "cluster CLI note"),
        "test_B1_the_sandbox_image_ships_no_credentialed_cli",
        "reword the build gate's message, which is what the assertion anchors "
        "on -- checks that the anchor is registered and policed",
    ),
    Mutation(
        "B1-image-gate-binaries",
        "deploy/docker/Dockerfile",
        ("for binary in gcloud kubectl gh git helm k9s yq; do", "for binary in gcloud kubectl; do"),
        "test_B1_the_sandbox_image_ships_no_credentialed_cli",
        "shorten the gate's binary list, the plausible edit when one of them "
        "is legitimately needed at build time",
    ),
            Mutation(
        # The bucket-2 gate check asserts the class-level skip flag by
        # inspection; this is its kill, proven by execution before it was
        # encoded: strip the decorator from one scenario and the check goes
        # red without anything running.
        "bucket2-scenario-ungated",
        "tests/conformance/bucket2/test_cluster_scenarios.py",
        ("@h.requires_cluster\nclass Scenario5", "class Scenario5"),
        "test_bucket_two_is_skipped_for_the_stated_reason",
        "drop the cluster gate from one scenario while renaming the class -- "
        "before the check went structural, the next bucket-1 run would have "
        "executed a mutating kubectl against the developer's ambient context",
    ),
    Mutation(
        # The merge rule is one of the two the wide review found covered by
        # nothing: the original B1 known violation absorbed every subset of
        # its corpus, so deleting this rule left the suite green.
        "B1-merge-rule-weakened",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("pr\\\\b(?:\\\\s+\\\\S+)*?\\\\s+merge\\\\b",
         "pr\\\\b(?:\\\\s+\\\\S+)*?\\\\s+mergeonly\\\\b"),
        "test_B1_the_denylist_refuses_merge_and_approve",
        "tighten github.merge to a spelling gh never emits; the agent merges "
        "its own pull request while every eyeball diff reads as a refactor",
    ),
    Mutation(
        "B1-assent-rule-weakened",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("review\\\\b(?:\\\\s+\\\\S+)*?\\\\s+(?:--approve|-a)\\\\b",
         "review\\\\b(?:\\\\s+\\\\S+)*?\\\\s+(?:--approveonly)\\\\b"),
        "test_B1_the_denylist_refuses_merge_and_approve",
        "tighten github.assent the same way; the approval half goes with it",
    ),
Mutation(
        # The genuine attack for the disclosure assertion. policy_blocks reads
        # the rendered policy out of the default golden fixture, so the
        # weakening that matters is there: one character appended to the
        # pattern and `gcloud auth print-access-token` no longer matches.
        "B1-denylist-pattern",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("print-(?:access|identity)-token\\\\b",
         "print-(?:access|identity)-tokensonly\\\\b"),
        "test_B1_the_shipped_denylist_refuses_credential_disclosure",
        "tighten the disclosure pattern to a spelling nothing uses while "
        "refactoring the escapes -- the rule survives every eyeball diff and "
        "matches nothing",
    ),
Mutation(
        "B1-denylist-rule",
        "k8s-operator/internal/controller/platformagent_manifests.go",
        ('{"id":"gcp.access-token-disclosure"', '{"id":"gcp.access-token-disclosure-XX"'),
        "test_B1_the_shipped_denylist_refuses_credential_disclosure",
        "renames a rule id without touching its pattern, so nothing is actually "
        "weakened. A control on the harness: the suite must not go red on a "
        "rename, or it becomes a suite people learn to override.",
        must_survive=True,
    ),
    Mutation(
        "B2-automerge",
        ".github/workflows/validate.yml",
        ("jobs:", "jobs:\n  merge:\n    runs-on: ubuntu-latest\n    steps:\n"
                  "      - run: gh pr merge --auto --squash \"$NUMBER\"\n"),
        "test_B2_no_workflow_approves_or_merges",
        "add an auto-merge job, which is the thing B2 exists to forbid",
    ),
    Mutation(
        # autopush-redeploy-agent.yml was deleted by #1199, which consolidated
        # the autopush deploys; every run of the whole sweep has crashed on
        # the missing path since. autopush-deploy.yml is the replacement and
        # carries the same predicate, once. This branch and #1310 found and
        # fixed that independently -- hence the missing-file case handled
        # below, which turns a crash into one stale mutation.
        "B4-workflow-run-gate",
        ".github/workflows/autopush-deploy.yml",
        ("github.event.workflow_run.head_branch == 'main'", "true"),
        "test_B4_every_workflow_run_deploy_gates",
        "drop the branch predicate while debugging a deploy, which is when it "
        "actually gets dropped",
    ),
    Mutation(
        # Originally inserted a checkout into auto_request_review.yml, which
        # has since moved off pull_request_target -- read its `on:` block: it
        # is `check_run: [completed]`, plus an `issue_comment` escape hatch --
        # so the insertion landed outside the test's trigger filter and proved
        # nothing. risk_classify.yml carries the trigger and runs a checkout
        # whose safety is exactly the pinned ref (three workflows carry
        # `pull_request_target` and two of them check anything out), so the
        # mutation is the one-line flip an author debugging the classifier
        # against their own PR would make.
        "B4-pull-request-target-checkout",
        ".github/workflows/risk_classify.yml",
        ("          ref: ${{ github.event.repository.default_branch }}",
         "          ref: ${{ github.event.pull_request.head.sha }}"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "point the classifier's checkout at the pull request so it classifies "
        "the new rules too -- arbitrary code execution with a writable token",
    ),
    Mutation(
        # GitHub resolves `uses:` case-insensitively, so `Actions/checkout`
        # runs the identical action, and the `.lower()` on that filter is what
        # keeps it matching its own name. This row pins that `.lower()`, and
        # it did not until 2026-09-19. It used to capitalise the name *and*
        # repoint the `ref:`, which measures nothing: the ref allowlist sits
        # outside the `actions/checkout` guard and reads every step whatever
        # it is called, so a repointed ref is RED with the `.lower()` and RED
        # without it. What the guard actually gates is the rule that a
        # checkout must carry a `ref:` at all, and `saw_a_checkout` with it.
        # So the mutation deletes the ref line instead. Measured: RED with the
        # `.lower()`, GREEN without. `persist-credentials: false` stays under
        # `with:`, so the block is still valid YAML and still a real checkout.
        #
        # Pins the action SHA because the name and the `with:` block are not
        # contiguous otherwise -- a pin bump reports STALE here, and the fix
        # is to paste the new SHA in.
        "B4-pull-request-target-checkout-case",
        ".github/workflows/risk_classify.yml",
        ("        uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n"
         "        with:\n"
         "          ref: ${{ github.event.repository.default_branch }}\n",
         "        uses: Actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n"
         "        with:\n"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "drop a checkout's ref while capitalising the action name, so the "
        "step falls back to the implicit ref under a name the rule about "
        "implicit refs cannot see",
    ),
    Mutation(
        # The third route to the same code: no action at all. `git fetch
        # origin pull/N/head` is the checkout action's own documented manual
        # equivalent, and a rule that reads `uses:` steps cannot see it.
        "B4-pull-request-target-run-fetch",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         '          git fetch --depth=1 origin "pull/${{ github.event.number }}/head"\n'
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "fetch the pull request by hand to get around a rule that only reads "
        "the checkout action's ref",
    ),
    Mutation(
        "B4-pull-request-target-run-fetch-sha",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         '          git fetch --depth=1 origin "${{ github.event.pull_request.head.sha }}"\n'
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "fetch the pull request head by SHA rather than by refspec, which the "
        "pull/N/head pattern does not match",
    ),
    Mutation(
        # The row that started the allowlist. `github.event.after` is the head
        # SHA delivered on every `synchronize`, so it names the pull request's
        # code using none of the words a denylist over `pull_request`, `head`
        # and `merge` looks for. Found live: this one line went in and the
        # whole suite stayed green.
        "B4-pull-request-target-checkout-after",
        ".github/workflows/risk_classify.yml",
        ("          ref: ${{ github.event.repository.default_branch }}",
         "          ref: ${{ github.event.after }}"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "check out the pull request under an event field that does not spell "
        "out what it is",
    ),
    Mutation(
        # Laundering the same ref through `env:`, which is the dodge the
        # allowlist closes as a side effect: an expression the test cannot
        # resolve to a known-safe ref is refused rather than read as innocent
        # text.
        #
        # The declaration order is deliberate and was wrong here until this
        # row was re-read. `_expand_env` substitutes in `env` insertion
        # order, so with TARGET_REV declared first its own pass rewrites the
        # ref to `${{ env.UPSTREAM_REV }}` and UPSTREAM_REV's pass -- later
        # in the same loop -- finishes the job in one go. Declaring
        # UPSTREAM_REV first inverts that: pass one substitutes UPSTREAM_REV
        # into a text that does not mention it yet, then TARGET_REV, and only
        # pass two reaches the SHA. The kill does not depend on which: a ref
        # still holding `${{ env.UPSTREAM_REV }}` is not on the allowlist
        # either, which is the allowlist's whole point. What the order buys
        # is that the fixed-point loop is exercised rather than accidentally
        # satisfied -- but only in the assertion message. Crippling
        # `_expand_env` to a single pass leaves this row KILLED either way,
        # measured: the ref is still unresolved, an unresolved ref is still
        # off the allowlist, and the runner records verdicts rather than
        # message text. So this row does not pin that loop and the order does
        # not make it pin it. Nor did anything else: the `$NAME` pickup's
        # own loop was credited to
        # B4-pull-request-target-run-fetch-verb-chain, was measured unpinned
        # on 2026-09-19, and went the same day. The loop this row is about
        # is `_expand_env`'s, which is the one that survived.
        "B4-pull-request-target-checkout-env",
        ".github/workflows/risk_classify.yml",
        ("        with:\n"
         "          ref: ${{ github.event.repository.default_branch }}",
         "        env:\n"
         "          UPSTREAM_REV: ${{ github.event.pull_request.head.sha }}\n"
         "          TARGET_REV: ${{ env.UPSTREAM_REV }}\n"
         "        with:\n"
         "          ref: ${{ env.TARGET_REV }}"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "pass the pull request head through two step-level env values so the "
        "checkout ref reads as a variable name",
    ),
    Mutation(
        # A `ref:` on something that is not `actions/checkout`. The action
        # fetches the same code; a rule written about one vendor is a rule
        # about that vendor. The fictitious action is SHA-pinned with a
        # version comment so it satisfies C4's sweep: an unpinned one breaks
        # two rules at once and the verdict stops saying which rule caught it.
        "B4-pull-request-target-checkout-third-party",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Check out the pull request with somebody else's action\n"
         "        uses: some-vendor/checkout-action"
         "@1b0c5f0f0f0e5ec9b0f4a2e6d7c8b9a0f1e2d3c4 # v1.2.3\n"
         "        with:\n"
         "          ref: ${{ github.event.pull_request.head.sha }}\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "reach the pull request through a checkout action the rule does not "
        "name",
    ),
    Mutation(
        # A job whose steps live in another file. The called workflow is keyed
        # `workflow_call`, so it is not in the trigger filter either, and the
        # test would have inspected nothing while reporting ok. Refusing the
        # shape is the only answer that does not require following the call.
        "B4-pull-request-target-reusable-job",
        ".github/workflows/risk_classify.yml",
        ("jobs:\n"
         "  classify:",
         "jobs:\n"
         "  prepare:\n"
         "    uses: ./.github/workflows/e2e-run.yml\n"
         "\n"
         "  classify:"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "move the checkout into a called workflow, where a test that reads "
        "`steps:` cannot see it",
    ),
    Mutation(
        # The run half of B4-pull-request-target-checkout-after. The existing
        # by-SHA row fetches `github.event.pull_request.head.sha`; this is the
        # adjacent spelling, and the `env:` indirection is this repository's
        # own house style rather than an exotic dodge.
        "B4-pull-request-target-run-fetch-after",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          REV: ${{ github.event.after }}\n"
         "        run: |\n"
         '          git fetch --depth=1 origin "$REV"\n'
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "fetch the same SHA under the event field the by-SHA row does not "
        "cover, through the environment",
    ),
    Mutation(
        # `ref:` is resolved inside `repository:`, so the ref allowlist on its
        # own does not say what gets checked out. The ref here is left exactly
        # as the carrier ships it -- the one expression the allowlist holds --
        # so every assertion in the ref half passes, and what gets checked out
        # is the fork's own default branch, which the fork wrote.
        "B4-pull-request-target-checkout-repository",
        ".github/workflows/risk_classify.yml",
        ("          ref: ${{ github.event.repository.default_branch }}",
         "          repository: ${{ github.event.pull_request.head.repo.full_name }}\n"
         "          ref: ${{ github.event.repository.default_branch }}"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "redirect the one allowlisted ref into the fork, which is the same "
        "code the ref allowlist exists to keep out",
    ),
    Mutation(
        # The allowlist is one entry, and this row is why it stays that way.
        # `github.base_ref` is the pull request's base branch, which its
        # *author* picks from the branches that already exist here: a stale
        # unprotected one is not the default branch and is not necessarily
        # code anybody has read this year. No fork can write it -- that needs
        # push access -- so what this row keeps out is the pull request's
        # author rather than the fork the test is named for. It is not the
        # ref an implicit checkout takes: since 2025-12-08 that is the default
        # branch, which is what the test's own assertion message says.
        "B4-pull-request-target-checkout-base-ref",
        ".github/workflows/risk_classify.yml",
        ("          ref: ${{ github.event.repository.default_branch }}",
         "          ref: ${{ github.base_ref }}"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "name the base branch outright, which is a ref the pull request's "
        "author chooses and nobody re-reviews",
    ),
    Mutation(
        # The literal spelling of B4-pull-request-target-checkout-repository
        # -- named rather than described as "the row above", which it stopped
        # being when the base-ref row was inserted between them. Neither value
        # carries an expression for either allowlist, and the ref half sees
        # only `main`, which is an ordinary ref and passes. `someone/else`
        # never reaches the ref half's `pull`/`head`/`merge` scan at all: it
        # is the `repository:` value, and what kills the row is the
        # repository half's own assertion, which refuses a literal outright
        # because nothing here can tell this repository's name from a
        # lookalike.
        "B4-pull-request-target-checkout-repository-literal",
        ".github/workflows/risk_classify.yml",
        ("          ref: ${{ github.event.repository.default_branch }}",
         "          repository: someone/else\n"
         "          ref: main"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "check out somebody else's repository by name, with a ref that looks "
        "like the most ordinary ref there is",
    ),
    Mutation(
        # Input keys are case-insensitive: the runner passes `with.Ref` to an
        # action as `INPUT_REF`, which is the same input `ref` sets. On
        # `actions/checkout` the uppercase spelling was caught by accident --
        # a `with.get("ref")` read nothing, and the must-carry-a-ref rule
        # fired -- and on any other action there was no such rule, which is
        # what `_with_inputs`'s case-fold closed.
        #
        # This row does not pin that case-fold, and its comment claimed to
        # until round 16. `github.event.pull_request.head.sha` is on no
        # allowlist in the file: `_step_scripts` folds every string `with:`
        # value into the step's script, and the script-expression allowlist
        # refuses the SHA there whether or not the checkout half ever reads
        # the key. Fold the case away -- `str(key) == name` -- and the row is
        # still KILLED, on that allowlist rather than on the ref one.
        # Measured, not reasoned. So what it pins is the carrier: an
        # upper-case input key on a third-party action is a checkout, and one
        # of the two rules that read a checkout has to refuse it. The
        # case-fold itself is pinned by the row below, which names an
        # expression the script half allows.
        #
        # Same fictitious SHA-pinned vendor as the third-party row, so C4's
        # pin sweep is not the thing that catches it.
        "B4-pull-request-target-checkout-ref-case",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Check out the pull request under an upper-case input\n"
         "        uses: some-vendor/checkout-action"
         "@1b0c5f0f0f0e5ec9b0f4a2e6d7c8b9a0f1e2d3c4 # v1.2.3\n"
         "        with:\n"
         "          Ref: ${{ github.event.pull_request.head.sha }}\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "spell the input key in a case a lowercase lookup does not read, "
        "which GitHub resolves to the same input",
    ),
    Mutation(
        # The case-fold in `_with_inputs`, isolated -- the row above cannot
        # isolate it, and this one is what proves the fold decides a verdict.
        # The value is the pull request's *number*, which is on
        # `_SAFE_SCRIPT_EXPRESSIONS` and not on `_SAFE_CHECKOUT_EXPRESSIONS`:
        # the number is the concession the two labelling carriers need, and a
        # number is not a ref, so nothing here can say what the checkout that
        # resolves it lands on in the fork. The script half therefore passes
        # this `with:` value and the checkout half is the only rule left that
        # can refuse it -- and the only way that half reads an input spelled
        # `Ref:` is the fold. Revert it to `str(key) == name` and this row
        # goes SURVIVED while every other checkout row stays KILLED.
        #
        # The lower-case spelling of the same value reds either way, which is
        # the measurement that says the fold is the difference rather than
        # the expression.
        "B4-pull-request-target-checkout-ref-case-number",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Check out a ref the script allowlist would pass\n"
         "        uses: some-vendor/checkout-action"
         "@1b0c5f0f0f0e5ec9b0f4a2e6d7c8b9a0f1e2d3c4 # v1.2.3\n"
         "        with:\n"
         "          Ref: ${{ github.event.pull_request.number }}\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "spell the input key in a case a lowercase lookup does not read, and "
        "fill it with the one pull-request expression a step may name",
    ),
    Mutation(
        # The run half's version of the `env:` dodge, named the way GitHub
        # names it. `${{ env.REV }}` is substituted before the shell
        # starts, so the script never contains a `$REV`, which is what the
        # shell-style pickup of the time looked for and what anything keyed
        # on a sigil still would.
        "B4-pull-request-target-run-fetch-interpolated",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          REV: ${{ github.event.after }}\n"
         "        run: |\n"
         '          git fetch --depth=1 origin "${{ env.REV }}"\n'
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "name the laundered ref the way GitHub names it rather than the way "
        "the shell does",
    ),
    Mutation(
        # Two hops through `env:`, both in GitHub's own syntax. Written to
        # pin the expansion inside the pickup -- `${{ env.A }}` becomes the
        # SHA before the haystack is rebuilt -- and it stopped pinning it on
        # 2026-09-19, when the rules over a fetching step started reading
        # every `env:` value in scope rather than the ones the pickup found:
        # A and REV are now both refused where they stand. Kept as the
        # two-hop spelling of the `env:` dodge. The loop it used to be about
        # is pinned by nothing and is gone as of 2026-09-19: it computed a
        # haystack no verdict read, which is what three rounds of review kept
        # finding and what the docstring kept promising to remove.
        "B4-pull-request-target-run-fetch-chained",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          A: ${{ github.event.after }}\n"
         "          REV: ${{ env.A }}\n"
         '        run: git fetch --depth=1 origin "$REV" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "put one env value in front of another so the value the script names "
        "is itself a name",
    ),
    Mutation(
        # `git pull` is a fetch and a merge in one verb, and the verb list
        # it dodged named only `fetch`, `checkout` and `clone`. That list is
        # gone and nothing replaced it: no rule in the test asks which git
        # verb a step runs, because the question stopped being which verb
        # and became which expression. The sentence this comment used to end
        # on -- matched as a command rather than as the word -- described a
        # pattern the file no longer contains.
        #
        # What the row pins now was settled by injecting the step and
        # reading which assertion fires rather than by reasoning about it:
        # the `_SAFE_SCRIPT_EXPRESSIONS` scan, `[] != ['github.event.after']`.
        # That scan reads the step's `env:` values as well as its script, so
        # the verb carries no weight here at all -- it is the pretext the
        # row wears, and the property under test is that an event field on
        # no allowlist is refused wherever a step carries it.
        "B4-pull-request-target-run-pull",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          REV: ${{ github.event.after }}\n"
         '        run: git pull origin "$REV"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "merge the pull request's head into the working tree with the one "
        "git verb the fetch list did not name",
    ),
    Mutation(
        # The allowlist reads `${{ ... }}` with a regex, and a regex without
        # `re.DOTALL` cannot see an expression with a newline in it. A block
        # scalar keeps the line break exactly as written, so `findall`
        # returned nothing, the allowlist loop never ran, `sub` removed
        # nothing, and the whole expression arrived at the literal scan as
        # text -- where `github.event.after` spells none of `pull`, `head` or
        # `merge`. The same ref in a double-quoted scalar folds to one line
        # and was always caught, which is what made this one quiet.
        "B4-pull-request-target-checkout-ref-newline",
        ".github/workflows/risk_classify.yml",
        ("          ref: ${{ github.event.repository.default_branch }}",
         "          ref: |\n"
         "            ${{ format('{0}',\n"
         "            github.event.after) }}"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "reflow a long interpolated ref across two lines, which a block "
        "scalar makes look like ordinary YAML tidying",
    ),
    Mutation(
        # A ref that is present and says nothing. `ref:` with no value is
        # valid YAML and parses to None; `str(None)` is `"None"`, which is
        # truthy, so the must-carry-a-`ref:` rule was satisfied by a checkout
        # that carries no ref, and `"none"` holds none of the three words the
        # literal scan looks for. The honest spelling, `ref: ""`, reddened.
        "B4-pull-request-target-checkout-ref-null",
        ".github/workflows/risk_classify.yml",
        ("          ref: ${{ github.event.repository.default_branch }}",
         "          ref:"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "delete a ref's value without deleting its key, which is what a "
        "half-finished edit leaves behind and what the default-branch "
        "fallback quietly restores",
    ),
    Mutation(
        # The same step over two lines. The `git pull` alternative was
        # matched on a single line by construction, and one backslash was
        # all it took to spell the same command over two; that alternative
        # went with the rest of the verb list, and what catches this step
        # now is `B4-pull-request-target-run-pull`'s assertion on the
        # identical `github.event.after`. Three rows above rather than the
        # row above: `checkout-ref-newline` and `checkout-ref-null` sit
        # between, and neither carries that expression at all. Measured by
        # injecting both steps and reading which assertion fires -- `[] !=
        # ['github.event.after']` for each.
        #
        # So this row does not pin the continuation join either, and saying
        # it did was the second half of the same stale comment. Measured:
        # neuter `_LINE_CONTINUATION` and one B4 row flips, and it is
        # `run-refspec-continued` rather than this one -- a refspec really
        # does have to be read across the backslash, and a step whose `env:`
        # carries the ref does not. Kept as the two-line spelling of
        # `B4-pull-request-target-run-pull`, on the argument that a
        # continuation is how anybody writes a git command with more flags
        # than fit.
        "B4-pull-request-target-run-pull-continued",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          REV: ${{ github.event.after }}\n"
         "        run: |\n"
         "          git \\\n"
         '            pull origin "$REV"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "wrap a `git pull` over two lines with a backslash, which reads as "
        "line-length housekeeping and unmatches a single-line pattern",
    ),
    Mutation(
        # A refspec that names no ref. `+refs/pull/${N}/*:refs/remotes/pr/*`
        # copies the whole namespace onto the runner and the checkout of
        # `pr/head` happens on the next line, where a pattern needing
        # `pull/` and `head` on one line can never join them. Refused on the
        # namespace now rather than on the two ref names.
        "B4-pull-request-target-run-refspec-wildcard",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          git fetch origin"
         " \"+refs/pull/${PR_NUMBER}/*:refs/remotes/pr/*\"\n"
         "          git checkout pr/head\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "fetch the pull request namespace with one wildcard refspec, which "
        "is how anybody mirrors several pull requests at once, and check the "
        "branch out by its local name on the next line",
    ),
    Mutation(
        # The glob without the `refs/` prefix. `git ls-remote` matches a
        # pattern against the tail of a refname, so `pull/N/h*` resolves
        # `refs/pull/N/head` while spelling neither the namespace nor the
        # ref, and what comes back is the SHA the fetch on the next line
        # wants.
        #
        # It pins neither rule it is refused by, and the row below is where
        # that was found out. Measured one at a time: neuter the third
        # alternative of `_PULL_REQUEST_REF` and this row is still KILLED,
        # because `_REMOTE_REF_ENUMERATION` reads the `ls-remote` on the same
        # line; neuter `ls-remote` and it is still KILLED on the glob; neuter
        # both and it goes SURVIVED. The third alternative is pinned by the
        # row below, which writes the same glob with no enumeration verb next
        # to it.
        "B4-pull-request-target-run-refspec-ls-remote",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          SHA=$(git ls-remote origin"
         " \"pull/${PR_NUMBER}/h*\" | cut -f1)\n"
         "          git fetch origin \"$SHA\" && git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "resolve the head with `ls-remote` before fetching it, which is the "
        "careful way to write a fetch that must not fail on a closed pull "
        "request",
    ),
    Mutation(
        # The same glob with no enumeration verb on the line, which is the
        # row that pins the third alternative. The row above does not, and
        # no longer says it does: the row above was corrected in ddd347b9
        # and this sentence went on citing it as a live error afterwards.
        # What it got wrong: `_REMOTE_REF_ENUMERATION` reads `ls-remote` and
        # decides that one first, and deleting the glob alternative outright
        # left all four refspec rows KILLED and the whole sweep
        # byte-identical -- the same class round 11 found once and this is
        # twice. `git rev-parse --glob=` prepends `refs/` itself, so
        # `pull/N/h*` resolves `refs/pull/N/head` out of whatever refs the
        # runner's clone already has, with no `refs/` prefix, no `head`, and
        # nothing that asks the remote anything. Measured against git: the
        # short globbed form is a pattern `git ls-remote` and `git rev-parse
        # --glob` both take and `git fetch` does not -- a `+pull/N/*:...`
        # refspec is accepted and silently matches nothing -- so this is the
        # spelling, not a second one.
        "B4-pull-request-target-run-refspec-glob-rev-parse",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          REV=$(git rev-parse --glob=\"pull/${PR_NUMBER}/h*\")\n"
         "          git checkout --detach \"$REV\" && ./ci.sh\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "resolve the head out of the refs already on the runner with a glob "
        "rather than by asking the remote, which is the form somebody "
        "writes when the fetch is somebody else's step",
    ),
    Mutation(
        # The short-form refspec split across a backslash continuation, and
        # the only row that reads `_join_continuations`. The namespace
        # alternative does not see this one: there is no `refs/` prefix, and
        # `pull/${N}/` and `head` are on different lines until the join puts
        # them back together the way the shell does. Distinct from
        # B4-pull-request-target-run-pull-continued, which wraps the *verb*
        # and is caught by the expression allowlist instead.
        "B4-pull-request-target-run-refspec-continued",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          git fetch origin pull/${PR_NUMBER}/\\\n"
         "          head\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "wrap a long refspec over two lines with a backslash, which is "
        "line-length housekeeping everywhere else in this repository",
    ),
    Mutation(
        # The namespace reached without being spelled, and the first of
        # the nine rows over `_REMOTE_REF_ENUMERATION`. Nine is measured
        # rather than counted by eye, and this comment said two until
        # round 26: neuter that rule and the sweep at `82d2b964` turns
        # exactly nine rows SURVIVED, in table order this one,
        # B4-pull-request-target-run-ls-remote-quoted-word,
        # B4-pull-request-target-run-wildcard-namespace-refspec,
        # B4-pull-request-target-run-smart-http-advertisement,
        # B4-pull-request-target-run-matching-refs-endpoint,
        # B4-pull-request-target-run-git-refs-endpoint,
        # B4-pull-request-target-run-ref-endpoint-encoded-letters,
        # B4-pull-request-target-run-clone-mirror and
        # B4-pull-request-target-run-clone-mirror-abbreviated. Every
        # refspec row above writes `pull` somewhere, which is what the
        # three alternatives of `_PULL_REQUEST_REF` read. A bare `git
        # ls-remote origin` writes none of it: the remote advertises
        # `refs/pull/N/head` to anyone who asks, `grep` picks this pull
        # request's line out of the listing, and `git fetch origin "$SHA"`
        # is a fetch by object name the server serves. Distinct from
        # B4-pull-request-target-run-refspec-ls-remote, which passes the
        # remote a `pull/N/h*` pattern and is caught by the ref half for
        # spelling it -- neuter the enumeration rule and that
        # row is still KILLED while this one goes SURVIVED, measured in the
        # same sweep, which is what separates the pair.
        "B4-pull-request-target-run-ls-remote-unfiltered",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          SHA=$(git ls-remote origin"
         " | grep \"/$PR_NUMBER/head\" | cut -f1)\n"
         "          git fetch origin \"$SHA\" && git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "list what the remote advertises and filter it locally, which is "
        "what somebody writes when they do not want to depend on the "
        "server's pattern matching",
    ),
    Mutation(
        # The same listing with the quoting inside the command's name, and
        # the second of the three. A shell drops a quote between two letters
        # of a word it is not part of, so `git ls-remo""te origin` runs the
        # row above's command and `\bls-remote\b` sees a quote where it
        # wanted a word boundary. Green at `d060691d`. It pins the
        # `_shell_word` treatment of the `ls-remote` alternative; the
        # `/info/refs` and `git-upload-pack` alternatives took the same
        # treatment in the same change and are pinned by no row, which is
        # written down rather than claimed here.
        "B4-pull-request-target-run-ls-remote-quoted-word",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         '          SHA=$(git ls-remo""te origin'
         ' | grep "/$PR_NUMBER/head" | cut -f1)\n'
         '          git fetch origin "$SHA" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "quote two letters of the subcommand's name, which the shell drops "
        "and the pattern over the letters could not see past",
    ),
    Mutation(
        # The same reach with the enumeration done by the fetch. `+refs/*`
        # copies every namespace the remote has onto the runner, the pull
        # namespace among them, and `git for-each-ref` then reads the head
        # out of the local copy -- so the only thing naming `pull` is a
        # `grep` pattern built from the number. This is the row that pins the
        # second alternative: the rule refuses a refspec that globs *before*
        # the namespace is fixed, and `refs/heads/*` a line away is an
        # ordinary mirror fetch it leaves alone.
        "B4-pull-request-target-run-wildcard-namespace-refspec",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          git fetch origin \"+refs/*:refs/remotes/all/*\"\n"
         "          SHA=$(git for-each-ref"
         " | grep \"/$PR_NUMBER/head\" | cut -d' ' -f1)\n"
         "          git checkout \"$SHA\"\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "mirror the remote's whole ref space in one fetch, which is the "
        "shape of a workflow that wants notes, tags and branches together, "
        "and read the head back out of it",
    ),
    Mutation(
        # The advertisement asked for without `git`. `info/refs` with
        # `service=git-upload-pack` is the wire protocol underneath
        # `ls-remote`: unauthenticated, and it answers with the same listing,
        # so `grep "/$PR_NUMBER/head"` reads the head out of it and the fetch
        # by object name follows. Refusing the command while conceding the
        # request it makes would have been a rule about which program is
        # installed rather than about what the step reaches, which is why the
        # path and the service name are their own alternatives.
        "B4-pull-request-target-run-smart-http-advertisement",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          SHA=$(curl -fsSL"
         " \"https://github.com/${{ github.repository }}.git"
         "/info/refs?service=git-upload-pack\""
         " | grep \"/$PR_NUMBER/head\" | cut -c5-44)\n"
         "          git fetch origin \"$SHA\" && git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "ask the remote for its ref advertisement over plain HTTP, which "
        "needs no token, no `gh` and no `git` subcommand the rule above "
        "knows by name",
    ),
    Mutation(
        # The advertisement over REST. `git/matching-refs/{ref}` returns
        # every ref beginning with what is asked for, so a truncated prefix
        # is the whole pull namespace in one authenticated request -- no
        # `ls-remote`, no `*`, no `/info/refs`, no `--mirror`, and a verb and
        # a variable both on their allowlists. `pul` rather than `pull` is
        # the point: a rule pinned to the namespace would be walked past by
        # dropping a letter, or by asking for the empty prefix.
        "B4-pull-request-target-run-matching-refs-endpoint",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         '          S=$(gh api "repos/$GITHUB_REPOSITORY/git/matching-refs'
         "/pul\" --jq '.[0].object.sha')\n"
         '          git fetch origin "$S" && git reset --hard FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "ask the REST endpoint that prefix-matches refs for a prefix of the "
        "pull namespace, which advertises it without naming it",
    ),
    Mutation(
        # And the legacy spelling of the same endpoint, which prefix-matches
        # the same way and is what the older documentation and every
        # half-remembered example write. Not its own alternative -- this
        # comment said so until round 20 and the regex has never had one.
        # There is a single alternative, and it is read off the pattern
        # rather than off this comment:
        # `(?<![\w.])git/(?:matching-refs\b|refs\b|ref/(?!(?:heads|tags)/))`
        # at `82d2b964`, one alternative shared with the exact-match
        # endpoint and three branches inside it. It was
        # `git/(?:matching-)?refs\b` until `50b7d89a` widened it, which is
        # the spelling this comment described until round 26 -- the
        # optional group is gone and the two prefix-matching spellings are
        # written out as branches of their own.
        #
        # Its own *row*, though, because `git/refs` is not a substring of
        # `git/matching-refs` and neither spelling covers the other, so
        # each branch needs pinning. Measured both ways at `82d2b964`:
        # delete the `refs\b` branch and this row is SURVIVED with
        # B4-pull-request-target-run-matching-refs-endpoint still KILLED;
        # delete the `matching-refs\b` branch instead and the two verdicts
        # swap.
        "B4-pull-request-target-run-git-refs-endpoint",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         '          S=$(gh api "repos/$GITHUB_REPOSITORY/git/refs'
         "/pul\" --jq '.[0].object.sha')\n"
         '          git fetch origin "$S" && git reset --hard FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "ask the older spelling of the same prefix-matching endpoint, which "
        "returns the identical listing",
    ),
    Mutation(
        # The safe side of both, and the reason the alternative carries a
        # lookbehind. `.git/refs/heads/main` is a file in the checkout this
        # workflow already has, and reading it reaches nothing the base
        # repository did not decide -- the `.` in front is the entire
        # difference between it and a request to GitHub. OVERSHOT here means
        # the endpoint has been refused as a bare `refs` path, which reds on
        # every step that reads its own git directory.
        "B4-pull-request-target-run-local-ref-file",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Record the base commit\n"
         "        run: cat .git/refs/heads/main > base.sha\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read a ref out of the checkout's own git directory, which is a "
        "path on disk rather than a request to the remote",
        must_survive=True,
    ),
    Mutation(
        # The ref written as a shell word rather than as a run of letters,
        # which is the round-23 hole and the round-16 one arriving at the
        # other rule. A shell drops a backslash before it hands git the
        # word, so `pull/$PR_NUMBER/he\ad` fetches `refs/pull/N/head` -- the
        # number is on the expression allowlist because two carriers label
        # with it, the token is in the job, and the backstop that is supposed
        # to make that concession safe was reading the four letters and the
        # shell was reading a word. `he""ad`, `'he'ad` and
        # `"pull/$PR_NUMBER/h"ead` are the same step and were green beside
        # it.
        #
        # It pins `_shell_word` inside `_PULL_REQUEST_REF`. Make it return
        # its argument unchanged and this row is SURVIVED with every other
        # ref row still KILLED, including the two below, which carry no
        # quote. Four rows in the whole sweep flip on that neuter at
        # `82d2b964`, measured: this one and the `-quoted-word` row over
        # each of the three other rules that call the helper --
        # `ls-remote-quoted-word`, `api-pulls-quoted-word` and
        # `event-file-quoted-word`.
        #
        # The control beneath them is what says the fix is the word and not
        # the boundary. `_shell_word` writes no *trailing* run of the
        # quoting class, and the reason is not that a trailing run would let
        # a quote stand in for `\b` -- this comment said that until round
        # 26, and it is the claim `_shell_word`'s docstring retracts. `\b`
        # already falls between a word's last letter and a quote, so the run
        # changes no verdict either way: add one to every word of
        # `_PULL_REQUEST_REF` and the sweep is identical row for row
        # (killed=238 noisy=22 survived=0), and the only text that reads
        # differently is the span of the match on `pull/$N/head"x"`. It is
        # left off as the run that buys nothing, not as one that would cost
        # something; `_enumeration_word` adds one because its terminator is
        # a character class rather than `\b`.
        "B4-pull-request-target-run-ref-quoted-word",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          git fetch --depth=1 origin pull/$PR_NUMBER/he\\ad\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "escape one letter of the ref, which a shell drops before git sees "
        "the word and which reads as a stray keystroke in a diff",
    ),
    Mutation(
        # The same ref with the slashes escaped instead of the letters.
        # GitHub's REST API takes a percent-encoded slash inside a `{ref}`
        # path parameter, so `repos/O/R/commits/pull%2FN%2Fhead` is a request
        # for the fork's head commit that writes no `pull/` anywhere, and
        # `/commits/` is not on `_PULL_REQUEST_API`'s path list because a
        # commit by SHA is what half this repository's workflows ask for.
        # It was green.
        #
        # It pins `_REF_SEPARATOR`. Cut it back to a literal `/` and this row
        # is SURVIVED with the quoted row above and the endpoint row below
        # both still KILLED. Aimed at `/commits/` rather than at `git/ref/`
        # deliberately: the endpoint concession below is narrowed by its own
        # rule, so a row written there would be killed twice over and would
        # pin neither half.
        "B4-pull-request-target-run-ref-percent-encoded-slash",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         '          S=$(gh api "repos/$GITHUB_REPOSITORY/commits'
         '/pull%2F$PR_NUMBER%2Fhead" --jq .sha)\n'
         '          git fetch origin "$S" && git reset --hard FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "percent-encode the slashes of the ref, which the API decodes and "
        "which looks like ordinary URL hygiene",
    ),
    Mutation(
        # The exact-match ref endpoint, which `_REMOTE_REF_ENUMERATION` used
        # to concede whole. It cannot enumerate, which is why it was left
        # green -- but it resolves whatever ref it is handed, and
        # `refs/pull/N/head` is a ref. The percent encoding here is over the
        # *letters* rather than the slashes: `%70ull` is `pull` and `%68ead`
        # is `head` once the server has decoded the path, so no pattern over
        # the four letters of either word reaches this request, however much
        # quoting or slash-spelling it reads. That is why the concession is
        # narrowed instead of defended by a second backstop.
        #
        # It pins the `ref/(?!(?:heads|tags)/)` alternative. Put the
        # alternative back as `git/(?:matching-)?refs` and this row is
        # SURVIVED with the two rows above still KILLED; the control below is
        # the other side of it.
        "B4-pull-request-target-run-ref-endpoint-encoded-letters",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         '          S=$(gh api "repos/$GITHUB_REPOSITORY/git/ref'
         '/%70ull/$PR_NUMBER/%68ead" --jq .object.sha)\n'
         '          git fetch origin "$S" && git reset --hard FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "ask the exact-match ref endpoint for the pull ref with its letters "
        "percent-encoded, which the server decodes and no pattern over the "
        "word reads",
    ),
    Mutation(
        # The safe side of the row above, and the concession the narrowing
        # keeps. Reading one ref by its full name over REST is the thing
        # `git/ref/` is for, and `heads/` and `tags/` are the two namespaces
        # a job here has any business resolving -- neither can be written by
        # a fork, both are exactly what a base-commit or a release step asks
        # for. OVERSHOT here means the endpoint has been refused outright,
        # which leaves the suite telling its author to enumerate instead.
        "B4-pull-request-target-run-ref-endpoint-namespaced",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Record the base commit\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         '          gh api "repos/$GITHUB_REPOSITORY/git/ref/heads/main"'
         " --jq .object.sha > base.sha\n"
         '          gh api "repos/$GITHUB_REPOSITORY/git/ref/tags/v1.0.0"'
         " --jq .object.sha > tag.sha\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "resolve the default branch and a tag by their full names over the "
        "exact-match endpoint, which is what that endpoint is conceded for",
        must_survive=True,
    ),
    Mutation(
        # The safe side of the quoted-word row, and the reason the quoting
        # went into the word rather than into the terminator. Every line here
        # writes one of the four ref words next to a quote and none of them
        # is a ref: the web link ends at the pull request's `files` tab, the
        # `jq` filter indexes two fields of a report by name, and `head -3`
        # is the end of a pipeline.
        #
        # What reaches this step is measured rather than reasoned, and
        # round 26 corrected it: no single weakening of `_PULL_REQUEST_REF`
        # measured at `82d2b964` does. Let the `\b` after `head|merge` go
        # and this row is SURVIVED (expected); let the `[^\n]*?` between
        # the ref's segments run across a newline and it is SURVIVED
        # (expected); widen `_REF_SEPARATOR` from a slash to any
        # punctuation and it is SURVIVED (expected). A trailing run of the
        # quoting class is the row above's note and moves no verdict
        # anywhere in the sweep. The cross-newline half is what this
        # comment credited on its own until round 26, and on its own it
        # matches nothing here.
        #
        # It takes two at once. Cross the newline *and* widen the separator
        # and this row is OVERSHOT, on the first line's `pull/$PR_NUMBER/`
        # reaching the `head` of the *second* line's `.["head"]`, where the
        # `["` stands in for the slash. Not the third line's `head -3`: a
        # space is in front of that one, which no widening of a separator
        # written as punctuation reaches.
        "B4-pull-request-target-run-ref-quoted-word-ordinary",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Summarise the report\n"
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         '          echo "the diff is at $GITHUB_SERVER_URL'
         '/$GITHUB_REPOSITORY/pull/$PR_NUMBER/files"\n'
         "          jq -r '.[\"head\"], .[\"merge\"]' ./risk-report.json"
         " || true\n"
         "          git log --format='%h' -n 5 | head -3\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "link the pull request's files tab, read two fields out of a report "
        "by quoted name and end a pipeline with `head`, none of which is a "
        "ref",
        must_survive=True,
    ),
    Mutation(
        # `printenv NAME` is a read of NAME that never writes `$NAME`. The
        # pickup was keyed on the sigil, so the value was never folded in and
        # the fetch read as innocent.
        "B4-pull-request-target-run-printenv",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          REV: ${{ github.event.after }}\n"
         "        run: |\n"
         '          git fetch --depth=1 origin "$(printenv REV)"\n'
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the laundered ref with `printenv` rather than with a sigil, "
        "which is the same read spelled as a command",
    ),
    Mutation(
        # Indirect expansion: `${!PTR}` is the value of the variable *named*
        # by PTR. Following it was a hop the `$NAME` pickup of the time did
        # not take -- it folded in PTR, whose value is the string `REV`, and
        # nothing in the haystack then named the head.
        #
        # This comment used to say the row pinned the environment refusal,
        # and it does not. Measured 2026-09-19: the expression allowlist
        # fires first, on `github.event.after` inside REV's value, and
        # deleting the environment refusal outright leaves this row KILLED.
        # The shape is still worth a row, because it is a read no pickup
        # keyed on a name can follow -- but the pin it was credited with
        # belongs to the row below, which is the only one here whose `env:`
        # carries the head with no expression in it for the allowlist to see.
        #
        # `_INDIRECT_EXPANSION` arrived 2026-09-19 and refuses `${!PTR}` on
        # sight, so this row now has two killers and still pins neither: the
        # allowlist fires first either way. The rule's own pin is
        # `B4-pull-request-target-run-indirect-assembled-name`, whose `env:`
        # holds no expression at all.
        "B4-pull-request-target-run-indirect-expansion",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          REV: ${{ github.event.after }}\n"
         "          PTR: REV\n"
         "        run: |\n"
         '          git fetch --depth=1 origin "${!PTR}"\n'
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "name the variable that names the ref, so the script mentions the "
        "pointer and never the pull request",
    ),
    Mutation(
        # The environment refusal, and the only row that pins it. Every other
        # candidate carries the head as `${{ ... }}`, which the expression
        # allowlist refuses before this rule is reached, so deleting the rule
        # leaves all of them KILLED and the pin is imaginary. Here the value
        # is a literal refspec -- no expression at all -- and the script never
        # names REV, so nothing folds the value in and the text the two
        # literal backstops read over the script holds one line of shell that
        # mentions nothing. What is left is the question the refusal
        # exists for: this step's environment holds the pull request's
        # head, and whether the Python file it runs reads it is not a thing
        # this file can know.
        # Measured both ways -- KILLED as it stands, SURVIVED with the
        # `carried` assertion deleted.
        "B4-pull-request-target-run-carried-literal",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          REV: refs/pull/42/head\n"
         "        run: python3 .ci/fetch.py\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "hand the refspec to a helper script through the environment, which "
        "is tidier than interpolating it and hides it from every scan here",
    ),
    Mutation(
        # The pull request over HTTP, which is the channel the number opened.
        # Deleting the fetch gate meant every step is read, and two
        # expressions had to go on the script allowlist for the carriers to
        # stay green; one of them is the pull request's number, and the number
        # plus the token is all `gh pr checkout` takes. This row and the three
        # below are four spellings of that reach, and only one of them is
        # `_PULL_REQUEST_API`'s: measured, neutering that rule outright leaves
        # this row, `gh pr diff` and `gh pr view` all KILLED and sends
        # `api-gh-api-pulls` alone to SURVIVED, and putting `checkout`, `diff`
        # and `view` on `_SAFE_GH_PULL_REQUEST_SUBCOMMANDS` instead does the
        # opposite. So what refuses a `gh pr` subcommand is that list, read
        # twice -- once under the verb allowlist and once under the
        # argument-shape backstop -- and the path rule reads the `gh api` row
        # only. The four exist because the concession is a real one: without a
        # backstop under the fetch gate these three were refused at 866fa939
        # and passed after it, which is a coverage loss a row has to be able
        # to see.
        "B4-pull-request-target-api-gh-pr-checkout",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          N: ${{ github.event.pull_request.number }}\n"
         "        run: gh pr checkout \"$N\"\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "let the CLI do the fetch and the checkout in one word, so the step "
        "names no ref and no remote",
    ),
    Mutation(
        # The same call spelled as a URL rather than as a subcommand, which is
        # why the backstop reads `/pulls/` as well as the four verbs. `curl`
        # against api.github.com is this row with the client swapped.
        "B4-pull-request-target-api-gh-api-pulls",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         "          REV=$(gh api \"repos/${{ github.repository }}"
         "/pulls/${{ github.event.pull_request.number }}\" --jq .head.sha)\n"
         "          git fetch --depth=1 origin \"$REV\"\n"
         "          git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "ask the API for the head rather than reading it off the event, so "
        "the SHA arrives over HTTP and no expression names it",
    ),
    Mutation(
        # The fork's code without its history. A diff applied to the base tree
        # is the same arbitrary code arriving, and it was green before this
        # round as well as after the number went on the allowlist -- the one
        # shape here the backstop closed rather than reopened.
        "B4-pull-request-target-api-gh-pr-diff",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         "          gh pr diff ${{ github.event.pull_request.number }}"
         " | git apply\n"
         "          ./run.sh\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "apply the diff instead of checking the branch out, which touches no "
        "remote at all",
    ),
    Mutation(
        # The head without the code, which is enough: the SHA is what the
        # fetch on the next line needs. `gh pr view` is on the backstop for
        # this and not because viewing is dangerous.
        #
        # Two rules read it since round 18 rather than one, and both read
        # the same list. The `pr` sits inside a `$(...)` whose enclosing
        # word is `REV=`, which is no name, so `_receiving_programs` hands
        # the argument backstop an unreadable program and it refuses `view`
        # for not being in `_SAFE_GH_PULL_REQUEST_SUBCOMMANDS` -- the
        # allowlist `_unsafe_gh_pull_request_commands` was already refusing
        # it against. Re-proved against the anchor rather than assumed: put
        # `view` on that list and this row is SURVIVED, because the constant
        # is what both readers read.
        "B4-pull-request-target-api-gh-pr-view",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         "          REV=$(gh pr view ${{ github.event.pull_request.number }}"
         " --json headRefOid --jq .headRefOid)\n"
         "          git fetch origin \"$REV\"\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the head out of the CLI's JSON, which is the API call with a "
        "friendlier front end",
    ),
    Mutation(
        # The subcommand the three-verb backstop did not name. `gh pr list`
        # returns every open pull request with `--json headRefOid`, and
        # filtering the array by number is one `--jq` away -- so this is `gh
        # pr view` with the lookup done client-side, past a pattern that read
        # `checkout`, `diff` and `view` and nothing else. The four rows here
        # and below are why that pattern is now an allowlist: this one, the
        # collection endpoint, the interposed flag and the client library
        # were all green at 5863df72. Read twice since round 18, for the
        # reason B4-pull-request-target-api-gh-pr-view is: the `pr` is
        # inside a `REV=$(...)` and the argument backstop reaches it too.
        # Re-proved the same way -- put `list` on
        # `_SAFE_GH_PULL_REQUEST_SUBCOMMANDS` and this row is SURVIVED,
        # because the allowlist is what both readers read.
        "B4-pull-request-target-api-gh-pr-list",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          REV=$(gh pr list --state all --json number,headRefOid"
         " --jq \".[] | select(.number == $PR_NUMBER) | .headRefOid\")\n"
         "          git fetch origin \"$REV\" && git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "ask the list endpoint for the head rather than the item endpoint, "
        "which is one subcommand's difference and reads as a survey of open "
        "pull requests rather than a lookup of this one",
    ),
    Mutation(
        # The same request as B4-pull-request-target-api-gh-api-pulls with
        # the trailing slash gone. `/pulls?state=all` is the collection, and
        # it hands back every head the item endpoint would -- the pattern
        # wanted `/pulls/`, so a `?` where a `/` was expected was the whole
        # evasion. The path segment is matched on its own boundary now.
        "B4-pull-request-target-api-pulls-collection",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         "          REV=$(gh api \"repos/${{ github.repository }}"
         "/pulls?state=all\" --jq '.[0].head.sha')\n"
         "          git fetch origin \"$REV\" && git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "page the pulls collection instead of naming a number, which is what "
        "a workflow that handles several pull requests at once would write",
    ),
    Mutation(
        # The same collection path with the shell's quoting inside it, and
        # the first of three rows over the same finding. `_PULL_REQUEST_REF`
        # has read a shell word since round 23 on the argument that the text
        # and what the shell hands the program are not the same string;
        # `_PULL_REQUEST_API` went on reading letters, so `/pul""ls` is the
        # row above's request character for character by the time `gh` is
        # handed it -- the two quotes come off between the two `l`s -- and it
        # was green at `d060691d`. It pins the `_shell_word` treatment of the
        # `/pulls` alternative: put the literal back and this row is the only
        # one in the table that moves.
        "B4-pull-request-target-api-pulls-quoted-word",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         '          SHA=$(gh api "repos/$GITHUB_REPOSITORY/pul""ls/42"'
         " --jq .head.sha)\n"
         '          git fetch --depth=1 origin "$SHA"\n'
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "close and reopen the quoting in the middle of the path, which is "
        "one character of shell syntax and the same request",
    ),
    Mutation(
        # `gh pr checkout` with a flag in front of the verb. `-R` is a
        # persistent flag and the CLI strips flags before it resolves the
        # subcommand, so this runs exactly what
        # B4-pull-request-target-api-gh-pr-checkout runs -- but a pattern
        # anchored on `pr\s+checkout` sees `pr -R` and reports nothing. This
        # row is the argument for walking the words rather than matching
        # them, and it is the one that has to stay green if anybody puts the
        # regex back.
        "B4-pull-request-target-api-gh-pr-interposed-flag",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: gh pr -R \"${{ github.repository }}\" checkout"
         " \"$PR_NUMBER\"\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "name the repository explicitly on a checkout that already worked, "
        "which is the tidying a reviewer asks for when a step runs `gh` "
        "outside a checked-out tree",
    ),
    Mutation(
        # The namespace indexed rather than accessed. One quote is the whole
        # difference from B4-pull-request-target-api-octokit-pulls, which is
        # why _PULL_REQUEST_API reads `rest` and the punctuation after it
        # instead of a literal `rest.pulls`. Without that this row survives
        # and B4-pull-request-target-api-octokit-pulls still kills, so the
        # pair is what pins the difference rather than either one alone.
        # Measured again in round 18: the plain-dot spelling leaves this row
        # SURVIVED and that one KILLED. The row it names sits two rows
        # *below* this one rather than above it, which is what this comment
        # said until then -- counting neighbours is how a comment goes wrong
        # when a row is inserted between them, so it names the id now.
        "B4-pull-request-target-api-octokit-pulls-indexed",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            const { data } = await github.rest['pulls'].get({\n"
         "              ...context.repo,\n"
         "              pull_number: ${{ github.event.pull_request.number }}"
         "\n"
         "            });\n"
         "            await exec.exec('git', ['fetch','origin',"
         " data.head.sha]);\n"
         "            await exec.exec('git', ['checkout','FETCH_HEAD']);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "reach the same namespace through a string index, which is ordinary "
        "JavaScript and defeats a rule written against a dot",
    ),
    Mutation(
        # The flag-value skip, from the other side: this is a *legitimate*
        # label write with `-R` interposed, and the suite must stay green on
        # it. Delete _GH_VALUE_FLAGS or the expression collapse and the walk
        # reads the repository as the subcommand, refuses it, and this row
        # reds -- which is backwards for a mutation, so the row inverts: the
        # edit is the safe spelling and `must_survive` says the suite has no
        # business objecting to it. Without this, nothing pins the skip and a
        # future round could delete it and see a clean sweep.
        "B4-pull-request-target-api-gh-pr-interposed-flag-write",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Label the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         "          gh pr -R \"${{ github.repository }}\" edit"
         " ${{ github.event.pull_request.number }} --add-label triage\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "name the repository explicitly on a label write, which is the same "
        "command the live carrier runs with one persistent flag in front of "
        "the verb",
        must_survive=True,
    ),
    Mutation(
        # The same endpoint from the language the shell rules cannot read.
        # `actions/github-script` hands the step an authenticated Octokit as
        # `github`, so this names no `gh`, no `/pulls` path and no refused
        # expression: `context.repo` is on _SAFE_SCRIPT_CONTEXTS and the
        # number is on _SAFE_SCRIPT_EXPRESSIONS. `data.head.sha` is a
        # property of the response rather than of the event, so
        # _PULL_REQUEST_HEAD does not see it either. Pinned to the real
        # action's SHA, like B4-pull-request-target-script-input: an unpinned
        # one would trip C4's sweep and the verdict would stop saying which
        # rule caught this.
        "B4-pull-request-target-api-octokit-pulls",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            const { data } = await github.rest.pulls.get({\n"
         "              ...context.repo,\n"
         "              pull_number: ${{ github.event.pull_request.number }}"
         "\n"
         "            });\n"
         "            await exec.exec('git', ['fetch','origin',"
         " data.head.sha]);\n"
         "            await exec.exec('git', ['checkout','FETCH_HEAD']);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "use the client the action already hands the script, rather than "
        "shelling out to `gh` -- which is the idiomatic way to write this "
        "step and the way that names none of the nouns the shell rules read",
    ),
    Mutation(
        # The program name in quotes, which is what `_GH_COMMAND`'s
        # whitespace lookahead never saw. A shell strips the quotes off a
        # bare word before it execs, so this runs exactly what
        # B4-pull-request-target-api-gh-pr-checkout runs -- but the character
        # after `gh` is an apostrophe, the pattern wanted a space, and the
        # walk was never handed the invocation at all. `gh'' pr checkout` is
        # the same trick with the quotes empty and is not a second row: both
        # die on the lookahead and nothing separates them.
        "B4-pull-request-target-api-gh-quoted-command",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          'gh' pr checkout \"$PR_NUMBER\"\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "quote the program name, which a shell-quoting pass over a script "
        "does to every word it is not sure about",
    ),
    Mutation(
        # The same command as an argument vector, which is the ordinary way
        # to run a program from a `script:` input rather than an
        # obfuscation: `actions/github-script` hands the body an `exec` and
        # this is what its documentation shows. What catches it is the
        # widened `_GH_COMMAND` lookahead alone -- the character after `gh`
        # is a quote followed by a comma, and without that the walk is never
        # handed the invocation. Measured, because the obvious reading is
        # wrong: revert `_GH_WORD_PUNCTUATION` to the old quote-only strip
        # and this row still KILLS, but on a word nobody would guess. The
        # walk starts after the letters `gh`, so its first word is what is
        # left of `', ` -- a bare `,` once the quote comes off -- and *that*
        # is the verb, refused for not being on `_SAFE_GH_VERBS`. `['pr',`
        # is one place further along, in the subcommand's seat, and is never
        # consulted because the verb failed first. The punctuation
        # strip earns its place on the safe side instead, at
        # B4-pull-request-target-api-gh-argv-vector-write. Pinned to the
        # real action's SHA, because an unpinned one would trip C4's sweep
        # and the verdict would stop saying which rule caught this.
        #
        # It is not the only row that runs an action, and counting them by
        # eye is what went wrong here twice. Counted over the table at
        # `82d2b964`: twenty-three rows insert a `uses:`, twenty-two of
        # them at a forty-character SHA and the twenty-third at a local
        # `./.github/workflows/` reusable job, and eighteen of the
        # twenty-three -- this one among them -- run actions/github-script
        # at the same pin written here. This comment said "the two rows
        # above" until round 18 and then named
        # B4-pull-request-target-api-octokit-pulls and
        # B4-pull-request-target-api-octokit-pulls-indexed as the only two
        # others until round 26. The row immediately above is
        # B4-pull-request-target-api-gh-quoted-command, which is a `run:`
        # step with no `uses:` in it, and that much is still true.
        "B4-pull-request-target-api-gh-argv-vector",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        with:\n"
         "          script: |\n"
         "            await exec.exec('gh', ['pr', 'checkout',"
         " '${{ github.event.pull_request.number }}']);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "run the CLI the way the action's own documentation runs a program, "
        "as a name and a list of arguments rather than as a command line",
    ),
    Mutation(
        # The program name written as a command substitution rather than as
        # a word. `command -v gh` resolves the path and the substitution
        # runs it, so the character after the letters `gh` is a `)`. Nothing
        # over the program *name* can close the general case: a shell has
        # unlimited ways to spell one, and `"$GH"` and `g'h'` in the rows
        # below are two more, which is what `_GH_PULL_REQUEST_WORD` is for.
        #
        # This said the shape came from "four of this repository's own
        # `scripts/release/*.sh`", and the pretext below said the release
        # scripts invoke `gh` this way. Both were wrong, corrected
        # 2026-09-21. Four release scripts do write `command -v gh` --
        # dispatch_nightly_pipeline.sh:19, dispatch_release_pipeline.sh:21,
        # publish_github_release.sh:79 and verify_release_eligibility.sh:96
        # -- and all four write it as a presence guard, `command -v gh
        # >/dev/null 2>&1`, then invoke the CLI as the bare word `gh`. No
        # file in this repository runs anything as `"$(command -v gh)"`; the
        # nearest real idiom is pr_evidence_screenshot.sh:145, which
        # resolves chromium that way into a variable it runs later. The
        # shape is a plausible thing for a contributor to write. It is not a
        # thing written here, and the row does not need it to be.
        #
        # This row does not pin it, and that is the round-17 correction to
        # this comment. It used to say the walk over `gh` was never started
        # -- true until round 16 put `_COMMAND_SEPARATORS` into
        # `_COMMAND_WORD_END`, and `)` is in that class -- and then credited
        # `_GH_PULL_REQUEST_WORD` with the kill. Measured against each rule
        # on its own: as it stands the verdict is the verb walk's,
        # `_unsafe_gh_verbs` reporting `gh ` because `_invocation_words`
        # stops at the same `)` before it reaches a word; neuter
        # `_GH_PULL_REQUEST_WORD` and this row is still KILLED on that,
        # while seven rows below flip -- every `g'h'` row there is, and
        # nothing else in the sweep, measured at `82d2b964`; this comment
        # said two until round 26. Neuter `_GH_COMMAND` instead and it is
        # still KILLED, now on `pr checkout "$PR_NUMBER"` from the
        # backstop; neuter both and it goes SURVIVED. Closed twice
        # over and pinning neither half, which is the shape
        # `api-gh-program-in-env` below already says it has.
        "B4-pull-request-target-api-gh-program-path",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          \"$(command -v gh)\" pr checkout \"$PR_NUMBER\"\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "resolve the CLI's path before running it, which is how a script "
        "invokes a tool it is not sure is on PATH",
    ),
    Mutation(
        # The same reach with the name in the environment, and the residual
        # round 9 wrote down and left open: the `run:` line `$C pr checkout`
        # is not a `gh` invocation to any rule keyed on the word. The `env:`
        # value carries no head, so the wholesale environment refusal does
        # not reach it either.
        #
        # It is closed twice over, and only the second half is the row
        # above's reason. Measured: neuter `_GH_PULL_REQUEST_WORD` and this
        # row is still KILLED, neuter `_GH_COMMAND` and it is still KILLED,
        # neuter both and it goes SURVIVED. The first kill is the verb
        # allowlist rather than the subcommand one, and it is the `env:`
        # block that hands it over: the text these rules read is the script
        # joined to the step's `env:` values, so `C: gh` is itself a `gh`
        # with nothing after it before the value ends, and
        # `_unsafe_gh_verbs` refuses an invocation whose arguments it cannot
        # read. Allowlist `checkout` as well and the row is still KILLED on
        # that; allowlist the empty verb beside it and it finally survives,
        # while the `"$(command -v gh)"` row above survives the same pair of
        # edits. The second kill is the argument shape, which holds here on
        # its own -- the arguments say `pr checkout` whoever runs them.
        "B4-pull-request-target-api-gh-program-in-env",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          C: gh\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          $C pr checkout \"$PR_NUMBER\"\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "put the program name in the environment with the rest of the step's "
        "configuration, which is this repository's house style for "
        "everything else a step needs",
    ),
    Mutation(
        # A `gh` whose arguments this file cannot read at all, and the row
        # for the other half of the round-10 `gh` fix. The vector is in a
        # data file, so the step names no verb, no subcommand and no path --
        # `_GH_PULL_REQUEST_WORD` sees no `pr` either, because there is none
        # in the workflow. The walk used to skip an invocation with no words
        # after it, on the reasoning that a bare `gh` runs nothing; a `gh`
        # taking its argv off a pipe runs whatever arrives, so an unreadable
        # invocation is refused for being unreadable, which is the answer
        # this file gives an unresolvable ref one field along.
        #
        # It is not the only thing pinning that, and the three rows are not
        # the three round 17 measured. Re-measured 2026-09-20 on the full
        # unfiltered sweep: restore the old skip and this one,
        # `api-gh-copied-to-a-readable-name` and
        # `api-gh-trailing-program-word-no-arguments` go SURVIVED, with the
        # other 253 verdicts unmoved. The count survived round 18 and the
        # membership did not. `api-gh-trailing-program-word` was the third
        # and is not one now: `_receiving_programs` reads the far side of
        # its pipe, the word standing there is `GH_TOKEN=${{`, which is no
        # name, so the argument backstop refuses that step whether or not
        # the verb walk ever saw a `gh`. The row added beside it in the same
        # round took its place.
        #
        # The reason round 17 gave for listing it has not survived either,
        # and that half was wrong in its own terms rather than overtaken.
        # It said `echo "pr checkout $N" | xargs gh` is declined because the
        # word in front of the `pr` is `echo`, a name `_READABLE_PROGRAM`
        # reads. The bare shape is still declined -- measured -- but the
        # backstop is handed `echo`, `xargs` and `gh` now and declines
        # because all three are names, not because the first one is; and the
        # row it was explaining does not write the bare shape.
        #
        # What is this row's alone is not the `pr` claim it replaces --
        # `...-no-arguments` has no `pr` in its workflow text either. It is
        # the terminator. This row's `gh` is followed by a newline, so it is
        # a command word under the set this file started with, `[\s'",]`,
        # while the other two are each the row for an alternative added to
        # `_COMMAND_WORD_END` later: `_COMMAND_SEPARATORS` for the `)` in
        # `copied-to-a-readable-name`, the end of the text for
        # `...-no-arguments`. Measured over the three step texts against
        # each terminator set in turn.
        "B4-pull-request-target-api-gh-arguments-unreadable",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          jq -r '.argv[]' .github/gh-argv.json | xargs gh\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "keep the CLI's arguments in a data file beside the workflow, which "
        "is the tidying somebody does to a step that runs the same command "
        "with a long argument list",
    ),
    Mutation(
        # The same unreadable `gh`, written as a plain scalar so that the
        # program name is the last two bytes of everything the rules read.
        # `_GH_COMMAND` needed a character after `gh` until 2026-09-19, and
        # every terminator it had was a character, so this was green while
        # the row above -- the identical command in a `|` block scalar, which
        # keeps the trailing newline -- was red. Two YAML spellings of one
        # step, opposite verdicts. It is reachable here because the text the
        # rules read is the script joined to the step's `env:` values and
        # this workflow declares none above the step, so a step that declares
        # none of its own ends where its script does; the token is passed as
        # a one-command assignment for the same reason.
        #
        # It stopped pinning that terminator in round 18 and no longer pins
        # anything on its own. The step writes `pr checkout` upstream of the
        # pipe, and `_receiving_programs` now reads the far side of a pipe
        # as a program those words could reach, so the backstop refuses this
        # step whether or not `_GH_COMMAND` matched: neuter the `\Z`
        # alternative and this row is still KILLED, where at 8afbbf68 it was
        # SURVIVED. Both measured. What pins the end of the text now is
        # B4-pull-request-target-api-gh-trailing-program-word-no-arguments
        # below, whose step has no `pr` in it for the backstop to read.
        "B4-pull-request-target-api-gh-trailing-program-word",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         '        run: echo "pr checkout '
         '${{ github.event.pull_request.number }}"'
         " | GH_TOKEN=${{ github.token }} xargs gh\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "write the one-line step as a plain scalar rather than a block one, "
        "which is what a step running a single command usually looks like "
        "and which puts the program name at the end of the file",
    ),
    Mutation(
        # The same last two bytes with no argument word anywhere in the
        # step, and the row that pins the end of the text on its own. The
        # row above stopped doing that in round 18: it writes `pr checkout`
        # upstream of the pipe, `_receiving_programs` now reads the far side
        # of that pipe as a program the words could reach, and the backstop
        # refuses the step whether or not `_GH_COMMAND` ever matched -- so
        # neuter the `\Z` alternative in `_COMMAND_WORD_END` and the row
        # above is still KILLED, where before round 18 it was SURVIVED.
        # Measured both ways round, against `8afbbf68` and against the
        # commit that widened the walk.
        #
        # This one keeps the argument vector in the data file, so the step
        # contains no `pr` for anything but `_GH_COMMAND` to read, and the
        # only `gh` in it is the last two bytes of everything the rules see.
        # Neuter `\Z` and it is SURVIVED with every other row still KILLED.
        # It is `api-gh-arguments-unreadable` as a plain scalar, and the
        # second line that row carries is dropped for the reason the row
        # above is a plain scalar at all: a `gh` with anything after it is
        # terminated by that instead.
        "B4-pull-request-target-api-gh-trailing-program-word-no-arguments",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: jq -r '.argv[]' .github/gh-argv.json |"
         " GH_TOKEN=${{ github.token }} xargs gh\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "keep the CLI's arguments in a data file and write the step as a "
        "plain scalar, which is the tidying of the two rows it is between "
        "applied at once",
    ),
    Mutation(
        # The CLI laundered into a name review *can* read, which is the
        # round-16 hole and the one shape both halves of the `gh` pair passed
        # for opposite reasons. `command -v gh` resolves the program and `cp`
        # puts it at `./g`; the second line then runs the same binary under a
        # name that says nothing. `_GH_COMMAND` never saw the first line,
        # because the `gh` in it is followed by a `)` and the word end took
        # whitespace, a quote, a comma or the end of the text; the `pr`
        # backstop then read the second line and dropped it, because `./g`
        # is a program name `_READABLE_PROGRAM` reads and the backstop is
        # only for invocations whose program is unreadable. So the pair's
        # division of labour had a seam in it, and this is the step that goes
        # through the seam.
        #
        # It pins the `_COMMAND_SEPARATORS` alternative in
        # `_COMMAND_WORD_END`: drop it and this row is SURVIVED with every
        # other row still KILLED. One row for the whole class rather than one
        # per character, unlike `_ENUMERATION_END`, and that is forced rather
        # than chosen -- the class is a named constant `_COMMAND_END` reads
        # too, so a neuter of one character out of it would change where the
        # argument walk stops as well, which is not a neuter of the rule this
        # row is about. The two other spellings measured on the same change,
        # `ln -s` in place of `cp` and the backtick form of the substitution,
        # are the same alternative and were green before it and red after.
        "B4-pull-request-target-api-gh-copied-to-a-readable-name",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         '          cp "$(command -v gh)" ./g\n'
         '          ./g pr checkout "$PR_NUMBER"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "copy the CLI to a name of their own choosing and run the copy, so "
        "that the rule over the program name never sees the program and the "
        "rule over the argument shape sees a name it can read",
    ),
    Mutation(
        # A readable word in front of an unreadable program, which is the
        # round-13 correction to the backstop under all of this. `timeout` is
        # not the program here and neither is `60`: `g'h'` is, and the rule
        # that refuses an unreadable one read the first word of the segment.
        # Eight other wrappers put the same readable word there -- `exec`,
        # `env`, `nice`, `command`, `sudo`, `xargs`, `time`, `stdbuf` --
        # which is nine with `timeout`, the count `_invocation_programs`
        # carries and the one this line took while naming eight of them. It
        # pins the second word `_invocation_programs` returns, the one
        # immediately before the `pr`, and nothing else reaches it: `g'h'`
        # has no `gh` in it for `_GH_COMMAND` to find.
        "B4-pull-request-target-api-gh-wrapped-unreadable-program",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          timeout 60 g'h' pr checkout \"$PR_NUMBER\"\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "put a wrapper in front of the quote-broken name, so the first word "
        "of the command is a readable program and the one that runs is not",
    ),
    Mutation(
        # The same fix read from the other end. The row above is an
        # unreadable program with a readable word in front of it, and it
        # pins the last of the two positions `_invocation_programs` returns;
        # this is an unreadable program with a readable word *behind* it, and
        # it is the only thing pinning the first. `-R owner/repo` is how `gh`
        # is told which repository to act on, and the flag's value is a plain
        # name, so the word nearest the `pr` is readable and the word that
        # runs is not. KILLED here means the wrapper fix has been narrowed to
        # the word before the subcommand, which is the round-6 shape wearing
        # a flag.
        "B4-pull-request-target-api-gh-flagged-unreadable-program",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          g'h' -R gke-labs/kube-agents pr checkout "
         '"$PR_NUMBER"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "put the repository flag between the quote-broken name and the "
        "subcommand, so the word nearest the `pr` is a readable program and "
        "the one that runs is not",
    ),
    Mutation(
        # The arguments written by one program and run by another, which is
        # the round-18 hole and the two closed shapes composed. `echo "pr
        # checkout $N" | xargs gh` is refused for naming the program and
        # hiding the arguments; `g'h' pr checkout "$N"` is refused for naming
        # the arguments and hiding the program; this writes the arguments
        # with a third program, so the word in front of the `pr` is `printf`
        # -- a name `_READABLE_PROGRAM` reads, which is the backstop's own
        # reason for declining -- and `g'h'` holds no `gh` for `_GH_COMMAND`
        # to find. Neither half had anything to hold and it was green,
        # measured the same way against the `06adabac` export rather than
        # introduced by the round that widened these rules last.
        #
        # It pins the downstream half of `_receiving_programs`: delete the
        # pipeline walk and this row is SURVIVED with every other row still
        # KILLED, and deleting the outward walk beside it leaves it KILLED,
        # so the two halves are pinned apart rather than together. `xargs`
        # is not what is being read -- the walk has no list of wrappers --
        # and the same line with `-I{}`, `-n9`, `-0`, `-t` or `env` between
        # the `xargs` and the name is refused for the same reason, measured.
        "B4-pull-request-target-api-gh-arguments-through-a-pipe",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          printf 'pr checkout %s' \"$PR_NUMBER\" | xargs g'h'\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "build the command line with `printf` and run it with `xargs`, which "
        "is how a step assembles a command out of a number it was handed",
    ),
    Mutation(
        # The same launder with the pipe turned inside out. A command
        # substitution's output is a word in the command around it, so this
        # runs what the row above runs and reads, to a rule that asks the
        # `pr`'s own segment, as a `printf` with readable arguments. It pins
        # the outward half of `_receiving_programs`: delete the walk out of
        # a substitution and this row is SURVIVED with every other row still
        # KILLED, and deleting the pipeline walk beside it leaves it KILLED.
        # A wrapper in front of the name -- `timeout 60 g'h' $(...)` -- is
        # the same edit and is measured rather than assumed. The backtick
        # spelling was claimed here on the same footing until round 21,
        # which found it carried by no row at all and gave it the one below.
        "B4-pull-request-target-api-gh-arguments-through-a-substitution",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          g'h' $(printf 'pr checkout %s' \"$PR_NUMBER\")\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "build the argument list in a substitution rather than writing it "
        "out, which is the line above with the pipe turned inside out",
    ),
    Mutation(
        # The same launder in the older spelling of a substitution. One
        # character opens a backtick and the same character closes it, so
        # the walk out of one cannot be a depth count and is a parity check
        # instead -- and this is the row that pins the reading half of it.
        # Take the read out of that branch and this is SURVIVED with the
        # `$( )` row above still KILLED, which is the two spellings pinned
        # apart rather than together.
        "B4-pull-request-target-api-gh-arguments-through-a-backtick",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          g'h' `printf 'pr checkout %s' \"$PR_NUMBER\"`\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "write the same substitution the older way, which is the spelling a "
        "script copied off a wiki page is written in",
    ),
    Mutation(
        # The same launder with one character of punctuation inside a quoted
        # word, which is where the walk downstream stopped. `_COMMAND_END`
        # is a character class and knows nothing about quotes, so the `)` in
        # `")"` -- an argument `%.0s` consumes and never prints -- ended the
        # walk at a parenthesis the shell runs straight past. The
        # `| xargs g'h'` was never read, both ends of the `pr`'s own segment
        # came back `printf`, and `_READABLE_PROGRAM` reads that, so the
        # backstop declined. Nothing else was left to decline: `g'h'` holds
        # no `gh` for `_GH_COMMAND`, and there is no expression, no runner
        # variable and no payload file anywhere in the step. Green at
        # `f14997a5`, where the line without the quoted word is red.
        #
        # It pins the second reading in the downstream half of
        # `_receiving_programs`: hand that walk the quote-blind reading
        # alone and this row is SURVIVED with every other row still KILLED,
        # the outward row below included. One row for the whole class rather
        # than one per character, and that is measured rather than chosen --
        # `;`, `&`, `` ` `` and `(` written in place of the `)` were green
        # before the fix and are red after it, and the readings the walk is
        # handed are one function rather than a branch per separator.
        "B4-pull-request-target-api-gh-arguments-past-a-quoted-separator",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          printf 'pr checkout %s %.0s' \"$PR_NUMBER\" \")\" "
         "| xargs g'h'\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "put a parenthesis in a quoted argument of the line that builds the "
        "command, so the walk to the program on the far side of the pipe "
        "ends at a separator the shell does not have",
    ),
    Mutation(
        # The same quoted separator read outward, which is the other walk
        # and the other branch. Coming back from the `pr` for the command
        # that encloses the substitution, the `;` is stepped over and the
        # `)` inside `true \")\"` is counted as a substitution that has
        # already closed -- so the real `$(` two separators back decrements
        # that count instead of being read as the opener it is, the walk
        # runs out of text, and the enclosing `g'h'` is never reached. Both
        # programs come back `printf` again. Green at `f14997a5`.
        #
        # It pins the second reading in the outward half: hand that walk the
        # quote-blind reading alone and this row is SURVIVED with the
        # downstream row above still KILLED. The two halves are pinned apart
        # rather than together, which is the division the pipe and `$( )`
        # rows above are already kept in, and the quoting had to be measured
        # in both because one fix does not reach both walks.
        "B4-pull-request-target-api-gh-substitution-past-a-quoted-separator",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          g'h' $(true \")\"; printf 'pr checkout %s' "
         "\"$PR_NUMBER\")\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "balance the substitution's own parenthesis with one inside a "
        "quoted word, so the walk out to the enclosing program counts the "
        "opener as already matched",
    ),
    Mutation(
        # The safe side of the same word, and the hole `_READABLE_PROGRAM`
        # leaves open on purpose. A program with a plain name of its own and
        # `gh`'s argument shape is conceded -- closing it means a denylist of
        # client names, which is the thing that rule exists to stop needing
        # -- and a wrapper in front of a readable program must not change
        # that. OVERSHOT here means the wrapper fix has been written as "any
        # `pr` behind more than one word", which reds on every local tool
        # this repository runs under `timeout`. The `gh` inside `high` is
        # deliberate: it is preceded by a word character, which is what
        # `_GH_COMMAND`'s lookbehind is for.
        "B4-pull-request-target-api-gh-wrapped-readable-program",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Summarise the pull request\n"
         "        run: timeout 60 ./tools/high pr checkout 42\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "run a local tool with a readable name under a wrapper, which is a "
        "program this file concedes and a shape it must not read as `gh`",
        must_survive=True,
    ),
    Mutation(
        # The same concession one pipe along, and the safe side of the
        # round-18 walk. `_receiving_programs` reads every segment
        # downstream of the `pr` as a program the words could reach, and
        # every one of them is still held to `_READABLE_PROGRAM`: a plain
        # name at the end of a pipeline is a program review can read, the
        # same way a plain name at the head of a segment is. OVERSHOT here
        # means the walk has been written as "any `pr` upstream of a pipe",
        # which reds on every step in this repository that builds a command
        # line and runs it.
        "B4-pull-request-target-api-gh-pipeline-readable-program",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Summarise the pull request\n"
         "        run: printf 'pr checkout %s' 42 | xargs ./tools/high\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "run a local tool with a readable name over a pipeline, which is a "
        "program this file concedes and a shape it must not read as `gh`",
        must_survive=True,
    ),
    Mutation(
        # The ordinary side of the quoting. What this row does *not* do is
        # show the second reading widening the walk, which is what its
        # comment claimed for two rounds: the parentheses in this log line
        # are balanced, and a balanced pair is stepped over by the depth
        # counter whether or not the reading can see the quotes. Measured on
        # 2026-09-20 -- both readings hand the walk `['tee', 'out.txt']`.
        # The row the second reading actually changes is the unbalanced
        # spelling, which is a control of its own below.
        # What this one pins is the shape of the fix rather than its reach.
        # OVERSHOT here means the quoted separator has been made the
        # objection itself rather than what it lets the walk reach, which
        # reds on every step in this repository that puts a parenthesis in a
        # message. Green before the second reading existed and green after.
        "B4-pull-request-target-run-quoted-paren-ordinary",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Note the pull request\n"
         "        run: |\n"
         '          echo "the pr is open (stage 1)" | tee out.txt\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "pipe a log line with a parenthesis in it into a plain program, "
        "which is the most ordinary thing a step does with a message",
        must_survive=True,
    ),
    Mutation(
        # The ordinary step the second quoting reading actually changes, and
        # the row the control above turned out not to be. Measured on
        # 2026-09-20: for `echo "the pr is open (stage 1)" | tee out.txt`
        # both readings hand the walk `['tee', 'out.txt']`, because the
        # parentheses are balanced and the depth counter steps over a pair
        # whether or not it can see the quotes. An *unbalanced* one is where
        # they differ -- the quote-blind reading counts the `(` open, never
        # closes it, and returns no downstream program at all, while the
        # quote-skipping reading crosses the pipe and finds `tee`. So this
        # is the line that is walked for the first time, and what the walk
        # finds at the end of it is a program review can read.
        "B4-pull-request-target-run-unbalanced-paren-ordinary",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Note the pull request\n"
         "        run: |\n"
         '          echo "the pr is open (stage 1" | tee out.txt\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "leave a parenthesis unclosed in a log line and pipe it into a "
        "plain program, which is the step the quoted reading newly walks",
        must_survive=True,
    ),
    Mutation(
        # Where the walk out of a substitution starts reading, which is one
        # character and the whole false-positive budget. The enclosing
        # command is read from the `$` rather than from the parenthesis,
        # because `$(` is the substitution's own punctuation and never a
        # program: read from the parenthesis and the word in front of every
        # `"$(...)"` in this repository is a lone `$`, which no name is made
        # of. OVERSHOT here means that character was lost, and this is the
        # most ordinary substitution there is.
        "B4-pull-request-target-api-gh-substitution-ordinary",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Describe the pull request\n"
         "        run: git commit --allow-empty -m"
         " \"$(printf 'pr is open')\"\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "put a generated message on a commit, which is the most ordinary "
        "command substitution a step writes",
        must_survive=True,
    ),
    Mutation(
        # The safe side of the backtick row above, and the round-21 false
        # positive. A backtick substitution that has *closed* encloses
        # nothing after it, so the walk has to count past it exactly as the
        # depth counter counts past a `$( )` that has closed -- and it did
        # not: it read the closing backtick as an opener, then the opening
        # one as the opener of the command enclosing *that*, and arrived at
        # `REV=`, which is not a name. An ordinary shell script and a line
        # of prose two lines under it were refused together. OVERSHOT here
        # means the parity pairing has come back out, and the `$( )`
        # spelling of this same script was green throughout, which is how
        # the false positive was measured rather than argued.
        "B4-pull-request-target-run-backtick-closed-then-prose",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Note the pull request\n"
         "        run: |\n"
         "          set -euo pipefail\n"
         "          REV=`git rev-parse --short HEAD`\n"
         '          echo "the pr is open for $REV"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "take a revision out of a backtick substitution and mention the "
        "pull request in prose on a later line, which is a shell script and "
        "an echo",
        must_survive=True,
    ),
    Mutation(
        # The verb renamed, and the row the verb allowlist exists for. `gh
        # alias set` writes a user-level alias into the CLI's config, so the
        # second line *is* `gh pr checkout` to the CLI and is `gh co` to any
        # reader of the file -- a word GitHub never shipped and no denylist
        # over subcommands can be written to contain. A walk that skips every
        # invocation whose verb is not `pr` reads neither line: the first is
        # `alias`, the second is `co`. Both are refused now for not being on
        # `_SAFE_GH_VERBS`, which is the same move one noun up that
        # `_SAFE_GH_PULL_REQUEST_SUBCOMMANDS` made a round earlier.
        "B4-pull-request-target-api-gh-alias-verb",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          gh alias set co 'pr checkout'\n"
         "          gh co \"$PR_NUMBER\"\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "alias the checkout to a short name first, which is the tidying "
        "somebody does to a script that runs the same subcommand repeatedly",
    ),
    Mutation(
        # The fork's changes over HTTP with no token in the request at all.
        # `https://github.com/O/R/pull/N.diff` is served off the pull
        # request's web page, so this is
        # B4-pull-request-target-api-gh-pr-diff with the client swapped for
        # `curl` and the credential dropped -- and it named nothing any rule
        # read: not `/pulls`, which is the API path and this is not; not
        # `refs/pull`, which the web URL has no prefix for; not `gh`. The
        # `.patch` sibling through `git am` is the same alternative and is
        # not a second row.
        "B4-pull-request-target-api-web-diff-endpoint",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         "          curl -fsSL \"https://github.com/"
         "${{ github.repository }}/pull/${PR_NUMBER}.diff\" | git apply\n"
         "          ./run.sh\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "fetch the diff off the pull request's web page, which needs no "
        "token and no authentication step ahead of it",
    ),
    Mutation(
        # The head through the noun this file used to leave open. A pull
        # request is an issue to the REST API, and `/issues/N/timeline`
        # returns its `committed` events, each carrying a head SHA -- so the
        # verb is `api`, which is allowlisted, and the path says `issues`.
        # This row was the reason `_PULL_REQUEST_API` carried two named issues
        # endpoints; as of round 10 it carries the namespace instead, because
        # the item endpoint one segment shorter hands back `patch_url` and no
        # list of endpoints was ever going to be finished. Kept as the
        # timeline spelling of a reach the namespace now covers whole.
        "B4-pull-request-target-api-issues-timeline",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "          REPO: ${{ github.repository }}\n"
         "        run: |\n"
         "          SHA=$(gh api \"repos/$REPO/issues/$PR_NUMBER/timeline\""
         " --jq '.[] | select(.event==\"committed\") | .sha' | tail -1)\n"
         "          git fetch origin \"$SHA\" && git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the head off the issue timeline rather than off the pull "
        "request, which is one endpoint over and returns the same commits",
    ),
    Mutation(
        # The shortest URL in the namespace, and the round-10 finding the
        # comment above `_PULL_REQUEST_API` was wrong about. `GET
        # /repos/O/R/issues/N` is the *item* endpoint, and for an issue that
        # is a pull request it returns `pull_request: {url, html_url,
        # diff_url, patch_url}` -- so the fork's patch is one `--jq` away, the
        # request names no `/pulls`, the `gh` verb is allowlisted, the only
        # expression in the step is the number, and `git am` puts the fork's
        # commits on the default-branch checkout with `make test` running them
        # under the write token. Two rounds of rules over this namespace read
        # `/issues/N/timeline` and `/issues/N/events` and let the endpoint
        # they are both suffixes of straight through. This row is why the rule
        # is the namespace.
        "B4-pull-request-target-api-issues-item-patch-url",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "          REPO: ${{ github.repository }}\n"
         "        run: |\n"
         "          URL=$(gh api \"repos/$REPO/issues/$PR_NUMBER\""
         " --jq .pull_request.patch_url)\n"
         "          curl -sL \"$URL\" | git am\n"
         "          make test\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the pull request off the issue it also is, which is one "
        "endpoint the API documents as returning the same object and the "
        "spelling a workflow that handles issues and pull requests together "
        "already has to hand",
    ),
    Mutation(
        # The same namespace with no number in it at all. `search/issues`
        # takes a query rather than an identifier, so `repo:O/R+type:pr`
        # returns every open pull request here, each item carrying the same
        # `pull_request.patch_url` the row above reads -- which means a step
        # does not even need `github.event.pull_request.number` on the
        # expression allowlist to reach a fork's code. Written with `type:pr`
        # rather than the more usual `is:pr`, which isolates nothing. The
        # `:` in `type:pr` is not a word character either, so
        # `_GH_PULL_REQUEST_WORD` reads that `pr` exactly as it reads
        # `is:pr`'s and the query is refused twice over whichever is written.
        # Measured: neuter the `/issues` alternative and this row is still
        # KILLED, neuter `_GH_PULL_REQUEST_WORD` and it is still KILLED,
        # neuter both and it goes SURVIVED -- and with `/issues` neutered the
        # row rewritten as `is:pr` is KILLED too, which is the comparison the
        # spelling was chosen on. Nothing here pins the path alternative
        # alone.
        "B4-pull-request-target-api-search-issues",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "          REPO: ${{ github.repository }}\n"
         "        run: |\n"
         "          U=$(gh api \"search/issues?q=repo:$REPO+type:pr\""
         " --jq '.items[0].pull_request.patch_url')\n"
         "          curl -sL \"$U\" | git am\n"
         "          make test\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "search for the repository's open pull requests rather than naming "
        "one, which is what a step that sweeps them all would write",
    ),
    Mutation(
        # The same endpoint from the language the shell rules cannot read,
        # and the pair to the row above the way
        # B4-pull-request-target-api-octokit-pulls is the pair to
        # B4-pull-request-target-api-gh-api-pulls. `listEventsForTimeline` is
        # the client method for `/issues/N/timeline`, so the path alternative
        # never sees it -- there is no slash anywhere in the call. The method
        # name was its own alternative until round 10 and is not one now:
        # what matches is the namespace, which every route to it writes once.
        #
        # Which alternative reads it is what this comment got wrong until
        # round 20. It credited `rest.issues` with reading "a destructured
        # one included", and that is the one route `rest.issues` cannot
        # read: `const { issues } = github.rest` writes a semicolon where
        # the `rest`-and-punctuation alternative wants a dot, and what picks
        # it up is the bare-namespace alternative --
        # B4-pull-request-target-api-octokit-activity-destructured is the
        # row that pins that, one namespace along. This row writes the
        # namespace plainly, as `github.rest.issues`, which *both*
        # alternatives match, so it pins neither on its own. Measured: take
        # `issues` out of the `rest`-and-punctuation alternative and this
        # row stays KILLED, take it out of the bare-namespace alternative
        # and it stays KILLED, and only with both gone is it SURVIVED.
        # Pinned to the real action's SHA for the same reason as its
        # neighbours.
        "B4-pull-request-target-api-octokit-issues-timeline",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            const ev = await"
         " github.rest.issues.listEventsForTimeline({\n"
         "              ...context.repo,\n"
         "              issue_number: ${{ github.event.pull_request.number }}"
         "\n"
         "            });\n"
         "            const sha = ev.data.pop().sha;\n"
         "            await exec.exec('git', ['fetch','origin', sha]);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "ask the same timeline through the client the action already hands "
        "the script, which names no path for a path rule to read",
    ),
    Mutation(
        # The event stream, which is the same question with the noun changed.
        # `GET /repos/O/R/events` returns the repository's public timeline
        # and a `PullRequestEvent` on it carries the whole pull request
        # object, head SHA included, so this reads the head out of an
        # endpoint that spells no `/pulls`, no `/issues`, no Octokit
        # namespace and no `pull/N.diff`. Written with `curl` and no `gh`
        # word anywhere on purpose: a fix hung off the `gh` walk would not
        # reach this shape, and the row is what says so. Nothing but the
        # `/events` alternative of `_PULL_REQUEST_API` pins it -- neuter that
        # one alternative and this goes SURVIVED with every other row still
        # KILLED. The SHA is grepped rather than named, so no head-spelling
        # rule is in the picture either.
        "B4-pull-request-target-api-events-endpoint",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          SHA=$(curl -s "
         "\"https://api.github.com/repos/$GITHUB_REPOSITORY/events\""
         " | grep -oE '[0-9a-f]{40}' | head -1)\n"
         "          git fetch --depth=1 origin \"$SHA\"\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the head off the repository's own event timeline, over the "
        "public endpoint that needs no token at all",
    ),
    Mutation(
        # The event stream from the client instead of over a path, indexed.
        # `activity` is the Octokit namespace `listRepoEvents` lives under,
        # and one quote is the whole difference from the row below the way it
        # is between B4-pull-request-target-api-octokit-pulls-indexed and its
        # own pair: `rest['activity']` puts a quote where the second
        # namespace alternative wants a dot, so only the `rest`-and-
        # punctuation alternative reads it. Measured: take `activity` out of
        # that alternative and this row is SURVIVED while the destructured
        # row below stays KILLED. The plainest spelling,
        # `github.rest.activity`, is matched by both and so pins neither,
        # which is why neither row is written that way.
        "B4-pull-request-target-api-octokit-activity-indexed",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            const ev = await github.rest['activity']"
         ".listRepoEvents({\n"
         "              ...context.repo\n"
         "            });\n"
         "            const sha = JSON.stringify(ev.data)"
         ".match(/[0-9a-f]{40}/)[0];\n"
         "            await exec.exec('git', ['fetch','origin', sha]);\n"
         "            await exec.exec('git', ['checkout','FETCH_HEAD']);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "reach the event list through the client the action already hands "
        "the script, indexing the namespace the way the neighbouring row "
        "indexes `pulls`",
    ),
    Mutation(
        # The same namespace destructured off the client, which is the half
        # the `rest`-and-punctuation alternative cannot read: `const {
        # activity } = github.rest` writes `rest` with a semicolon after it
        # and the call two lines later writes no `rest` at all. What reads it
        # is the bare-namespace alternative. Measured: take `activity` out of
        # that one and this row is SURVIVED while the indexed row above stays
        # KILLED, which is the pair doing the same job the `pulls` pair does
        # one rule along. Worth saying that the bare-namespace alternative
        # had no row of its own before this one -- `pulls` and `issues` are
        # both written as `rest.pulls` and `rest['pulls']` in the table, and
        # both of those are matched by the first alternative too.
        "B4-pull-request-target-api-octokit-activity-destructured",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            const { activity } = github.rest;\n"
         "            const ev = await activity.listRepoEvents({\n"
         "              ...context.repo\n"
         "            });\n"
         "            const sha = JSON.stringify(ev.data)"
         ".match(/[0-9a-f]{40}/)[0];\n"
         "            await exec.exec('git', ['fetch','origin', sha]);\n"
         "            await exec.exec('git', ['checkout','FETCH_HEAD']);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "pull the namespace off the client once and call it by its own name, "
        "which is ordinary JavaScript and writes the namespace where no rule "
        "keyed on `rest` is looking",
    ),
    Mutation(
        # The workflow run, which is this repository's own API telling a step
        # what the fork pushed. Every entry of `GET /repos/O/R/actions/runs`
        # carries `head_sha` beside `head_repository`, whose `clone_url` is
        # the fork -- so one request hands back both halves of a fetch and
        # the step never names a remote it did not learn from GitHub. It was
        # green against every rule in the file before round 20: no `/pulls`,
        # no `/issues`, no `/events`, no Octokit namespace, no `pull/N.diff`,
        # and `api` is one of the three verbs on _SAFE_GH_VERBS. `gh run
        # view` reaches the same object and has always been refused, by the
        # verb allowlist rather than by anything here, which is why this
        # alternative is about the path.
        #
        # It pins the `runs` word of the `/actions/` alternative. Take that
        # one word out and this row is SURVIVED with the other three
        # `/actions/` rows still KILLED.
        "B4-pull-request-target-api-actions-runs-endpoint",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         "          read -r URL SHA < <(gh api \"repos/$GITHUB_REPOSITORY"
         "/actions/runs?event=pull_request&per_page=1\" \\\n"
         "            --jq '.workflow_runs[0] | .head_repository.clone_url"
         " + \" \" + .head_sha')\n"
         "          git fetch \"$URL\" \"$SHA\"\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the fork's clone URL and head SHA off this repository's own "
        "workflow-run list, which is one request for both halves of a fetch",
    ),
    Mutation(
        # The same list scoped to one workflow file, which writes no
        # `actions/runs` anywhere: the run id goes in the middle of the path
        # and `runs` comes after it. Written with `curl` and no `gh` word for
        # the reason B4-pull-request-target-api-events-endpoint is -- a fix
        # hung off the `gh` walk would not reach this shape -- and the SHA is
        # grepped out rather than named, so no head-spelling rule is in the
        # picture.
        #
        # It pins the `workflows` word. Take that one out and this row is
        # SURVIVED with the other three still KILLED.
        "B4-pull-request-target-api-actions-workflow-runs",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          SHA=$(curl -s \"https://api.github.com/repos"
         "/$GITHUB_REPOSITORY/actions/workflows/risk_classify.yml/runs\""
         " | grep -oE '[0-9a-f]{40}' | head -1)\n"
         "          git fetch --depth=1 origin \"$SHA\"\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "ask one workflow file for its own runs, which is the same list with "
        "the run word moved past the file name",
    ),
    Mutation(
        # A job rather than the run it belongs to. `GET
        # /repos/O/R/actions/jobs/{id}` carries the run's `head_sha`, so the
        # noun changes and the answer does not. The id comes off disk here
        # rather than out of another refused request, which is the point: a
        # rule that only refused the list would leave the item endpoint
        # reachable to anything that had written an id down.
        #
        # It pins the `jobs` word. Take that one out and this row is
        # SURVIVED with the other three still KILLED.
        "B4-pull-request-target-api-actions-job",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         "          J=$(cat .ci/job-id)\n"
         "          SHA=$(gh api \"repos/$GITHUB_REPOSITORY/actions/jobs/$J\""
         " --jq .head_sha)\n"
         "          git fetch origin \"$SHA\" && git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the head off a single job instead of the run that owns it, "
        "which is the same field under a different noun",
    ),
    Mutation(
        # The fork's code arriving as a build product rather than as a
        # commit. `GET /repos/O/R/actions/artifacts` lists every artifact in
        # the repository without naming a run, and the `{id}/zip` download
        # under it is whatever the fork's own job uploaded -- so this row
        # fetches no SHA at all and still puts attacker-controlled bytes on a
        # runner holding this token, and then runs them. It is the reason the
        # alternative is not only about `runs`.
        #
        # It pins the `artifacts` word. Take that one out and this row is
        # SURVIVED with the other three still KILLED.
        "B4-pull-request-target-api-actions-artifacts",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         "          A=$(gh api \"repos/$GITHUB_REPOSITORY/actions/artifacts"
         "?per_page=1\" --jq '.artifacts[0].id')\n"
         "          gh api \"repos/$GITHUB_REPOSITORY/actions/artifacts"
         "/$A/zip\" > a.zip\n"
         "          unzip -o a.zip -d ./incoming && ./incoming/run.sh\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "download whatever the fork's own job uploaded and run it, which is "
        "the fork's code without a commit anywhere in the story",
    ),
    Mutation(
        # The safe side of the `/actions/` alternative, and the reason it
        # carries four last words instead of closing the segment. A
        # composite action lives at `.github/actions/<name>/action.yml` in
        # this repository, so `/actions/` with an ordinary directory after it
        # is a path a lint step has every reason to write, and the word
        # `actions` next to a slash is not an endpoint. OVERSHOT here means
        # the alternative has been widened to the segment -- which is the
        # obvious tightening, and a suite that reds on the first step that
        # looks at a composite action.
        "B4-pull-request-target-api-actions-path-ordinary",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Lint the composite actions\n"
         "        run: |\n"
         "          ls .github/actions/\n"
         "          yamllint .github/actions/setup/action.yml\n"
         "          echo \"searching the actions directory\"\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "list and lint this repository's own composite actions, which is a "
        "path with the endpoint's first word in it and no endpoint anywhere",
        must_survive=True,
    ),
    Mutation(
        # The run list from the client instead of over a path, indexed, and
        # the pair to B4-pull-request-target-api-octokit-actions-destructured
        # the way the `activity` pair two rows up works: `rest['actions']`
        # puts a quote where the bare-namespace alternative wants a dot, so
        # only the `rest`-and-punctuation alternative reads it. Measured:
        # take `actions` out of that alternative and this row is SURVIVED
        # while the destructured row below stays KILLED. `head_sha` is a
        # field of the response rather than of the event, so no
        # head-spelling rule sees it. Pinned to the real action's SHA like
        # its neighbours.
        "B4-pull-request-target-api-octokit-actions-indexed",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            const r = await github.rest['actions']"
         ".listWorkflowRunsForRepo({\n"
         "              ...context.repo\n"
         "            });\n"
         "            const sha = r.data.workflow_runs[0].head_sha;\n"
         "            await exec.exec('git', ['fetch','origin', sha]);\n"
         "            await exec.exec('git', ['checkout','FETCH_HEAD']);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "reach the workflow-run list through the client the action already "
        "hands the script, indexing the namespace the way the `activity` "
        "row above indexes its own",
    ),
    Mutation(
        # The same namespace destructured off the client, which the
        # `rest`-and-punctuation alternative cannot read: `const { actions }
        # = github.rest` writes `rest` with a semicolon after it and the call
        # below writes no `rest` at all. Measured: take `actions` out of the
        # bare-namespace alternative and this row is SURVIVED while the
        # indexed row above stays KILLED.
        "B4-pull-request-target-api-octokit-actions-destructured",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            const { actions } = github.rest;\n"
         "            const r = await actions.listWorkflowRunsForRepo({\n"
         "              ...context.repo\n"
         "            });\n"
         "            const sha = r.data.workflow_runs[0].head_sha;\n"
         "            await exec.exec('git', ['fetch','origin', sha]);\n"
         "            await exec.exec('git', ['checkout','FETCH_HEAD']);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "pull the run namespace off the client once and call it by its own "
        "name, which is ordinary JavaScript and writes no `rest` where a "
        "rule keyed on `rest` is looking",
    ),
    Mutation(
        # Search, indexed, and this is the round-10 lesson arriving one
        # namespace late. `gh api "search/issues?q=..."` has had a row here
        # since that round and is refused by the `/issues` path -- and the
        # client spelling of the identical query was green until round 20,
        # which is precisely the mistake round 10 was a correction for.
        # `issuesAndPullRequests` needs no number: every item it returns that
        # is a pull request carries `pull_request.patch_url`, and `git am` on
        # the other end of a `curl` puts the fork's changes on the default
        # branch. `is:pr` is deliberately *not* in the query -- the argument
        # backstop reads the `pr` in it and would kill this row for a reason
        # that has nothing to do with the namespace.
        #
        # It pins `search` in the `rest`-and-punctuation alternative, the way
        # the indexed `actions` row does one namespace along: take that word
        # out and this row is SURVIVED with the destructured row below still
        # KILLED.
        "B4-pull-request-target-api-octokit-search-indexed",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            const r = await github.rest['search']"
         ".issuesAndPullRequests({\n"
         "              q: 'repo:${{ github.repository }} state:open'\n"
         "            });\n"
         "            const u = r.data.items[0].pull_request.patch_url;\n"
         "            await exec.exec('bash', ['-c',\n"
         "              'curl -sL ' + u + ' | git am']);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "search the repository for its own open items and apply the first "
        "one's patch, which needs no pull request number anywhere",
    ),
    Mutation(
        # The same namespace destructured, which is the half no rule keyed on
        # `rest` can read. Measured: take `search` out of the bare-namespace
        # alternative and this row is SURVIVED while the indexed row above
        # stays KILLED.
        "B4-pull-request-target-api-octokit-search-destructured",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            const { search } = github.rest;\n"
         "            const r = await search.issuesAndPullRequests({\n"
         "              q: 'repo:${{ github.repository }} state:open'\n"
         "            });\n"
         "            const u = r.data.items[0].pull_request.patch_url;\n"
         "            await exec.exec('bash', ['-c',\n"
         "              'curl -sL ' + u + ' | git am']);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "pull the search namespace off the client once and call it by its "
        "own name, which writes the namespace where no rule keyed on `rest` "
        "is looking",
    ),
    Mutation(
        # The web link the diff rule must not catch, and the control that
        # says so. `_PULL_REQUEST_API` now reads the `pull/` web path, and
        # the ref half has always conceded a bare `.../pull/1781` on purpose:
        # it is a link to a page, and a workflow that comments on a pull
        # request has every reason to write one down. There is no
        # `\.(?:diff|patch)` fragment to widen past: both alternatives are
        # built from `_shell_word`, which escapes per character and
        # interleaves the quoting class between the letters, so
        # `_PULL_REQUEST_API.pattern` holds zero occurrences of `diff` and
        # zero of `patch`. The widening is at the source: cut the
        # `[^\n'"]*?` and the `_shell_word(".diff")` and
        # `_shell_word(".patch")` tail off both alternatives, leaving a bare
        # `_shell_word("pull/")`, and this row is OVERSHOT -- measured, the
        # suite green before the mutation and red on it, which is backwards
        # for a mutation, so the row inverts. Without it nothing pins the
        # difference between the endpoint that serves the fork's code and the
        # page a human reads.
        "B4-pull-request-target-api-web-link-comment",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Link the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         "          # convention:"
         " https://github.com/gke-labs/kube-agents/pull/1781\n"
         "          gh pr comment"
         " ${{ github.event.pull_request.number }} --body \"see above\"\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "cite a pull request by its web link in a comment the workflow "
        "posts, which is what every reference to one in this repository "
        "looks like",
        must_survive=True,
    ),
    Mutation(
        # The `issue` verb, from the safe side. A pull request is an issue to
        # GitHub, so `gh issue comment 1781` comments on this pull request --
        # the same write `gh pr comment` does, through the other noun, and
        # the reason `issue` is on `_SAFE_GH_VERBS` at all. Take it off and
        # this row reds: the suite would be refusing an ordinary labelling
        # write and telling its author to allowlist a verb whose subcommands
        # are already governed. So the row inverts, and it is the only thing
        # pinning the third entry of that list against a future round
        # tightening it to `pr` and `api` and seeing a clean sweep.
        "B4-pull-request-target-api-gh-issue-comment-write",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Comment on the pull request\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        run: |\n"
         "          gh issue comment"
         " ${{ github.event.pull_request.number }} --body \"triaged\"\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "post the comment through the issues noun rather than the pull "
        "requests one, which is the same API call and the spelling somebody "
        "reaches for when the workflow handles issues too",
        must_survive=True,
    ),
    Mutation(
        # The argument vector from the safe side, and the only thing that
        # pins `_GH_WORD_PUNCTUATION`. This is the live milestone carrier's
        # label write spelled the way a `script:` input spells it, and it has
        # to stay green. Strip only quotes off each word, as the walk did
        # before this round, and the verb reads as `['pr',` -- which is on no
        # allowlist, so the suite refuses an ordinary label write and tells
        # its author to allowlist a fragment of JavaScript. The killer row
        # above cannot see that: refusing the fragment is the right verdict
        # there for the wrong reason, so it kills either way. Pinned to the
        # real action's SHA for the same reason as its neighbours.
        "B4-pull-request-target-api-gh-argv-vector-write",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Label the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        env:\n"
         "          GH_TOKEN: ${{ github.token }}\n"
         "        with:\n"
         "          script: |\n"
         "            await exec.exec('gh', ['pr', 'edit',"
         " '${{ github.event.pull_request.number }}',"
         " '--add-label', 'triage']);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "write the label through the `exec` the action already hands the "
        "script, which is one step fewer than shelling out and is the same "
        "command the live carrier runs",
        must_survive=True,
    ),
    Mutation(
        # Not a `run:` step at all. `actions/github-script` takes JavaScript
        # as an input and runs it in the job with the same token and the same
        # working directory, so the fetch is the identical hazard in another
        # language -- and a haystack built from `run:` alone reads none of it.
        # Pinned to the real action's SHA: an unpinned one would trip C4's
        # sweep and the verdict would stop saying which rule caught this.
        "B4-pull-request-target-script-input",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            await exec.exec('git', ['fetch','origin',\n"
         "              context.payload.pull_request.head.sha]);\n"
         "            await exec.exec('git', ['checkout','FETCH_HEAD']);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "move the fetch into a `script:` input, where the shell the test "
        "reads is not the language that runs",
    ),
    Mutation(
        # The widest grant GitHub offers, spelled as a string rather than as
        # a mapping. Both halves of this test filtered on `isinstance(scope,
        # dict)`, so `write-all` at the workflow level put `contents: write`
        # and `id-token: write` in a block neither assertion could see a
        # field of. The job-level spelling reddened -- but through
        # test_B2_no_workflow_grants_a_bot_the_ability_to_approve, which is a
        # neighbour asking a different question, and a neighbour's red is not
        # this assertion working. NOISY by construction now that both halves
        # read the shorthand: `write-all` includes `contents: write`, so
        # test_B4_contents_write_is_confined_to_the_release_path gains a
        # holder too. That second red is the other half of the same fix.
        #
        # What the edit does *not* do is hand this workflow's job anything,
        # and the pretext said it did until round 19. The row rewrites the
        # workflow-level block; `risk_classify.yml` has one job and it
        # declares a `permissions:` block of its own, and a job-level block
        # replaces the workflow-level one whole rather than merging with it,
        # so the token `classify` runs with is the same four scopes after the
        # mutation as before it. That last step is GitHub's documented
        # inheritance rule read against the file, not something this harness
        # can measure -- what the file shows is the one job and the one
        # block. Nor is it the thing `risk_classify.yml`'s own header
        # comment is about: the paragraph there saying `permissions:` cannot
        # raise a token is the argument for not using `pull_request`, where a
        # fork's `GITHUB_TOKEN` is read-only whatever any block says. On
        # `pull_request_target` the token is the base repository's and a
        # block does decide it -- the job's block, here.
        #
        # So what the row removes is reach, not a credential.
        # `_permission_scopes` reads every `permissions:` block in the file
        # rather than a job's effective grant, deliberately -- the assertion
        # is that no block in a `pull_request_target` workflow names these
        # two scopes, wherever it is written and whoever inherits it -- and
        # before the shorthand expanded, a block spelled as a string was a
        # block it could not read at all.
        "B4-pull-request-target-permissions-write-all",
        ".github/workflows/risk_classify.yml",
        ("permissions: {}", "permissions: write-all"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "widen the workflow-level default from `{}` to the string shorthand "
        "GitHub documents, rather than naming a scope -- the one-word edit "
        "somebody makes when a job turns out to be short of a permission",
    ),
    Mutation(
        # A fetch whose arguments are assembled out of a second env value,
        # so the name the script reads is one hop from the name that matters.
        # This was written as the row for the pickup's outer loop and it has
        # not been that for two rounds; the correction it then got --
        # crediting the environment refusal -- was wrong too. Measured
        # 2026-09-19: the expression allowlist kills it, on
        # `github.event.after` in B's value, ahead of both. With the pickup
        # reduced to a single iteration, and again with the environment
        # refusal deleted, this row died either way -- and the pickup itself
        # went on 2026-09-19. It pins the expression allowlist over a
        # chained `env:`, which is worth a row, and it pins nothing else.
        "B4-pull-request-target-run-fetch-shell-chain",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          B: ${{ github.event.after }}\n"
         "          A: origin $B\n"
         '        run: git fetch --depth=1 $A && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "build the fetch's arguments out of a second env value, so the name "
        "the script reads is one hop from the name that matters",
    ),
    Mutation(
        # The same hazard as B4-pull-request-target-script-input written on
        # one line. `_step_scripts` folded in only the `with:` values spanning
        # more than one line, on the reasoning that a program has newlines in
        # it, so a `script:` short enough to fit on one was never read at all.
        # The real action at its real SHA, so C4's pin sweep is not the thing
        # that catches this.
        "B4-pull-request-target-script-input-one-line",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: await exec.exec('git', ['fetch', 'origin', "
         "context.payload.pull_request.head.sha])\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "write the fetch as a one-line `script:`, which is how anybody writes "
        "a script that fits on one line",
    ),
    Mutation(
        # The same one-line value wearing a multi-line coat. A `>-` scalar is
        # folded to one line by the YAML parser -- folding it is what the
        # scalar means -- so a filter keyed on the newline read it as an
        # ordinary input while it looks like a program in the file, which is
        # the worse half of the pair: a reviewer sees a program and the test
        # does not.
        "B4-pull-request-target-script-input-folded",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: >-\n"
         "            await exec.exec('git', ['fetch', 'origin',\n"
         "            context.payload.pull_request.head.sha])\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "reflow the same one-line script across two lines with a folded "
        "scalar, which reads as line-length housekeeping",
    ),
    Mutation(
        # `context.payload.after` is `${{ github.event.after }}` in the
        # language `actions/github-script` actually runs, and it interpolates
        # nothing, so no expression allowlist ever sees it. Distinct from the
        # two rows above on purpose: their scripts say `pull_request.head`,
        # which the literal backstop matches, and this one says none of the
        # three words. What kills it is the `context` allowlist and nothing
        # else.
        "B4-pull-request-target-script-context-payload",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            await exec.exec('git', ['fetch', 'origin',\n"
         "              context.payload.after]);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "name the head through the payload object the script is handed, "
        "rather than through an expression anything here can read",
    ),
    Mutation(
        # The same object in a `run:` step, which is where the rule above
        # was said not to apply. `@actions/github` is a dependency anyone
        # can install in a line, its `context` reads `GITHUB_EVENT_PATH`
        # inside the library, and what the step writes down is a property
        # access -- no expression, no path, no payload file, nothing any
        # other rule here reads. So the `context` allowlist is read over a
        # step's whole script rather than over its `script:` input, and this
        # is the row that says why: scope it to `script:` inputs and this
        # goes green with every other rule still in place, which was
        # measured before the round that proposed the scoping was answered.
        "B4-pull-request-target-run-context-payload",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          npm install @actions/github\n"
         "          node -e 'const a=require(\"@actions/github\");"
         "require(\"child_process\").execSync(\"git fetch --depth=1 "
         "origin \"+a.context.payload.after)'\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "reach the payload from a `run:` through the library that reads it "
        "for you, which is how a step gets the object without a `script:`",
    ),
    Mutation(
        # The same program one field over. The row above writes it in the
        # `run:`; this writes it in an `env:` value and runs `node -e
        # "$FETCH"`, which is the same library, the same property access and
        # the same fetch, and it was green at `f14997a5` with the row above
        # red. The `context` scan was the one rule in this half read over
        # the script alone: the expression allowlist above it and the four
        # request rules below it are all read over the step's environment
        # too, on the stated principle that a request assembled out of an
        # `env:` value is the same request.
        #
        # It pins that read and nothing else pins it: scope the scan back to
        # the script and this row is SURVIVED with every other row still
        # KILLED, the row above included. Nothing else in the step is left
        # to read -- `@actions/github` finds `GITHUB_EVENT_PATH` inside the
        # library, so there is no expression, no path and no payload file
        # written down anywhere.
        "B4-pull-request-target-run-context-payload-env",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          FETCH: const a=require(\"@actions/github\");"
         "require(\"child_process\").execSync(\"git fetch --depth=1 "
         "origin \"+a.context.payload.after)\n"
         "        run: |\n"
         "          npm install @actions/github\n"
         '          node -e "$FETCH"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "carry the program in an `env:` value and run it from the script, "
        "which is where a step puts a one-liner it does not want to quote "
        "twice",
    ),
    Mutation(
        # And the allowlisted spelling in the same place, because the
        # widening above is only honest if the entry it carries still means
        # what it means in a `script:`. `context.repo` is this repository's
        # own owner and name, in a `run:` as in a `script:`, and the step
        # below reaches nothing else. It stays green, which is the other
        # half of the round-22 answer: the wide read refuses the word
        # `context` and not the program that writes it.
        "B4-pull-request-target-run-context-repo",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Report the repository\n"
         "        run: |\n"
         "          npm install @actions/github\n"
         "          node -e 'const a=require(\"@actions/github\");"
         "console.log(a.context.repo)'\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "log the repository the workflow lives in through the same library, "
        "naming the one context property that is not the pull request",
        must_survive=True,
    ),
    Mutation(
        # And the allowlisted spelling in the field the rule has just
        # started reading, for the reason the row above it exists one field
        # over: the widening is only honest if `context.repo` still means in
        # an `env:` value what it means in a `run:` and in a `script:`.
        # OVERSHOT here means the env read was written as the word `context`
        # rather than as the allowlist over the property after it, which
        # reds on the one spelling that opens nothing -- and it is the whole
        # false-positive budget of the widening, because a `\bcontext\b`
        # with nothing after it matches `kubectl config current-context`
        # too. No `env:` value in the three live `pull_request_target`
        # workflows holds the word today, measured before the rule was
        # widened, so this row is the only place the concession is written
        # down.
        "B4-pull-request-target-run-context-repo-env",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Report the repository\n"
         "        env:\n"
         "          REPORT: const a=require(\"@actions/github\");"
         "console.log(a.context.repo)\n"
         "        run: |\n"
         "          npm install @actions/github\n"
         '          node -e "$REPORT"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "carry the allowlisted property in an `env:` value, which is the "
        "same one-liner the row above hides a payload read in",
        must_survive=True,
    ),
    Mutation(
        # A step whose `shell:` is not a shell. `os.environ["REV"]` is a read
        # of REV that writes no `$REV` and calls no `printenv`, so nothing
        # keyed on a variable *name* reads it.
        # The answer is not to learn Python -- what this step reaches is
        # answerable without reading the program.
        #
        # This said "every scan over the script comes back empty" until
        # 2026-09-21 and that was never true of this script.
        # `_EVENT_PAYLOAD_FILE` carries an alternative for `os.environ` and
        # matches those ten characters on the script's second line,
        # measured. What comes back empty is the scans keyed on a name.
        #
        # Which rule answers it was miscredited here until 2026-09-19. The
        # comment claimed the environment refusal; measured, the expression
        # allowlist gets there first, on
        # `github.event.pull_request.head.sha` in REV's value. The
        # environment refusal is pinned by
        # B4-pull-request-target-run-carried-literal and by nothing else.
        # `_EVENT_PAYLOAD_FILE` stands behind both: write this same step
        # with `REV: deadbeef` so the expression allowlist has nothing to
        # hold and the test reds at it instead, on `os.environ`. That is a
        # probe rather than a row, because a row whose value carries no head
        # is not this row.
        "B4-pull-request-target-run-fetch-python-shell",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        shell: python\n"
         "        env:\n"
         "          REV: ${{ github.event.pull_request.head.sha }}\n"
         "        run: |\n"
         "          import os, subprocess\n"
         '          subprocess.run(["git", "fetch", "origin", os.environ["REV"]])\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "write the fetch in Python, which `shell:` makes a supported thing to "
        "do and which no scan over shell syntax reads",
    ),
    Mutation(
        # The same read without declaring a `shell:` at all: one `python3 -c`
        # under the default bash and the environment is reached by a program
        # this file does not parse. It is the row that says the rule cannot be
        # about which shell the step names -- any program a script starts
        # inherits the whole environment without naming a field of it.
        "B4-pull-request-target-run-fetch-inline-interpreter",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          REV: ${{ github.event.pull_request.head.sha }}\n"
         "        run: |\n"
         "          python3 -c 'import os, subprocess; "
         "subprocess.run([\"git\", \"fetch\", \"origin\", os.environ[\"REV\"]])'\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "reach the environment through an inline interpreter, under the "
        "default shell, with nothing in the step declaring anything unusual",
    ),
    Mutation(
        # GitHub reads an index into a context as the property access it
        # is, so this is `github.event.pull_request.head.sha` with one pair
        # of brackets in it. This comment used to say the brackets were what
        # `pull_request\.head` could not match, which named the wrong rule
        # and stopped being true on 2026-09-20 besides: the ref is inside a
        # `${{ ... }}`, so what refuses it is the expression allowlist,
        # which reads an expression it does not recognise whichever way the
        # property is spelled and runs before either literal backstop.
        # Measured: neuter `_PULL_REQUEST_HEAD` and `_PULL_REQUEST_REF`
        # together and this row is still KILLED. See
        # B4-pull-request-target-run-head-indexed for the same brackets in
        # text carrying no expression, where the backstop is the only reader
        # and was blind to them until that date.
        "B4-pull-request-target-run-fetch-index-syntax",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          git fetch --depth=1 origin "
         "\"${{ github.event.pull_request['head'].sha }}\"\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "index the property rather than naming it, which GitHub resolves to "
        "the same field",
    ),
    Mutation(
        # The same index one language over, and the row that says the
        # literal backstop had the bug the allowlist did not. The row above
        # writes its brackets inside a `${{ ... }}`, so the expression
        # allowlist reads it and refuses it before any literal pattern runs
        # -- measured, by neutering `_PULL_REQUEST_HEAD` and
        # `_PULL_REQUEST_REF` together, at which that row is still KILLED.
        # This one carries no expression at all, a `jq` filter over a file
        # an earlier step wrote, so `_PULL_REQUEST_HEAD` is the only thing
        # that can read it -- and it was `pull_request\.head`, dots only,
        # until 2026-09-20. Revert `_PROPERTY` to a bare `\.` and this row
        # is SURVIVED with every other row still KILLED.
        "B4-pull-request-target-run-head-indexed",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          SHA=$(jq -r '.pull_request[\"head\"].sha'"
         " ./risk-report.json)\n"
         "          git fetch --depth=1 origin \"$SHA\"\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the head out of a JSON file with the index syntax jq and "
        "JavaScript both take, which is the dot spelling with one pair of "
        "brackets in it",
    ),
    Mutation(
        # The payload as a file. `GITHUB_EVENT_PATH` holds the whole webhook
        # event on disk, so the head SHA is reachable with no expression for
        # the script allowlist to read and no `context` property for the
        # JavaScript rule -- the same field, in the one language neither of
        # them parses. Found by an adversarial pass over the allowlist that
        # replaced the denylist, not by the review that prompted it.
        "B4-pull-request-target-run-fetch-event-file",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          REV=$(jq -r .after \"$GITHUB_EVENT_PATH\")\n"
         "          git fetch --depth=1 origin \"$REV\"\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the head out of the event file, which names no expression and "
        "no context property and so reaches neither allowlist",
    ),
    Mutation(
        # The same file read from a `script:` input, where `process.env`
        # rather than a shell gets at the path. Separate row because the two
        # go through different halves of the read: this one is only visible
        # at all because `_step_scripts` folds `with:` values in.
        "B4-pull-request-target-script-event-file",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            const fs = require('fs');\n"
         "            const ev = JSON.parse(fs.readFileSync("
         "process.env.GITHUB_EVENT_PATH, 'utf8'));\n"
         "            await exec.exec('git', ['fetch', 'origin', ev.after]);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the head out of the event file from JavaScript, which builds "
        "its own path to the payload rather than using the one it is handed",
    ),
    Mutation(
        # Expressions are case-insensitive to GitHub and a Python regex is
        # not. Same field as B4-pull-request-target-run-fetch-after, same
        # runner behaviour, shifted key.
        "B4-pull-request-target-run-fetch-upper-case",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          git fetch --depth=1 origin \"${{ GITHUB.EVENT.AFTER }}\"\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "shift the case of an event field, which changes nothing about what "
        "the runner resolves and everything about what a regex matches",
    ),
    Mutation(
        # `github.event.before` is the commit the push moved *from* on a
        # `synchronize`. It is the pull request's code one commit back, which
        # the fork also wrote, and it names none of `pull_request`, `head` or
        # `merge`. This row and the one below are the two fields that made
        # widening the denylist look like the fix.
        "B4-pull-request-target-run-fetch-before",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          git fetch --depth=1 origin \"${{ github.event.before }}\"\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "fetch the commit the push moved from, which is the same fork's code "
        "under a field name that sounds like the base",
    ),
    Mutation(
        # The merge commit GitHub computes for the pull request: the fork's
        # code merged into the base, which is the fork's code. Spelled through
        # `pull_request` but not through `pull_request.head`, so the literal
        # backstop does not match it either.
        "B4-pull-request-target-run-fetch-merge-commit",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          git fetch --depth=1 origin "
         "\"${{ github.event.pull_request.merge_commit_sha }}\"\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "fetch the computed merge commit, which contains the fork's code and "
        "is not spelled `head` anywhere",
    ),
    Mutation(
        # The fetch verb built two hops deep out of `env:`, so that nothing
        # in the script says `fetch` at all. This was the row credited with
        # pinning the loop around the `$NAME` pickup, and that was true
        # only while a gate stood in front of this half: the loop had to run
        # twice for the gate to open. The gate went, and measured 2026-09-19
        # so had the pin -- the head is in the `run:` line in plain sight,
        # the expression allowlist reads it there, and neutering the pickup
        # entirely left this row KILLED. Nothing pinned that loop, and the
        # loop went the same day; what is left is `_expand_env`'s, which this
        # row does not pin either. The shape stays because two-hop verb
        # laundering is a thing somebody will write.
        "B4-pull-request-target-run-fetch-verb-chain",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          VERB: fetch\n"
         "          CMD: git $VERB\n"
         '        run: $CMD --depth=1 origin "${{ github.event.after }}"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "build the fetch verb itself out of two env values, so the step's "
        "script never contains a word this test is watching for",
    ),
    Mutation(
        # The fetch verb itself, laundered through an index into `env`. The
        # gate that used to stand in front of this half matched four words,
        # and `${{ env['CMD'] }}` is none of them: GitHub substitutes the verb
        # in before the shell starts, so the YAML this file reads says `fetch`
        # nowhere. Two rows already launder the verb through a shell variable;
        # this one launders it through an expression, which nothing keyed on
        # `$NAME` could follow because there is no `$CMD` to find. Killed
        # by the expression allowlist, which reads `env['CMD']` as
        # `env.cmd` -- not a recognised expression -- and the head on the
        # same line.
        "B4-pull-request-target-run-verb-indexed-env",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          CMD: git fetch origin\n"
         "        run: ${{ env['CMD'] }} ${{ github.event.after }} && "
         "git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "assemble the command out of an env value indexed by name, which is "
        "the tidying that keeps a long `run:` line under the margin",
    ),
    Mutation(
        # The same laundering with an interpreter instead of an expression.
        # `os.environ["CMD"].split()` builds the argv out of the environment in
        # a language nothing here parses, so neither the verb nor the ref is in
        # any text a pattern could read. It is
        # B4-pull-request-target-run-fetch-python-shell one field further along
        # -- there the ref was in Python and the verb was in the open, here both
        # are -- and it dies on the same assertion, which is the answer to the
        # whole family: the rule never needed to know what the program does.
        "B4-pull-request-target-run-verb-python-env",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        shell: python\n"
         "        env:\n"
         "          CMD: git fetch origin\n"
         "          REV: ${{ github.event.after }}\n"
         "        run: |\n"
         "          import os, subprocess\n"
         '          subprocess.run(os.environ["CMD"].split() + '
         '[os.environ["REV"]])\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "move both halves of the command into the environment and assemble "
        "them in Python, which `shell:` makes a supported thing to do",
    ),
    Mutation(
        # The payload file reached by its path rather than by its variable.
        # `GITHUB_EVENT_PATH` is a convenience the runner sets;
        # `$RUNNER_TEMP/_github_workflow/event.json` is where it points, and a
        # step that writes the path out reads the same bytes while naming the
        # variable nowhere. Separate from B4-pull-request-target-run-fetch-
        # event-file for the reason that row exists at all: the same read, one
        # spelling along, is how this half has been wrong every round.
        "B4-pull-request-target-run-event-file-path",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         '          REV=$(jq -r .after "$RUNNER_TEMP/_github_workflow/'
         'event.json")\n'
         '          git fetch --depth=1 origin "$REV"\n'
         "          git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the event file by its path, which is what a script does when "
        "it wants the payload from somewhere the variable is not set",
    ),
    Mutation(
        # And the variable's name split in two. `'GITHUB' + '_EVENT_PATH'` is
        # one string to JavaScript and no string to a regex, which puts every
        # row that pins the name two characters from useless. The answer is not
        # a longer pattern over names: `process.env` is refused whole, because
        # a `script:` is handed `context`, `github`, `core` and its own inputs
        # and has no business in the process environment at all. Pinned to the
        # real action's SHA so C4's sweep is not what catches this.
        #
        # Split before the underscore rather than after `GITHUB_EVENT_`, which
        # is where it was until 2026-09-19 and which aimed this row at the
        # wrong rule: `GITHUB_EVENT_` is a reserved-prefix word and
        # `_SAFE_RUNNER_VARIABLES` refuses it, so the row died on the
        # runner-variable allowlist and would have died there with
        # `_EVENT_PAYLOAD_FILE` deleted outright. `'GITHUB'` alone is not a
        # reserved-prefix word, so the accessor is now the only thing deciding.
        "B4-pull-request-target-script-event-file-split",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            const fs = require('fs');\n"
         "            const p = JSON.parse(fs.readFileSync("
         "process.env['GITHUB' + '_EVENT_PATH']));\n"
         "            await exec.exec('git', ['fetch', 'origin', p.after]);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "build the environment variable's name out of two literals, which is "
        "what a minifier does and what nothing reading for the name sees",
    ),
    Mutation(
        # The accessor itself indexed rather than dotted. `process['env']` is
        # `process.env` to JavaScript and not `process.env` to a pattern that
        # wanted a literal dot, so the row above could be repaired by writing
        # one more bracket. This is the row that says the rule is the accessor
        # and not its punctuation: `process` reaching `env` by either route is
        # refused, and the name it goes on to build is not read at all. The
        # name is split off the reserved prefix for the reason the row above
        # gives, so that this one is decided by the accessor too.
        "B4-pull-request-target-script-event-file-indexed-accessor",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        uses: actions/github-script"
         "@60a0d83039c74a4aee543508d2ffcb1c3799cdea # v7.0.1\n"
         "        with:\n"
         "          script: |\n"
         "            const fs = require('fs');\n"
         "            const p = JSON.parse(fs.readFileSync("
         "process['env']['GITHUB' + '_EVENT_PATH']));\n"
         "            await exec.exec('git', ['fetch', 'origin', p.after]);\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "index the accessor instead of dotting it, which is the same read "
        "with the one character a pattern over `process.env` required",
    ),
    Mutation(
        # The directory half globbed. `_github_*` is the runner's payload
        # directory and is not the string `_github_workflow`, so the path row
        # above could be repaired with a wildcard and no string arithmetic
        # anywhere. It pins neither half, and not the pair either, which is
        # measured rather than reasoned: neuter the `_github` prefix
        # alternative and this row is still KILLED, neuter `RUNNER_TEMP` and
        # it is still KILLED, neuter both and it is still KILLED, because
        # `RUNNER_TEMP` is off `_SAFE_RUNNER_VARIABLES` and the runner
        # variable allowlist refuses the line a third time. Round 25 added a
        # fourth: the glob alternative reads `_github_*/*.json` as a path
        # arriving at JSON through a globbed directory, without reference to
        # either name. Re-measured on 2026-09-20 -- with the two
        # alternatives, the allowlist *and* the glob neutered it goes
        # SURVIVED, and with any one of the four left it is KILLED. What the
        # row records is the reach -- the payload read with a wildcard where
        # a name used to be -- and that four readings answer it.
        "B4-pull-request-target-run-event-file-glob",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         '          REV=$(jq -r .after "$RUNNER_TEMP"/_github_*/*.json)\n'
         '          git fetch --depth=1 origin "$REV"\n'
         "          git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "glob the runner's payload directory, which reaches the file without "
        "writing any of the names the path was pinned by",
    ),
    Mutation(
        # The payload named by neither variable. `GITHUB_EVENT_PATH` and
        # `RUNNER_TEMP` are both off `_SAFE_RUNNER_VARIABLES`, so every row
        # above that reaches the file through one of them dies on the
        # runner-variable allowlist and would die there with
        # `_EVENT_PAYLOAD_FILE` deleted -- measured by neutering that
        # assertion, at which all seven still went red.
        #
        # What this row does not do is pin the rule, which is what it
        # claimed until the credit was measured. Neuter that assertion and
        # three rows go SURVIVED together: this one, the temp glob below it
        # and the container mount under that, all three of which reach the
        # payload without naming a variable. Nor does it pin an alternative
        # of the pattern on its own -- neuter `work/_temp` and it is still
        # KILLED on `_github`, neuter `_github` and it is still KILLED on
        # `event.json`, neuter `event.json` and it is still KILLED, and only
        # with all three gone does it go SURVIVED. Where those three are
        # pinned moved in round 25 and was re-measured on 2026-09-20 rather
        # than carried: the temp glob below used to pin `work/_temp` alone,
        # the new glob alternative reads that row's path too, and the `find`
        # row under it -- which writes no wildcard -- pins `work/_temp`
        # instead. The container mount still pins `/github/workflow` alone,
        # its glob being in the file name rather than in a directory.
        # `_github` and `event.json` are pinned by no row at all, which is
        # worth knowing and is not a claim this row gets to make.
        #
        # What it records is the reach: the path a GitHub-hosted Linux
        # runner actually uses, written out, with no `GITHUB_`- or
        # `RUNNER_`-prefixed word anywhere for the allowlist to read. The
        # directory and the file name are what is left, and they are what
        # the rule refuses.
        "B4-pull-request-target-run-event-file-literal-path",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          REV=$(jq -r .after /home/runner/work/_temp/"
         "_github_workflow/event.json)\n"
         '          git fetch --depth=1 origin "$REV"\n'
         "          git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the payload at the literal path the runner puts it at, which "
        "names no variable and so reaches no allowlist over names",
    ),
    Mutation(
        # That path with the directory component globbed, which is the row
        # above repaired by one character. `_github_workflow` is the only
        # thing under `$RUNNER_TEMP` and `*` is shorter to write, so this
        # reads the same bytes while naming neither variable, neither
        # prefix, nor `event.json`. It is the reason the directory half of
        # `_EVENT_PAYLOAD_FILE` stopped claiming to be complete: the claim
        # was that the payload lives under `RUNNER_TEMP` and nowhere else,
        # which is true, and that a step therefore has to name the variable
        # to reach it, which is not -- the variable has a value and the
        # value can be typed out.
        "B4-pull-request-target-run-event-file-temp-glob",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          REV=$(jq -r .after /home/runner/work/_temp/*/*.json)\n"
         '          git fetch --depth=1 origin "$REV"\n'
         "          git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "glob one directory further up the runner's temporary tree, which "
        "is the same read with the only component a pattern was watching "
        "for replaced by a star",
    ),
    Mutation(
        # And the same directory seen from inside a container, where the
        # path has no underscore in it anywhere. A `container:` job gets the
        # runner's `_github_workflow` directory bind-mounted at
        # `/github/workflow`, so the payload is at `/github/workflow/
        # event.json` and a glob over it names nothing the runner's own path
        # spelled. Separate row from the one above because it pins a
        # separate alternative: they share no substring.
        "B4-pull-request-target-run-event-file-container-mount",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          REV=$(jq -r .after /github/workflow/*.json)\n"
         '          git fetch --depth=1 origin "$REV"\n'
         "          git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the payload at the path a container job sees it at, which is "
        "the same file the runner mounted and none of the words the "
        "runner's own path is written in",
    ),
    Mutation(
        # The path with the shell's quoting inside it, and the third of the
        # three rows over round 25's finding. The four path alternatives read
        # letters while `_PULL_REQUEST_REF` read a shell word, so `jq -r
        # .after /home/runner/work/_te""mp/_gith""ub_workflow/even""t.json`
        # opens the same file as the literal-path row above and spells none
        # of `work/_temp`, `_github` or `event.json` to a pattern reading
        # them as letters. Green at `d060691d`. It records the `_shell_word`
        # treatment of the path half rather than pinning one alternative of
        # it, and the difference is measured rather than assumed: the step
        # spells `work/_temp`, `_github` and `event.json` quoted, so any one
        # of those three still reading a shell word keeps this row KILLED,
        # and it is SURVIVED only with all three put back. Measured over
        # every subset of the three on 2026-09-20, and at the suite level
        # with all three reverted together, where it is the only row in the
        # table that moves. `/github/workflow` took the same treatment in
        # the same change, is not in this step, and is pinned by no row.
        "B4-pull-request-target-run-event-file-quoted-word",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         '          REV=$(jq -r .after /home/runner/work/_te""mp/'
         '_gith""ub_workflow/even""t.json)\n'
         '          git fetch --depth=1 origin "$REV"\n'
         "          git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "quote two letters of each directory and of the file name, which "
        "the shell drops on the way to `jq` and the path rule read as text",
    ),
    Mutation(
        # The same path with every component globbed, and the row the glob
        # alternative exists for. The three rows above each leave one of the
        # names written down -- `work/_temp` on the temp glob, `_github` on
        # the runner glob, `/github/workflow` on the container mount -- and
        # `/home/runner/*/*/*/*.json` leaves none: the shell assembles the
        # path out of what is on disk, and what is on disk is the payload.
        # Green at `d060691d`, where each of the other three is red.
        #
        # It pins the glob alternative alone: neuter it and this row is
        # SURVIVED, measured on 2026-09-20, and it is the only row of the
        # four that goes with it. That is also why the alternative is worth
        # a row of its own rather than a wider reading of the names -- every
        # segment of the path can be globbed, so a shorter prefix moves the
        # hole one directory up rather than closing it.
        "B4-pull-request-target-run-event-file-any-glob",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          REV=$(jq -r .after /home/runner/*/*/*/*.json)\n"
         '          git fetch --depth=1 origin "$REV"\n'
         "          git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "glob every component of the path, which reaches the payload with "
        "none of the four names anywhere in the step",
    ),
    Mutation(
        # The ordinary side of the row above, and the whole cost of reading
        # a glob. A path rule that tolerates wildcards is the rule in this
        # file most likely to red a step that is doing its job, so the three
        # shapes an ordinary job writes are held green together: `find .
        # -name '*.json'` and `jq -s . coverage/*.json` glob the file name
        # rather than a directory, and `cp dist/*/bundle.json out/` globs a
        # directory inside the workspace rather than one on a path rooted
        # outside it. OVERSHOT here means the alternative has been written as
        # "a glob and a `.json` somewhere in the same word", which was the
        # first draft of it and reds on all three.
        "B4-pull-request-target-run-json-glob-ordinary",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Collect the reports\n"
         "        run: |\n"
         "          find . -name '*.json' -maxdepth 2 | head -5\n"
         "          jq -s . coverage/*.json > all.json\n"
         "          cp dist/*/bundle.json out/\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "glob for JSON three ways a build step does, which is the shape "
        "the payload rule has to read past rather than refuse",
        must_survive=True,
    ),
    Mutation(
        # The payload found rather than named, and the row that carries a
        # pin the glob alternative took off another one. `find` walks the
        # runner's temporary tree and `head -1` takes the payload out of it,
        # so the step names `work/_temp` and nothing else: no glob, no
        # `_github`, no `event.json`, no variable. Before round 25 the temp
        # glob two rows up pinned `work/_temp` alone; the glob alternative
        # now reads that row's path too, so neutering `work/_temp` leaves it
        # KILLED and would have left the alternative unpinned. Measured on
        # 2026-09-20: with `work/_temp` neutered this row is SURVIVED and
        # the temp glob is still KILLED, which is the pin moving here rather
        # than being lost. Red at `d060691d` as well -- it is not a finding,
        # it is where the finding's fix put the credit.
        "B4-pull-request-target-run-event-file-temp-find",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          F=$(find /home/runner/work/_temp -type f | head -1)\n"
         '          REV=$(jq -r .after "$F")\n'
         '          git fetch --depth=1 origin "$REV"\n'
         "          git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "search the runner's temporary tree for the payload instead of "
        "spelling the two directories under it",
    ),
    Mutation(
        # And the same reach in the other language a step can be written in.
        # An earlier round claimed `shell: python` was handled while reading
        # only the JavaScript accessor, so `os.environ["GITHUB_EVENT_" +
        # "PATH"]` was the split name with nothing watching for it. Python's
        # accessors are refused for the reason JavaScript's are: a `run:` step
        # is handed its `env:` block as ordinary variables, so a body that
        # goes to the process environment programmatically is going around
        # the route it was given.
        "B4-pull-request-target-python-event-file-environ",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        shell: python\n"
         "        run: |\n"
         "          import json, os, subprocess\n"
         '          rev = json.load(open(os.environ["GITHUB" + '
         '"_EVENT_PATH"]))["after"]\n'
         '          subprocess.run(["git", "fetch", "origin", rev])\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the payload from Python, where the variable's name splits off "
        "the reserved prefix the same way and the accessor is `os.environ`",
    ),
    Mutation(
        # The accessor imported under another name, which is the row above
        # repaired by an import. `from os import environ as e` binds the
        # mapping to a one-letter name and reads it as `e.items()`, so
        # `os.environ` appears nowhere, the subscript a pattern wanted is on
        # a name this file cannot predict, and the word `environ` survives
        # only at the import. The rule reads the bare word for that reason.
        "B4-pull-request-target-python-environ-aliased-import",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        shell: python\n"
         "        run: |\n"
         "          from os import environ as e\n"
         '          rev = [v for k, v in e.items() '
         'if k.endswith("HEAD_REF")][0]\n'
         "          import subprocess\n"
         '          subprocess.run(["git", "fetch", "origin", rev])\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "alias the environment mapping on import and match its keys by "
        "suffix, which spells neither the accessor nor the variable",
    ),
    Mutation(
        # The same mapping in the language a `run:` step reaches for when it
        # wants a field out of a line. awk's environment is `ENVIRON`, and
        # `for (n in ENVIRON)` walks it, so the branch is chosen by a name
        # this file never sees and the reserved prefix is never written. The
        # word was already in the rule; what was not was awk's case. Two
        # languages was never the language list -- a `run:` step runs what is
        # installed on the runner, and perl and awk both are.
        "B4-pull-request-target-awk-environ-enumeration",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          B=$(awk 'BEGIN { for (n in ENVIRON) "
         "if (n ~ /HEAD_REF$/) print ENVIRON[n] }')\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "walk the environment from awk, whose mapping is spelled `ENVIRON` "
        "in the only case awk accepts",
    ),
    Mutation(
        # perl's whole environment, which is `%ENV`. The copy into `%e` is
        # not obfuscation for its own sake: it is what keeps this row a pin
        # on the hash sigil rather than on the element accessor below, since
        # the natural spelling of this loop reads `$ENV{$_}` and would be
        # refused by either alternative.
        "B4-pull-request-target-perl-env-hash",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          B=$(perl -e 'my %e = %ENV; for (keys %e) "
         "{ print $e{$_} if /HEAD_REF/ }')\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "copy perl's whole environment out of `%ENV` and match its keys by "
        "suffix, which spells neither the accessor python and node use nor "
        "the variable",
    ),
    Mutation(
        # One element of it, by a name assembled at run time. perl is the
        # language where the element read is the hole rather than a
        # narrower case of the enumeration above: python and node spell the
        # accessor the same way whether the key is a literal or a variable,
        # so `os.environ[k]` is already refused, while perl changes sigil
        # between the hash and its element and `$ENV{$k}` would otherwise
        # reach the value with no accessor either rule knew and no name
        # anywhere in the text.
        "B4-pull-request-target-perl-env-element",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          N=GITHUB\n"
         "          B=$(perl -e 'print $ENV{$ARGV[0] . \"_HEAD_REF\"}' "
         '"$N")\n'
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read one element of perl's environment by a key the step builds "
        "out of an argument, which is the sigil the hash rule above does "
        "not cover",
    ),
    Mutation(
        # ruby, which is a `shell:` the moment somebody writes `ruby {0}`.
        # Its environment is the constant `ENV` and `ENV.to_h` hands the lot
        # over as a hash, so the walk that picks the head-ref key out of it
        # spells neither the accessor python and node use nor the variable.
        # It pins the `.` in the `ENV` alternative.
        "B4-pull-request-target-ruby-env-mapping",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        shell: ruby {0}\n"
         "        run: |\n"
         "          b = nil\n"
         "          ENV.to_h.each { |k, v| b = v if k =~ /HEAD_REF/ }\n"
         '          system("git fetch origin #{b} && git checkout FETCH_HEAD")\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "walk the environment from ruby, whose mapping is a constant read "
        "as an object rather than a function called with a name",
    ),
    Mutation(
        # The same constant subscripted, with the reserved prefix split off
        # the way the python row splits it. `ENV["GITHUB" + "_HEAD_REF"]`
        # leaves no `GITHUB_`-prefixed word for the runner-variable
        # allowlist, so the accessor is the only thing deciding -- which is
        # the argument for reading the accessor, made in a fifth language.
        # It pins the `[` in the `ENV` alternative.
        "B4-pull-request-target-ruby-env-element",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        shell: ruby {0}\n"
         "        run: |\n"
         '          b = ENV["GITHUB" + "_HEAD_REF"]\n'
         '          system("git fetch origin #{b} && git checkout FETCH_HEAD")\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read one element of ruby's environment by a key built out of two "
        "literals, which is the subscript rather than the mapping",
    ),
    Mutation(
        # go, and the reason this one is about case rather than about go.
        # `getenv` was matched in lower case only -- the spelling C and php
        # use, and the one spelling go does not -- so `os.Getenv` walked
        # past the alternative written for it. It pins the `(?i:` on that
        # alternative: with the case sensitivity back, nothing here matches.
        "B4-pull-request-target-go-getenv-case",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          cat > m.go <<'EOF'\n"
         "          package main\n"
         '          import ("os"; "os/exec")\n'
         "          func main() {\n"
         '            b := os.Getenv("GITHUB" + "_HEAD_REF")\n'
         '            exec.Command("git", "fetch", "origin", b).Run()\n'
         "          }\n"
         "          EOF\n"
         "          go run m.go && git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "compile the read out of a heredoc in go, whose getter is spelled "
        "with the capital the lower-case alternative could not see",
    ),
    Mutation(
        # The other half of go's pair. `os.LookupEnv` is `os.Getenv` with a
        # second return value saying whether the variable was set at all,
        # and it shares no substring with the first, so it is its own
        # alternative and needs its own row.
        "B4-pull-request-target-go-lookup-env",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          cat > m.go <<'EOF'\n"
         "          package main\n"
         '          import ("os"; "os/exec")\n'
         "          func main() {\n"
         '            b, _ := os.LookupEnv("GITHUB" + "_HEAD_REF")\n'
         '            exec.Command("git", "fetch", "origin", b).Run()\n'
         "          }\n"
         "          EOF\n"
         "          go run m.go && git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "use the other getter go's os package exports, which asks the same "
        "question and is spelled nothing like the first",
    ),
    Mutation(
        # .NET's, which is what the word boundary on the *end* of the
        # `getenv` alternative was costing. PowerShell reaches the whole
        # mapping as `[Environment]::GetEnvironmentVariables()`, where
        # `environ` has `ment` after it and `GetEnv` has `ironmentVariables`
        # after it -- so the bare-word alternative missed it and a trailing
        # `\b` on the getter alternative would miss it too. An identifier
        # that begins `getenv` is a getter whatever it goes on to spell.
        "B4-pull-request-target-dotnet-environment-variables",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        shell: pwsh\n"
         "        run: |\n"
         "          $all = [Environment]::GetEnvironmentVariables()\n"
         "          $k = $all.Keys | Where-Object { $_ -like '*HEAD_REF' }\n"
         "          git fetch origin $all[$k]\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "take the whole mapping off .NET's static class, whose method name "
        "buries both of the words this file reads for",
    ),
    Mutation(
        # The false-positive direction both of the alternatives above open,
        # in one step. Matching `environ` without regard to case puts
        # `ENVIRONMENT` one word boundary away from a red, and `$ENV` is the
        # shell's startup-file variable rather than the mapping, which is
        # why the perl element rule wants the brace. Neither is a read of
        # the runner's environment and neither may go red.
        "B4-pull-request-target-run-environment-word",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Name the deployment\n"
         "        run: |\n"
         "          ENVIRONMENT=staging\n"
         '          echo "deploying to $ENVIRONMENT, '
         'startup file ${ENV:-none}, raw $ENV"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "name a deployment environment and mention the shell's own `$ENV`, "
        "neither of which reaches the runner's environment",
        must_survive=True,
    ),
    Mutation(
        # The environment read whole, in the shell. `env` hands the step
        # every variable the runner set, `GITHUB_HEAD_REF` among them, and
        # the only spelling of that name on the line is lower case -- which
        # is not the name the shell would expand and is exactly the name
        # `grep -i` matches. The allowlist over the names reads nothing
        # here, and this was green until 2026-09-19. It is the row
        # `_ENVIRONMENT_ENUMERATION` exists for, and the rule's eight other
        # alternatives are spelled out one at a time below -- `printenv`,
        # `declare`, `export`, `set`, `compgen`, the `${!prefix@}` listing,
        # BSD's `ps e` and PowerShell's `Env:` drive. They share no
        # substring, so no one mutation reaches two. A row each is the floor
        # rather than the count: `compgen` takes three of its own for the two
        # letters and the long form, and `env` takes four more for its
        # terminators and for being run by its path.
        "B4-pull-request-target-run-env-dump-grep",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          B=$(env | grep -i '^github_head_ref=' | sed -e 's/.*=//')\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "dump the environment and pick the branch out of it by a "
        "case-insensitive match, which is the shortest way to read a "
        "variable without writing the name the shell knows it by",
    ),
    Mutation(
        # The same dump from the program this file already refuses *with* an
        # operand. `printenv GITHUB_HEAD_REF` names the variable and dies on
        # the allowlist over the names; `printenv` alone names nothing and
        # prints the same value. The pair is the whole argument for this
        # rule in two rows.
        "B4-pull-request-target-run-printenv-dump",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          B=$(printenv | grep -i '^github_head_ref=' "
         "| sed -e 's/.*=//')\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "use the other program that prints the environment, with the "
        "operand that would have named a variable left off",
    ),
    Mutation(
        # The shell builtin that prints the table, rather than the program
        # that prints the environment. `declare -p` is a dump and `declare
        # -a xs` is a declaration, which is why the rule is anchored on what
        # follows the word rather than on the word.
        "B4-pull-request-target-run-declare-dump",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          B=$(declare -p | grep -i 'github_head_ref=' "
         "| sed -e 's/.*=//')\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "print the shell's own variable table, which is the environment "
        "plus the shell's and is one flag away from an assignment",
    ),
    Mutation(
        # And the builtin whose job is setting a variable, printing them
        # instead. `export -p` and `export PATH=x` are the same word in
        # opposite directions, and one of them is a read.
        "B4-pull-request-target-run-export-dump",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          B=$(export -p | grep -i 'github_head_ref=' "
         "| sed -e 's/.*=//')\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "print the exported variables, which is the environment under the "
        "name of the builtin that sets it",
    ),
    Mutation(
        # The word every script in this repository already writes, with the
        # options left off. `set -euo pipefail` is a directive and a bare
        # `set` is a dump of everything the shell has, and a rule that
        # cannot tell them apart is a rule nobody can keep.
        "B4-pull-request-target-run-set-dump",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          B=$(set | grep -i '^github_head_ref=' | sed -e 's/.*=//')\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "print everything the shell has, which is what `set` with no "
        "operands does and what `set -euo pipefail` does not",
    ),
    Mutation(
        # The names alone, with nothing but the name written down.
        # `compgen -e` prints what is exported and the loop picks the
        # head-ref one out of the listing by a glob, so the variable's name
        # is in the text in neither case the shell knows it by.
        #
        # This row has been rewritten twice for the same reason, and the
        # reason is worth keeping: a mutation that finishes its story with a
        # read of the *value* gets killed by whichever rule refuses that
        # read, and the alternative it was written to pin goes unpinned. It
        # was `${!N}` until `_INDIRECT_EXPANSION` landed, then `eval
        # "B=\$$N"` until `_COMPUTED_NAME_READ` landed, and it hands the
        # name to `$GITHUB_OUTPUT` now -- which is a step the runner
        # allowlists, so the listing is the only thing here to refuse.
        "B4-pull-request-target-run-compgen-dump",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          for N in $(compgen -e); do\n"
         "            case $N in [Gg][Ii][Tt][Hh][Uu][Bb]_[Hh][Ee][Aa][Dd]*)"
         ' echo "name=$N" >> "$GITHUB_OUTPUT";; esac\n'
         "          done\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "list the names of the exported variables and hand the head-ref "
        "one's name to the next step, which reaches the listing with the "
        "name written nowhere",
    ),
    Mutation(
        # The same dump with stderr folded into it, which is what somebody
        # writes when a program is noisy. `>` was a terminator and `2>` was
        # not, so this walked past a rule that caught the identical line
        # without the `2` in it. It pins the `\d+[<>]` alternative in
        # `_ENUMERATION_END`, which no other row reaches.
        "B4-pull-request-target-run-env-dump-fd-redirect",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          B=$(env 2>&1 | grep -i '^github_head_ref=' "
         "| sed -e 's/.*=//')\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "write the file descriptor in front of the redirect, which is the "
        "same dump with stderr folded in and was not a terminator",
    ),
    Mutation(
        # The same dump inside the older command substitution. A backtick
        # closes it, so ``E=`env` `` is `E=$(env)` with the paren spelled
        # another way -- and `_COMMAND_END`, the command-separator class
        # defined near the top of the same file, has read a backtick as the
        # end of a command since it was written.
        # The terminator set had not, so this walked past a rule that caught
        # the identical dump in the `$(...)` spelling. It pins the backtick
        # in `_ENUMERATION_END`, which no other row reaches.
        "B4-pull-request-target-run-env-dump-backtick",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          E=`env`\n"
         "          B=$(grep -i '^github_head_ref=' <<<\"$E\" | cut -d= -f2)\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "run the dump in the command substitution bash inherited from the "
        "Bourne shell, whose closing character the terminator set left out",
    ),
    Mutation(
        # And the same dump with prose after it. A `#` starts a comment, so
        # the command ends there as surely as it ends at a newline, and
        # `env  # every variable there is` with the `)` on the line below is
        # a dump somebody annotated. It pins the `#` in `_ENUMERATION_END`.
        "B4-pull-request-target-run-env-dump-comment",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          E=$(env  # every variable there is\n"
         "          )\n"
         "          B=$(grep -i '^github_head_ref=' <<<\"$E\" | cut -d= -f2)\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "end the dump with a comment rather than with an operator, which "
        "ends the command and was not in the terminator set",
    ),
    Mutation(
        # The same dump with the program name quoted, which is the round-16
        # hole. A quote inside a word ends and reopens the quoting rather
        # than joining the name, so `$('env')` runs `env` -- and the letters
        # were all there for the pattern to read. What was not there was a
        # terminator: the character after the last letter is a `'`, and the
        # terminator set is where an operator goes. `"printenv"`, `e"n"v`
        # and `en''v` are the same step and were green with it.
        #
        # It used to pin `_enumeration_word` on its own. It does not now,
        # and that claim is withdrawn rather than reworded. Measured: with
        # `_enumeration_word` returning its argument unchanged the full
        # sweep is `killed=238 noisy=22 survived=0 stale=0`, which is the
        # baseline -- not one row flips, this one included. The reason is
        # that `_ENUMERATION_STOP` opens with the same `_SHELL_QUOTING` run,
        # so the terminator eats the closing `'` itself and the `)` behind
        # it ends the command as it always did. Drop that run instead and
        # keep the word and this row still does not flip, because the word's
        # own trailing run eats the `'`: the nine env-dump rows come back
        # `killed=8 noisy=0 survived=1`, and the survivor is the spaced-quote
        # row below rather than this one. Both have to go before this row is
        # SURVIVED. With both neutered the sweep is
        # `killed=236 noisy=22 survived=2 stale=0` and the two survivors are
        # this row and `B4-pull-request-target-run-env-dump-spaced-quote`.
        # So the run in the terminator is the half a row pins alone, and what
        # the interleaving still buys with that run in place is `e"n"v` and
        # `en''v` -- which no row here carries, a gap in the rows rather than
        # in the rule. The control beside them is what says the fix is the
        # word and not the terminator: `grep "env" Makefile` has to stay
        # green, and it does not if a quote is allowed to end a dump.
        "B4-pull-request-target-run-env-dump-quoted-program",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          E=$('env')\n"
         "          B=$(grep -i '^github_head_ref=' <<<\"$E\" | cut -d= -f2)\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "quote the program name, which changes nothing about what the shell "
        "runs and puts a quote where the rule wanted an operator",
    ),
    Mutation(
        # The same dump from the eighth language, and the one already on the
        # runner. `jq` is preinstalled and embedded in `gh` as `--jq`, and
        # `$ENV` is its whole environment as an object, so this is `env |
        # grep` written as a jq program: no `env` word for the enumeration
        # rule, no accessor any of the seven languages above spell, and no
        # variable name for the runner allowlist to hold. Green at
        # `d060691d`. It pins jq's `$ENV` in `_EVENT_PAYLOAD_FILE`, which is
        # read where it is *used* rather than as three bare letters --
        # `_ENVIRONMENT_ENUMERATION`'s control writes `raw $ENV` in a message
        # and stays green, measured, which a `\$ENV\b` alternative would
        # have OVERSHOT.
        "B4-pull-request-target-run-jq-env-object",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          B=$(jq -rn '$ENV | to_entries[]"
         ' | select(.key | test("head_ref"; "i")) | .value\')\n'
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "ask jq for its own environment as an object, which is the dump "
        "written in the one language on this runner that has it built in",
    ),
    Mutation(
        # The same dump with one space in it, and the hole the trailing
        # quoting run left. `jq -rn 'env'` is refused because the word's
        # trailing run eats the quote and the end of the line terminates it;
        # `jq -rn 'env '` is the identical program -- jq ignores the space --
        # and the run never reaches the quote, because a space is in front of
        # it. Green at `d060691d`, where the line without the space is red.
        # It pins the quoting run's new position in `_ENUMERATION_STOP`,
        # which reads horizontal space on both sides of it.
        "B4-pull-request-target-run-env-dump-spaced-quote",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          B=$(jq -rn 'env ' | grep -i head_ref"
         " | head -1 | cut -d'\"' -f4)\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "put one space between the dump and the quote that closes the jq "
        "program, which is the same program and was not a terminator",
    ),
    Mutation(
        # The same word finished with an expansion instead. `NOPE` is unset,
        # an unset variable expands to nothing, and `env$NOPE` is therefore
        # `env` -- with a `$` after the name where the rule wanted a
        # terminator, which is the quoted row's mechanism in the one
        # punctuation character quoting does not cover.
        #
        # It pins `_ENUMERATION_GLUE`. Drop it from `_ENUMERATION_STOP` and
        # this row is SURVIVED on its own, the quoted row above staying
        # KILLED.
        #
        # What it does not pin is the glue being *adjacency-only*, and this
        # comment credited two controls with holding that until round 20.
        # Neither does. `set $FLAGS` -- the line the argument at
        # `_ENUMERATION_GLUE` is written about, where a space means the
        # expansion is an argument rather than the rest of the program's
        # name -- is carried by no row in this table. And `export
        # PATH="$PWD/bin:$PATH"`, which B4-pull-request-target-run-backtick-
        # and-comment and B4-pull-request-target-run-env-ordinary-shell both
        # do carry, puts its `$` after a `PATH="` rather than after the
        # builtin, so a glue relaxed to `[ \t]*\$` never reaches it either.
        # Measured: relax it that way and the whole sweep is unchanged --
        # nothing OVERSHOT, nothing SURVIVED. The adjacency is an argument
        # with no row behind it, which is what this now says instead of
        # naming two rows that do not make it.
        "B4-pull-request-target-run-env-dump-glued-expansion",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          E=$(env$NOPE)\n"
         "          B=$(grep -i '^github_head_ref=' <<<\"$E\" | cut -d= -f2)\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "glue an unset variable to the end of the program name, which the "
        "shell drops and the terminator set could not see past",
    ),
    Mutation(
        # The safe side of the two rows above, and the reason the quoting
        # went into the word rather than into the terminator set. Every line
        # here writes one of the enumeration words next to a quote and none
        # of them runs it: `grep "env"` and `jq '.env'` pass the word to a
        # program as a pattern, and `echo 'export PATH=/x'` writes a line
        # into a file. OVERSHOT here means a quote has been let loose as a
        # terminator, which is the naive fix for the rows above and a suite
        # that reds on the first step that greps for a word.
        "B4-pull-request-target-run-quoted-word-ordinary",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Inspect the build files\n"
         "        run: |\n"
         '          grep "env" Makefile || true\n'
         "          jq -r '.env' package.json || true\n"
         "          echo 'export PATH=/x' >> ./profile\n"
         "          cat ./profile\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "grep for one of the words the enumeration rule reads and write "
        "another of them into a file, quoting both because a shell needs it",
        must_survive=True,
    ),
    Mutation(
        # The ordinary side of the row that put a space in front of the
        # quote. Both lines here write an enumeration word, a space and a
        # quote, and neither is a read: `grep -n 'env ' Makefile` searches
        # for the word with a trailing space in it, and `jq -r '.env | keys'`
        # asks a JSON file for one of its keys. OVERSHOT here means the
        # horizontal space around the quoting run has been let loose from
        # the terminator that has to follow it, which reds on the first step
        # that greps for a word and on the round-16 control above as well.
        "B4-pull-request-target-run-spaced-quoted-word-ordinary",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Check the makefile\n"
         "        run: |\n"
         "          grep -n 'env ' Makefile | head -5\n"
         "          jq -r '.env | keys' package.json\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "grep for the enumeration word with a space inside the quotes, "
        "which is the same two characters the dump above is refused for",
        must_survive=True,
    ),
    Mutation(
        # The safe side of both rows above. A backtick substitution and a
        # trailing comment are ordinary shell, and this step writes three of
        # the words the enumeration rule reads with one of the two new
        # terminators after each of them: `set -euo pipefail`, `export
        # PATH=...` and `declare -a` all carry a `#` further along the line,
        # and the `REV=` line closes a backtick. None of them is a read.
        # OVERSHOT here means a terminator was let loose from the word it has
        # to follow, which is a suite that reds on the first commented shell
        # script somebody writes.
        "B4-pull-request-target-run-backtick-and-comment",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Record the build\n"
         "        run: |\n"
         "          set -euo pipefail  # fail fast\n"
         '          export PATH="$PWD/bin:$PATH"  # the tools we just built\n'
         "          declare -a steps=(fetch build)  # in order\n"
         "          REV=`git rev-parse HEAD`\n"
         '          echo "built $REV for ${steps[*]}"  # done\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "write an ordinary shell script that comments four of its lines "
        "and takes a revision out of a backtick substitution",
        must_survive=True,
    ),
    Mutation(
        # The same dump run by its path. `env` is a program, and the
        # lookbehind that keeps `venv` and `foo.env` from reading as one
        # excluded a `/` in front of it -- which was how the shebang stayed
        # green and how this did too. It pins the narrower
        # `_ENUMERATION_PROGRAM_START`; the shebang is exempted by its
        # operand now and is held green by the control below.
        "B4-pull-request-target-run-env-dump-by-path",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          B=$(/usr/bin/env | grep -i '^github_head_ref=' "
         "| sed -e 's/.*=//')\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "run the same program by its absolute path, which is the spelling "
        "the shebang exemption used to cover",
    ),
    Mutation(
        # `compgen -e` lists what is exported; `compgen -v` lists every
        # variable the shell has, which is a superset of it. The rule read
        # the narrower one only. The name handed on rather than the value
        # read is deliberate, for the reason set out on the `-e` row: every
        # spelling of the value read is refused by a rule of its own now,
        # and any of them here would leave the `v` in the compgen
        # alternative pinned by nothing.
        "B4-pull-request-target-run-compgen-variable-dump",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          for N in $(compgen -v); do\n"
         "            case $N in [Gg][Ii][Tt][Hh][Uu][Bb]_[Hh][Ee][Aa][Dd]*)"
         ' echo "name=$N" >> "$GITHUB_OUTPUT";; esac\n'
         "          done\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "ask compgen for every shell variable rather than for the exported "
        "ones, which is the superset and the same listing",
    ),
    Mutation(
        # And the long spelling of the pair. `-A variable` is `-v` and `-A
        # export` is `-e`, written as the action rather than as the letter,
        # so a rule that reads only the letters misses both. Its own
        # alternative, and its own row.
        "B4-pull-request-target-run-compgen-variable-action",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          for N in $(compgen -A variable); do\n"
         "            case $N in [Gg][Ii][Tt][Hh][Uu][Bb]_[Hh][Ee][Aa][Dd]*)"
         ' echo "name=$N" >> "$GITHUB_OUTPUT";; esac\n'
         "          done\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "spell the same action as a word rather than as a letter, which is "
        "the documented long form and reads nothing like `-v`",
    ),
    Mutation(
        # Bash's own listing, with no program in it at all. `${!GITHUB_@}`
        # expands to the *names* of every variable with that prefix, so the
        # enumeration is a parameter expansion and the eight alternatives
        # above -- every one of them anchored on a command word or, for the
        # `Env:` drive, on the cmdlet in front of it -- read none of
        # it. Only the name is handed on, which keeps this off both
        # `_INDIRECT_EXPANSION` and `_COMPUTED_NAME_READ`, so the name
        # listing is what kills it.
        "B4-pull-request-target-run-prefix-name-listing",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         '          for v in "${!GITHUB_@}"; do\n'
         '            case $v in *HEAD_REF) echo "name=$v" >> "$GITHUB_OUTPUT";; esac\n'
         "          done\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "ask the shell itself for the names, which is a parameter expansion "
        "and not a command the enumeration rule can anchor on",
    ),
    Mutation(
        # The name assembled a piece at a time and then read through it.
        # Nothing here enumerates anything -- one variable, chosen by the
        # script -- and `GITHUB_HEAD_REF` appears nowhere for
        # `_RUNNER_VARIABLE` to hold against the allowlist. This is the only
        # row that pins `_INDIRECT_EXPANSION`: the `env:` carries no
        # expression, so the allowlist that kills
        # `B4-pull-request-target-run-indirect-expansion` first has nothing
        # to read here.
        "B4-pull-request-target-run-indirect-assembled-name",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          v=GITHUB\n"
         '          v="${v}_HEAD_REF"\n'
         '          git fetch origin "${!v}" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "build the variable's name out of two halves and read it "
        "indirectly, so the name the read resolves to is never in the text",
    ),
    Mutation(
        # The same assembled name bound to a second word instead of read
        # through a sigil. `declare -n r=$v` is a bash nameref: `r` becomes
        # another name for the variable *named* by `v`, so `"$r"` is an
        # ordinary-looking expansion of a variable this file cannot
        # identify. It is not `${!`, and it was green. It pins the nameref
        # alternative of `_COMPUTED_NAME_READ`.
        "B4-pull-request-target-run-nameref",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          v=GITHUB\n"
         '          v="${v}_HEAD_REF"\n'
         "          declare -n r=$v\n"
         '          git fetch origin "$r" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "bind a nameref to the variable whose name the script assembled, "
        "which reads it through a word of the script's own choosing",
    ),
    Mutation(
        # And the same read done by re-parsing. The backslash keeps the
        # first `$` out of the first expansion, so the shell expands `$v`
        # into the text `$GITHUB_HEAD_REF` and then runs the assignment
        # against what it just built. No `${!`, no nameref, and the variable
        # spelled nowhere. It pins the `eval` alternative of
        # `_COMPUTED_NAME_READ`, and it is the read three rows in the
        # enumeration block used to finish their story with.
        "B4-pull-request-target-run-eval-deferred-dollar",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          v=GITHUB\n"
         '          v="${v}_HEAD_REF"\n'
         '          eval "B=\\$$v"\n'
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "escape the dollar so the second expansion resolves it, which "
        "reaches the variable with the name built between the two passes",
    ),
    Mutation(
        # The same re-parse with the dollar deferred by quotes instead of by
        # a backslash. `'B=${'$v'}'` is three quoted runs the shell
        # concatenates into `B=${GITHUB_HEAD_REF}`, which `eval` then runs:
        # the dollar that survives the first pass is inside single quotes
        # rather than behind a `\`, and `(?:\\\$|\$\$)` read neither of those
        # spellings. Green at `d060691d`, as were `eval echo '$'"$v"` and
        # `eval B='$v`. It pins the single-quoted branch of
        # `_DEFERRED_DOLLAR`, which is anchored on the quote *closing* right
        # after the dollar -- `awk '{print $1}'` and `sed 's/$//p'` carry a
        # dollar inside single quotes too, and the control below holds them
        # green.
        "B4-pull-request-target-run-eval-quoted-dollar",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          v=GITHUB\n"
         '          v="${v}_HEAD_REF"\n'
         "          eval 'B=${'$v'}'\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "defer the dollar in single quotes rather than behind a backslash, "
        "which is the same two passes with the other quoting character",
    ),
    Mutation(
        # The safe side of the row above, and the reason it is anchored on a
        # deferred dollar rather than on the word. `eval "$(ssh-agent -s)"`
        # is how every workflow that loads a key starts and `eval "$cmd"`
        # runs a command line built earlier; both expand once, and neither
        # has a dollar that survives the first pass to name something the
        # second one finds. OVERSHOT here means `eval` has been banned, which
        # is a suite that reds on the standard two lines of ssh setup.
        "B4-pull-request-target-run-eval-ordinary",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Load the deploy key\n"
         "        run: |\n"
         '          eval "$(ssh-agent -s)"\n'
         '          cmd="git fetch origin main"\n'
         '          eval "$cmd"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "start an ssh agent and run a command line held in a variable, "
        "which is `eval` used the two ways a workflow legitimately uses it",
        must_survive=True,
    ),
    Mutation(
        # The ordinary side of the quoted dollar, and the reason that branch
        # requires the quote to close on it. `awk '{print $1}'` is the most
        # ordinary thing downstream of an `eval "$cmd"` there is, and its
        # dollar is inside single quotes with three characters after it.
        # OVERSHOT here means the branch has been written as "a dollar
        # anywhere inside single quotes on an `eval` line", which reds on
        # every field an awk or sed program names.
        "B4-pull-request-target-run-eval-quoted-dollar-ordinary",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Summarise the log\n"
         "        run: |\n"
         '          cmd="git log -1 --format=%H"\n'
         "          eval \"$cmd\" | awk '{print $1}' | tee out.txt\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "pipe an evaluated command line into awk, which puts a dollar in "
        "single quotes on the same line as an `eval` and reads nothing",
        must_survive=True,
    ),
    Mutation(
        # The environment as the file the kernel writes, reached with a
        # glob. `/proc/<pid>/environ` is the same bytes `env` prints and
        # `\b(?i:environ)\b` was credited with catching it -- but the last
        # four letters can be left to the shell, and `cat /proc/self/env*`
        # reads the identical file with nothing for a pattern over the word
        # to hold. Green at `d060691d`, as was `/proc/self/e*`.
        # `/proc/*/environ` was not: it writes the word out, so the literal
        # already covered it, and the claim that it did not was measured
        # wrong. It pins the `/proc` glob alternative, which
        # reads a wildcard anywhere under procfs rather than chasing the
        # spellings of one word; the literal `environ` goes on covering the
        # path written out, and is pinned by no row.
        "B4-pull-request-target-run-proc-environ-glob",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          E=$(cat /proc/self/env* | tr '\\0' '\\n')\n"
         '          B=$(echo "$E" | grep -i \'^github_head_ref=\''
         " | cut -d= -f2)\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "leave the last four letters of the file name to the shell, which "
        "opens the same file and writes none of the word",
    ),
    Mutation(
        # The ordinary side of the row above, and the cost of reading a
        # glob under procfs. A step asking the kernel about the machine it
        # is on writes these paths out in full, and neither of them is an
        # environment: `/proc/self/status` is this process's memory and
        # `/proc/cpuinfo` is the runner's hardware. OVERSHOT here means
        # procfs itself has been refused, which is a suite that reds on the
        # first step that reports how big the runner is.
        "B4-pull-request-target-run-proc-path-ordinary",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Report the runner\n"
         "        run: |\n"
         "          grep VmRSS /proc/self/status\n"
         "          head -3 /proc/cpuinfo\n"
         "          nproc\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read two procfs files by their full names, which is how a step "
        "reports the machine and is not a dump of anything",
        must_survive=True,
    ),
    Mutation(
        # The same table out of the kernel rather than out of the shell.
        # `ps eww $$` prints this process's environment, which is the bytes
        # `env` prints, and BSD's `e` carries no `-` -- which is what tells
        # it from the `ps -ef` every ordinary script writes and what the
        # control below holds green.
        "B4-pull-request-target-run-ps-environment",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         "          B=$(ps eww $$ | tr ' ' '\\n' "
         "| grep -i '^github_head_ref=' | sed -e 's/.*=//')\n"
         '          git fetch origin "$B" && git checkout FETCH_HEAD\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "read the environment out of the process table with BSD's `e` "
        "option, which is neither a shell builtin nor a program named for "
        "the environment",
    ),
    Mutation(
        # The same listing in the shell GitHub documents beside `bash`.
        # `pwsh` is a first-class `shell:` and is preinstalled on
        # `ubuntu-latest`, and PowerShell exposes the environment as a
        # filesystem, so `Get-ChildItem Env:` is `env` and the pipe into
        # `Where-Object` is the `grep -i` -- with none of the five words the
        # rule was five words of. It pins the `Env:` drive alternative.
        "B4-pull-request-target-pwsh-env-drive",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        shell: pwsh\n"
         "        run: |\n"
         "          $v = Get-ChildItem Env: | "
         "Where-Object Name -like '*HEAD_REF'\n"
         "          git fetch origin $v.Value\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "list the environment as a drive from PowerShell, which asks for "
        "every variable without running any of the programs that print them",
    ),
    Mutation(
        # The safe side of the row above, and the review finding this file
        # declined. PowerShell has no bare `$NAME` for an environment
        # variable: `$env:GITHUB_REPOSITORY` *is* how a `pwsh` step reads
        # the value `bash` reads as `$GITHUB_REPOSITORY`, the name is
        # written down either way, and `_SAFE_RUNNER_VARIABLES` holds it.
        # OVERSHOT here means `$env:` has been read as an accessor -- which is
        # a suite that reds on every ordinary `pwsh` step for spelling a
        # safe variable the only way its shell spells one.
        "B4-pull-request-target-pwsh-named-variable",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Report the build\n"
         "        shell: pwsh\n"
         "        run: |\n"
         '          Write-Host "building $env:GITHUB_REPOSITORY '
         'at $env:GITHUB_SHA"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "name two allowlisted variables from PowerShell, which spells a "
        "named read with the same `env` the drive listing uses",
        must_survive=True,
    ),
    Mutation(
        # The same drive with a wildcard on it. PowerShell's `Env:` is a
        # provider drive, so `Env:*` is a path *pattern* over it and
        # `Get-ChildItem Env:*` lists exactly what `Get-ChildItem Env:`
        # lists -- with the terminator the drive row above is refused by
        # replaced by a character that is not one. Green at `d060691d`, as
        # were `Env:/`, `Env:?*`, `Env:[A-Z]*` and `Env:GITHUB_*`. It pins
        # `_ENUMERATION_DRIVE_END`, the class that reads a drive still being
        # a drive after the colon: a wildcard, a bracket, or the separator
        # that makes it a root. `Env:\` was already red before that class
        # existed, but only because the word's trailing quoting run ate the
        # backslash and the end of the line terminated it -- a drive root
        # read as a quoted name, which is the right verdict for the wrong
        # reason and is now the same alternative as the rest of them.
        "B4-pull-request-target-pwsh-env-drive-wildcard",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        shell: pwsh\n"
         "        run: |\n"
         "          $v = Get-ChildItem Env:* | "
         "Where-Object Name -like '*HEAD_REF'\n"
         "          git fetch origin $v.Value\n"
         "          git checkout FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "put a wildcard after the drive's colon, which lists the same "
        "variables and is not the terminator the rule was reading for",
    ),
    Mutation(
        # The ordinary side of the drive class. Both lines write `env:`
        # followed by something in the class's neighbourhood and neither is
        # a listing: `$env:GITHUB_WORKSPACE\dist` is a named read of an
        # allowlisted variable with a Windows path separator after it, and
        # `  env: production` is a line of YAML a step appends to a file.
        # OVERSHOT here means the class has been let loose from the space
        # that has to precede the drive, or the backslash from the colon it
        # has to follow, which reds on every `pwsh` step that joins a path
        # and on every step that writes a config file.
        "B4-pull-request-target-pwsh-drive-neighbourhood-ordinary",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Write the build config\n"
         "        shell: pwsh\n"
         "        run: |\n"
         '          Write-Host "artifacts in $env:GITHUB_WORKSPACE\\dist"\n'
         '          Add-Content build.yml "  env: production"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "write a Windows path off an allowlisted variable and a YAML key "
        "into a file, which is `env:` next to the class and reads nothing",
        must_survive=True,
    ),
    Mutation(
        # The safe side of `_INDIRECT_EXPANSION`, and the reason that rule
        # needs a lookahead at all. `${!xs[@]}` is an array's indices and
        # `for i in "${!xs[@]}"` is the ordinary way to walk one in bash --
        # no environment in it, no name assembled anywhere. OVERSHOT here
        # means the rule has been widened into a ban on the `${!` sigil,
        # which is a suite that reds on the first bash array somebody
        # iterates.
        "B4-pull-request-target-run-array-index-walk",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Walk the tool list\n"
         "        run: |\n"
         "          xs=(jq yq shellcheck)\n"
         '          for i in "${!xs[@]}"; do\n'
         '            echo "$i ${xs[$i]}"\n'
         "          done\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "iterate an array by its indices, which is the one spelling of "
        "`${!` that asks for nothing but a subscript",
        must_survive=True,
    ),
    Mutation(
        # The safe side of the rule above, and the reason it is anchored on
        # a terminator rather than on the words. Every one of these lines
        # contains a word the enumeration rule reads and none of them is a
        # read: `set -euo pipefail` is a directive, `export PATH=...` and
        # `declare -a` are assignments, `env FOO=1 cmd` sets a variable for
        # one command, `/usr/bin/env python3` runs an interpreter with an
        # operand rather than dumping anything, and `ps -ef` is the UNIX
        # spelling of "every process", which says nothing about an
        # environment and is not BSD's `ps e`. OVERSHOT
        # here means the rule has been widened into a ban on the words,
        # which is a suite that reds on the first ordinary shell script
        # somebody writes.
        "B4-pull-request-target-run-env-ordinary-shell",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Build the tools\n"
         "        run: |\n"
         "          set -euo pipefail\n"
         '          export PATH="$PWD/bin:$PATH"\n'
         "          declare -a tools=(jq yq)\n"
         '          echo "${tools[@]}" > tools.txt\n'
         "          env GOFLAGS=-mod=readonly ./bin/build\n"
         "          /usr/bin/env python3 ./bin/check.py\n"
         "          ps -ef | head -3\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "write an ordinary shell script that sets a variable, exports a "
        "path, declares an array and runs a command under `env`, naming "
        "nothing the pull request decides",
        must_survive=True,
    ),
    Mutation(
        # `shell:` is a command line, and nothing here opened it until
        # 2026-09-19. The runner builds the step's command by substituting
        # the script's temporary file in at `{0}`, so everything before that
        # placeholder runs first -- and a `run:` of `true` is enough to make
        # the step look like it does nothing. Every scan in the test reads
        # `run:` and the `with:` values, and this field carried a checkout
        # of the pull request past all of them.
        "B4-pull-request-target-step-shell-command-line",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Lint\n"
         '        shell: bash -c "gh pr checkout $PR_NUMBER && ./ci.sh" {0}\n'
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}\n"
         '        run: "true"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "put the checkout in the step's `shell:` and leave the `run:` doing "
        "nothing, which reads as a step that configures its interpreter",
    ),
    Mutation(
        # And the same command line set once for the whole job, which is
        # where a reader is least likely to look for a program. A job's
        # `defaults.run.shell` is the command line every step of it runs
        # under, so this is the row above applied to steps that do not
        # mention `shell:` at all -- and a rule that read only
        # `steps[*].shell` would be a rule about where the author put it.
        "B4-pull-request-target-job-defaults-shell-command-line",
        ".github/workflows/risk_classify.yml",
        ("    runs-on: ubuntu-latest",
         "    runs-on: ubuntu-latest\n"
         "    env:\n"
         "      PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "    defaults:\n"
         "      run:\n"
         '        shell: bash -c "gh pr checkout $PR_NUMBER && ./ci.sh" {0}'),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "set the job's default shell to a command line that checks the pull "
        "request out, which runs once per step and appears in none of them",
    ),
    Mutation(
        # The safe side of both rows above. `shell: bash` and a
        # `defaults.run` block naming a shell and a working directory are
        # ordinary workflow, and folding those values into the scripts must
        # not red them. OVERSHOT here means `shell:` has been turned into a
        # field a `pull_request_target` job may not set, which is not what
        # any of this is about.
        "B4-pull-request-target-shell-ordinary",
        ".github/workflows/risk_classify.yml",
        ("    runs-on: ubuntu-latest",
         "    runs-on: ubuntu-latest\n"
         "    defaults:\n"
         "      run:\n"
         "        shell: bash\n"
         "        working-directory: scripts"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "name the job's default shell and working directory, which is the "
        "ordinary use of the block the rule above reads",
        must_survive=True,
    ),
    Mutation(
        # `git remote add` and `git remote update`, which put the fork's code
        # on the runner using none of the four words the old gate watched for.
        # This is the row that says that gate could not have been repaired by
        # adding a fifth: `remote update` is a fetch spelled as configuration,
        # and the three rows after it are three more spellings from three more
        # tools. What catches it reads what the step names -- the fork's clone
        # URL, and its head branch -- rather than what the step does.
        "B4-pull-request-target-run-remote-update",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         '          git remote add pr "${{ github.event.pull_request.head'
         '.repo.clone_url }}"\n'
         "          git remote update pr\n"
         '          git reset --hard "pr/${{ github.event.pull_request.head'
         '.ref }}"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "add the fork as a remote and update it, which is how anybody who "
        "wants the branch rather than the commit writes this",
    ),
    Mutation(
        # The fork's tree as a tarball over HTTP. No git verb at all: the API
        # serves a commit as an archive, `tar xz` unpacks it, and the next line
        # runs something out of it. A reviewer scanning for `git` sees nothing,
        # and the gate saw nothing either. Killed by the two expressions in the
        # URL, which is the point -- the URL has to say which repository and
        # which commit, and saying that is naming the pull request.
        "B4-pull-request-target-run-tarball",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         '          curl -sL "https://api.github.com/repos/${{ github.event'
         '.pull_request.head.repo.full_name }}/tarball/${{ github.event.after '
         '}}" | tar xz\n'
         "          ./kube-agents/ci/run.sh\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "download the commit as an archive instead of cloning it, which is "
        "the fast way to get one tree without a git history",
    ),
    Mutation(
        # The same code arriving through a package manager. `pip install
        # "git+https://...@<sha>"` clones the fork and runs its build backend
        # -- arbitrary code at install time, on a runner holding a writable
        # token -- and the word `git` appears only inside a URL scheme. A gate
        # over verbs would have to know pip's requirement grammar to see it.
        # The allowlist only has to notice that the requirement names a
        # repository the pull request chose.
        "B4-pull-request-target-run-pip-vcs",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         '          pip install "git+https://github.com/${{ github.event'
         '.pull_request.head.repo.full_name }}@${{ github.event.after }}"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "install the pull request as a package, which reads as testing the "
        "change the way a consumer would get it",
    ),
    Mutation(
        # And through a build context. `docker build "https://host/repo.git
        # #<sha>"` is a documented remote context: the daemon clones the
        # repository at that ref and builds it, so every `RUN` in the fork's
        # Dockerfile executes. Fourth tool, fourth grammar, same one-line
        # answer. Four rows for four tools rather than one representative one,
        # because "there are more of these than the gate knew" is a claim that
        # has to be able to fail.
        "B4-pull-request-target-run-docker-context",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         '          docker build "https://github.com/fork/kube-agents.git'
         '#${{ github.event.after }}"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "hand docker a remote build context, which is one line shorter than "
        "checking the tree out and building it",
    ),
    Mutation(
        # An `env:` block that is an expression rather than a mapping. GitHub
        # evaluates `env: ${{ fromJSON(...) }}` into the environment at run
        # time, and `_env_values` returned {} for it -- so every rule about
        # what a step's environment may carry read a whole environment built
        # out of the head as "this step carries nothing". Folded in whole under
        # a placeholder name now, which puts it in front of the expression
        # allowlist, where `fromJSON(...)` is refused for being unresolvable.
        # Same answer `with:` gets when it is not a mapping either.
        "B4-pull-request-target-env-expression",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env: ${{ fromJSON(format('{{\"REV\":\"{0}\"}}', "
         "github.event.after)) }}\n"
         '        run: git fetch --depth=1 origin "$REV" && '
         "git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "build the whole `env:` block from one expression, which is how a "
        "workflow shares one environment between several steps",
    ),
    Mutation(
        # A control on the substitution rather than on a hole. GitHub resolves
        # `env.SAFE`, `env.safe` and `Env.SAFE` to one value, and this is the
        # allowlisted ref reached through the third spelling -- the same ref,
        # the same checkout, nothing weakened. A case-sensitive substitution
        # left it unexpanded, and an unexpanded expression is off the ref
        # allowlist, so the suite reddened on a workflow that had done nothing
        # wrong. That is the failure mode a `must_survive` row exists for: a
        # suite that reds on a harmless change is a suite people learn to
        # override. OVERSHOT here means the substitution has gone
        # case-sensitive again.
        "B4-pull-request-target-checkout-ref-env-case",
        ".github/workflows/risk_classify.yml",
        ("        with:\n"
         "          ref: ${{ github.event.repository.default_branch }}",
         "        env:\n"
         "          SAFE: ${{ github.event.repository.default_branch }}\n"
         "        with:\n"
         "          ref: ${{ Env.SAFE }}"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "name an env value in the case GitHub accepts and this file did not, "
        "carrying the one ref the allowlist holds",
        must_survive=True,
    ),
    Mutation(
        # The ref space copied rather than enumerated. `git clone --mirror`
        # sets up a refmap of `+refs/*:refs/*`, so it brings
        # `refs/pull/N/head` onto the runner while writing no refspec, no
        # `ls-remote` and no HTTP path -- none of the four spellings the
        # enumeration rule was built out of. `for-each-ref` in the clone
        # reads the head SHA back out.
        "B4-pull-request-target-run-clone-mirror",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         '          git clone --mirror "https://github.com/'
         '$GITHUB_REPOSITORY" m\n'
         "          SHA=$(git -C m for-each-ref --format='%(objectname)"
         " %(refname)' | grep \"/$PR_NUMBER/head\" | cut -d' ' -f1)\n"
         '          git fetch origin "$SHA"\n'
         "          git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "take a mirror clone and read the head out of it, which writes none "
        "of the four spellings the enumeration rule collected",
    ),
    Mutation(
        # The same clone with the option abbreviated. Git takes any
        # unambiguous prefix of a long option, and `--mirror` is the only
        # `git clone` long option beginning with `--m`, so `git clone --m` is
        # `git clone --mirror` and `--mir` and `--mirr` are too -- all of them
        # green against a pattern that wanted the full spelling. `--m` rather
        # than `--mir` because it is the shortest rung git accepts: a row at
        # the bottom of the ladder dies wherever the pattern is cut, and a row
        # further up survives a pattern narrowed beneath it.
        "B4-pull-request-target-run-clone-mirror-abbreviated",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        env:\n"
         "          PR_NUMBER: ${{ github.event.pull_request.number }}\n"
         "        run: |\n"
         '          git clone --m "https://github.com/'
         '$GITHUB_REPOSITORY" m\n'
         "          SHA=$(git -C m for-each-ref --format='%(objectname)"
         " %(refname)' | grep \"/$PR_NUMBER/head\" | cut -d' ' -f1)\n"
         '          git fetch origin "$SHA"\n'
         "          git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "abbreviate the mirror flag, which git resolves to the same option "
        "and a pattern over its full spelling does not see",
    ),
    Mutation(
        # And the neighbouring flag, from the safe side. `git clone --bare`
        # copies the branches an ordinary clone does and not the pull
        # namespace, so refusing it would be a rule about the shape of a
        # clone rather than about what the clone reaches. OVERSHOT here means
        # the mirror pattern has been widened to `--(?:mirror|bare)`, and a
        # release step that takes a bare clone of this repository now reds.
        # The prefix ladder the pattern carries is not that widening: it runs
        # `--m` to `--mirror` and `--bare` is on none of its rungs.
        "B4-pull-request-target-run-clone-bare",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Archive the default branch\n"
         "        run: |\n"
         '          git clone --bare "https://github.com/'
         '$GITHUB_REPOSITORY" archive.git\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "take a bare clone of this repository, which is what an archiving "
        "step does and which reaches no ref an ordinary clone does not",
        must_survive=True,
    ),
    Mutation(
        # And the ladder's neighbours, from the safe side. The mirror rule
        # refuses every prefix of `--mirror` down to `--m`, and the price of
        # that is a pattern that has to stop at the end of the option: `--m`
        # also begins `--merges`, `--max-count` and `--milestone`. OVERSHOT
        # here means the ladder lost its anchor and every `--m...` option a
        # workflow writes now reds.
        #
        # Today the anchor has a live control as well, which is worth saying
        # so that this row is not read as the only one: `auto-assign-
        # milestone.yml` writes `gh pr edit --milestone` on this same trigger,
        # so replacing the ladder with a bare `--m` reds that carrier before a
        # single mutation runs and the harness refuses the sweep outright.
        # This row is what remains when a carrier changes, and that is
        # measured rather than asserted -- with the carrier's flag renamed and
        # the anchor dropped in the same tree, the baseline goes green again
        # and this row reports OVERSHOT.
        "B4-pull-request-target-run-log-m-flags",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Summarise the default branch\n"
         "        run: |\n"
         "          git log --merges --max-count=1 --format='%H'\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "log one merge commit from the checkout, whose two flags begin `--m` "
        "and are no abbreviation of `--mirror`",
        must_survive=True,
    ),
    Mutation(
        # The head branch with nothing naming it. `GITHUB_HEAD_REF` is set by
        # the runner on this trigger and holds the pull request's branch, so
        # this fetches the fork's code with no expression, no `context`, no
        # `env:` block, and none of the words `pull`, `head` or `merge`
        # reaching a pattern that reads for them. It was green.
        "B4-pull-request-target-run-runner-head-ref",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         '          git fetch origin "$GITHUB_HEAD_REF"\n'
         "          git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "fetch the branch the runner already put in the environment, which "
        "is the shortest way to write this and names nothing",
    ),
    Mutation(
        # And the fork itself, assembled out of two more of them.
        # `GITHUB_ACTOR` is whoever opened the pull request and is therefore
        # the owner half of the fork's clone URL, and `GITHUB_REPOSITORY`
        # trimmed at the slash is the other half. Separate row from the one
        # above because it pins a different entry off the list: repair the
        # allowlist by adding `GITHUB_HEAD_REF` back to it and this row still
        # dies.
        "B4-pull-request-target-run-runner-actor-fork",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Fetch the pull request\n"
         "        run: |\n"
         '          git fetch "https://github.com/$GITHUB_ACTOR/'
         '${GITHUB_REPOSITORY#*/}" "$GITHUB_HEAD_REF"\n'
         "          git reset --hard FETCH_HEAD\n"
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "build the fork's clone URL out of the author's login, which the "
        "runner hands every step whether or not the workflow asks",
    ),
    Mutation(
        # The runner's environment from the safe side. `GITHUB_REPOSITORY`
        # and `GITHUB_SHA` are what the base repository decides -- this
        # repository, and the head commit of its default branch on this
        # trigger -- and naming them in a log line is ordinary. OVERSHOT here
        # means the allowlist above has been replaced by a refusal of the
        # prefix, which reds on a step doing nothing wrong.
        "B4-pull-request-target-run-runner-repository-name",
        ".github/workflows/risk_classify.yml",
        ("      - name: Set up Python",
         "      - name: Describe the build\n"
         "        run: |\n"
         '          echo "building ${GITHUB_REPOSITORY} at ${GITHUB_SHA}"\n'
         "\n"
         "      - name: Set up Python"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "log which repository and commit the job is building, naming two "
        "variables the pull request has no say in",
        must_survive=True,
    ),
    Mutation(
        # The image the job runs in, which is the job's code. Nothing read
        # `container:` until 2026-09-19: the steps below it are the live
        # carrier's own innocent ones, every rule over `run:`, `uses:` and
        # `ref:` passes, and the job runs inside something the fork pushed.
        # This is the row that says the rule is over the block rather than
        # over the steps.
        "B4-pull-request-target-container-image-fork",
        ".github/workflows/risk_classify.yml",
        ("    runs-on: ubuntu-latest",
         "    runs-on: ubuntu-latest\n"
         "    container:\n"
         "      image: ghcr.io/${{ github.event.pull_request.head.repo"
         ".full_name }}/runner:latest"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "run every step of the job inside an image the fork pushed, which "
        "no rule over the steps can see",
    ),
    Mutation(
        # The same image in GitHub's string shorthand. `container: <image>`
        # is `container: {image: <image>}`, and a rule that reads only the
        # mapping is a rule the shorthand walks past while looking like the
        # form the rule reads. Separate row because it pins
        # `_container_mapping` rather than the block walk: normalise the
        # string away and the row above still dies while this one lives.
        "B4-pull-request-target-container-image-shorthand",
        ".github/workflows/risk_classify.yml",
        ("    runs-on: ubuntu-latest",
         "    runs-on: ubuntu-latest\n"
         "    container: ghcr.io/${{ github.event.pull_request.head.repo"
         ".full_name }}/runner:latest"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "name the fork's image in the one-line form, which is the form "
        "somebody writes when the image is the only thing they are setting",
    ),
    Mutation(
        # The head in the container's `env:`. This row is decided by the
        # block rule above it and not by the merge into the steps: `_flatten`
        # holds the whole container block against the expression allowlist
        # before any step is read, so `${{ github.event.after }}` reds here
        # wherever in the block it sits. The row below is the one that pins
        # the merge.
        "B4-pull-request-target-container-env-head",
        ".github/workflows/risk_classify.yml",
        ("    runs-on: ubuntu-latest",
         "    runs-on: ubuntu-latest\n"
         "    container:\n"
         "      image: ubuntu:24.04\n"
         "      env:\n"
         "        REV: ${{ github.event.after }}"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "put the head in the container's environment, one level below the "
        "job's, where every step of the job still has it",
    ),
    Mutation(
        # The merge itself, which is the fourth level `_env_values` claimed
        # to consult and which nothing pinned until now. A container `env:`
        # is set in the container every step of the job runs in, so it
        # reaches the steps exactly as the job's own does -- but the rules
        # over the block are an expression allowlist and two literal
        # backstops, and `$GITHUB_HEAD_REF` is none of the three. It carries
        # no expression, it is not `pull_request.head`, and it names no ref
        # namespace, so the block passes it. What refuses it is the
        # runner-variable allowlist, which runs over the steps' environment
        # and is not applied to the container block's text -- so it sees this
        # value only because the merge put it there. Delete the merge and
        # this row lives while the one above still dies.
        "B4-pull-request-target-container-env-runner-head-ref",
        ".github/workflows/risk_classify.yml",
        ("    runs-on: ubuntu-latest",
         "    runs-on: ubuntu-latest\n"
         "    container:\n"
         "      image: ubuntu:24.04\n"
         "      env:\n"
         "        HEAD_BRANCH: $GITHUB_HEAD_REF"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "hand every step the head branch through the container's "
        "environment, in the one spelling the rules over the block itself "
        "do not read",
    ),
    Mutation(
        # And a service container, which is the same choice of image made
        # once per entry under a different key. It starts before the steps,
        # on the job's network, with the job's secrets available to whatever
        # the workflow hands it.
        "B4-pull-request-target-service-image-fork",
        ".github/workflows/risk_classify.yml",
        ("    runs-on: ubuntu-latest",
         "    runs-on: ubuntu-latest\n"
         "    services:\n"
         "      db:\n"
         "        image: ghcr.io/${{ github.event.pull_request.head.repo"
         ".full_name }}/db:latest"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "start a service container from the fork's image, which runs beside "
        "the steps rather than under them",
    ),
    Mutation(
        # The container from the safe side, and the reason the rule over it
        # is an expression allowlist rather than a refusal. A pinned public
        # image is the ordinary way to run a job with a toolchain in it, and
        # both live forms -- the mapping and the string -- have to stay
        # green. OVERSHOT here means the block rule has been tightened into a
        # ban on `container:`, which is a suite that reds on a workflow doing
        # nothing wrong.
        "B4-pull-request-target-container-pinned-image",
        ".github/workflows/risk_classify.yml",
        ("    runs-on: ubuntu-latest",
         "    runs-on: ubuntu-latest\n"
         "    container:\n"
         "      image: ubuntu:24.04\n"
         "    services:\n"
         "      cache: redis:7"),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "run the job in a pinned public image with a pinned public service "
        "beside it, naming nothing that came from the pull request",
        must_survive=True,
    ),
    Mutation(
        "B4-pull-request-target-checkout-guard",
        "tests/conformance/test_B_write_path.py",
        ('if uses.startswith("actions/checkout"):',
         'if uses.startswith("actions/checkout-nonesuch"):'),
        "test_B4_no_pull_request_target_workflow_checks_out_the_pull_request",
        "stop `actions/checkout` matching its own name. The ref allowlist "
        "sits outside this guard and goes on reading every step, so what "
        "this removes is `saw_a_checkout` and the must-carry-a-`ref:` rule -- "
        "which is the vacuity `saw_a_checkout` exists to refuse",
    ),
    Mutation(
        "B6-codeowners-bot",
        "examples/gitops-repo/CODEOWNERS.example",
        ("@your-org/security", "@kube-agents-bot[bot]"),
        "test_B6_the_gitops_template_names_no_automation_identity",
        "name an automation identity as a code owner, defeating the one rule "
        "a GitHub App cannot satisfy",
    ),
    Mutation(
        "B1-read-path-narrowed",
        "agents/platform/scripts/command_policy.py",
        ('        # Writes a kubeconfig in the sidecar and nothing in the cloud. It is\n'
         '        # also how a Cluster Agent points itself at its target cluster, so\n'
         '        # refusing it would break the read path this module is protecting.\n'
         '        ("container", "clusters", "get-credentials"),\n',
         ""),
        "test_B1_ordinary_reads_still_work",
        "harden the allowlist by dropping the one entry with `credentials` in "
        "its name. The over-strict direction, which costs an operator the whole "
        "posture rather than one command -- the gate that gets globally "
        "disabled a week later",
    ),
    Mutation(
        "B1-gcloud-group-prefix",
        "agents/platform/scripts/command_policy.py",
        ('        ("container", "node-pools", "describe"),\n'
         '        ("container", "node-pools", "list"),\n',
         '        ("container", "node-pools"),\n'),
        "test_B1_gcloud_write_commands_are_refused",
        "collapse two adjacent entries onto their common prefix while tidying "
        "the list -- _gcloud_is_read_only matches on prefix, so the group entry "
        "allows every verb beneath it, `delete` included",
    ),
    Mutation(
        "B2-second-pull-requests-write",
        ".github/workflows/conformance.yml",
        ("permissions:\n  contents: read\n\njobs:\n  conformance:\n",
         "permissions:\n  contents: read\n  pull-requests: write\n\njobs:\n  conformance:\n"),
        "test_B2_no_workflow_grants_a_bot_the_ability_to_approve",
        "let the conformance job post its findings as a pull-request comment. "
        "The scope that buys a comment is the scope that buys an approval, on a "
        "workflow that runs on every pull_request",
    ),
    Mutation(
        "B3-apply-read-verb",
        "agents/platform/scripts/command_policy.py",
        ('        ("get",),\n', '        ("apply",),\n        ("get",),\n'),
        "test_B3_the_agent_cannot_reach_the_admission_policy_through_kubectl",
        "add apply so a manifest-generation skill can preview with "
        "--dry-run=server. The verb is allowed whatever follows it, and what "
        "follows it here is the ClusterRoleBinding that grants the agent write. "
        "NOISY against B1 by construction: B3's corpus is refused by the same "
        "verb allowlist, with no resource-aware layer between them",
    ),
    Mutation(
        # Originally aimed at chart-release.yml, which main has since deleted;
        # the ghcr publisher has the identical permissions shape and the same
        # story -- an image pusher quietly gaining the credential that can
        # push to this repository.
        "B4-extra-contents-write-holder",
        ".github/workflows/docker-publish-ghcr.yml",
        ("      contents: read\n      packages: write\n",
         "      contents: write\n      packages: write\n"),
        "test_B4_contents_write_is_confined_to_the_release_path",
        "give the ghcr image publisher contents: write so it can cut a GitHub "
        "release alongside the OCI push -- an extra holder of the credential "
        "that can push to this repository, added in a one-word diff",
    ),
    Mutation(
        "B6-guarded-path-unowned",
        "examples/gitops-repo/CODEOWNERS.example",
        ("\n# Admission policies (the security backstop itself)\n"
         "/policy/                          @your-org/security\n",
         "\n"),
        "test_B6_every_guarded_path_in_the_template_has_an_owner",
        "drop the rule for the one directory nobody edits often, so the ruleset "
        "requiring code-owner review on /policy/ requires review from nobody "
        "and the admission backstop merges unreviewed",
    ),
    # ---- C. Enforcement --------------------------------------------------
    Mutation(
        "C1-git-ext-transport",
        "agents/platform/scripts/credential_proxy.py",
        # Indented for the same reason as A3-kubectl-kuberc-env above.
        ('            "GIT_ALLOW_PROTOCOL": "https",',
         '            "GIT_ALLOW_PROTOCOL": "https:ext",'),
        "test_C1_git_in_the_broker_cannot_execute_arbitrary_code",
        "re-admit the ext:: transport, which is the whole of the RCE: "
        "`git clone \'ext::sh -c <cmd>\'` runs <cmd> in the credential holder. "
        "The one-word widening is the shape the real regression would take",
    ),
    Mutation(
        "C1-socket-umask",
        "agents/platform/scripts/credential_proxy.py",
        ("previous_umask = os.umask(0o177)", "previous_umask = os.umask(0o022)"),
        "test_C1_the_broker_backend_socket_is_bound_private",
        "widen the umask the socket is bound under -- the slice 2b near-miss, "
        "where a umask added for the shared PVC reached the socket",
    ),
    Mutation(
        "C1-shell-true",
        "agents/platform/scripts/credential_proxy.py",
        ("            start_new_session=True,", "            start_new_session=True,\n            shell=True,"),
        "test_C1_the_executor_never_reaches_a_shell",
        "interpose a shell, which is what makes `;` and `#` live again",
    ),
    Mutation(
        "C1-executable-allowlist",
        "agents/platform/scripts/credential_proxy.py",
        ('return ("gcloud", "kubectl", "git", *providers.Registry().executables)',
         'return ("gcloud", "kubectl", "git", "sh", *providers.Registry().executables)'),
        "test_C1_the_executor_refuses_an_executable_it_does_not_ship",
        "add sh to the allowlist, giving a compound command somewhere to land",
    ),
    # C1-share-process-namespace and C1-uid-collapse are retired WITH their
    # tests, not orphaned. Both attacked same-Pod mitigations the split-broker
    # layout needed -- a shared PID namespace, and the credential holder running
    # at the sandbox UID. #913 removed the thing they mitigated: nothing in the
    # agent Pod holds a credential any more, and the shell runs in a Pod of its
    # own. The replacement property (ShareProcessNamespace stays unset) is
    # asserted in the operator's own suite; duplicating it here would pin
    # someone else's invariant. What C1 asserts instead is the sandbox
    # ServiceAccount's missing Workload Identity annotation, which has its own
    # mutation above.
    # C1-egress-whole-internet is retired, not lost. It injected the exact
    # construction slice 2b 1.3 refused -- `0.0.0.0/0 except metadata`, which
    # adds the internet rather than subtracting an address -- into the
    # allowlist golden, and killed test_C1_the_rendered_egress_policy*.
    #
    # Both of those assertions are known violations as of
    # gke-labs/kube-agents#676: platformagent-gateway-netpol selects the same
    # pods and already allows the whole internet and the metadata addresses,
    # so the tests fail before the mutation is applied and applying it changes
    # nothing. A mutation against an expected failure can only report
    # SURVIVED, which reads as "the test is theatre" and is the wrong
    # diagnosis. A known violation is verified by its precondition instead.
    #
    # Restore this the day the C1 decorators come off.
    #
    # The SHAPE half is no longer in that bind. It used to share a method with
    # the whole-internet violation, so it inherited the expectedFailure and no
    # mutation could report anything but SURVIVED against it. It is its own
    # test now, passing, and so has a real kill below.
    Mutation(
        "C1-egress-rule-without-destination",
        "k8s-operator/internal/testing/testdata/platform/expected/"
        "platformagent-egress-allowlist.yaml",
        ("""    - ports:
        - port: 443
          protocol: TCP
      to:
        - ipBlock:
            cidr: 140.82.112.0/20
""",
         """    - ports:
        - port: 443
          protocol: TCP
"""),
        "test_C1_the_rendered_egress_rules_are_shaped_to_deny_by_default",
        "strip the `to` from a rendered egress rule, which opens 443 to every "
        "destination while still looking like an allowlist entry",
    ),
    Mutation(
        "C1-cidr-guard-inert",
        "k8s-operator/internal/controller/platformagent_egress_policy.go",
        ("\tif reason := ipv4MappedRefusal(prefix, cidr); reason != \"\" {\n\t\treturn reason\n\t}\n", ""),
        "test_C1_every_operator_supplied_cidr_reaches_the_refusal_guards",
        "rename the 4-in-6 guard so every call site misses it; Go would not "
        "compile, but the point is that the conformance suite says so first",
    ),
    Mutation(
        "C1-gateway-oauth-shape",
        "charts/kube-agents/files/redactor.py",
        ('        text = cls.GCP_OAUTH_TOKEN_PATTERN.sub("[REDACTED_SECRET]", text)\n', ""),
        "test_C1_the_gateway_redactor_matches_the_leaked_credential_shapes",
        "drop the ya29 substitution from the chain while reordering it -- the "
        "pattern constant stays, so anything that greps for it is satisfied, "
        "and the shape gke-labs/kube-agents#603 measured leaves for the "
        "provider in the clear",
    ),
    Mutation(
        "C1-gateway-sa-exemption",
        "charts/kube-agents/files/redactor.py",
        ('r"[a-zA-Z0-9._%+\\-]+@(?!(?:[a-zA-Z0-9\\-]+\\.)*gserviceaccount\\.com(?!\\.?[\\w\\-]))"',
         'r"[a-zA-Z0-9._%+\\-]+@"'),
        "test_C1_the_gateway_redactor_leaves_ordinary_manifest_content_alone",
        "simplify the e-mail pattern by dropping the service-account exemption; "
        "every IAM principal in a tool result then reaches the model as "
        "[REDACTED_EMAIL], which is the over-eager shape that gets redaction "
        "turned off",
    ),
    Mutation(
        "C2-unknown-flag-fail-open",
        "agents/platform/scripts/command_policy.py",
        ("            if name not in _KUBECTL_FLAGS_WITH_VALUE and name not in _KUBECTL_BOOLEAN_FLAGS:\n"
         "                return None, name\n",
         "            if name not in _KUBECTL_FLAGS_WITH_VALUE and name not in _KUBECTL_BOOLEAN_FLAGS:\n"
         "                index += 1\n                continue\n"),
        "test_C2_an_unparseable_argv_is_refused",
        "skip unknown flags instead of refusing -- fail open, and the reason "
        "the module enumerates arity rather than allowlisting flags",
    ),
    Mutation(
        "C2-read-only-default",
        "agents/platform/scripts/credential_proxy.py",
        ('return os.getenv("CREDENTIAL_PROXY_ENFORCE_READ_ONLY", "true").strip().lower() != "false"',
         'return os.getenv("CREDENTIAL_PROXY_ENFORCE_READ_ONLY", "true").strip().lower() == "true"'),
        "test_C2_the_read_only_gate_survives_a_typo",
        "compare for truth rather than against falsehood -- looks equivalent, "
        "and disarms the gate on every typo",
    ),
    Mutation(
        "C2-external-key-default",
        "agents/platform/scripts/credential_proxy.py",
        ('external_key = os.getenv("API_SERVER_EXTERNAL_KEY", "").strip()',
         'external_key = os.getenv("API_SERVER_EXTERNAL_KEY", "dev").strip()'),
        "test_C2_the_agent_api_proxy_refuses_to_start_without_its_key",
        "give the external key a development default, which is how the "
        "loopback sentinel got there in the first place",
    ),
    Mutation(
        "C3-policy-reads-a-file",
        "agents/platform/scripts/command_policy.py",
        ('    for token in argv[1:]:\n        name, _, _ = token.partition("=")\n        if name == "--kuberc":',
         '    for token in argv[1:]:\n        name, _, value = token.partition("=")\n'
         '        if name == "--kuberc" and value and open(value):\n'
         '            pass\n        if name == "--kuberc":'),
        "test_C3_the_policy_module_imports_nothing_that_can_read",
        "check whether the kuberc file exists before refusing, the "
        "helpful-looking change that reintroduces a rewrite-after-check race",
    ),
    Mutation(
        "C3-log-sanitiser",
        "agents/platform/scripts/credential_proxy.py",
        ("    filtered = ''.join(\n"
         "        c for c in s if unicodedata.category(c) not in ('Cc', 'Cf', 'Cs', 'Zl', 'Zp')\n"
         "    )\n",
         "    filtered = s\n"),
        "test_C3_untrusted_output_cannot_forge_a_log_line",
        "stop stripping control characters, so tool output can forge a record",
    ),
    Mutation(
        "C4-unpinned-action",
        ".github/workflows/prettier.yml",
        ("actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1",
         "actions/checkout@v7"),
        "test_C4_every_third_party_action_is_pinned_to_a_commit",
        "float one action back to a tag, which a Dependabot conflict "
        "resolution can produce by hand",
    ),
    Mutation(
        "C4-base-image-digest",
        "tags.env",
        ("@sha256:3811ed13da874fba2ac99b6d492db9a203d34cb6dccf90d886948c00d0ccec09", ""),
        "test_C4_the_agent_base_image_is_pinned_by_digest",
        "drop the digest and keep the tag, which reads as equivalent",
    ),
    Mutation(
        "C5-minted-write-verb",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("      - get\n      - list\n", "      - get\n      - list\n      - patch\n"),
        "test_C5_no_minted_role_grants_a_write_verb",
        "add patch to a minted explorer role, the change a feature request for "
        "annotating resources produces",
    ),
    Mutation(
        "C5-bind-to-edit",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("kind: ClusterRole\n  name: kubeagents:minimal:kubeagents-system:platformagent\n",
         "kind: ClusterRole\n  name: edit\n"),
        "test_C5_the_agent_is_bound_to_no_write_capable_builtin_role",
        "bind the agent to `edit` while leaving every minted rule read-only, "
        "which the verb-level assertion alone cannot see. Retargeted from "
        "`name: view`: main replaced the built-in binding with a purpose-built "
        "kubeagents:minimal ClusterRole, so there is no `view` left to swap -- "
        "the roleRef is still the thing that has to be attacked",
    ),
    Mutation(
        "C5-reaper-eats-guardrail",
        "k8s-operator/internal/controller/platformagent_controller.go",
        ('&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: agent.Name + "-sandbox", Namespace: agent.Namespace}},',
         '&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: agent.Name + "-sandbox", Namespace: agent.Namespace}},\n'
         '\t\t&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: agent.Name + "-sandbox-metadata-deny", Namespace: agent.Namespace}},'),
        "test_C5_the_controller_does_not_reap_the_metadata_deny_guardrail",
        "restore the reaper's reach over the guardrail -- slice 2b 1.5, "
        "verbatim",
    ),
    Mutation(
        "C5-admission-binding-drift",
        "k8s-operator/config/admission/agent-rbac-policy.yaml",
        ("  policyName: kube-agents-agent-readonly", "  policyName: prefixed-kube-agents-agent-readonly"),
        "test_C5_the_admission_binding_names_a_policy_that_exists",
        "the kustomize namePrefix outcome slice 2b 1.2 caught: both objects "
        "exist, the binding points at nothing, and kubectl get looks right",
    ),
    Mutation(
        "C5-admission-fail-open",
        "k8s-operator/config/admission/agent-rbac-policy.yaml",
        ("  failurePolicy: Fail", "  failurePolicy: Ignore"),
        "test_C5_the_admission_policy_fails_closed",
        "the one-line edit B3 names: 'unblock apply during upgrade window'",
    ),
    Mutation(
        "C1-broker-colocation-flag-restored",
        "k8s-operator/internal/controller/platformagent_manifests.go",
        ("// buildPodTemplateSpec generates the shared PodTemplateSpec for Deployment and StatefulSet\n",
         "// buildPodTemplateSpec generates the shared PodTemplateSpec for Deployment and StatefulSet\n"
         "// TODO: honour splitCredentialBrokerPod again for single-node installs.\n"),
        "test_C1_the_credential_broker_is_its_own_deployment",
        "reintroduce a co-location switch by name. #913 made the broker's own "
        "Deployment unconditional, so the separation is topology rather than a "
        "setting; a flag coming back is the regression that turns it into a "
        "setting again, and it starts life looking like a harmless TODO",
    ),
    Mutation(
        "C1-sandbox-sa-annotated-for-workload-identity",
        "k8s-operator/internal/controller/shell_sandbox_manifests.go",
        ("""			Name:      shellSandboxServiceAccountName(agent),
			Namespace: agent.Namespace,
			Labels:    shellSandboxSelector(agent),
""",
         """			Name:      shellSandboxServiceAccountName(agent),
			Namespace: agent.Namespace,
			Labels:    shellSandboxSelector(agent),
			Annotations: map[string]string{
				"iam.gke.io/gcp-service-account": agentGSAEmail(agent),
			},
"""),
        "test_C1_the_sandbox_identity_carries_no_cloud_annotation",
        "annotate the sandbox ServiceAccount for Workload Identity, the one "
        "edit an operator would plausibly make to 'let the shell use gcloud'. "
        "GKE resolves WI by pod IP, so this hands every model-authored command "
        "a GSA token from the metadata server with no proxy in front of it -- "
        "and nothing else in the suite would notice",
    ),
    Mutation(
        "C2-phase-two-skips-unknown-flag",
        "agents/platform/scripts/command_policy.py",
        ("            # Stop on unknown flags (arity unknown, could hide the subcommand).\n"
         "            if name not in _KUBECTL_FLAGS_WITH_VALUE and name not in _KUBECTL_BOOLEAN_FLAGS:\n"
         "                break\n",
         "            # Unknown command-specific flags are boolean far more often\n"
         "            # than not, so skip rather than stop.\n"
         "            if name not in _KUBECTL_FLAGS_WITH_VALUE and name not in _KUBECTL_BOOLEAN_FLAGS:\n"
         "                index += 1\n"
         "                continue\n"),
        "test_C2_an_unknown_flag_cannot_swallow_a_write_subcommand",
        "make phase 2 skip an unrecognised command-specific flag instead of "
        "stopping at it -- the symmetry a reader expects with phase 1's loop. "
        "`rollout --someflag status restart web` then reads as `rollout status`",
    ),
    Mutation(
        "C2-cluster-info-dump-allowed",
        "agents/platform/scripts/command_policy.py",
        ('        ("cluster-info", "dump"),\n', ""),
        "test_C2_cluster_info_dump_is_refused_by_both_of_its_guards",
        "empty the refused-subcommand set on the grounds that a dump only "
        "reads. `cluster-info` is allowed alone and evaluate falls back to "
        "verb[:1], so the deletion is silent",
    ),
    Mutation(
        "C2-output-directory-demoted",
        "agents/platform/scripts/command_policy.py",
        ('        "--profile", "--profile-output", "--cache-dir", "--output-directory",\n',
         '        "--profile", "--profile-output", "--cache-dir",\n'),
        "test_C2_cluster_info_dump_is_refused_by_both_of_its_guards",
        "drop --output-directory from a set of kubectl *global* flags because "
        "it belongs to cluster-info -- exactly the tidy-up the comment above it "
        "argues against, and the guard that does not need the verb parse. The "
        "pair with C2-cluster-info-dump-allowed: the test names two guards, so "
        "each is removed on its own",
    ),
    Mutation(
        "C3-policy-opens-the-kuberc-file",
        "agents/platform/scripts/command_policy.py",
        ('    for token in argv[1:]:\n        name, _, _ = token.partition("=")\n'
         '        if name == "--kuberc":\n            return "--kuberc"\n',
         '    import codecs\n\n'
         '    for token in argv[1:]:\n        name, _, value = token.partition("=")\n'
         '        if name == "--kuberc":\n'
         '            if value:\n'
         '                try:\n'
         '                    with codecs.open(value, encoding="utf-8") as preference:\n'
         '                        if "as" not in preference.read():\n'
         '                            return None\n'
         '                except OSError:\n'
         '                    pass\n'
         '            return "--kuberc"\n'),
        "test_C3_the_policy_decision_reads_nothing_but_its_argv",
        "refuse --kuberc only when the file it names actually sets an "
        "impersonation default -- the same helpful-looking check as "
        "C3-policy-reads-a-file, spelled through codecs.open so neither the AST "
        "test's import list nor its builtin-name list is touched. os.stat is "
        "the audit hook's blind spot and an enumerated list is the AST test's; "
        "this is the half only the hook can see",
    ),
    Mutation(
        "C5-tokenreview-gets-subjectaccessreviews",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent-scoped-sa.yaml",
        ("    resources:\n      - tokenreviews\n    verbs:\n      - create\n",
         "    resources:\n      - tokenreviews\n      - subjectaccessreviews\n    verbs:\n      - create\n"),
        "test_C5_the_tokenreview_role_is_the_narrowest_form_of_itself",
        "give the broker subjectaccessreviews alongside tokenreviews -- what "
        "binding system:auth-delegator would have handed it in one line, and an "
        "authorization oracle over the whole cluster",
    ),
    Mutation(
        "C5-binds-auth-delegator",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent-scoped-sa.yaml",
        ("roleRef:\n  apiGroup: rbac.authorization.k8s.io\n  kind: ClusterRole\n"
         "  name: kubeagents:tokenreview:kubeagents-system:platformagent",
         "roleRef:\n  apiGroup: rbac.authorization.k8s.io\n  kind: ClusterRole\n"
         "  name: system:auth-delegator"),
        "test_C5_no_agent_binding_names_the_auth_delegator_role",
        "bind the built-in system:auth-delegator instead of the minted one-verb "
        "role -- the shortcut every TokenReview how-to recommends. The minted "
        "role is left in place and still narrow, so the rule-level assertions "
        "cannot see it",
    ),
    Mutation(
        "C5-leader-reaches-configmaps",
        # platformagent-ha.yaml, not platformagent.yaml: the leader Role's pods
        # rule renders only above one replica, and the HA fixture is the only
        # golden that sets it.
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent-ha.yaml",
        ("    resources:\n      - pods\n    verbs:\n      - get\n      - patch\n",
         "    resources:\n      - configmaps\n      - pods\n    verbs:\n      - get\n      - patch\n"),
        "test_C5_the_leader_role_stays_confined_to_coordination",
        "add the ConfigMap lock client-go's configmapsleases mode still "
        "supports, which turns a coordination role into a namespace read-write "
        "grant one resource at a time",
    ),
    # ---- D. Accountability ------------------------------------------------
    Mutation(
        "D1-principal-not-logged",
        "agents/platform/scripts/credential_proxy.py",
        ("            _sanitize_for_logging(principal.describe(), max_length=512),", "            \"-\","),
        "test_D1_the_exec_route_records_a_principal",
        "drop the principal from the exec record while refactoring a handler "
        "that does not yet read it",
    ),
    Mutation(
        "D1-hint-not-sanitised",
        "agents/platform/scripts/credential_proxy.py",
        ('safe_hint = _sanitize_for_logging(log_hint) if log_hint else "unknown"',
         'safe_hint = log_hint if log_hint else "unknown"'),
        "test_D1_a_log_hint_cannot_forge_a_record",
        "log the hint raw, which is the state the sanitiser was added to fix",
    ),
    Mutation(
        "D2-workflow-mode",
        "k8s-operator/api/v1alpha1/common_types.go",
        ("type SecuritySpec struct {", "type SecuritySpec struct {\n\tWorkflowMode string `json:\"workflowMode,omitempty\"`"),
        "test_D2_no_direct_apply_mode_exists",
        "add the break-glass field another design document already offers",
    ),
    Mutation(
        "D4-token-never-expires",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("              expirationSeconds: 3600", "              expirationSeconds: 86400"),
        "test_D4_every_projected_token_expires",
        "stretch the projection to a day to stop a rotation warning",
    ),
    Mutation(
        "D4-audience-dropped",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("              audience: kubeagents-credential-proxy\n", ""),
        "test_D4_the_broker_token_is_audience_bound",
        "drop the audience, making the broker's token a general-purpose "
        "cluster bearer token and TokenReview a formality",
    ),
    Mutation(
        "D4-secret-becomes-literal",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("- name: API_SERVER_EXTERNAL_KEY\n              valueFrom:\n                secretKeyRef:\n"
         "                  key: api-key\n                  name: platformagent-secrets",
         "- name: API_SERVER_EXTERNAL_KEY\n              value: hunter2"),
        "test_D4_the_customer_api_key_is_secret_backed",
        "inline the external key as a literal, the way the loopback sentinel "
        "already is",
    ),
    Mutation(
        "D2-read-only-becomes-a-chart-value",
        "charts/kube-agents/values.yaml",
        ("    serviceAccountName: kubeagents-platform-agent\n",
         "    serviceAccountName: kubeagents-platform-agent\n"
         "    # Sets CREDENTIAL_PROXY_ENFORCE_READ_ONLY on the broker. Set to false\n"
         "    # to recover from a bad allowlist without waiting on an image build.\n"
         "    enforceReadOnly: true\n"),
        "test_D2_the_read_only_posture_is_not_a_customer_facing_knob",
        "promote the outage stopgap to a documented chart value, which is how a "
        "global, unscoped, never-expiring autonomy switch actually gets offered "
        "to a customer -- as a helpful comment next to a boolean",
    ),
    Mutation(
        "D5-cross-reference-renamed",
        "tests/conformance/test_C_enforcement.py",
        ("    def test_C3_the_policy_decision_reads_nothing_but_its_argv(self) -> None:",
         "    def test_C3_the_policy_decision_is_a_pure_function_of_argv(self) -> None:"),
        "test_D5_the_enforcement_tier_cannot_be_lowered_by_routing",
        "shorten an over-long test name. D5 owns no control of its own -- its "
        "single assertion is a cross-reference to C3's purity test -- so this "
        "checks the reference is load-bearing rather than decorative, and that "
        "a rename cannot silently empty the invariant",
    ),
    Mutation(
        "D6-switch-renamed-out-from-under-its-name",
        "agents/platform/scripts/credential_proxy.py",
        ('os.getenv("CREDENTIAL_PROXY_ENFORCE_READ_ONLY", "true")',
         'os.getenv("CREDENTIAL_PROXY_READ_ONLY", "true")'),
        "test_D6_the_read_only_switch_is_not_mistaken_for_a_kill_switch",
        "shorten the variable name while tidying. The switch is the only global "
        "control in the product and D6 exists to say what it does not do; a "
        "rename means the documented spelling silently does nothing. NOISY "
        "against C2 by construction -- both invariants read the same call, so "
        "no edit reaches one without the other",
    ),
    Mutation(
        "D3-bucket-marker-tidied-away",
        "tests/conformance/test_D_accountability.py",
        ('"""D3: BUCKET 3 -- no mechanism exists, and a weak test would be worse than none.\n',
         '"""D3: no mechanism exists, and a weak test would be worse than none.\n'),
        "test_D3_is_recorded_as_bucket_three_rather_than_missing",
        "reword a class docstring's opening line, dropping the marker that is "
        "the only thing distinguishing a recorded bucket-3 reason from an "
        "invariant nobody wrote a test for. Harness-class: for bucket 3 the "
        "written reason IS the control, so the suite is the file to mutate",
    ),
    Mutation(
        "D6-bucket-three-exit-criterion-deleted",
        "tests/conformance/test_D_accountability.py",
        ("    What would make this bucket 1: a halt control with a stated N. Then the\n"
         "    assertion is that a halted agent refuses, that the halt survives a restart,\n"
         "    and that setting it does not require touching the agent's own Deployment.\n",
         ""),
        "test_D6_is_recorded_as_bucket_three_rather_than_missing",
        "delete the forward-looking paragraph as speculative, leaving BUCKET 3 "
        "a status with no exit criterion -- the shape in which a gap stops "
        "being a plan and becomes a permanent excuse",
    ),
    # ---- D15 and the harness ----------------------------------------------
    Mutation(
        "D15-guard-normalises",
        "k8s-operator/internal/controller/platformagent_egress_policy.go",
        ("Overlaps(ipv4MappedSpace)", "Contains(ipv4MappedSpace.Addr())"),
        "test_D15_the_guard_refuses_the_ambiguous_form_rather_than_normalising",
        "swap Overlaps for the Contains that produced the finding -- the same "
        "spelling, the same cross-family blind spot",
    ),
    Mutation(
        "D15-executor-absolute-path",
        "agents/platform/scripts/credential_proxy.py",
        ('return ("gcloud", "kubectl", "git", *providers.Registry().executables)',
         'return ("gcloud", "kubectl", "git", "/usr/bin/kubectl", *providers.Registry().executables)'),
        "test_D15_the_two_layers_agree_on_the_governed_tool",
        "pin kubectl to an absolute path so PATH cannot be shadowed -- a "
        "hardening on its face, and a spelling _GOVERNED_TOOLS matches exactly "
        "and therefore does not govern. `/usr/bin/kubectl delete ns prod` reads "
        "as an ungoverned tool to the policy and as kubectl to the executor",
    ),
    Mutation(
        "D15-kuberc-scan-stops-at-the-verb",
        "agents/platform/scripts/command_policy.py",
        ('    for token in argv[1:]:\n        name, _, _ = token.partition("=")\n'
         '        if name == "--kuberc":\n            return "--kuberc"\n',
         '    for token in argv[1:]:\n        if not token.startswith("-"):\n            break\n'
         '        name, _, _ = token.partition("=")\n'
         '        if name == "--kuberc":\n            return "--kuberc"\n'),
        "test_D15_a_refused_flag_is_refused_wherever_it_appears",
        "stop the kuberc scan at the first bare word, reasoning that a global "
        "flag precedes the verb. cobra does not agree: the post-verb spelling "
        "falls through to the identity check and earns a different rule id, so "
        "the verdict now depends on where the flag sits",
    ),
    Mutation(
        "D15-differential-loses-its-test",
        "tests/conformance/test_A_authority.py",
        ("    def test_A3_rejects_attached_shorthand_server(self) -> None:",
         "    def test_A3_rejects_the_attached_shorthand(self) -> None:"),
        "test_D15_every_known_differential_has_a_test",
        "rename the -shttp:// test. The checklist looks its findings up by "
        "string, which is the only way a differential stops being covered "
        "without a single assertion being deleted",
    ),
    Mutation(
        "D15-readme-closes-the-class",
        "tests/conformance/README.md",
        ("**The class is open.** Four instances now across three slices.",
         "**Four instances now across three slices**, each with a test."),
        "test_D15_the_readme_says_the_class_is_open",
        "rewrite the standing hedge as a coverage claim now that all four "
        "differentials have tests -- the reading the sentence exists to "
        "prevent, and the one a reader of a finished-looking table takes anyway",
    ),
    Mutation(
        "harness-source-moved",
        "agents/platform/scripts/command_policy.py",
        ("def evaluate(", "def evaluate_command("),
        "test_every_anchor_is_still_present",
        "rename the entry point. Nothing here should pass quietly: the "
        "self-check has to be the thing that goes red first",
    ),
    Mutation(
        "harness-mutation-quietly-unhooked",
        "hack/conformance-mutations.py",
        ('"test_C5_the_leader_role_stays_confined_to_coordination",\n'
         '        "add the ConfigMap lock',
         '"test_C5_the_leader_role_stays_bounded",\n'
         '        "add the ConfigMap lock'),
        "test_every_bucket_one_assertion_is_named_by_a_mutation",
        "rename a test and update the mutation's `kills` to something that no "
        "longer matches it. The mutation still runs and still reports a verdict, "
        "so the run stays green-looking while one assertion quietly stops being "
        "attacked -- the exact drift the coverage check exists to catch. The "
        "list is read once at import, so this cannot disturb the run applying it",
    ),
    Mutation(
        "harness-exemption-unargued",
        "tests/conformance/test_harness_selfcheck.py",
        ('            "asserts a property of the ipaddress module: that ::ffff:0.0.0.0/96 "\n'
         '            "unmaps to 0.0.0.0/0 and contains the metadata address. It holds the "\n'
         '            "premise the Go guard rests on as an executable statement rather "\n'
         '            "than a comment, and reads no repository artifact, so any edit that "\n'
         '            "reddens it is an edit to the assertion. The controls the premise "\n'
         '            "underwrites are mutated: D15-guard-normalises and C1-cidr-guard-inert."',
         '            "no in-repo control."'),
        "test_the_exemptions_are_argued_rather_than_listed",
        "shorten an exemption's reason to a note. An exemption list is the only "
        "way out of the coverage floor, so it stays honest exactly as long as "
        "entering it costs an argument",
    ),
    Mutation(
        # The guard over the suite's own prose, one character past the range
        # it used to read. A docstring that is not a raw string hands its
        # escapes to the parser, and what reaches the file is the character
        # rather than the six letters somebody typed -- which is how a
        # sentence about `\x1b` loses its point silently. The guard said "any
        # control character" and stopped at 0x20, so DEL and the whole C1
        # block went through it, and round 25 widened it to Unicode's `Cc`
        # category. This row is that widening's only pin: the same edit is
        # green with the class written `ord(character) < 0x20` and red with
        # it written `unicodedata.category(character) == "Cc"`, measured both
        # ways on 2026-09-21. No other row names this test.
        "harness-docstring-control-character-outside-c0",
        "tests/conformance/test_D_accountability.py",
        ('"""D2: defaults off, earned per domain, never a global setting."""',
         '"""D2: defaults off, earned per domain, never a global setting.\x7f"""'),
        "test_no_docstring_in_the_suite_contains_a_control_character",
        "leave a consumed escape at the end of a docstring, which is what a "
        "pasted delete character is and what the guard exists to catch",
    ),
    Mutation(
        "C1-session-fence-selector-drift",
        "a2a/gateway/spawn.go",
        ('\tsessionRole = "a2a-session"', '\tsessionRole = "a2a-worker"'),
        "test_C1_the_session_fence_selects_the_pods_the_spawner_stamps",
        "rename the session pod's component label on the spawner side only -- "
        "the shape a rename that misses the other Go module takes. Both Go "
        "suites stay green and the operator's NetworkPolicy then selects no "
        "pod, which the API server reports as success",
    ),
    Mutation(
        "C1-session-pod-gets-a-second-token",
        "a2a/gateway/spawn.go",
        ("AutomountServiceAccountToken: ptr.To(false),",
         "AutomountServiceAccountToken: ptr.To(true),"),
        "test_C1_a_session_pod_carries_no_kubernetes_identity",
        "automount a SECOND token into a session pod, beside the bus token it "
        "is supposed to have. A session pod now names a ServiceAccount -- the "
        "callout resolves a Kubernetes identity, so it has to -- and this is "
        "the flip that turns that identity from inert into a cluster "
        "credential: the automounted token carries the API server's default "
        "audience, so unlike the projected bus token it authenticates against "
        "the API server, which the session fence's rule set does not account "
        "for",
    ),
    Mutation(
        "C1-session-token-loses-its-audience",
        "a2a/gateway/spawn.go",
        ("Audience:          lib.BusTokenAudience,", ""),
        "test_C1_a_session_pod_carries_no_kubernetes_identity",
        "drop the audience from the session pod's projected token. An "
        "audience-less projection is a default-audience token by another name, "
        "so automount staying off would stop meaning anything -- and this is "
        "the quiet version, because the pod keeps exactly one token file at "
        "exactly the path the worker reads",
    ),
    Mutation(
        "C1-session-account-gets-rbac",
        "k8s-operator/internal/controller/platformagent_a2a_callout.go",
        ("""\t\tRoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: a2aCalloutName(agent)},
\t\tSubjects: []rbacv1.Subject{{
\t\t\tKind:      "ServiceAccount",
\t\t\tName:      a2aCalloutName(agent),""",
         """\t\tRoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: a2aCalloutName(agent)},
\t\tSubjects: []rbacv1.Subject{{
\t\t\tKind:      "ServiceAccount",
\t\t\tName:      a2aSessionServiceAccountName(agent),"""),
        "test_C1_a_session_pod_carries_no_kubernetes_identity",
        "point an RBAC binding at the session ServiceAccount instead of the "
        "callout's. The session account holds no permissions, which is the "
        "third thing keeping a session pod's token inert; this is the "
        "cross-module half, because the account is named by the gateway "
        "(module a2a) and granted by the operator (module k8s-operator) and no "
        "Go test in either can see both",
    ),
    Mutation(
        "C1-session-account-gets-rbac-in-the-sibling-file",
        "k8s-operator/internal/controller/platformagent_a2a_manifests.go",
        ("""\t\tSubjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: name, Namespace: agent.Namespace}},""",
         """\t\tSubjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: a2aSessionServiceAccountName(agent), Namespace: agent.Namespace}},"""),
        "test_C1_a_session_pod_carries_no_kubernetes_identity",
        "the same grant as the mutation above, in the other file that renders "
        "A2A RBAC. The scan used to read only the callout's file and to match "
        "one literal space after `Subjects:`, so a binding added here -- where "
        "gofmt aligns the field -- passed it twice over. Both halves of that "
        "hole are what this mutation holds shut",
    ),
    Mutation(
        "A3-supervisor-terminal-back-on-events",
        "k8s-operator/internal/controller/platformagent_a2a_identities.go",
        ('\t\t\t"a2a.tasks.*.*.in",\n\t\t\t"a2a.tasks.*.*.supervisor",',
         '\t\t\t"a2a.tasks.*.*.in",\n\t\t\t"a2a.tasks.*.*.events",'),
        "test_A3_the_supervisor_holds_no_publish_on_the_executors_events_subject",
        "move the gateway's supervisor publish back onto the executors' events "
        "subject -- the pre-split render, and the change a rollback of the "
        "relay durable would tempt. Every executor's subject is two-writer "
        "again and a forged supervisor terminal is indistinguishable on replay",
    ),
    Mutation(
        "A3-second-supervisor-writer",
        "k8s-operator/internal/controller/platformagent_a2a_identities.go",
        ('\t\t"a2a.tasks." + a2aBridgeAddressee + ".*.events",\n\t\t"$KV.runtime-state.>",',
         '\t\t"a2a.tasks." + a2aBridgeAddressee + ".*.events",\n\t\t"a2a.tasks.*.*.supervisor",\n\t\t"$KV.runtime-state.>",'),
        "test_A3_the_supervisor_subject_has_exactly_one_writer",
        "grant the static bridge publish on the supervisor subject, the shape "
        "a bridge-side janitor would take -- finalising a task it executed "
        "reads like the executor's own business. The subject then no longer "
        "says who wrote there. Retargeted from `worker` when A5 retired that "
        "user; the static credential it names is the half the bridge inherited",
    ),
    Mutation(
        "A3-session-writes-its-own-supervisor-subject",
        "a2a/authcallout/session.go",
        ('\t\tPublish: []string{\n\t\t\tlib.TaskEventsSubject(pod, "*"),\n\t\t},',
         '\t\tPublish: []string{\n\t\t\tlib.TaskEventsSubject(pod, "*"),\n\t\t\tlib.TaskSupervisorSubject(pod, "*"),\n\t\t},'),
        "test_A3_the_supervisor_subject_has_exactly_one_writer",
        "derive a session a grant on its own supervisor subject -- the "
        "helpful-looking change that lets a worker adapter finalise itself "
        "after a harness crash. An executor can then declare itself dead as "
        "infrastructure",
    ),
    Mutation(
        "A3-session-per-task-wildcard",
        "a2a/authcallout/session.go",
        ('\t\tPublish: []string{\n\t\t\tlib.TaskEventsSubject(pod, "*"),\n\t\t},',
         '\t\tPublish: []string{\n\t\t\tlib.TaskEventsSubject(pod, "*"),\n\t\t\tlib.TaskInSubject(pod, "*"),\n\t\t},'),
        "test_A3_the_executors_grant_does_not_reach_its_own_in_subject",
        "widen the session's task-plane grant toward the per-task wildcard the "
        "cards sketched, which puts the executor in its own in-subject writer "
        "set: it can steer and cancel itself as if from the user",
    ),
    Mutation(
        "A3-bridge-events-grant-rewildcarded",
        "k8s-operator/internal/controller/platformagent_a2a_identities.go",
        ('"a2a.tasks." + a2aBridgeAddressee + ".*.events",',
         '"a2a.tasks.*.*.events",'),
        "test_A3_the_events_subject_has_no_rendered_writer",
        "put the addressee wildcard back on the bridge's events grant, which "
        "is what `worker` held and the one edit that reopens the violation A5 "
        "closed. It reads as a generalisation -- one bridge build serving any "
        "addressee -- and it costs every chat session's `…events` its writer "
        "set, so a forged terminal from the shared credential is "
        "indistinguishable from the executor's on replay",
    ),
    Mutation(
        "C1-bus-user-env-renamed-on-one-side",
        "a2a/lib/credentials.go",
        ('EnvBusUser = "A2A_BUS_USER"', 'EnvBusUser = "A2A_BUS_PRINCIPAL"'),
        "test_C1_the_agent_containers_bus_identity_env_is_spelled_the_same_in_both_modules",
        "rename the bus identity env var in a2a/lib without touching the "
        "operator that renders it -- the shape a rename takes when the two "
        "literals live in modules that cannot import each other. Both modules "
        "build and both Go suites stay green, because no test binary links "
        "them. What breaks is every `a2a` invocation in the agent container: "
        "busUser() reads the new name, finds nothing, falls back to NATS_USER "
        "which A5 stopped rendering, and connect() refuses with `no bus "
        "identity` before it dials. Loud where it runs and invisible where it "
        "is reviewed, and the half a reviewer has to think to check is the "
        "operator's render rather than this file",
    ),
    Mutation(
        "C1-bus-token-path-moved-on-the-operator-side",
        "k8s-operator/internal/controller/platformagent_a2a_callout.go",
        ('a2aBusTokenPath      = "/var/run/secrets/a2a-bus"',
         'a2aBusTokenPath      = "/var/run/secrets/kubeagents/a2a-bus"'),
        "test_C1_the_bus_token_path_and_audience_agree_across_the_module_boundary",
        "tidy the projected token under a vendor-prefixed directory, touching "
        "only the module that renders the mount. The client half of the "
        "contract lives in a2a/lib and is not rebuilt by this edit, so it keeps "
        "os.Stat-ing the old path, finds nothing, and falls back to a password "
        "this change stopped rendering -- an agent container that offers the "
        "empty string to the callout and loses the bus entirely, with both Go "
        "suites green because no test binary links both modules",
    ),
    Mutation(
        "C1-bus-token-file-env-renamed-on-the-client-side",
        "a2a/lib/credentials.go",
        ('EnvBusTokenFile = "A2A_BUS_TOKEN_FILE"',
         'EnvBusTokenFile = "A2A_BUS_TOKEN_PATH"'),
        "test_C1_the_reserved_bus_token_file_env_is_spelled_the_same_in_both_modules",
        "tidy the client's override variable to match BusTokenPath beside it, "
        "in the module that reads it. Nothing in a2a notices, because a2a is "
        "the only module that consumes this name -- and the operator, which "
        "does not consume it but RESERVES it, is not rebuilt by this edit. It "
        "goes on refusing A2A_BUS_TOKEN_FILE in spec.deployment.env and in an "
        "AgentPlugin's spec.env, and A2A_BUS_TOKEN_PATH is reserved nowhere: "
        "a plugin sets it, connect() prefers it over the projection with no "
        "fallback, and the agent container presents a file the plugin chose",
    ),
    Mutation(
        "C1-bus-token-file-reservation-spelled-by-hand",
        "k8s-operator/internal/controller/platformagent_manifests.go",
        ('\t\t\t\t\te.Name == a2aBusTokenFileEnv ||',
         '\t\t\t\t\te.Name == "A2A_BUS_TOKEN_FILE" ||'),
        "test_C1_the_reserved_bus_token_file_env_is_spelled_the_same_in_both_modules",
        "inline the constant at the plugin-env drop, which changes no "
        "behaviour today and is the shape a reviewer waves through. It costs "
        "the cross-module comparison its subject: a2aBusTokenFileEnv is what "
        "the conformance suite pins against a2a/lib, and after this edit the "
        "name the operator actually refuses is a literal no test reads. The "
        "next rename moves the constant and leaves the drop behind",
    ),
    Mutation(
        "C1-agent-principal-gets-a-static-password",
        "k8s-operator/internal/controller/platformagent_a2a_identities.go",
        ('\t\tuser:           a2aAgentBusUser,',
         '\t\tuser:           a2aAgentBusUser,\n\t\tcredsKey:       a2aBridgePasswordKey,'),
        "test_C1_the_agent_principal_carries_no_static_bus_password",
        "give the agent's callout principal a Secret key as well, so the same "
        "name is answered for by both the callout and nats.conf's auth_users "
        "exemption and a client is authenticated by whichever path it happened "
        "to take. This is how the retired `worker` credential comes back: one "
        "field, added by someone wiring up a local test that could not present "
        "a token",
    ),
    Mutation(
        "C1-bridge-principal-keyed-on-a-service-account",
        "k8s-operator/internal/controller/platformagent_a2a_identities.go",
        ('\t\tuser:     a2aBridgeUser,',
         '\t\tuser:     a2aBridgeUser,\n'
         '\t\tserviceAccount: a2aServiceAccountName(ns, agentServiceAccountName(agent)),'),
        "test_C1_the_agent_principal_carries_no_static_bus_password",
        "move the bridge sidecar onto the callout, which reads as tightening "
        "and is the exact opposite. A sidecar shares its pod's ServiceAccount, "
        "so the bridge's entry and the agent's would key on one username and "
        "each workload would hold the union of the two grant sets -- the task "
        "plane and the blackboard in one credential, which is `worker` rebuilt "
        "by the mechanism meant to retire it",
    ),
    Mutation(
        "harness-fixture-emptied",
        "k8s-operator/internal/testing/testdata/platform/expected/platformagent.yaml",
        ("\nkind: StatefulSet\n", "\nkind: StatefulSetXX\n"),
        "test_the_golden_fixtures_render_more_than_a_stub",
        "corrupt a fixture's object kinds, which would turn every assertion "
        "that iterates it vacuously green",
    ),
]


def _purge_bytecode() -> None:
    """Delete every __pycache__ the suite could import from.

    CPython decides a .pyc is current by comparing the source's mtime *in whole
    seconds* and its size. A mutation that preserves file size -- renaming a
    symbol to another of the same length is the obvious one -- and is restored
    by `git checkout` inside the same second produces a source file that is
    byte-identical to HEAD and a cache entry compiled from the mutated text,
    with no way to tell them apart. That leaks into every subsequent mutation:
    the baseline is no longer the tree, and a later mutation can be credited
    with a kill that belongs to the leftover.

    Found the hard way -- see overnight-b/findings.md.
    """
    for cache in REPO.rglob("__pycache__"):
        if ".git" in cache.parts:
            continue
        for entry in cache.glob("*.pyc"):
            entry.unlink()


def _run_suite() -> tuple[set[str], set[str]]:
    """(failed test names, unexpectedly-successful test names)."""
    _purge_bytecode()
    process = subprocess.run(
        # -B: write no bytecode at all, so nothing survives to go stale. The
        # purge above covers caches written before this ran.
        [sys.executable, "-B", "tests/conformance/run.py"],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=300,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    output = process.stdout + process.stderr
    failed = set(re.findall(r"^(?:FAIL|ERROR): (\S+)", output, re.MULTILINE))
    # An expected failure that starts passing is reported as an unexpected
    # success, which is also the suite noticing the mutation.
    unexpected = set(re.findall(r"^UNEXPECTED SUCCESS: (\S+)", output, re.MULTILINE))
    if "unexpected successes" in output:
        unexpected |= set(re.findall(r"(\S+) \(.*\) \.\.\. unexpected success", output))
    return failed, unexpected


def _git_clean() -> bool:
    return not subprocess.run(
        ["git", "status", "--porcelain"], cwd=REPO, capture_output=True, text=True
    ).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("-k", "--filter", default="")
    arguments = parser.parse_args()

    selected = [m for m in MUTATIONS if arguments.filter in m.id]
    if arguments.list:
        for mutation in selected:
            # The `[control]` suffix rather than a column of its own: the
            # `must_survive` comment calls this output the authority on which
            # rows are controls, and it was not one while the two kinds
            # printed identically. Nothing parses these lines -- `--list` is
            # named in this file's usage and in the suite's README and
            # nowhere else -- so the marker goes on the end, where it cannot
            # move the two fields anybody reads.
            control = "  [control]" if mutation.must_survive else ""
            print(f"{mutation.id:38} {mutation.path}{control}")
        return 0

    if not _git_clean():
        print(
            "the working tree is dirty. This edits tracked files in place and "
            "restores them with git checkout; refusing rather than risking it.",
            file=sys.stderr,
        )
        return 2

    baseline_failed, baseline_unexpected = _run_suite()
    if baseline_failed or baseline_unexpected:
        print(f"baseline is not green: {sorted(baseline_failed | baseline_unexpected)}")
        return 2
    print(f"baseline green. {len(selected)} mutations.\n")

    verdicts = []
    for mutation in selected:
        path = REPO / mutation.path
        if not path.exists():
            verdicts.append((mutation, "STALE", []))
            print(f"STALE    {mutation.id}: {mutation.path} does not exist")
            continue
        original = path.read_text()
        old, new = mutation.edit
        if old not in original:
            verdicts.append((mutation, "STALE", []))
            print(f"STALE    {mutation.id}: the text it edits is not in {mutation.path}")
            continue
        try:
            path.write_text(original.replace(old, new, 1))
            failed, unexpected = _run_suite()
        finally:
            subprocess.run(["git", "checkout", "--", mutation.path], cwd=REPO, check=True)

        noticed = failed | unexpected
        killers = {name for name in noticed if mutation.kills in name}
        others = sorted(name.split(".")[-1] for name in noticed - killers)
        if mutation.must_survive:
            verdict = "OVERSHOT" if noticed else "SURVIVED (expected)"
        elif killers:
            verdict = "NOISY" if others else "KILLED"
        else:
            verdict = "SURVIVED"
        verdicts.append((mutation, verdict, others))
        detail = f"  (also: {', '.join(others[:3])}{'…' if len(others) > 3 else ''})" if others else ""
        print(f"{verdict:8} {mutation.id}{detail}")

    # Re-baseline. Every mutation is restored in a `finally`, so the tree is
    # clean by construction -- but "the tree is clean" and "the suite is back
    # where it started" are different claims, and the second is the one the
    # verdicts above rest on. A run that ends dirty has been scoring later
    # mutations against a polluted baseline.
    closing_failed, closing_unexpected = _run_suite()
    leaked = sorted(closing_failed | closing_unexpected)
    if leaked:
        print(
            f"\nBASELINE POLLUTED: the suite is not green after restoring "
            f"every mutation: {leaked}. Verdicts after the mutation that "
            f"caused it are not trustworthy."
        )

    survived = [m.id for m, verdict, _ in verdicts if verdict in ("SURVIVED", "OVERSHOT")]
    stale = [m.id for m, verdict, _ in verdicts if verdict == "STALE"]
    print(
        f"\nkilled={sum(1 for _, v, _ in verdicts if v == 'KILLED')} "
        f"noisy={sum(1 for _, v, _ in verdicts if v == 'NOISY')} "
        f"survived={len(survived)} stale={len(stale)}"
    )
    if survived:
        print(
            f"UNRESOLVED: {survived} -- a SURVIVED mutation means the test does "
            f"not test what it claims to; an OVERSHOT one means the suite goes "
            f"red on a change that weakens nothing"
        )
    if stale:
        print(f"STALE: {stale} -- the mutation no longer applies; rewrite it")
    return 1 if survived or stale or leaked else 0


if __name__ == "__main__":
    raise SystemExit(main())
