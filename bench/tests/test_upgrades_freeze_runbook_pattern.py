"""upgrades-freeze-runbook-probe's node-pool check wants the pull-request form.

Since #2937 gke-upgrades sends a node-pool upgrade out as a pull request
changing ``spec.version`` on the pool's ``ContainerNodePool``, never as a
``gcloud`` write. The check accepts the field as a dotted path in prose or as
a ``version:`` line in proposed YAML; a gcloud-only runbook, or YAML that sets
only the cluster's ``minMasterVersion``, does not pass.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from kube_agents_bench.verifiers import _normalize, _normalize_lines

TASK = Path(__file__).resolve().parent.parent / "tasks" / "upgrades-freeze-runbook-probe" / "task.yaml"
CHECK = "the-runbook-upgrades-the-pool-by-pull-request"


def _passes(report: str) -> bool:
    doc = yaml.safe_load(TASK.read_text())
    (check,) = [c for c in doc["verification_spec"] if c.get("name") == CHECK]
    spec = check["check"]
    text = _normalize(report)
    if any(_normalize(p) not in text for p in spec["required_phrases"]):
        return False
    lines = _normalize_lines(report)
    return any(_normalize(p) in text for p in spec["any_of_phrases"]) or any(
        re.search(p, lines) for p in spec["any_of_patterns"]
    )


def test_pull_request_runbooks_pass():
    for report in (
        "1. Open a pull request setting `spec.version` on the `default-pool` ContainerNodePool to 1.33.2-gke.1.",
        "```yaml\napiVersion: container.cnrm.cloud.google.com/v1beta1\nkind: ContainerNodePool\n"
        "metadata:\n  name: default-pool\nspec:\n  version: 1.33.2-gke.1\n```",
    ):
        assert _passes(report), report


def test_gcloud_and_cluster_only_runbooks_fail():
    for report in (
        "gcloud container clusters upgrade prod-1 --node-pool default-pool --cluster-version 1.33.2-gke.1",
        "```yaml\nkind: ContainerCluster\nspec:\n  minMasterVersion: 1.33.2-gke.1\n```\n"
        "The ContainerNodePool follows on auto-upgrade.",
    ):
        assert not _passes(report), report
