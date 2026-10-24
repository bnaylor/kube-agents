"""Tests for the fan-out's infra-lock slot rule in hack/ci-eval-pr.sh (#3384, #3821).

A stack-bearing unit runs under lock-infra, one at a time. Before this rule
`launch_units` gave every unit a parallelism slot as soon as one was free, so a
second stack-bearing unit launched while the first ran sat in its slot waiting
for the lock: on the 2026-10-24 main nightly those waits held 25.7 of ~70
slot-hours and the run timed out. Now at most one lock-taking unit is in
flight, and the slots it would have parked go to units that do not take the
lock.

`launch_units`, `unit_takes_infra_lock` and `infra_lock_exempt` are extracted
from the script and run with `run_one_unit` stubbed: the stub takes a real
`mkdir` lock for a stack-bearing unit, as the script does, and records when
each unit started, got the lock and ended. A `sleep` executable first on PATH
turns the dispatcher's 3s polls into a tenth of a second (a shell function
named `sleep` breaks background jobs under bash 3.2).
"""

import os
import pathlib
import re
import subprocess
import tempfile
import unittest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
_CI_EVAL_PR = _REPO_ROOT / "hack" / "ci-eval-pr.sh"


def _function(name):
    text = _CI_EVAL_PR.read_text(encoding="utf-8")
    match = re.search(rf"^{name}\(\) \{{.*?^\}}\n", text, re.S | re.M)
    assert match, f"could not find {name}() in hack/ci-eval-pr.sh"
    return match.group(0)


_HARNESS = r"""
set -euo pipefail
LOG="$1"; LOCK="$2/lock-infra"
# Task table: index -> name, stack flag, how long the run takes (seconds).
TASK_NAMES=(stack-a stack-b stack-c plain-1 plain-2 plain-3 plain-4 plain-5 plain-6 oobe-first-run-audits)
TASK_HAS_STACK=(true true true "" "" "" "" "" "" true)
RUN_SECONDS=(1.0 1.0 1.0 0.6 0.6 0.6 0.6 0.6 0.6 0.4)
TASKS=("${TASK_NAMES[@]}"); TASK_REUSE=(); TASK_STREAMS=()
for i in "${!TASK_NAMES[@]}"; do TASK_REUSE[i]=""; TASK_STREAMS[i]=""; done
EVAL_REPETITIONS=1
now() { python3 -c 'import time; print(f"{time.monotonic():.3f}")'; }
run_one_unit() { # <task-path> <task-name> <rep> <reuse> <has-stack> <seq> <streams>
  local name="$2" start got i
  start="$(now)"
  for i in "${!TASK_NAMES[@]}"; do
    if [ "${TASK_NAMES[i]}" = "${name}" ]; then break; fi
  done
  got="${start}"
  if [ -n "$5" ] && ! infra_lock_exempt "${name}"; then
    until mkdir "${LOCK}" 2>/dev/null; do /bin/sleep 0.05; done
    got="$(now)"
  fi
  /bin/sleep "${RUN_SECONDS[i]}"
  if [ -n "$5" ] && ! infra_lock_exempt "${name}"; then rmdir "${LOCK}"; fi
  # One write per unit, under 512 bytes, so concurrent appends stay whole lines.
  printf '%s %s %s %s\n' "${name}" "${start}" "${got}" "$(now)" >> "${LOG}"
}
"""


class InfraSlotTest(unittest.TestCase):
    maxDiff = None

    def _run(self, queue_indexes, parallelism):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = pathlib.Path(tmp)
            log = tmpdir / "units.log"
            log.write_text("")
            bindir = tmpdir / "bin"
            bindir.mkdir()
            fake_sleep = bindir / "sleep"
            fake_sleep.write_text('#!/bin/sh\nif [ "$1" = "3" ]; then exec /bin/sleep 0.1; fi\nexec /bin/sleep "$@"\n')
            fake_sleep.chmod(0o755)
            script = "\n".join([
                _HARNESS,
                _function("infra_lock_exempt"),
                _function("unit_takes_infra_lock"),
                _function("launch_units"),
                "QUEUE=\"$(printf '1 0 %s\\n' " + " ".join(str(i) for i in queue_indexes) + ")\"",
                f'launch_units "${{QUEUE}}" {parallelism} 0 > /dev/null',
                "wait",
            ])
            env = dict(os.environ, PATH=f"{bindir}{os.pathsep}{os.environ.get('PATH', '')}")
            result = subprocess.run(
                ["bash", "-c", script, "harness", str(log), str(tmpdir)],
                capture_output=True, text=True, timeout=120, env=env,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            rows = []
            for line in log.read_text().splitlines():
                name, start, got, end = line.split()
                rows.append((name, float(start), float(got), float(end)))
            return rows

    def test_no_unit_waits_in_its_slot_for_the_infra_lock(self):
        # Three stack-bearing units at the head of the queue, as a cost-sorted
        # queue puts them: before this rule two of them would park in slots.
        rows = self._run([0, 1, 2, 3, 4, 5, 6, 7, 8], parallelism=3)
        self.assertEqual(sorted(r[0] for r in rows), sorted(["stack-a", "stack-b", "stack-c"] + [f"plain-{n}" for n in range(1, 7)]))
        waits = {name: got - start for name, start, got, _ in rows}
        for name in ("stack-a", "stack-b", "stack-c"):
            self.assertLess(waits[name], 0.3, f"{name} waited {waits[name]:.2f}s in its slot for lock-infra")

    def test_at_most_one_lock_taking_unit_is_in_flight(self):
        rows = self._run([0, 1, 2, 3, 4, 5, 6, 7, 8], parallelism=3)
        stack = sorted((start, end) for name, start, _, end in rows if name.startswith("stack-"))
        for (s1, e1), (s2, _e2) in zip(stack, stack[1:]):
            self.assertGreaterEqual(s2, e1 - 0.05, "a second lock-taking unit launched while the first was in flight")

    def test_the_freed_slots_go_to_units_that_do_not_take_the_lock(self):
        rows = self._run([0, 1, 2, 3, 4, 5, 6, 7, 8], parallelism=3)
        first_stack_end = min(end for name, _, _, end in rows if name.startswith("stack-"))
        started_meanwhile = [name for name, start, _, _ in rows if name.startswith("plain-") and start < first_stack_end]
        self.assertGreaterEqual(len(started_meanwhile), 2, f"only {started_meanwhile} started while the first stack unit ran")

    def test_parallelism_is_still_the_ceiling(self):
        rows = self._run([0, 1, 2, 3, 4, 5, 6, 7, 8], parallelism=3)
        events = sorted([(start, 1) for _, start, _, _ in rows] + [(end, -1) for *_, end in rows])
        running = peak = 0
        for _, delta in events:
            running += delta
            peak = max(peak, running)
        self.assertLessEqual(peak, 3)

    def test_an_infra_lock_exempt_stack_runs_beside_a_lock_holder(self):
        # oobe-first-run-audits has a stack but is infra_lock_exempt: it must not
        # be held back behind a lock-taking unit.
        rows = self._run([0, 9, 3], parallelism=3)
        starts = {name: start for name, start, _, _ in rows}
        ends = {name: end for name, _, _, end in rows}
        self.assertLess(starts["oobe-first-run-audits"], ends["stack-a"])


if __name__ == "__main__":
    unittest.main()
