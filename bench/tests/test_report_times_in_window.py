"""Tests for ``report_times_in_window``: an observation time a report states falls inside the run (#3256)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from devops_bench.verification.spec import parse_node
from kube_agents_bench import transcript
from kube_agents_bench.verifiers import ReportTimesInWindowVerifier
from pydantic import ValidationError

# The labels the cluster-agent-observation-time-read case uses.
LABELS = [r"\bobservation\b", r"\bobserved (?:at|on)\b", r"\bas of\b", r"\bchecked at\b", r"\bretrieved at\b"]
# How long before the check the fake run started.
RUN_LENGTH = timedelta(minutes=5)
# Further than the default skew (120 s) outside the window.
OUTSIDE = timedelta(minutes=5)


def _stamp(instant: datetime) -> str:
    return instant.strftime("%Y-%m-%d %H:%M:%S UTC")


def _run(report: str, *, started: datetime | None = None, **fields):
    started = started or datetime.now(tz=timezone.utc) - RUN_LENGTH
    transcript.set("ack", [], final_message=report, started_at=started.timestamp())
    fields.setdefault("label_patterns", LABELS)
    return ReportTimesInWindowVerifier(type="report_times_in_window", **fields).verify(timeout_sec=1.0)


@pytest.fixture(autouse=True)
def _clean_stash():
    transcript.clear()
    yield
    transcript.clear()


def test_the_incident_report_fails_on_a_time_after_the_run():
    # #3256: the audit trail stated 21:38:00 for an answer posted at 21:33:04.
    future = datetime.now(tz=timezone.utc) + OUTSIDE
    report = (
        "Total Pod Count: 16\n\n"
        "Grounding Sources & Audit Trail\n"
        "- Diagnostic Command Executed: kubectl get pods -n kubeagents-system\n"
        f"- Timestamp of Observation: {_stamp(future)}\n"
    )
    outcome = _run(report)
    assert not outcome.success
    assert "outside the run's window" in outcome.reason
    assert _stamp(future) in outcome.reason


def test_a_time_before_the_run_started_fails():
    started = datetime.now(tz=timezone.utc) - RUN_LENGTH
    outcome = _run(f"**Observed at:** {_stamp(started - OUTSIDE)}", started=started)
    assert not outcome.success


def test_a_report_with_no_time_passes():
    outcome = _run("**There are 2 pods in seeded-reliability.** `kubectl get pods` lists both Running.")
    assert outcome.success, outcome.reason
    assert "states no observation time" in outcome.reason


def test_a_time_inside_the_run_passes():
    now = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
    outcome = _run(f"- Timestamp of Observation: {_stamp(now)}")
    assert outcome.success, outcome.reason
    assert "1 observation time(s) inside" in outcome.reason


@pytest.mark.parametrize(
    "spelling",
    ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.123456+00:00", "%Y-%m-%d %H:%M UTC", "`%Y-%m-%dT%H:%M:%SZ`"],
)
def test_the_iso_spellings_are_all_read(spelling):
    future = datetime.now(tz=timezone.utc) + OUTSIDE
    assert not _run(f"As of {future.strftime(spelling)} there are 2 pods.").success


def test_a_numeric_offset_is_converted():
    # The same instant written in UTC-04:00 is inside; read as UTC it would be four hours off.
    now = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
    local = (now - timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%S-04:00")
    assert _run(f"Checked at {local}.").success


def test_a_clock_time_with_utc_and_no_date_is_read():
    future = datetime.now(tz=timezone.utc) + OUTSIDE
    outcome = _run(f"As of {future.strftime('%H:%M')} UTC, 2 pods.")
    assert not outcome.success
    now = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
    assert _run(f"As of {now.strftime('%H:%M:%S')} UTC, 2 pods.").success


def test_the_value_on_the_line_after_a_bare_label_is_checked():
    future = datetime.now(tz=timezone.utc) + OUTSIDE
    assert not _run(f"**Time of observation**\n\n{_stamp(future)}\n").success


def test_a_time_on_an_unlabelled_line_is_not_checked():
    # An event timestamp quoted from command output may be days old; a
    # certificate's expiry is in the future. Neither is an observation time.
    old = datetime.now(tz=timezone.utc) - timedelta(days=3)
    future = datetime.now(tz=timezone.utc) + timedelta(days=30)
    report = f"- Last BackOff event: {_stamp(old)}\n- Certificate expires {_stamp(future)}\n"
    assert _run(report).success


def test_a_named_zone_is_named_and_not_graded():
    future = datetime.now(tz=timezone.utc) + OUTSIDE
    outcome = _run(f"Observed at {future.strftime('%Y-%m-%d %H:%M:%S')} EDT")
    assert outcome.success, outcome.reason
    assert "a named zone other than UTC" in outcome.reason


def test_the_skew_widens_the_window():
    just_after = datetime.now(tz=timezone.utc) + timedelta(seconds=60)
    assert _run(f"Observed at {_stamp(just_after)}").success
    assert not _run(f"Observed at {_stamp(just_after)}", max_clock_skew_sec=0).success


def test_scope_full_reads_the_output():
    future = datetime.now(tz=timezone.utc) + OUTSIDE
    started = datetime.now(tz=timezone.utc) - RUN_LENGTH
    transcript.set(f"Observed at {_stamp(future)}", [], final_message="2 pods.", started_at=started.timestamp())
    final = ReportTimesInWindowVerifier(type="report_times_in_window", label_patterns=LABELS)
    full = ReportTimesInWindowVerifier(type="report_times_in_window", label_patterns=LABELS, scope="full")
    assert final.verify(1.0).success
    assert not full.verify(1.0).success


def test_without_a_transcript_it_is_an_error():
    check = ReportTimesInWindowVerifier(type="report_times_in_window", label_patterns=LABELS)
    assert check.verify(1.0).status == "error"


def test_without_a_run_start_it_is_an_error():
    transcript.set("ack", [], final_message="Observed at 2026-10-15 21:38:00 UTC")
    check = ReportTimesInWindowVerifier(type="report_times_in_window", label_patterns=LABELS)
    outcome = check.verify(1.0)
    assert outcome.status == "error"
    assert "no start time" in outcome.reason


@pytest.mark.parametrize("patterns", [[], ["", "  "]])
def test_no_label_pattern_is_refused(patterns):
    with pytest.raises(ValidationError):
        ReportTimesInWindowVerifier(type="report_times_in_window", label_patterns=patterns)


def test_a_label_pattern_that_does_not_compile_is_refused():
    with pytest.raises(ValidationError):
        ReportTimesInWindowVerifier(type="report_times_in_window", label_patterns=["(unclosed"])


def test_the_registered_type_parses_from_a_spec():
    node = parse_node({"type": "report_times_in_window", "label_patterns": ["observ"]})
    assert isinstance(node, ReportTimesInWindowVerifier)


def test_a_pm_time_is_read_on_the_24_hour_clock():
    now = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
    assert _run(f"Observed at {now.strftime('%Y-%m-%d %I:%M:%S %p')} UTC").success
    wrong_half = now + timedelta(hours=12)
    flipped = now.strftime("%Y-%m-%d ") + wrong_half.strftime("%I:%M:%S %p") + " UTC"
    assert not _run(f"Observed at {flipped}").success


def test_a_nanosecond_fraction_is_read():
    # kubectl logs --timestamps prints nine fractional digits.
    future = datetime.now(tz=timezone.utc) + OUTSIDE
    assert not _run(f"As of {future.strftime('%Y-%m-%dT%H:%M:%S')}.123456789Z").success


def test_a_date_that_does_not_exist_is_named_and_not_graded():
    outcome = _run("Observed at 2026-02-30 21:38:00 UTC")
    assert outcome.success, outcome.reason
    assert "not a real date and time" in outcome.reason


def test_a_sentence_with_a_label_word_does_not_lend_the_next_line():
    old = datetime.now(tz=timezone.utc) - timedelta(days=14)
    report = f"Nothing in this observation needs action.\n- Pod created: {_stamp(old)}\n"
    assert _run(report).success


def test_a_label_line_followed_by_a_labelled_line_is_read_once():
    now = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
    outcome = _run(f"**Observation**\n- Checked at {_stamp(now)}\n")
    assert outcome.success, outcome.reason
    assert outcome.reason.startswith("1 observation time(s)")
