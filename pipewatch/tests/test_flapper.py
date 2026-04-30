"""Unit tests for pipewatch.flapper."""

from __future__ import annotations

import pytest

from pipewatch.flapper import (
    FlapReport,
    count_transitions,
    detect_flap,
    save_flap_report,
    load_flap_report,
)
from pipewatch.state import PipelineState, PipelineRun


def _run(status: str, run_id: str = "r") -> PipelineRun:
    return PipelineRun(
        run_id=run_id,
        status=status,
        started_at="2024-01-01T00:00:00",
        finished_at="2024-01-01T00:01:00",
        message="",
    )


def _state_with(statuses):
    runs = [_run(s, f"r{i}") for i, s in enumerate(statuses)]
    return PipelineState(runs=runs, consecutive_failures=0)


def test_count_transitions_empty():
    assert count_transitions([]) == 0


def test_count_transitions_single():
    assert count_transitions(["ok"]) == 0


def test_count_transitions_no_change():
    assert count_transitions(["ok", "ok", "ok"]) == 0


def test_count_transitions_alternating():
    assert count_transitions(["ok", "fail", "ok", "fail"]) == 3


def test_count_transitions_one_flip():
    assert count_transitions(["ok", "ok", "fail", "fail"]) == 1


def test_detect_flap_not_flapping():
    state = _state_with(["ok", "ok", "ok", "fail", "ok"])
    report = detect_flap("pipe", state, threshold=3)
    assert not report.is_flapping
    assert report.flap_count == 2


def test_detect_flap_is_flapping():
    state = _state_with(["ok", "fail", "ok", "fail", "ok", "fail"])
    report = detect_flap("pipe", state, threshold=3)
    assert report.is_flapping
    assert report.flap_count == 5


def test_detect_flap_pipeline_name():
    state = _state_with(["ok"])
    report = detect_flap("my-pipeline", state)
    assert report.pipeline == "my-pipeline"


def test_detect_flap_no_runs():
    state = PipelineState(runs=[], consecutive_failures=0)
    report = detect_flap("empty", state, threshold=3)
    assert not report.is_flapping
    assert report.flap_count == 0


def test_save_and_load_flap_report(tmp_path):
    report = FlapReport(
        pipeline="p1",
        flap_count=4,
        is_flapping=True,
        transitions=["ok", "fail", "ok", "fail", "ok"],
    )
    save_flap_report(str(tmp_path), report)
    loaded = load_flap_report(str(tmp_path), "p1")
    assert loaded is not None
    assert loaded.pipeline == "p1"
    assert loaded.flap_count == 4
    assert loaded.is_flapping is True


def test_load_flap_report_none_for_unknown(tmp_path):
    assert load_flap_report(str(tmp_path), "ghost") is None
