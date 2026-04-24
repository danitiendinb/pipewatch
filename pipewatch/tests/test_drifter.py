"""Unit tests for pipewatch.drifter."""
from __future__ import annotations

import pytest
from unittest.mock import patch

from pipewatch.drifter import (
    DriftReport,
    detect_drift,
    save_drift_baseline,
    load_drift_baseline,
    clear_drift_baseline,
)
from pipewatch.state import PipelineState, PipelineRun


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def _run(started: str, finished: str, status: str = "ok") -> PipelineRun:
    return PipelineRun(
        run_id="r1",
        status=status,
        started_at=started,
        finished_at=finished,
        message="",
    )


def _state_with(runs: list) -> PipelineState:
    s = PipelineState(pipeline="pipe", runs=runs)
    return s


def test_load_baseline_none_for_unknown(state_dir):
    result = load_drift_baseline(state_dir, "missing")
    assert result is None


def test_save_and_load_baseline(state_dir):
    save_drift_baseline(state_dir, "pipe", 42.5)
    assert load_drift_baseline(state_dir, "pipe") == pytest.approx(42.5)


def test_clear_baseline_removes_file(state_dir):
    save_drift_baseline(state_dir, "pipe", 10.0)
    clear_drift_baseline(state_dir, "pipe")
    assert load_drift_baseline(state_dir, "pipe") is None


def test_detect_drift_no_runs_returns_none_avg(state_dir):
    state = _state_with([])
    report = detect_drift(state, state_dir, "pipe")
    assert report.current_avg_duration is None
    assert report.has_drift is False


def test_detect_drift_no_baseline_no_drift(state_dir):
    runs = [_run("2024-01-01T00:00:00", "2024-01-01T00:01:00")]
    state = _state_with(runs)
    report = detect_drift(state, state_dir, "pipe")
    assert report.previous_avg_duration is None
    assert report.has_drift is False


def test_detect_drift_within_threshold(state_dir):
    save_drift_baseline(state_dir, "pipe", 60.0)
    runs = [_run("2024-01-01T00:00:00", "2024-01-01T00:01:05")]  # 65s
    state = _state_with(runs)
    report = detect_drift(state, state_dir, "pipe", threshold_pct=20.0)
    assert report.has_drift is False
    assert report.drift_pct == pytest.approx(8.33, rel=0.01)


def test_detect_drift_above_threshold(state_dir):
    save_drift_baseline(state_dir, "pipe", 60.0)
    runs = [_run("2024-01-01T00:00:00", "2024-01-01T00:02:00")]  # 120s
    state = _state_with(runs)
    report = detect_drift(state, state_dir, "pipe", threshold_pct=20.0)
    assert report.has_drift is True
    assert report.drift_pct == pytest.approx(100.0)


def test_detect_drift_averages_multiple_runs(state_dir):
    save_drift_baseline(state_dir, "pipe", 60.0)
    runs = [
        _run("2024-01-01T00:00:00", "2024-01-01T00:01:00"),  # 60s
        _run("2024-01-01T01:00:00", "2024-01-01T01:01:00"),  # 60s
    ]
    state = _state_with(runs)
    report = detect_drift(state, state_dir, "pipe")
    assert report.current_avg_duration == pytest.approx(60.0)
    assert report.has_drift is False
