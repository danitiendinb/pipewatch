"""Unit tests for pipewatch.trendline"""
from __future__ import annotations

import datetime
from typing import List

import pytest

from pipewatch.state import PipelineRun, PipelineState
from pipewatch.trendline import (
    TrendlineReport,
    _finished_durations,
    _linear_regression,
    compute_trendline,
    compute_all,
)


def _run(
    status: str = "ok",
    duration: float = 10.0,
    ts: str = "2024-01-01T00:00:00",
) -> PipelineRun:
    return PipelineRun(
        run_id="r1",
        started_at=ts,
        finished_at=ts,
        status=status,
        duration_seconds=duration,
        message=None,
    )


def _state_with(runs: List[PipelineRun]) -> PipelineState:
    s = PipelineState.__new__(PipelineState)
    s.runs = runs
    return s


# --- _linear_regression ---

def test_linear_regression_flat():
    slope, intercept = _linear_regression([5.0, 5.0, 5.0])
    assert slope == pytest.approx(0.0)
    assert intercept == pytest.approx(5.0)


def test_linear_regression_rising():
    slope, intercept = _linear_regression([1.0, 2.0, 3.0])
    assert slope == pytest.approx(1.0)


def test_linear_regression_falling():
    slope, _ = _linear_regression([3.0, 2.0, 1.0])
    assert slope == pytest.approx(-1.0)


# --- compute_trendline ---

def test_compute_trendline_none_when_fewer_than_two_runs():
    state = _state_with([_run(duration=10.0)])
    assert compute_trendline("p", state) is None


def test_compute_trendline_none_when_no_runs():
    state = _state_with([])
    assert compute_trendline("p", state) is None


def test_compute_trendline_returns_report():
    runs = [_run(duration=float(i + 1)) for i in range(5)]
    state = _state_with(runs)
    report = compute_trendline("pipe", state)
    assert isinstance(report, TrendlineReport)
    assert report.pipeline == "pipe"
    assert report.sample_size == 5


def test_compute_trendline_degrading():
    runs = [_run(duration=float(i * 5)) for i in range(1, 6)]
    state = _state_with(runs)
    report = compute_trendline("pipe", state, stable_threshold=1.0)
    assert report is not None
    assert report.direction == "degrading"


def test_compute_trendline_stable():
    runs = [_run(duration=10.0) for _ in range(5)]
    state = _state_with(runs)
    report = compute_trendline("pipe", state, stable_threshold=1.0)
    assert report is not None
    assert report.direction == "stable"


def test_compute_trendline_improving():
    runs = [_run(duration=float(10 - i)) for i in range(5)]
    state = _state_with(runs)
    report = compute_trendline("pipe", state, stable_threshold=0.5)
    assert report is not None
    assert report.direction == "improving"


def test_compute_all_skips_insufficient():
    states = {
        "a": _state_with([_run(duration=5.0)]),        # only 1 run
        "b": _state_with([_run(duration=5.0), _run(duration=6.0)]),
    }
    reports = compute_all(states)
    assert len(reports) == 1
    assert reports[0].pipeline == "b"


def test_compute_all_sorted_by_name():
    states = {
        "z": _state_with([_run(duration=1.0), _run(duration=2.0)]),
        "a": _state_with([_run(duration=1.0), _run(duration=2.0)]),
    }
    reports = compute_all(states)
    assert [r.pipeline for r in reports] == ["a", "z"]
