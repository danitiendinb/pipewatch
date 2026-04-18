"""Unit tests for pipewatch.baseliner."""

from __future__ import annotations

import pytest
from pathlib import Path

from pipewatch.baseliner import (
    Baseline, load_baseline, save_baseline, clear_baseline,
    compute_baseline, exceeds_baseline,
)
from pipewatch.state import PipelineState, PipelineRun


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _run(status: str = "ok", duration: float = 10.0) -> PipelineRun:
    return PipelineRun(
        run_id="r1", pipeline="pipe", status=status,
        started_at="2024-01-01T00:00:00Z", finished_at="2024-01-01T00:00:10Z",
        duration_seconds=duration, message=None,
    )


def test_load_baseline_none_for_unknown(state_dir):
    assert load_baseline(state_dir, "pipe") is None


def test_save_and_load_baseline(state_dir):
    b = Baseline(pipeline="pipe", mean_duration=42.5, sample_count=3, recorded_at="2024-01-01T00:00:00Z")
    save_baseline(state_dir, b)
    loaded = load_baseline(state_dir, "pipe")
    assert loaded is not None
    assert loaded.mean_duration == 42.5
    assert loaded.sample_count == 3


def test_clear_baseline_removes_file(state_dir):
    b = Baseline(pipeline="pipe", mean_duration=5.0, sample_count=1, recorded_at="2024-01-01T00:00:00Z")
    save_baseline(state_dir, b)
    clear_baseline(state_dir, "pipe")
    assert load_baseline(state_dir, "pipe") is None


def test_compute_baseline_none_when_no_ok_runs(state_dir):
    state = PipelineState(runs=[_run(status="fail")])
    assert compute_baseline(state, "pipe") is None


def test_compute_baseline_mean(state_dir):
    runs = [_run(duration=10.0), _run(duration=20.0)]
    state = PipelineState(runs=runs)
    b = compute_baseline(state, "pipe")
    assert b is not None
    assert b.mean_duration == 15.0
    assert b.sample_count == 2


def test_exceeds_baseline_true():
    b = Baseline(pipeline="p", mean_duration=10.0, sample_count=5, recorded_at="x")
    assert exceeds_baseline(25.0, b, factor=2.0) is True


def test_exceeds_baseline_false():
    b = Baseline(pipeline="p", mean_duration=10.0, sample_count=5, recorded_at="x")
    assert exceeds_baseline(15.0, b, factor=2.0) is False
