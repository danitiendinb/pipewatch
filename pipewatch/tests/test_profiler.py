"""Unit tests for pipewatch.profiler."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.profiler import (
    DurationProfile,
    compute_profile,
    save_profile,
    load_profile,
    clear_profile,
)
from pipewatch.state import PipelineState, PipelineRun


def _run(started: str, finished: str, status: str = "ok") -> PipelineRun:
    return PipelineRun(
        run_id="r1",
        started_at=started,
        finished_at=finished,
        status=status,
        message="",
        consecutive_failures=0,
    )


def _state_with(*runs: PipelineRun) -> PipelineState:
    s = PipelineState(pipeline="pipe", runs=list(runs))
    return s


def test_compute_profile_none_when_fewer_than_two_runs():
    state = _state_with(_run("2024-01-01T00:00:00", "2024-01-01T00:01:00"))
    assert compute_profile("pipe", state) is None


def test_compute_profile_returns_dataclass():
    state = _state_with(
        _run("2024-01-01T00:00:00", "2024-01-01T00:01:00"),
        _run("2024-01-01T01:00:00", "2024-01-01T01:03:00"),
    )
    profile = compute_profile("pipe", state)
    assert isinstance(profile, DurationProfile)
    assert profile.pipeline == "pipe"
    assert profile.sample_size == 2


def test_compute_profile_mean_correct():
    state = _state_with(
        _run("2024-01-01T00:00:00", "2024-01-01T00:01:00"),  # 60s
        _run("2024-01-01T01:00:00", "2024-01-01T01:03:00"),  # 180s
    )
    profile = compute_profile("pipe", state)
    assert profile.mean_seconds == pytest.approx(120.0)


def test_compute_profile_min_max():
    state = _state_with(
        _run("2024-01-01T00:00:00", "2024-01-01T00:01:00"),  # 60s
        _run("2024-01-01T01:00:00", "2024-01-01T01:05:00"),  # 300s
    )
    profile = compute_profile("pipe", state)
    assert profile.min_seconds == pytest.approx(60.0)
    assert profile.max_seconds == pytest.approx(300.0)


def test_save_and_load_profile(tmp_path):
    profile = DurationProfile(
        pipeline="pipe", sample_size=5,
        mean_seconds=90.0, median_seconds=85.0,
        p95_seconds=150.0, p99_seconds=200.0,
        min_seconds=60.0, max_seconds=210.0,
    )
    save_profile(str(tmp_path), profile)
    loaded = load_profile(str(tmp_path), "pipe")
    assert loaded is not None
    assert loaded.mean_seconds == pytest.approx(90.0)
    assert loaded.p95_seconds == pytest.approx(150.0)


def test_load_profile_none_when_missing(tmp_path):
    assert load_profile(str(tmp_path), "ghost") is None


def test_clear_profile_removes_file(tmp_path):
    profile = DurationProfile(
        pipeline="pipe", sample_size=2,
        mean_seconds=60.0, median_seconds=60.0,
        p95_seconds=60.0, p99_seconds=60.0,
        min_seconds=60.0, max_seconds=60.0,
    )
    save_profile(str(tmp_path), profile)
    clear_profile(str(tmp_path), "pipe")
    assert load_profile(str(tmp_path), "pipe") is None
