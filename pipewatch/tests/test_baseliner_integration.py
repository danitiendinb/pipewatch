"""Integration tests for baseliner using real PipelineState storage."""

from __future__ import annotations

import pytest
from pathlib import Path

from pipewatch.state import PipelineState, start, finish
from pipewatch.baseliner import compute_baseline, save_baseline, load_baseline, exceeds_baseline


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


@pytest.fixture()
def store(state_dir):
    for duration in [10.0, 20.0, 30.0]:
        run = start(state_dir, "etl")
        finish(state_dir, "etl", run.run_id, status="ok",
               message=None, duration_seconds=duration)
    return state_dir


def test_baseline_mean_over_stored_runs(store):
    state = PipelineState.load(store, "etl")
    b = compute_baseline(state, "etl")
    assert b is not None
    assert b.mean_duration == 20.0
    assert b.sample_count == 3


def test_baseline_persists_and_reloads(store):
    state = PipelineState.load(store, "etl")
    b = compute_baseline(state, "etl")
    save_baseline(store, b)
    loaded = load_baseline(store, "etl")
    assert loaded.mean_duration == b.mean_duration
    assert loaded.pipeline == "etl"


def test_exceeds_baseline_after_spike(store):
    state = PipelineState.load(store, "etl")
    b = compute_baseline(state, "etl")
    save_baseline(store, b)
    loaded = load_baseline(store, "etl")
    # 20.0 mean * 2.0 factor = 40.0 threshold; 90s exceeds it
    assert exceeds_baseline(90.0, loaded, factor=2.0) is True
    assert exceeds_baseline(30.0, loaded, factor=2.0) is False
