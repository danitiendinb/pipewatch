"""Integration tests for the sampler module."""

from __future__ import annotations

import pytest

from pipewatch.state import PipelineState
from pipewatch.sampler import sample_runs, save_sample, load_sample


@pytest.fixture()
def store(tmp_path):
    return PipelineState(str(tmp_path))


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def _record(store: PipelineState, pipeline: str, status: str, msg: str = "") -> str:
    run_id = store.start(pipeline)
    store.finish(pipeline, run_id, status, msg)
    return run_id


def test_sample_ids_are_real_run_ids(store, state_dir):
    ids = {_record(store, "pipe", "ok") for _ in range(8)}
    results = sample_runs(store, "pipe", n=5, seed=3)
    for r in results:
        assert r.run_id in ids


def test_sample_reflects_mixed_statuses(store, state_dir):
    for _ in range(5):
        _record(store, "mixed", "ok")
    for _ in range(5):
        _record(store, "mixed", "failed", "err")
    results = sample_runs(store, "mixed", n=10, seed=0)
    statuses = {r.status for r in results}
    assert "ok" in statuses
    assert "failed" in statuses


def test_save_load_round_trip_preserves_all_fields(store, state_dir):
    _record(store, "rt", "failed", "disk full")
    results = sample_runs(store, "rt", n=1, seed=0)
    save_sample(state_dir, "rt", results)
    loaded = load_sample(state_dir, "rt")
    assert loaded[0].message == "disk full"
    assert loaded[0].status == "failed"
    assert loaded[0].pipeline == "rt"


def test_multiple_pipelines_independent(store, state_dir):
    for _ in range(6):
        _record(store, "pipe_a", "ok")
    for _ in range(6):
        _record(store, "pipe_b", "failed")
    a = sample_runs(store, "pipe_a", n=3, seed=1)
    b = sample_runs(store, "pipe_b", n=3, seed=1)
    assert all(r.pipeline == "pipe_a" for r in a)
    assert all(r.pipeline == "pipe_b" for r in b)
    assert all(r.status == "ok" for r in a)
    assert all(r.status == "failed" for r in b)
