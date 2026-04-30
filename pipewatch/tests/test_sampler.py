"""Unit tests for pipewatch.sampler."""

from __future__ import annotations

import pytest

from pipewatch.state import PipelineState
from pipewatch.sampler import sample_runs, save_sample, load_sample, clear_sample


@pytest.fixture()
def store(tmp_path):
    return PipelineState(str(tmp_path))


def _record(store: PipelineState, pipeline: str, status: str, msg: str = "") -> None:
    run_id = store.start(pipeline)
    store.finish(pipeline, run_id, status, msg)


def test_sample_runs_empty_when_no_runs(store, tmp_path):
    results = sample_runs(store, "alpha", n=3, seed=0)
    assert results == []


def test_sample_runs_returns_up_to_n(store, tmp_path):
    for i in range(10):
        _record(store, "alpha", "ok")
    results = sample_runs(store, "alpha", n=4, seed=42)
    assert len(results) == 4


def test_sample_runs_capped_at_available(store, tmp_path):
    for i in range(3):
        _record(store, "beta", "ok")
    results = sample_runs(store, "beta", n=10, seed=1)
    assert len(results) == 3


def test_sample_runs_result_fields(store, tmp_path):
    _record(store, "gamma", "failed", "boom")
    results = sample_runs(store, "gamma", n=1, seed=7)
    assert len(results) == 1
    r = results[0]
    assert r.pipeline == "gamma"
    assert r.status == "failed"
    assert r.message == "boom"
    assert r.run_id
    assert r.started_at


def test_sample_runs_reproducible_with_seed(store, tmp_path):
    for i in range(20):
        _record(store, "delta", "ok")
    r1 = [x.run_id for x in sample_runs(store, "delta", n=5, seed=99)]
    r2 = [x.run_id for x in sample_runs(store, "delta", n=5, seed=99)]
    assert r1 == r2


def test_save_and_load_sample(store, tmp_path):
    _record(store, "eta", "ok")
    results = sample_runs(store, "eta", n=1, seed=0)
    save_sample(str(tmp_path), "eta", results)
    loaded = load_sample(str(tmp_path), "eta")
    assert len(loaded) == 1
    assert loaded[0].pipeline == "eta"
    assert loaded[0].status == "ok"


def test_load_sample_empty_when_no_file(tmp_path):
    assert load_sample(str(tmp_path), "unknown") == []


def test_clear_sample_removes_file(store, tmp_path):
    _record(store, "zeta", "ok")
    results = sample_runs(store, "zeta", n=1, seed=0)
    save_sample(str(tmp_path), "zeta", results)
    clear_sample(str(tmp_path), "zeta")
    assert load_sample(str(tmp_path), "zeta") == []


def test_clear_sample_noop_when_missing(tmp_path):
    # Should not raise
    clear_sample(str(tmp_path), "nonexistent")
