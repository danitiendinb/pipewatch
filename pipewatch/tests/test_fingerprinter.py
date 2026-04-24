"""Tests for pipewatch.fingerprinter."""

from __future__ import annotations

import pytest

from pipewatch.fingerprinter import (
    FingerprintRecord,
    _hash_run,
    clear_fingerprint,
    fingerprint_all,
    fingerprint_latest,
    load_fingerprint,
    save_fingerprint,
)
from pipewatch.state import PipelineRun, PipelineState


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def _run(status: str = "ok", message: str = "", duration: float = 10.0) -> PipelineRun:
    return PipelineRun(
        run_id="run-1",
        started_at="2024-01-01T00:00:00",
        finished_at="2024-01-01T00:00:10",
        status=status,
        message=message,
        duration_seconds=duration,
    )


def _state(pipeline: str = "etl", runs=None) -> PipelineState:
    return PipelineState(pipeline=pipeline, runs=runs or [])


# --- _hash_run ---

def test_hash_run_is_deterministic():
    run = _run()
    assert _hash_run(run) == _hash_run(run)


def test_hash_run_differs_by_status():
    assert _hash_run(_run(status="ok")) != _hash_run(_run(status="fail"))


def test_hash_run_differs_by_message():
    assert _hash_run(_run(message="")) != _hash_run(_run(message="error"))


def test_hash_run_is_hex_string():
    h = _hash_run(_run())
    assert len(h) == 64
    int(h, 16)  # should not raise


# --- load / save / clear ---

def test_load_fingerprint_none_for_unknown(state_dir):
    assert load_fingerprint(state_dir, "missing") is None


def test_save_and_load_fingerprint(state_dir):
    save_fingerprint(state_dir, "etl", "abc123")
    assert load_fingerprint(state_dir, "etl") == "abc123"


def test_clear_fingerprint_removes_record(state_dir):
    save_fingerprint(state_dir, "etl", "abc123")
    clear_fingerprint(state_dir, "etl")
    assert load_fingerprint(state_dir, "etl") is None


def test_clear_fingerprint_noop_when_missing(state_dir):
    clear_fingerprint(state_dir, "ghost")  # should not raise


# --- fingerprint_latest ---

def test_fingerprint_latest_none_when_no_runs(state_dir):
    assert fingerprint_latest(_state(runs=[]), state_dir) is None


def test_fingerprint_latest_changed_on_first_run(state_dir):
    state = _state(runs=[_run()])
    record = fingerprint_latest(state, state_dir)
    assert isinstance(record, FingerprintRecord)
    assert record.changed is True


def test_fingerprint_latest_unchanged_on_identical_run(state_dir):
    state = _state(runs=[_run()])
    fingerprint_latest(state, state_dir)  # first call stores
    record = fingerprint_latest(state, state_dir)  # second call same data
    assert record.changed is False


def test_fingerprint_latest_changed_after_status_flip(state_dir):
    state_ok = _state(runs=[_run(status="ok")])
    fingerprint_latest(state_ok, state_dir)
    state_fail = _state(runs=[_run(status="fail")])
    record = fingerprint_latest(state_fail, state_dir)
    assert record.changed is True


# --- fingerprint_all ---

def test_fingerprint_all_skips_empty_pipelines(state_dir):
    states = [_state("a", runs=[]), _state("b", runs=[_run()])]
    results = fingerprint_all(states, state_dir)
    assert len(results) == 1
    assert results[0].pipeline == "b"


def test_fingerprint_all_returns_record_per_pipeline(state_dir):
    states = [_state("p1", runs=[_run()]), _state("p2", runs=[_run()])]
    results = fingerprint_all(states, state_dir)
    assert {r.pipeline for r in results} == {"p1", "p2"}
