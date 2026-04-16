"""Tests for pipewatch.state."""

import json
from pathlib import Path

import pytest

from pipewatch.state import PipelineRun, PipelineState, StateStore


@pytest.fixture()
def store(tmp_path: Path) -> StateStore:
    return StateStore(str(tmp_path / "state"))


def _finished_run(pipeline: str, exit_code: int) -> PipelineRun:
    run = PipelineRun.start(pipeline)
    run.finish(exit_code=exit_code)
    return run


def test_load_returns_empty_state_for_unknown_pipeline(store: StateStore) -> None:
    state = store.load("nonexistent")
    assert state.pipeline == "nonexistent"
    assert state.last_run is None
    assert state.consecutive_failures == 0


def test_record_success_resets_consecutive_failures(store: StateStore) -> None:
    run_fail = _finished_run("etl", 1)
    store.record_run(run_fail)
    run_ok = _finished_run("etl", 0)
    state = store.record_run(run_ok)
    assert state.consecutive_failures == 0
    assert state.last_run is not None
    assert state.last_run.status == "success"


def test_record_failure_increments_counter(store: StateStore) -> None:
    for i in range(3):
        run = _finished_run("etl", 1)
        state = store.record_run(run)
    assert state.consecutive_failures == 3


def test_state_persisted_to_disk(store: StateStore, tmp_path: Path) -> None:
    run = _finished_run("my_pipe", 0)
    store.record_run(run)
    state_file = list((tmp_path / "state").glob("*.json"))[0]
    data = json.loads(state_file.read_text())
    assert data["pipeline"] == "my_pipe"
    assert len(data["history"]) == 1


def test_history_capped_at_50(store: StateStore) -> None:
    for _ in range(60):
        store.record_run(_finished_run("pipe", 0))
    state = store.load("pipe")
    assert len(state.history) <= 50
