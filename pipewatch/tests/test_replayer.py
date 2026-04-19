"""Unit tests for pipewatch.replayer."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from pipewatch.state import PipelineState, PipelineRun
from pipewatch.replayer import (
    load_replayed_ids,
    replay_runs,
    clear_replay,
    ReplayResult,
)


def _run(run_id, success=True, finished_at="2024-01-10T10:00:00"):
    r = MagicMock(spec=PipelineRun)
    r.run_id = run_id
    r.success = success
    r.finished_at = finished_at
    return r


@pytest.fixture()
def store(tmp_path):
    return PipelineState(str(tmp_path))


def test_load_replayed_ids_empty_for_new(tmp_path):
    ids = load_replayed_ids(str(tmp_path), "pipe_a")
    assert ids == set()


def test_replay_calls_handler_for_each_run(tmp_path, store):
    state = store.load("pipe_a")
    state.runs = [_run("r1"), _run("r2")]
    store._states = {"pipe_a": state}

    calls = []
    result = replay_runs(store, str(tmp_path), "pipe_a", lambda r: calls.append(r.run_id))
    assert sorted(calls) == ["r1", "r2"]
    assert result.replayed == 2
    assert result.skipped == 0


def test_replay_skips_already_seen(tmp_path, store):
    state = store.load("pipe_a")
    state.runs = [_run("r1"), _run("r2")]
    store._states = {"pipe_a": state}

    calls = []
    replay_runs(store, str(tmp_path), "pipe_a", lambda r: calls.append(r.run_id))
    calls.clear()
    result = replay_runs(store, str(tmp_path), "pipe_a", lambda r: calls.append(r.run_id))
    assert calls == []
    assert result.skipped == 2


def test_replay_dry_run_does_not_persist(tmp_path, store):
    state = store.load("pipe_a")
    state.runs = [_run("r1")]
    store._states = {"pipe_a": state}

    replay_runs(store, str(tmp_path), "pipe_a", lambda r: None, dry_run=True)
    ids = load_replayed_ids(str(tmp_path), "pipe_a")
    assert ids == set()


def test_replay_since_filter(tmp_path, store):
    state = store.load("pipe_a")
    state.runs = [_run("old", finished_at="2024-01-01T00:00:00"), _run("new", finished_at="2024-06-01T00:00:00")]
    store._states = {"pipe_a": state}

    calls = []
    result = replay_runs(store, str(tmp_path), "pipe_a", lambda r: calls.append(r.run_id), since="2024-03-01T00:00:00")
    assert calls == ["new"]
    assert result.skipped == 1


def test_clear_replay_removes_file(tmp_path, store):
    state = store.load("pipe_a")
    state.runs = [_run("r1")]
    store._states = {"pipe_a": state}
    replay_runs(store, str(tmp_path), "pipe_a", lambda r: None)
    clear_replay(str(tmp_path), "pipe_a")
    assert load_replayed_ids(str(tmp_path), "pipe_a") == set()
