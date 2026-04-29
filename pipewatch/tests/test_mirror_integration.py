"""Integration tests for the mirror feature."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.state import PipelineState
from pipewatch.snapshotter import take_snapshot
from pipewatch.mirror import (
    mirror_pipeline,
    mirror_all,
    load_mirror,
    clear_mirror,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


@pytest.fixture()
def store(state_dir):
    return PipelineState(state_dir)


NOW = "2024-06-01T12:00:00+00:00"


def test_mirror_reflects_snapshot_count(store, state_dir):
    store.record_success("pipe_a", "run-1", None)
    take_snapshot(state_dir, "pipe_a", store)
    store.record_success("pipe_a", "run-2", None)
    take_snapshot(state_dir, "pipe_a", store)

    rec = mirror_pipeline(state_dir, "pipe_a", "remote", NOW)
    assert rec.snapshot_count == 2


def test_mirror_zero_snapshots_when_none_taken(store, state_dir):
    store.record_success("pipe_b", "run-1", None)
    rec = mirror_pipeline(state_dir, "pipe_b", "remote", NOW)
    assert rec.snapshot_count == 0


def test_mirror_all_creates_records_for_each(store, state_dir):
    for name in ["alpha", "beta", "gamma"]:
        store.record_success(name, "r1", None)
    records = mirror_all(state_dir, ["alpha", "beta", "gamma"], "dest", NOW)
    assert len(records) == 3
    for rec in records:
        loaded = load_mirror(state_dir, rec.pipeline)
        assert loaded is not None
        assert loaded.destination == "dest"


def test_clear_mirror_after_mirror(store, state_dir):
    mirror_pipeline(state_dir, "pipe_a", "remote", NOW)
    clear_mirror(state_dir, "pipe_a")
    assert load_mirror(state_dir, "pipe_a") is None


def test_mirror_overwrites_previous_record(store, state_dir):
    mirror_pipeline(state_dir, "pipe_a", "dest-1", NOW)
    now2 = "2024-06-02T12:00:00+00:00"
    mirror_pipeline(state_dir, "pipe_a", "dest-2", now2)
    loaded = load_mirror(state_dir, "pipe_a")
    assert loaded.destination == "dest-2"
    assert loaded.last_mirrored == now2
