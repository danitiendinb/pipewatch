"""Tests for pipewatch.snapshotter."""
import pytest

from pipewatch.snapshotter import take_snapshot, load_snapshots, clear_snapshots
from pipewatch.state import PipelineState


@pytest.fixture()
def store(tmp_path):
    return PipelineState(str(tmp_path))


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def test_load_snapshots_empty_when_no_file(state_dir):
    assert load_snapshots(state_dir) == []


def test_take_snapshot_returns_dict(state_dir):
    snap = take_snapshot(state_dir, [])
    assert isinstance(snap, dict)
    assert "ts" in snap
    assert snap["total"] == 0


def test_take_snapshot_persists(state_dir):
    take_snapshot(state_dir, [])
    snaps = load_snapshots(state_dir)
    assert len(snaps) == 1


def test_multiple_snapshots_appended(state_dir):
    take_snapshot(state_dir, [])
    take_snapshot(state_dir, [])
    snaps = load_snapshots(state_dir)
    assert len(snaps) == 2


def test_snapshot_counts_reflect_state(state_dir):
    store = PipelineState(state_dir)
    store.record_success("pipe_a", "run-1", duration=1.0)
    store.record_failure("pipe_b", "run-2", duration=0.5, message="err")
    snap = take_snapshot(state_dir, ["pipe_a", "pipe_b"])
    assert snap["total"] == 2
    assert snap["ok"] == 1
    assert snap["failing"] == 1


def test_snapshot_pipeline_names_present(state_dir):
    store = PipelineState(state_dir)
    store.record_success("alpha", "r1", duration=1.0)
    snap = take_snapshot(state_dir, ["alpha"])
    names = [p["name"] for p in snap["pipelines"]]
    assert "alpha" in names


def test_snapshot_timestamps_are_ordered(state_dir):
    """Snapshots appended over time should have non-decreasing timestamps."""
    snap1 = take_snapshot(state_dir, [])
    snap2 = take_snapshot(state_dir, [])
    assert snap2["ts"] >= snap1["ts"]


def test_clear_snapshots_removes_file(state_dir):
    take_snapshot(state_dir, [])
    clear_snapshots(state_dir)
    assert load_snapshots(state_dir) == []


def test_clear_snapshots_noop_when_missing(state_dir):
    clear_snapshots(state_dir)  # should not raise
