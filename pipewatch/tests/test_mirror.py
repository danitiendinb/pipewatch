"""Unit tests for pipewatch.mirror."""

from __future__ import annotations

import json
import pytest
from pathlib import Path

from pipewatch.mirror import (
    MirrorRecord,
    _mirror_path,
    load_mirror,
    save_mirror,
    clear_mirror,
    mirror_pipeline,
    mirror_all,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


NOW = "2024-06-01T12:00:00+00:00"


def test_load_mirror_none_for_unknown(state_dir):
    assert load_mirror(state_dir, "pipe_a") is None


def test_save_and_load_mirror(state_dir):
    rec = MirrorRecord(
        pipeline="pipe_a",
        destination="s3://bucket",
        last_mirrored=NOW,
        snapshot_count=3,
    )
    save_mirror(state_dir, rec)
    loaded = load_mirror(state_dir, "pipe_a")
    assert loaded is not None
    assert loaded.pipeline == "pipe_a"
    assert loaded.destination == "s3://bucket"
    assert loaded.last_mirrored == NOW
    assert loaded.snapshot_count == 3


def test_clear_mirror_removes_file(state_dir):
    rec = MirrorRecord(pipeline="pipe_a", destination="remote", last_mirrored=NOW)
    save_mirror(state_dir, rec)
    clear_mirror(state_dir, "pipe_a")
    assert load_mirror(state_dir, "pipe_a") is None


def test_clear_mirror_noop_when_missing(state_dir):
    # Should not raise
    clear_mirror(state_dir, "nonexistent")


def test_mirror_pipeline_returns_record(state_dir):
    rec = mirror_pipeline(state_dir, "pipe_a", "remote", NOW)
    assert isinstance(rec, MirrorRecord)
    assert rec.pipeline == "pipe_a"
    assert rec.destination == "remote"
    assert rec.last_mirrored == NOW


def test_mirror_pipeline_persists(state_dir):
    mirror_pipeline(state_dir, "pipe_a", "remote", NOW)
    loaded = load_mirror(state_dir, "pipe_a")
    assert loaded is not None
    assert loaded.destination == "remote"


def test_mirror_all_returns_one_per_pipeline(state_dir):
    records = mirror_all(state_dir, ["a", "b", "c"], "dest", NOW)
    assert len(records) == 3
    assert {r.pipeline for r in records} == {"a", "b", "c"}


def test_mirror_file_is_valid_json(state_dir):
    mirror_pipeline(state_dir, "pipe_a", "remote", NOW)
    path = _mirror_path(state_dir, "pipe_a")
    data = json.loads(path.read_text())
    assert data["pipeline"] == "pipe_a"
