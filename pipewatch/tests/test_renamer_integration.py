"""Integration tests for the renamer across multiple state-file types."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from pipewatch.renamer import load_rename_log, rename_pipeline
from pipewatch.state import PipelineState


@pytest.fixture()
def state_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


def _write_state(state_dir: str, name: str) -> None:
    store = PipelineState(state_dir)
    store.record_success(name, "run-1", duration=1.0)


def test_rename_preserves_state_content(state_dir):
    _write_state(state_dir, "etl_load")
    rename_pipeline(state_dir, "etl_load", "etl_load_v2")
    store = PipelineState(state_dir)
    state = store.load("etl_load_v2")
    assert state.consecutive_failures == 0


def test_rename_old_name_no_longer_has_state(state_dir):
    _write_state(state_dir, "old_etl")
    rename_pipeline(state_dir, "old_etl", "new_etl")
    files = list(Path(state_dir).glob("old_etl.*"))
    # Rename log is keyed on _rename_log, not old_etl
    assert files == []


def test_rename_log_accumulates_multiple_renames(state_dir):
    _write_state(state_dir, "pipe_a")
    rename_pipeline(state_dir, "pipe_a", "pipe_b")
    _write_state(state_dir, "pipe_x")
    rename_pipeline(state_dir, "pipe_x", "pipe_y")
    log = load_rename_log(state_dir)
    assert len(log) == 2
    assert {e["from"] for e in log} == {"pipe_a", "pipe_x"}


def test_rename_no_state_files_leaves_log_clean(state_dir):
    result = rename_pipeline(state_dir, "ghost", "phantom")
    assert result == []
    # No log entry created when there is nothing to rename
    log = load_rename_log(state_dir)
    assert log == []
