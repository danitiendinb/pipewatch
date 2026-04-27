"""Unit tests for pipewatch.renamer."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest

from pipewatch.renamer import (
    _state_files_for,
    clear_rename_log,
    load_rename_log,
    rename_pipeline,
)


@pytest.fixture()
def state_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


def _touch(state_dir: str, stem: str, *suffixes: str):
    for suf in suffixes:
        Path(state_dir, stem + suf).write_text("{}")


def test_state_files_for_returns_matching(state_dir):
    _touch(state_dir, "alpha", ".json", ".state")
    _touch(state_dir, "beta", ".json")
    files = _state_files_for(state_dir, "alpha")
    stems = {p.stem for p in files}
    assert stems == {"alpha"}
    assert len(files) == 2


def test_rename_pipeline_moves_files(state_dir):
    _touch(state_dir, "old", ".json", ".state")
    renamed = rename_pipeline(state_dir, "old", "new")
    assert len(renamed) == 2
    assert not any(Path(r).stem == "old" for r in renamed)
    assert all(Path(r).stem == "new" for r in renamed)
    assert all(Path(r).exists() for r in renamed)


def test_rename_pipeline_same_name_returns_empty(state_dir):
    _touch(state_dir, "same", ".json")
    result = rename_pipeline(state_dir, "same", "same")
    assert result == []


def test_rename_pipeline_raises_on_conflict(state_dir):
    _touch(state_dir, "old", ".json")
    _touch(state_dir, "new", ".json")
    with pytest.raises(ValueError, match="already has state files"):
        rename_pipeline(state_dir, "old", "new")


def test_rename_pipeline_raises_on_empty_name(state_dir):
    with pytest.raises(ValueError):
        rename_pipeline(state_dir, "", "new")


def test_rename_records_log_entry(state_dir):
    _touch(state_dir, "pipe1", ".json")
    rename_pipeline(state_dir, "pipe1", "pipe2")
    log = load_rename_log(state_dir)
    assert len(log) == 1
    assert log[0]["from"] == "pipe1"
    assert log[0]["to"] == "pipe2"


def test_load_rename_log_empty_when_no_file(state_dir):
    assert load_rename_log(state_dir) == []


def test_clear_rename_log_removes_file(state_dir):
    _touch(state_dir, "x", ".json")
    rename_pipeline(state_dir, "x", "y")
    clear_rename_log(state_dir)
    assert load_rename_log(state_dir) == []


def test_rename_no_files_returns_empty(state_dir):
    result = rename_pipeline(state_dir, "ghost", "phantom")
    assert result == []
