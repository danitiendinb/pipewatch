"""Unit tests for pipewatch.archiver."""
from __future__ import annotations

import pytest
from pathlib import Path

from pipewatch.archiver import archive_pipeline, load_archive, clear_archive, archive_all
from pipewatch.state import PipelineState, PipelineRun


def _run(status: str = "ok", duration: float = 1.0) -> PipelineRun:
    return PipelineRun(
        run_id="r1",
        started_at="2024-01-01T00:00:00+00:00",
        finished_at="2024-01-01T00:00:01+00:00",
        status=status,
        duration_seconds=duration,
        message=None,
    )


@pytest.fixture
def state_dir(tmp_path):
    return str(tmp_path)


def _make_store(*runs) -> PipelineState:
    return PipelineState(runs=list(runs), consecutive_failures=0)


def test_load_archive_empty_for_new_pipeline(state_dir):
    records = load_archive(state_dir, "pipe_a")
    assert records == []


def test_archive_pipeline_returns_path(state_dir):
    store = _make_store(_run())
    path = archive_pipeline(state_dir, "pipe_a", store)
    assert Path(path).exists()


def test_archive_pipeline_records_loadable(state_dir):
    store = _make_store(_run(), _run(status="fail"))
    archive_pipeline(state_dir, "pipe_a", store)
    records = load_archive(state_dir, "pipe_a")
    assert len(records) == 2


def test_archive_pipeline_appends_on_second_call(state_dir):
    store = _make_store(_run())
    archive_pipeline(state_dir, "pipe_a", store)
    archive_pipeline(state_dir, "pipe_a", store)
    records = load_archive(state_dir, "pipe_a")
    assert len(records) == 2


def test_clear_archive_removes_file(state_dir):
    store = _make_store(_run())
    archive_pipeline(state_dir, "pipe_a", store)
    clear_archive(state_dir, "pipe_a")
    assert load_archive(state_dir, "pipe_a") == []


def test_clear_archive_noop_when_missing(state_dir):
    clear_archive(state_dir, "nonexistent")  # should not raise


def test_archive_all_returns_counts(state_dir):
    stores = {
        "pipe_a": _make_store(_run(), _run()),
        "pipe_b": _make_store(_run()),
    }
    result = archive_all(state_dir, ["pipe_a", "pipe_b"], stores)
    assert result["pipe_a"] == 2
    assert result["pipe_b"] == 1


def test_archive_record_contains_pipeline_name(state_dir):
    store = _make_store(_run())
    archive_pipeline(state_dir, "my_pipe", store)
    records = load_archive(state_dir, "my_pipe")
    assert records[0]["pipeline"] == "my_pipe"
