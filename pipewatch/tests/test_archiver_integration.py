"""Integration tests for archiver using real filesystem."""
from __future__ import annotations

import pytest

from pipewatch.state import PipelineState, PipelineRun, start, finish, load as load_state
from pipewatch.archiver import archive_pipeline, load_archive, clear_archive


def _utc(ts: str) -> str:
    return ts + "+00:00"


@pytest.fixture
def store(tmp_path):
    s = load_state(str(tmp_path), "integration_pipe")
    run = start("integration_pipe")
    s = finish(s, run, status="ok", message=None)
    run2 = start("integration_pipe")
    s = finish(s, run2, status="fail", message="oops")
    return s


@pytest.fixture
def state_dir(tmp_path):
    return str(tmp_path)


def test_archive_and_reload_preserves_status(state_dir, store):
    archive_pipeline(state_dir, "integration_pipe", store)
    records = load_archive(state_dir, "integration_pipe")
    statuses = {r["status"] for r in records}
    assert "ok" in statuses
    assert "fail" in statuses


def test_archive_preserves_message(state_dir, store):
    archive_pipeline(state_dir, "integration_pipe", store)
    records = load_archive(state_dir, "integration_pipe")
    messages = [r.get("message") for r in records]
    assert "oops" in messages


def test_clear_and_rearchive(state_dir, store):
    archive_pipeline(state_dir, "integration_pipe", store)
    clear_archive(state_dir, "integration_pipe")
    archive_pipeline(state_dir, "integration_pipe", store)
    records = load_archive(state_dir, "integration_pipe")
    assert len(records) == len(store.runs)
