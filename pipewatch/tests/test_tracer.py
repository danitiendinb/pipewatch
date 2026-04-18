"""Unit tests for pipewatch.tracer."""
import pytest
from pathlib import Path
from pipewatch.tracer import (
    load_traces,
    add_event,
    get_run_traces,
    clear_traces,
)


@pytest.fixture
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_load_traces_empty_for_new_pipeline(state_dir):
    assert load_traces(state_dir, "pipe_a") == []


def test_add_event_returns_entry(state_dir):
    entry = add_event(state_dir, "pipe_a", "run-1", "start")
    assert entry["event"] == "start"
    assert entry["run_id"] == "run-1"
    assert "timestamp" in entry


def test_add_event_persists(state_dir):
    add_event(state_dir, "pipe_a", "run-1", "start", "beginning")
    traces = load_traces(state_dir, "pipe_a")
    assert len(traces) == 1
    assert traces[0]["detail"] == "beginning"


def test_add_multiple_events(state_dir):
    add_event(state_dir, "pipe_a", "run-1", "start")
    add_event(state_dir, "pipe_a", "run-1", "step", "transform")
    add_event(state_dir, "pipe_a", "run-1", "finish")
    assert len(load_traces(state_dir, "pipe_a")) == 3


def test_get_run_traces_filters_by_run_id(state_dir):
    add_event(state_dir, "pipe_a", "run-1", "start")
    add_event(state_dir, "pipe_a", "run-2", "start")
    add_event(state_dir, "pipe_a", "run-1", "finish")
    result = get_run_traces(state_dir, "pipe_a", "run-1")
    assert len(result) == 2
    assert all(e["run_id"] == "run-1" for e in result)


def test_get_run_traces_empty_for_unknown_run(state_dir):
    add_event(state_dir, "pipe_a", "run-1", "start")
    assert get_run_traces(state_dir, "pipe_a", "run-99") == []


def test_clear_traces_removes_file(state_dir):
    add_event(state_dir, "pipe_a", "run-1", "start")
    clear_traces(state_dir, "pipe_a")
    assert load_traces(state_dir, "pipe_a") == []


def test_clear_traces_noop_when_no_file(state_dir):
    clear_traces(state_dir, "pipe_a")  # should not raise
