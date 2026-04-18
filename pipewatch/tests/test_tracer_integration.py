"""Integration tests for tracer: records events alongside pipeline state."""
import pytest
from pathlib import Path
from pipewatch.state import PipelineState
from pipewatch.tracer import add_event, load_traces, get_run_traces


@pytest.fixture
def store(tmp_path: Path):
    return PipelineState(str(tmp_path))


@pytest.fixture
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_trace_events_stored_per_run(state_dir):
    add_event(state_dir, "etl", "run-1", "start")
    add_event(state_dir, "etl", "run-1", "extract", "rows=500")
    add_event(state_dir, "etl", "run-1", "finish")
    traces = get_run_traces(state_dir, "etl", "run-1")
    assert len(traces) == 3
    assert traces[1]["detail"] == "rows=500"


def test_multiple_pipelines_independent(state_dir):
    add_event(state_dir, "pipe_a", "r1", "start")
    add_event(state_dir, "pipe_b", "r1", "start")
    assert len(load_traces(state_dir, "pipe_a")) == 1
    assert len(load_traces(state_dir, "pipe_b")) == 1


def test_trace_survives_multiple_runs(state_dir):
    for i in range(3):
        add_event(state_dir, "etl", f"run-{i}", "start")
        add_event(state_dir, "etl", f"run-{i}", "finish")
    all_traces = load_traces(state_dir, "etl")
    assert len(all_traces) == 6
    run2 = get_run_traces(state_dir, "etl", "run-2")
    assert len(run2) == 2
