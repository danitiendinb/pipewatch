"""Integration tests for the event sink wired to the state store."""
from __future__ import annotations

from pathlib import Path

import pytest

from pipewatch.state import PipelineState, PipelineRun
from pipewatch.eventsink import flush_from_state, load_events, drain_events, push_event


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _run(status: str, msg: str | None = None) -> PipelineRun:
    return PipelineRun(
        run_id="r1",
        started_at="2024-06-01T10:00:00",
        finished_at="2024-06-01T10:05:00",
        status=status,
        message=msg,
    )


def test_flush_multiple_runs_only_last_pushed(state_dir):
    state = PipelineState(
        pipeline="etl",
        runs=[_run("ok"), _run("fail", "timeout")],
    )
    flush_from_state(state_dir, "etl", state, "2024-06-01T10:06:00")
    events = load_events(state_dir, "etl")
    assert len(events) == 1
    assert events[0].event_type == "failure"


def test_drain_after_two_flushes_returns_both(state_dir):
    state_ok = PipelineState(pipeline="etl", runs=[_run("ok")])
    state_fail = PipelineState(pipeline="etl", runs=[_run("fail", "err")])
    flush_from_state(state_dir, "etl", state_ok, "2024-06-01T10:00:00")
    flush_from_state(state_dir, "etl", state_fail, "2024-06-01T11:00:00")
    drained = drain_events(state_dir, "etl")
    assert len(drained) == 2
    assert drained[0].event_type == "success"
    assert drained[1].event_type == "failure"
    assert load_events(state_dir, "etl") == []


def test_custom_metadata_round_trips(state_dir):
    push_event(
        state_dir,
        "etl",
        "custom",
        "2024-06-01T12:00:00",
        message="deploy",
        metadata={"version": "1.2.3"},
    )
    events = load_events(state_dir, "etl")
    assert events[0].metadata == {"version": "1.2.3"}


def test_independent_pipelines_do_not_cross_contaminate(state_dir):
    push_event(state_dir, "pipe_a", "success", "2024-06-01T12:00:00")
    push_event(state_dir, "pipe_b", "failure", "2024-06-01T12:01:00")
    drain_events(state_dir, "pipe_a")
    assert len(load_events(state_dir, "pipe_b")) == 1
