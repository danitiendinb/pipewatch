"""Unit tests for pipewatch.eventsink."""
from __future__ import annotations

import pytest
from pathlib import Path

from pipewatch.eventsink import (
    load_events,
    push_event,
    clear_events,
    drain_events,
    flush_from_state,
    SinkEvent,
)
from pipewatch.state import PipelineState, PipelineRun


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _ts() -> str:
    return "2024-06-01T12:00:00"


def test_load_events_empty_for_new_pipeline(state_dir):
    assert load_events(state_dir, "pipe_a") == []


def test_push_event_returns_sink_event(state_dir):
    ev = push_event(state_dir, "pipe_a", "success", _ts(), message="ok")
    assert isinstance(ev, SinkEvent)
    assert ev.event_type == "success"
    assert ev.pipeline == "pipe_a"


def test_push_event_persists(state_dir):
    push_event(state_dir, "pipe_a", "failure", _ts(), message="boom")
    events = load_events(state_dir, "pipe_a")
    assert len(events) == 1
    assert events[0].message == "boom"


def test_push_multiple_events_appends(state_dir):
    push_event(state_dir, "pipe_a", "success", _ts())
    push_event(state_dir, "pipe_a", "failure", _ts(), message="err")
    events = load_events(state_dir, "pipe_a")
    assert len(events) == 2


def test_clear_events_removes_file(state_dir):
    push_event(state_dir, "pipe_a", "success", _ts())
    clear_events(state_dir, "pipe_a")
    assert load_events(state_dir, "pipe_a") == []


def test_drain_events_returns_and_clears(state_dir):
    push_event(state_dir, "pipe_a", "success", _ts())
    drained = drain_events(state_dir, "pipe_a")
    assert len(drained) == 1
    assert load_events(state_dir, "pipe_a") == []


def test_flush_from_state_success(state_dir):
    run = PipelineRun(
        run_id="r1", started_at=_ts(), finished_at=_ts(), status="ok", message=None
    )
    state = PipelineState(pipeline="pipe_a", runs=[run])
    ev = flush_from_state(state_dir, "pipe_a", state, _ts())
    assert ev is not None
    assert ev.event_type == "success"


def test_flush_from_state_failure(state_dir):
    run = PipelineRun(
        run_id="r1", started_at=_ts(), finished_at=_ts(), status="fail", message="oops"
    )
    state = PipelineState(pipeline="pipe_a", runs=[run])
    ev = flush_from_state(state_dir, "pipe_a", state, _ts())
    assert ev is not None
    assert ev.event_type == "failure"
    assert ev.message == "oops"


def test_flush_from_state_no_runs_returns_none(state_dir):
    state = PipelineState(pipeline="pipe_a", runs=[])
    ev = flush_from_state(state_dir, "pipe_a", state, _ts())
    assert ev is None


def test_pipelines_independent(state_dir):
    push_event(state_dir, "pipe_a", "success", _ts())
    push_event(state_dir, "pipe_b", "failure", _ts())
    assert len(load_events(state_dir, "pipe_a")) == 1
    assert len(load_events(state_dir, "pipe_b")) == 1
    assert load_events(state_dir, "pipe_a")[0].event_type == "success"
