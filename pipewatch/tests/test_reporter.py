"""Tests for pipewatch.reporter."""
import pytest
from pipewatch.reporter import (
    pipeline_status,
    render_pipeline,
    render_summary,
    STATUS_OK,
    STATUS_FAILING,
    STATUS_UNKNOWN,
)
from pipewatch.state import PipelineState, PipelineRun


def _state(failures: int = 0, runs=None) -> PipelineState:
    r = runs or []
    last = r[-1] if r else None
    return PipelineState(consecutive_failures=failures, last_run=last, runs=r)


def _run(success: bool, started="2024-01-01T10:00:00", finished="2024-01-01T10:01:00") -> PipelineRun:
    return PipelineRun(started_at=started, finished_at=finished, success=success, exit_code=0 if success else 1)


def test_pipeline_status_unknown_when_no_runs():
    assert pipeline_status(_state()) == STATUS_UNKNOWN


def test_pipeline_status_ok_on_success():
    state = _state(failures=0, runs=[_run(True)])
    assert pipeline_status(state) == STATUS_OK


def test_pipeline_status_failing():
    state = _state(failures=2, runs=[_run(False)])
    assert pipeline_status(state) == STATUS_FAILING


def test_render_pipeline_contains_name():
    state = _state(failures=0, runs=[_run(True)])
    output = render_pipeline("my_pipe", state)
    assert "my_pipe" in output


def test_render_pipeline_shows_no_runs_message():
    output = render_pipeline("empty", _state())
    assert "No runs recorded" in output


def test_render_pipeline_lists_runs():
    state = _state(failures=1, runs=[_run(False)])
    output = render_pipeline("pipe", state)
    assert "failure" in output


def test_render_summary_empty():
    assert render_summary({}) == "No pipelines tracked."


def test_render_summary_lists_all_pipelines():
    states = {
        "alpha": _state(failures=0, runs=[_run(True)]),
        "beta": _state(failures=3, runs=[_run(False)]),
    }
    output = render_summary(states)
    assert "alpha" in output
    assert "beta" in output
    assert "FAILING" in output
    assert "OK" in output
