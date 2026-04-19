"""Tests for pipewatch.forecaster."""
from __future__ import annotations

import pytest
from pipewatch.forecaster import (
    Forecast,
    forecast_all,
    forecast_pipeline,
    success_rate,
    _confidence,
)
from pipewatch.state import PipelineState, PipelineRun


def _run(status: str, pipeline: str = "pipe") -> PipelineRun:
    return PipelineRun(
        run_id="r1",
        pipeline=pipeline,
        started_at="2024-01-01T00:00:00",
        finished_at="2024-01-01T00:01:00",
        status=status,
        message="",
        duration_seconds=60.0,
    )


def _state_with(runs: list, pipeline: str = "pipe") -> PipelineState:
    return PipelineState(runs={pipeline: runs})


def test_success_rate_empty():
    assert success_rate([]) == 0.0


def test_success_rate_all_ok():
    runs = [_run("ok") for _ in range(5)]
    assert success_rate(runs) == 1.0


def test_success_rate_mixed():
    runs = [_run("ok"), _run("ok"), _run("failed")]
    assert abs(success_rate(runs) - 2 / 3) < 1e-9


def test_confidence_low():
    assert _confidence(3) == "low"


def test_confidence_medium():
    assert _confidence(10) == "medium"


def test_confidence_high():
    assert _confidence(25) == "high"


def test_forecast_pipeline_none_when_no_runs():
    state = _state_with([])
    assert forecast_pipeline(state, "pipe") is None


def test_forecast_pipeline_returns_forecast():
    runs = [_run("ok")] * 4 + [_run("failed")]
    state = _state_with(runs)
    f = forecast_pipeline(state, "pipe")
    assert isinstance(f, Forecast)
    assert f.pipeline == "pipe"
    assert f.total_runs == 5
    assert abs(f.success_rate - 0.8) < 1e-9
    assert f.predicted_success is True
    assert f.confidence == "medium"


def test_forecast_pipeline_predicts_failure():
    runs = [_run("failed")] * 3 + [_run("ok")]
    state = _state_with(runs)
    f = forecast_pipeline(state, "pipe")
    assert f.predicted_success is False


def test_forecast_all_sorted_by_rate():
    state = PipelineState(
        runs={
            "a": [_run("ok", "a")] * 10,
            "b": [_run("failed", "b")] * 10,
        }
    )
    results = forecast_all(state)
    assert results[0].pipeline == "b"
    assert results[1].pipeline == "a"


def test_forecast_all_skips_empty_pipelines():
    state = PipelineState(runs={"empty": []})
    assert forecast_all(state) == []
