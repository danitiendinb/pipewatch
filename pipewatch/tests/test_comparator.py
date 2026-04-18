"""Unit tests for pipewatch.comparator."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from pipewatch.state import PipelineRun
from pipewatch.comparator import compute_stats, detect_anomaly, check_all_pipelines


def _run(run_id: str, duration: float) -> PipelineRun:
    r = MagicMock(spec=PipelineRun)
    r.run_id = run_id
    r.finished_at = "2024-01-01T00:00:00Z"
    r.duration_seconds = duration
    return r


def _state_with(pipeline: str, durations: list[float]):
    from pipewatch.state import PipelineData
    ps = MagicMock()
    pd = MagicMock(spec=PipelineData)
    pd.runs = [_run(f"r{i}", d) for i, d in enumerate(durations)]
    ps.load.return_value = pd
    return ps


def test_compute_stats_none_when_insufficient():
    state = _state_with("pipe", [10.0])
    assert compute_stats(state, "pipe") is None


def test_compute_stats_returns_mean():
    state = _state_with("pipe", [10.0, 20.0, 30.0])
    stats = compute_stats(state, "pipe")
    assert stats is not None
    assert stats.mean_seconds == pytest.approx(20.0)
    assert stats.sample_size == 3


def test_compute_stats_stddev():
    state = _state_with("pipe", [10.0, 10.0, 10.0])
    stats = compute_stats(state, "pipe")
    assert stats.stddev_seconds == pytest.approx(0.0)


def test_detect_anomaly_none_when_zero_stddev():
    state = _state_with("pipe", [10.0, 10.0, 10.0])
    assert detect_anomaly(state, "pipe") is None


def test_detect_anomaly_none_when_within_threshold():
    state = _state_with("pipe", [10.0, 10.0, 10.0, 11.0])
    assert detect_anomaly(state, "pipe", z_threshold=2.5) is None


def test_detect_anomaly_returns_anomaly_on_spike():
    # mean=10, last run is 100 — big spike
    state = _state_with("pipe", [10.0, 10.0, 10.0, 10.0, 10.0, 100.0])
    anomaly = detect_anomaly(state, "pipe", z_threshold=2.5)
    assert anomaly is not None
    assert anomaly.pipeline == "pipe"
    assert anomaly.z_score > 2.5


def test_check_all_pipelines_aggregates():
    state = _state_with("pipe", [10.0, 10.0, 10.0, 10.0, 10.0, 100.0])
    anomalies = check_all_pipelines(state, ["pipe"], z_threshold=2.5)
    assert len(anomalies) == 1


def test_check_all_pipelines_empty_when_no_anomalies():
    state = _state_with("pipe", [10.0, 10.0, 10.0])
    anomalies = check_all_pipelines(state, ["pipe"])
    assert anomalies == []
