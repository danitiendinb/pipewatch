"""Unit tests for pipewatch.watchdog"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from pipewatch.state import PipelineRun, PipelineState
from pipewatch.watchdog import StaleReport, hours_since, is_stale, stale_pipelines


def _utc(**kw) -> str:
    return (datetime.now(timezone.utc) - timedelta(**kw)).isoformat()


def _run(finished_at: str) -> PipelineRun:
    r = MagicMock(spec=PipelineRun)
    r.started_at = finished_at
    r.finished_at = finished_at
    return r


def test_hours_since_recent():
    ts = _utc(minutes=30)
    assert hours_since(ts) < 1.0


def test_hours_since_old():
    ts = _utc(hours=48)
    assert hours_since(ts) >= 47.9


def test_is_stale_no_runs():
    state = MagicMock(spec=PipelineState)
    state.runs = []
    assert is_stale(state, threshold_hours=24) is True


def test_is_stale_recent_run():
    state = MagicMock(spec=PipelineState)
    state.runs = [_run(_utc(hours=1))]
    assert is_stale(state, threshold_hours=24) is False


def test_is_stale_old_run():
    state = MagicMock(spec=PipelineState)
    state.runs = [_run(_utc(hours=25))]
    assert is_stale(state, threshold_hours=24) is True


def test_stale_pipelines_returns_only_stale():
    config = MagicMock()
    pc_a = MagicMock()
    pc_a.name = "alpha"
    pc_b = MagicMock()
    pc_b.name = "beta"
    config.pipelines = [pc_a, pc_b]

    store = MagicMock()
    state_a = MagicMock(spec=PipelineState)
    state_a.runs = [_run(_utc(hours=2))]  # fresh
    state_b = MagicMock(spec=PipelineState)
    state_b.runs = [_run(_utc(hours=30))]  # stale

    store.load.side_effect = lambda name: {"alpha": state_a, "beta": state_b}[name]

    reports = stale_pipelines(config, store, threshold_hours=24)
    assert len(reports) == 1
    assert reports[0].pipeline == "beta"


def test_stale_pipelines_never_reported():
    config = MagicMock()
    pc = MagicMock()
    pc.name = "ghost"
    config.pipelines = [pc]

    store = MagicMock()
    state = MagicMock(spec=PipelineState)
    state.runs = []
    store.load.return_value = state

    reports = stale_pipelines(config, store, threshold_hours=1)
    assert reports[0].last_seen is None
    assert reports[0].hours_silent is None
