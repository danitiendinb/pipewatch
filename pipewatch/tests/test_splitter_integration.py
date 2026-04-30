"""Integration tests for pipewatch.splitter using a real PipelineState store."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone, timedelta

from pipewatch.state import PipelineState
from pipewatch.splitter import split_runs


@pytest.fixture()
def store(tmp_path):
    return PipelineState(str(tmp_path))


def _utc(**kwargs) -> datetime:
    return datetime.now(timezone.utc) - timedelta(**kwargs)


def _record(store: PipelineState, pipeline: str, status: str, hours_ago: float):
    run_id = f"run-{hours_ago}"
    started = _utc(hours=hours_ago)
    finished = started + timedelta(minutes=1)
    store.record_start(pipeline, run_id, started.isoformat())
    if status == "ok":
        store.record_success(pipeline, run_id, finished.isoformat())
    else:
        store.record_failure(pipeline, run_id, "err", finished.isoformat())


def test_recent_run_appears_in_bucket(store):
    _record(store, "pipe", "ok", hours_ago=1)
    buckets = split_runs(store, "pipe", days=2, granularity="day")
    total = sum(b.total for b in buckets)
    assert total == 1


def test_old_run_not_included(store):
    _record(store, "pipe", "failure", hours_ago=24 * 8)
    buckets = split_runs(store, "pipe", days=7, granularity="day")
    assert sum(b.total for b in buckets) == 0


def test_multiple_runs_distributed(store):
    _record(store, "pipe", "ok", hours_ago=1)
    _record(store, "pipe", "failure", hours_ago=2)
    _record(store, "pipe", "ok", hours_ago=25)
    buckets = split_runs(store, "pipe", days=3, granularity="day")
    total = sum(b.total for b in buckets)
    assert total == 3


def test_hourly_granularity_places_run_correctly(store):
    _record(store, "pipe", "ok", hours_ago=0.5)
    buckets = split_runs(store, "pipe", days=1, granularity="hour")
    total = sum(b.total for b in buckets)
    assert total == 1


def test_unknown_pipeline_returns_empty_buckets(store):
    buckets = split_runs(store, "ghost", days=3, granularity="day")
    assert all(b.total == 0 for b in buckets)
