"""Tests for pipewatch.pruner."""
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

from pipewatch.pruner import cutoff_datetime, prune_runs, prune_pipeline, prune_all
from pipewatch.state import PipelineRun


def _utc(days_ago: float = 0) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return dt.isoformat()


def _run(days_ago: float) -> PipelineRun:
    ts = _utc(days_ago)
    return PipelineRun(started_at=ts, finished_at=ts, success=True, message=None, duration_seconds=0.0)


def test_cutoff_datetime_is_n_days_before_now():
    now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    cutoff = cutoff_datetime(7, now=now)
    assert cutoff == datetime(2024, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


def test_prune_runs_keeps_recent():
    runs = [_run(1), _run(3)]
    kept = prune_runs(runs, days=7)
    assert len(kept) == 2


def test_prune_runs_removes_old():
    runs = [_run(1), _run(10), _run(20)]
    kept = prune_runs(runs, days=7)
    assert len(kept) == 1
    assert kept[0] is runs[0]


def test_prune_runs_empty_list():
    assert prune_runs([], days=7) == []


def test_prune_pipeline_returns_removed_count():
    from pipewatch.state import PipelineHistory
    history = PipelineHistory(runs=[_run(1), _run(30)], consecutive_failures=0)
    store = MagicMock()
    store.load.return_value = history
    removed = prune_pipeline(store, "etl", days=7)
    assert removed == 1
    store.save.assert_called_once()


def test_prune_pipeline_no_removal_skips_save():
    from pipewatch.state import PipelineHistory
    history = PipelineHistory(runs=[_run(1)], consecutive_failures=0)
    store = MagicMock()
    store.load.return_value = history
    removed = prune_pipeline(store, "etl", days=7)
    assert removed == 0
    store.save.assert_not_called()


def test_prune_all_returns_dict():
    from pipewatch.state import PipelineHistory
    store = MagicMock()
    store.load.return_value = PipelineHistory(runs=[_run(20)], consecutive_failures=0)
    result = prune_all(store, ["a", "b"], days=7)
    assert set(result.keys()) == {"a", "b"}
    assert result["a"] == 1
