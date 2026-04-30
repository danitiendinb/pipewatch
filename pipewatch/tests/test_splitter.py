"""Unit tests for pipewatch.splitter."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

from pipewatch.splitter import (
    SplitBucket,
    _bucket_label,
    format_split_row,
    split_runs,
)


def _utc(offset_hours: float = 0) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=offset_hours)


def _run(status: str, hours_ago: float):
    r = MagicMock()
    r.status = status
    r.started_at = (_utc(hours_ago)).isoformat().replace("+00:00", "Z")
    return r


def _make_state(runs):
    ps = MagicMock()
    ps.runs = runs
    state = MagicMock()
    state.load.return_value = ps
    return state


class TestSplitBucket:
    def test_total_counts_all_runs(self):
        b = SplitBucket(
            label="2024-01-01",
            start=_utc(48),
            end=_utc(24),
        )
        b.runs = [_run("ok", 30), _run("failure", 32)]
        assert b.total == 2

    def test_failures_counts_only_failures(self):
        b = SplitBucket(label="x", start=_utc(48), end=_utc(24))
        b.runs = [_run("ok", 30), _run("failure", 32), _run("failure", 33)]
        assert b.failures == 2

    def test_success_rate_none_when_empty(self):
        b = SplitBucket(label="x", start=_utc(48), end=_utc(24))
        assert b.success_rate is None

    def test_success_rate_all_ok(self):
        b = SplitBucket(label="x", start=_utc(48), end=_utc(24))
        b.runs = [_run("ok", 30), _run("ok", 31)]
        assert b.success_rate == pytest.approx(1.0)

    def test_success_rate_mixed(self):
        b = SplitBucket(label="x", start=_utc(48), end=_utc(24))
        b.runs = [_run("ok", 30), _run("failure", 31)]
        assert b.success_rate == pytest.approx(0.5)


class TestBucketLabel:
    def test_day_label(self):
        dt = datetime(2024, 6, 15, 10, 30, tzinfo=timezone.utc)
        assert _bucket_label(dt, "day") == "2024-06-15"

    def test_hour_label(self):
        dt = datetime(2024, 6, 15, 10, 30, tzinfo=timezone.utc)
        assert _bucket_label(dt, "hour") == "2024-06-15T10:00Z"


class TestSplitRuns:
    def test_invalid_granularity_raises(self):
        state = _make_state([])
        with pytest.raises(ValueError):
            split_runs(state, "pipe", granularity="week")

    def test_returns_correct_bucket_count_days(self):
        state = _make_state([])
        buckets = split_runs(state, "pipe", days=7, granularity="day")
        assert len(buckets) == 7

    def test_run_placed_in_correct_bucket(self):
        runs = [_run("ok", 2)]  # 2 hours ago → today
        state = _make_state(runs)
        buckets = split_runs(state, "pipe", days=3, granularity="day")
        totals = sum(b.total for b in buckets)
        assert totals == 1

    def test_old_run_excluded(self):
        runs = [_run("failure", 24 * 10)]  # 10 days ago
        state = _make_state(runs)
        buckets = split_runs(state, "pipe", days=7, granularity="day")
        assert sum(b.total for b in buckets) == 0


def test_format_split_row_contains_label():
    b = SplitBucket(label="2024-01-01", start=_utc(48), end=_utc(24))
    b.runs = [_run("ok", 30)]
    row = format_split_row(b)
    assert "2024-01-01" in row
    assert "100%" in row
