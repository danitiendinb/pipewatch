"""Tests for pipewatch.scheduler."""
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

from pipewatch.scheduler import next_run, last_expected_run, is_overdue, overdue_pipelines

EVERY_HOUR = "0 * * * *"


def _utc(year, month, day, hour=0, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def test_next_run_returns_future_datetime():
    after = _utc(2024, 1, 1, 12, 0)
    result = next_run(EVERY_HOUR, after=after)
    assert result > after


def test_next_run_on_the_hour():
    after = _utc(2024, 1, 1, 12, 0)
    result = next_run(EVERY_HOUR, after=after)
    assert result.minute == 0
    assert result.hour == 13


def test_last_expected_run_before_now():
    before = _utc(2024, 1, 1, 12, 30)
    result = last_expected_run(EVERY_HOUR, before=before)
    assert result <= before
    assert result.minute == 0
    assert result.hour == 12


def test_is_overdue_no_last_success():
    now = _utc(2024, 1, 1, 13, 5)
    assert is_overdue(EVERY_HOUR, None, now=now) is True


def test_is_overdue_recent_success():
    now = _utc(2024, 1, 1, 13, 5)
    last_ok = _utc(2024, 1, 1, 13, 1).isoformat()
    assert is_overdue(EVERY_HOUR, last_ok, now=now) is False


def test_is_overdue_stale_success():
    now = _utc(2024, 1, 1, 14, 10)
    last_ok = _utc(2024, 1, 1, 12, 55).isoformat()  # before 14:00 window
    assert is_overdue(EVERY_HOUR, last_ok, now=now) is True


def test_overdue_pipelines_returns_names():
    configs = [
        {"name": "pipe_a", "schedule": EVERY_HOUR},
        {"name": "pipe_b", "schedule": EVERY_HOUR},
    ]
    now = _utc(2024, 1, 1, 15, 5)
    state_a = MagicMock(last_success=_utc(2024, 1, 1, 15, 1).isoformat())
    state_b = MagicMock(last_success=_utc(2024, 1, 1, 13, 1).isoformat())
    result = overdue_pipelines(configs, {"pipe_a": state_a, "pipe_b": state_b}, now=now)
    assert "pipe_b" in result
    assert "pipe_a" not in result


def test_overdue_pipelines_skips_no_schedule():
    configs = [{"name": "pipe_c"}]
    result = overdue_pipelines(configs, {}, now=_utc(2024, 1, 1, 10, 0))
    assert result == []
