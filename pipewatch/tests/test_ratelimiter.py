"""Tests for pipewatch.ratelimiter."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.ratelimiter import (
    clear_ratelimit,
    is_rate_limited,
    load_last_alert,
    record_alert,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def _utc(**kw) -> datetime:
    return datetime.now(timezone.utc) + timedelta(**kw)


def test_load_last_alert_none_for_unknown(state_dir):
    assert load_last_alert(state_dir, "pipe1") is None


def test_record_and_load_alert(state_dir):
    record_alert(state_dir, "pipe1")
    result = load_last_alert(state_dir, "pipe1")
    assert result is not None
    assert isinstance(result, datetime)


def test_clear_ratelimit_removes_record(state_dir):
    record_alert(state_dir, "pipe1")
    clear_ratelimit(state_dir, "pipe1")
    assert load_last_alert(state_dir, "pipe1") is None


def test_clear_ratelimit_noop_when_missing(state_dir):
    clear_ratelimit(state_dir, "nonexistent")  # should not raise


def test_is_rate_limited_false_when_no_record(state_dir):
    assert is_rate_limited(state_dir, "pipe1", cooldown_minutes=30) is False


def test_is_rate_limited_true_within_cooldown(state_dir):
    record_alert(state_dir, "pipe1")
    assert is_rate_limited(state_dir, "pipe1", cooldown_minutes=30) is True


def test_is_rate_limited_false_after_cooldown(state_dir):
    past = _utc(minutes=-60)
    with patch("pipewatch.ratelimiter._now", return_value=past):
        record_alert(state_dir, "pipe1")
    assert is_rate_limited(state_dir, "pipe1", cooldown_minutes=30) is False


def test_multiple_pipelines_independent(state_dir):
    record_alert(state_dir, "pipe1")
    assert is_rate_limited(state_dir, "pipe2", cooldown_minutes=30) is False
