"""Unit tests for pipewatch.throttler."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from pipewatch.throttler import (
    clear_throttle,
    is_throttled,
    load_last_check,
    record_check,
    throttled_pipelines,
)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_load_last_check_none_for_unknown(state_dir: str) -> None:
    assert load_last_check(state_dir, "pipe_a") is None


def test_record_check_returns_datetime(state_dir: str) -> None:
    from datetime import datetime
    ts = record_check(state_dir, "pipe_a")
    assert isinstance(ts, datetime)


def test_record_and_load_check(state_dir: str) -> None:
    ts = record_check(state_dir, "pipe_a")
    loaded = load_last_check(state_dir, "pipe_a")
    assert loaded is not None
    assert abs((loaded - ts).total_seconds()) < 1


def test_is_throttled_false_when_no_record(state_dir: str) -> None:
    assert is_throttled(state_dir, "pipe_a", 60) is False


def test_is_throttled_true_immediately_after_check(state_dir: str) -> None:
    record_check(state_dir, "pipe_a")
    assert is_throttled(state_dir, "pipe_a", 60) is True


def test_is_throttled_false_when_interval_zero(state_dir: str) -> None:
    record_check(state_dir, "pipe_a")
    assert is_throttled(state_dir, "pipe_a", 0) is False


def test_clear_throttle_removes_record(state_dir: str) -> None:
    record_check(state_dir, "pipe_a")
    clear_throttle(state_dir, "pipe_a")
    assert load_last_check(state_dir, "pipe_a") is None


def test_clear_throttle_noop_when_no_record(state_dir: str) -> None:
    clear_throttle(state_dir, "pipe_a")  # should not raise


def test_throttled_pipelines_returns_subset(state_dir: str) -> None:
    record_check(state_dir, "pipe_a")
    result = throttled_pipelines(state_dir, ["pipe_a", "pipe_b"], 60)
    assert result == ["pipe_a"]
