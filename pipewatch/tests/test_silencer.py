"""Unit tests for pipewatch.silencer."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.silencer import (
    clear_silence,
    is_silenced,
    set_silence,
    silence_path,
    silence_until,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def _future(hours: float = 1.0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=hours)


def _past(hours: float = 1.0) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours)


def test_is_silenced_false_when_no_file(state_dir):
    assert is_silenced(state_dir, "pipe-a") is False


def test_set_and_is_silenced(state_dir):
    set_silence(state_dir, "pipe-a", _future())
    assert is_silenced(state_dir, "pipe-a") is True


def test_is_silenced_false_after_expiry(state_dir):
    set_silence(state_dir, "pipe-a", _past())
    assert is_silenced(state_dir, "pipe-a") is False


def test_expired_file_removed(state_dir):
    set_silence(state_dir, "pipe-a", _past())
    is_silenced(state_dir, "pipe-a")
    assert not os.path.exists(silence_path(state_dir, "pipe-a"))


def test_clear_silence_removes_active(state_dir):
    set_silence(state_dir, "pipe-a", _future())
    clear_silence(state_dir, "pipe-a")
    assert is_silenced(state_dir, "pipe-a") is False


def test_clear_silence_noop_when_absent(state_dir):
    clear_silence(state_dir, "pipe-a")  # should not raise


def test_silence_until_returns_datetime(state_dir):
    until = _future(2)
    set_silence(state_dir, "pipe-a", until)
    result = silence_until(state_dir, "pipe-a")
    assert result is not None
    assert abs((result - until).total_seconds()) < 1


def test_silence_until_returns_none_when_expired(state_dir):
    set_silence(state_dir, "pipe-a", _past())
    assert silence_until(state_dir, "pipe-a") is None
