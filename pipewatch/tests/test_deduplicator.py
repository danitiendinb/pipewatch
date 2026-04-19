"""Tests for pipewatch.deduplicator."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
from pathlib import Path

from pipewatch.deduplicator import (
    is_duplicate,
    record_sent,
    clear_dedup,
    load_dedup,
    _fingerprint,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def _utc(offset_minutes: int = 0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)


def test_fingerprint_is_deterministic():
    assert _fingerprint("pipe", "msg") == _fingerprint("pipe", "msg")


def test_fingerprint_differs_by_message():
    assert _fingerprint("pipe", "a") != _fingerprint("pipe", "b")


def test_is_duplicate_false_when_no_record(state_dir):
    assert is_duplicate(state_dir, "pipe", "some alert") is False


def test_record_sent_creates_entry(state_dir):
    record_sent(state_dir, "pipe", "alert msg")
    data = load_dedup(state_dir, "pipe")
    assert len(data) == 1


def test_is_duplicate_true_within_window(state_dir):
    record_sent(state_dir, "pipe", "alert msg")
    assert is_duplicate(state_dir, "pipe", "alert msg", window_minutes=60) is True


def test_is_duplicate_false_after_window(state_dir):
    record_sent(state_dir, "pipe", "alert msg")
    future = _utc(offset_minutes=120)
    with patch("pipewatch.deduplicator._now", return_value=future):
        assert is_duplicate(state_dir, "pipe", "alert msg", window_minutes=60) is False


def test_different_messages_not_duplicate(state_dir):
    record_sent(state_dir, "pipe", "alert msg")
    assert is_duplicate(state_dir, "pipe", "different msg") is False


def test_clear_dedup_removes_file(state_dir):
    record_sent(state_dir, "pipe", "alert msg")
    clear_dedup(state_dir, "pipe")
    assert load_dedup(state_dir, "pipe") == {}


def test_record_sent_returns_fingerprint(state_dir):
    fp = record_sent(state_dir, "pipe", "msg")
    assert isinstance(fp, str) and len(fp) == 40


def test_multiple_pipelines_independent(state_dir):
    record_sent(state_dir, "pipe_a", "alert")
    assert is_duplicate(state_dir, "pipe_b", "alert") is False
