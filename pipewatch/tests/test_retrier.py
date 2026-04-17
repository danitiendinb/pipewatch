"""Tests for pipewatch.retrier."""
import pytest
from pathlib import Path
from pipewatch.retrier import (
    RetryPolicy, RetryRecord,
    load_retry, save_retry, clear_retry,
    should_retry, increment_retry,
)


@pytest.fixture
def state_dir(tmp_path):
    return str(tmp_path)


def test_load_retry_returns_default_for_unknown(state_dir):
    r = load_retry(state_dir, "pipe_a")
    assert r.pipeline == "pipe_a"
    assert r.attempt == 0
    assert r.last_retry_at is None


def test_save_and_load_retry(state_dir):
    rec = RetryRecord(pipeline="pipe_a", attempt=2, last_retry_at="2024-01-01T00:00:00")
    save_retry(state_dir, rec)
    loaded = load_retry(state_dir, "pipe_a")
    assert loaded.attempt == 2
    assert loaded.last_retry_at == "2024-01-01T00:00:00"


def test_clear_retry_removes_file(state_dir):
    rec = RetryRecord(pipeline="pipe_a", attempt=1)
    save_retry(state_dir, rec)
    clear_retry(state_dir, "pipe_a")
    r = load_retry(state_dir, "pipe_a")
    assert r.attempt == 0


def test_should_retry_true_below_max(state_dir):
    policy = RetryPolicy(max_retries=3)
    record = RetryRecord(pipeline="p", attempt=2)
    assert should_retry(record, policy) is True


def test_should_retry_false_at_max(state_dir):
    policy = RetryPolicy(max_retries=3)
    record = RetryRecord(pipeline="p", attempt=3)
    assert should_retry(record, policy) is False


def test_increment_retry_increases_attempt(state_dir):
    rec = increment_retry(state_dir, "pipe_a", "2024-06-01T10:00:00")
    assert rec.attempt == 1
    assert rec.last_retry_at == "2024-06-01T10:00:00"


def test_increment_retry_accumulates(state_dir):
    increment_retry(state_dir, "pipe_a", "2024-06-01T10:00:00")
    rec = increment_retry(state_dir, "pipe_a", "2024-06-01T10:01:00")
    assert rec.attempt == 2
