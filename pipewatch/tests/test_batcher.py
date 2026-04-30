"""Unit tests for pipewatch.batcher."""
from __future__ import annotations

import pytest

from pipewatch.batcher import (
    BatchEntry,
    BatchRecord,
    clear_batch,
    create_batch,
    load_batch,
    record_batch_result,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def test_load_batch_none_for_unknown(state_dir):
    assert load_batch(state_dir, "missing") is None


def test_create_batch_returns_record(state_dir):
    record = create_batch(state_dir, "b1", ["pipe_a", "pipe_b"])
    assert record.batch_id == "b1"
    assert record.total == 2
    assert record.pending == 2
    assert record.passed == 0


def test_create_batch_persists(state_dir):
    create_batch(state_dir, "b2", ["pipe_a"])
    loaded = load_batch(state_dir, "b2")
    assert loaded is not None
    assert loaded.batch_id == "b2"


def test_record_batch_result_updates_entry(state_dir):
    create_batch(state_dir, "b3", ["pipe_a", "pipe_b"])
    result = record_batch_result(state_dir, "b3", "pipe_a", "ok")
    assert result is not None
    assert result.passed == 1
    assert result.pending == 1


def test_record_batch_result_returns_none_for_missing_batch(state_dir):
    result = record_batch_result(state_dir, "nope", "pipe_a", "ok")
    assert result is None


def test_batch_complete_when_no_pending(state_dir):
    create_batch(state_dir, "b4", ["pipe_a"])
    record_batch_result(state_dir, "b4", "pipe_a", "ok")
    record = load_batch(state_dir, "b4")
    assert record.complete is True


def test_batch_healthy_only_when_complete_and_no_failures(state_dir):
    create_batch(state_dir, "b5", ["pipe_a", "pipe_b"])
    record_batch_result(state_dir, "b5", "pipe_a", "ok")
    record_batch_result(state_dir, "b5", "pipe_b", "fail")
    record = load_batch(state_dir, "b5")
    assert record.complete is True
    assert record.healthy is False


def test_clear_batch_removes_file(state_dir):
    create_batch(state_dir, "b6", ["pipe_a"])
    clear_batch(state_dir, "b6")
    assert load_batch(state_dir, "b6") is None


def test_clear_batch_noop_when_missing(state_dir):
    clear_batch(state_dir, "ghost")  # should not raise


def test_batch_record_entry_recorded_at_set(state_dir):
    create_batch(state_dir, "b7", ["pipe_a"])
    record_batch_result(state_dir, "b7", "pipe_a", "fail")
    record = load_batch(state_dir, "b7")
    assert record.entries[0].recorded_at is not None
