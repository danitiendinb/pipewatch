"""Integration tests for the batch tracker."""
from __future__ import annotations

import pytest

from pipewatch.batcher import (
    clear_batch,
    create_batch,
    load_batch,
    record_batch_result,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def test_full_batch_lifecycle(state_dir):
    """Create → record all → verify healthy."""
    create_batch(state_dir, "run1", ["ingest", "transform", "load"])
    for p in ["ingest", "transform", "load"]:
        record_batch_result(state_dir, "run1", p, "ok")
    record = load_batch(state_dir, "run1")
    assert record.healthy is True
    assert record.complete is True
    assert record.failed == 0


def test_partial_failure_marks_batch_unhealthy(state_dir):
    create_batch(state_dir, "run2", ["ingest", "transform"])
    record_batch_result(state_dir, "run2", "ingest", "ok")
    record_batch_result(state_dir, "run2", "transform", "fail")
    record = load_batch(state_dir, "run2")
    assert record.healthy is False
    assert record.failed == 1


def test_multiple_batches_independent(state_dir):
    create_batch(state_dir, "a", ["p1"])
    create_batch(state_dir, "b", ["p2"])
    record_batch_result(state_dir, "a", "p1", "ok")
    a = load_batch(state_dir, "a")
    b = load_batch(state_dir, "b")
    assert a.passed == 1
    assert b.pending == 1


def test_clear_then_recreate(state_dir):
    create_batch(state_dir, "c", ["p1", "p2"])
    record_batch_result(state_dir, "c", "p1", "fail")
    clear_batch(state_dir, "c")
    assert load_batch(state_dir, "c") is None
    create_batch(state_dir, "c", ["p1"])
    record = load_batch(state_dir, "c")
    assert record.pending == 1
    assert record.failed == 0
