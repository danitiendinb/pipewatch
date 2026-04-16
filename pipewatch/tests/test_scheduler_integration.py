"""Integration tests: scheduler + state store interaction."""
import os
import tempfile
from datetime import datetime, timezone

import pytest

from pipewatch.state import PipelineState
from pipewatch.scheduler import overdue_pipelines, is_overdue

EVERY_MIN = "* * * * *"


@pytest.fixture()
def store(tmp_path):
    from pipewatch.state import StateStore
    return StateStore(str(tmp_path))


def _utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


def test_overdue_after_recorded_failure(store):
    store.record_failure("etl", "something broke")
    state = store.load("etl")
    now = _utc(2024, 6, 1, 10, 5)
    # last_success is None after only failures
    assert is_overdue(EVERY_MIN, state.last_success, now=now) is True


def test_not_overdue_after_recent_success(store):
    store.record_success("etl")
    state = store.load("etl")
    now = datetime.now(timezone.utc)
    # success was just recorded, should not be overdue
    assert is_overdue(EVERY_MIN, state.last_success, now=now) is False


def test_overdue_pipelines_uses_store(store):
    store.record_success("fresh_pipe")
    store.record_failure("stale_pipe", "err")

    configs = [
        {"name": "fresh_pipe", "schedule": EVERY_MIN},
        {"name": "stale_pipe", "schedule": EVERY_MIN},
    ]
    states = {
        "fresh_pipe": store.load("fresh_pipe"),
        "stale_pipe": store.load("stale_pipe"),
    }
    now = datetime.now(timezone.utc)
    result = overdue_pipelines(configs, states, now=now)
    assert "stale_pipe" in result
    assert "fresh_pipe" not in result
