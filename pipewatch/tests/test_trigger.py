"""Unit tests for pipewatch.trigger."""
from __future__ import annotations

import pytest
from pathlib import Path

from pipewatch.trigger import (
    load_trigger,
    set_trigger,
    clear_trigger,
    pending_triggers,
)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_load_trigger_none_for_unknown(state_dir):
    assert load_trigger(state_dir, "pipe-a") is None


def test_set_trigger_returns_record(state_dir):
    rec = set_trigger(state_dir, "pipe-a", "manual")
    assert rec.pipeline == "pipe-a"
    assert rec.reason == "manual"
    assert rec.triggered_by == "user"


def test_set_trigger_persists(state_dir):
    set_trigger(state_dir, "pipe-a", "scheduled", triggered_by="cron")
    rec = load_trigger(state_dir, "pipe-a")
    assert rec is not None
    assert rec.reason == "scheduled"
    assert rec.triggered_by == "cron"


def test_set_trigger_overwrites_existing(state_dir):
    set_trigger(state_dir, "pipe-a", "first")
    set_trigger(state_dir, "pipe-a", "second")
    rec = load_trigger(state_dir, "pipe-a")
    assert rec.reason == "second"


def test_clear_trigger_removes_record(state_dir):
    set_trigger(state_dir, "pipe-a", "manual")
    clear_trigger(state_dir, "pipe-a")
    assert load_trigger(state_dir, "pipe-a") is None


def test_clear_trigger_noop_when_absent(state_dir):
    clear_trigger(state_dir, "pipe-a")  # should not raise


def test_pending_triggers_returns_only_set(state_dir):
    set_trigger(state_dir, "pipe-a", "manual")
    result = pending_triggers(state_dir, ["pipe-a", "pipe-b"])
    assert len(result) == 1
    assert result[0].pipeline == "pipe-a"


def test_pending_triggers_empty_when_none_set(state_dir):
    result = pending_triggers(state_dir, ["pipe-a", "pipe-b"])
    assert result == []
