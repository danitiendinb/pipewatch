"""Tests for pipewatch.suspender."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.suspender import (
    active_suspensions,
    clear_suspension,
    is_suspended,
    load_suspension,
    suspend_pipeline,
)


def _utc(offset_hours: float = 0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=offset_hours)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_load_suspension_none_for_unknown(state_dir: str) -> None:
    assert load_suspension(state_dir, "pipe_a") is None


def test_suspend_pipeline_returns_future_expiry(state_dir: str) -> None:
    expiry = suspend_pipeline(state_dir, "pipe_a", hours=2)
    assert expiry > _utc()


def test_suspend_pipeline_persists(state_dir: str) -> None:
    suspend_pipeline(state_dir, "pipe_a", hours=1)
    loaded = load_suspension(state_dir, "pipe_a")
    assert loaded is not None
    assert loaded > _utc()


def test_is_suspended_true_within_window(state_dir: str) -> None:
    suspend_pipeline(state_dir, "pipe_a", hours=1)
    assert is_suspended(state_dir, "pipe_a") is True


def test_is_suspended_false_when_no_record(state_dir: str) -> None:
    assert is_suspended(state_dir, "pipe_x") is False


def test_is_suspended_false_after_expiry(state_dir: str) -> None:
    past = _utc(offset_hours=-1)
    path = Path(state_dir) / "pipe_a" / "suspend.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"until": past.isoformat(), "suspended_at": past.isoformat()}))
    assert is_suspended(state_dir, "pipe_a") is False


def test_is_suspended_cleans_up_expired_file(state_dir: str) -> None:
    past = _utc(offset_hours=-1)
    path = Path(state_dir) / "pipe_a" / "suspend.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"until": past.isoformat(), "suspended_at": past.isoformat()}))
    is_suspended(state_dir, "pipe_a")
    assert not path.exists()


def test_clear_suspension_removes_file(state_dir: str) -> None:
    suspend_pipeline(state_dir, "pipe_a", hours=4)
    clear_suspension(state_dir, "pipe_a")
    assert load_suspension(state_dir, "pipe_a") is None


def test_clear_suspension_noop_when_absent(state_dir: str) -> None:
    # Should not raise
    clear_suspension(state_dir, "pipe_z")


def test_active_suspensions_returns_suspended_only(state_dir: str) -> None:
    suspend_pipeline(state_dir, "pipe_a", hours=2)
    # pipe_b not suspended
    result = active_suspensions(state_dir, ["pipe_a", "pipe_b"])
    assert result == ["pipe_a"]


def test_active_suspensions_empty_when_none_suspended(state_dir: str) -> None:
    result = active_suspensions(state_dir, ["pipe_a", "pipe_b"])
    assert result == []
