"""Tests for pipewatch.lockout."""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.lockout import (
    load_lockout,
    set_lockout,
    clear_lockout,
    is_locked_out,
    locked_out_pipelines,
)


def _utc(offset_minutes: int = 0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_load_lockout_none_for_unknown(state_dir: str) -> None:
    assert load_lockout(state_dir, "pipe_a") is None


def test_set_lockout_returns_future_datetime(state_dir: str) -> None:
    expires = set_lockout(state_dir, "pipe_a", duration_minutes=15)
    assert expires > _utc()


def test_set_lockout_persists(state_dir: str) -> None:
    set_lockout(state_dir, "pipe_a", duration_minutes=60)
    loaded = load_lockout(state_dir, "pipe_a")
    assert loaded is not None
    assert loaded > _utc()


def test_set_lockout_stores_duration(state_dir: str) -> None:
    set_lockout(state_dir, "pipe_a", duration_minutes=45)
    path = Path(state_dir) / "pipe_a.lockout.json"
    data = json.loads(path.read_text())
    assert data["duration_minutes"] == 45


def test_clear_lockout_removes_file(state_dir: str) -> None:
    set_lockout(state_dir, "pipe_a")
    clear_lockout(state_dir, "pipe_a")
    assert load_lockout(state_dir, "pipe_a") is None


def test_clear_lockout_no_error_when_missing(state_dir: str) -> None:
    clear_lockout(state_dir, "ghost")  # should not raise


def test_is_locked_out_false_when_no_file(state_dir: str) -> None:
    assert is_locked_out(state_dir, "pipe_a") is False


def test_is_locked_out_true_within_window(state_dir: str) -> None:
    set_lockout(state_dir, "pipe_a", duration_minutes=30)
    assert is_locked_out(state_dir, "pipe_a") is True


def test_is_locked_out_false_after_expiry(state_dir: str) -> None:
    # Write a lockout that expired in the past
    path = Path(state_dir) / "pipe_a.lockout.json"
    path.write_text(json.dumps({
        "pipeline": "pipe_a",
        "locked_at": _utc(-60).isoformat(),
        "expires_at": _utc(-1).isoformat(),
        "duration_minutes": 30,
    }))
    assert is_locked_out(state_dir, "pipe_a") is False
    # File should be cleaned up automatically
    assert not path.exists()


def test_locked_out_pipelines_filters_correctly(state_dir: str) -> None:
    set_lockout(state_dir, "pipe_a", duration_minutes=30)
    result = locked_out_pipelines(state_dir, ["pipe_a", "pipe_b"])
    assert result == ["pipe_a"]


def test_locked_out_pipelines_empty_when_none_locked(state_dir: str) -> None:
    result = locked_out_pipelines(state_dir, ["pipe_a", "pipe_b"])
    assert result == []
