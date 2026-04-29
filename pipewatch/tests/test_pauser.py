"""Unit tests for pipewatch.pauser."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.pauser import (
    clear_pause,
    is_paused,
    load_pause,
    pause_pipeline,
    paused_pipelines,
)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _utc(offset_hours: float = 0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=offset_hours)


def test_load_pause_none_for_unknown(state_dir: str) -> None:
    assert load_pause(state_dir, "pipe_a") is None


def test_pause_pipeline_returns_future_expiry(state_dir: str) -> None:
    expiry = pause_pipeline(state_dir, "pipe_a", hours=2.0)
    assert expiry > datetime.now(timezone.utc)


def test_pause_pipeline_persists(state_dir: str) -> None:
    pause_pipeline(state_dir, "pipe_a", hours=3.0)
    loaded = load_pause(state_dir, "pipe_a")
    assert loaded is not None
    assert loaded > datetime.now(timezone.utc)


def test_is_paused_false_when_no_file(state_dir: str) -> None:
    assert is_paused(state_dir, "pipe_a") is False


def test_is_paused_true_within_window(state_dir: str) -> None:
    pause_pipeline(state_dir, "pipe_a", hours=1.0)
    assert is_paused(state_dir, "pipe_a") is True


def test_is_paused_false_after_expiry(state_dir: str) -> None:
    past = _utc(offset_hours=-1).isoformat()
    p = Path(state_dir) / "pipe_a.pause.json"
    p.write_text(json.dumps({"pipeline": "pipe_a", "paused_at": past, "expires_at": past, "hours": 0.001}))
    assert is_paused(state_dir, "pipe_a") is False


def test_clear_pause_removes_file(state_dir: str) -> None:
    pause_pipeline(state_dir, "pipe_a", hours=1.0)
    clear_pause(state_dir, "pipe_a")
    assert load_pause(state_dir, "pipe_a") is None


def test_clear_pause_noop_when_missing(state_dir: str) -> None:
    clear_pause(state_dir, "pipe_a")  # should not raise


def test_paused_pipelines_returns_subset(state_dir: str) -> None:
    pause_pipeline(state_dir, "pipe_a", hours=1.0)
    result = paused_pipelines(state_dir, ["pipe_a", "pipe_b"])
    assert result == ["pipe_a"]


def test_paused_pipelines_empty_when_none_paused(state_dir: str) -> None:
    result = paused_pipelines(state_dir, ["pipe_a", "pipe_b"])
    assert result == []
