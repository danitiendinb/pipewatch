"""Unit tests for pipewatch.fencer."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.fencer import (
    FenceWindow,
    active_fences,
    clear_fence,
    is_fenced,
    load_fence,
    save_fence,
)


def _utc(offset_hours: float = 0.0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=offset_hours)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_load_fence_none_for_unknown(state_dir: str) -> None:
    assert load_fence(state_dir, "pipe_a") is None


def test_save_and_load_fence(state_dir: str) -> None:
    window = FenceWindow(
        start_iso=_utc(-1).isoformat(),
        end_iso=_utc(1).isoformat(),
        reason="maintenance",
    )
    save_fence(state_dir, "pipe_a", window)
    loaded = load_fence(state_dir, "pipe_a")
    assert loaded is not None
    assert loaded.reason == "maintenance"


def test_is_fenced_false_when_no_file(state_dir: str) -> None:
    assert is_fenced(state_dir, "pipe_a") is False


def test_is_fenced_true_during_window(state_dir: str) -> None:
    window = FenceWindow(
        start_iso=_utc(-1).isoformat(),
        end_iso=_utc(1).isoformat(),
    )
    save_fence(state_dir, "pipe_a", window)
    assert is_fenced(state_dir, "pipe_a") is True


def test_is_fenced_false_after_window(state_dir: str) -> None:
    window = FenceWindow(
        start_iso=_utc(-3).isoformat(),
        end_iso=_utc(-1).isoformat(),
    )
    save_fence(state_dir, "pipe_a", window)
    assert is_fenced(state_dir, "pipe_a") is False


def test_is_fenced_false_before_window(state_dir: str) -> None:
    window = FenceWindow(
        start_iso=_utc(1).isoformat(),
        end_iso=_utc(3).isoformat(),
    )
    save_fence(state_dir, "pipe_a", window)
    assert is_fenced(state_dir, "pipe_a") is False


def test_clear_fence_removes_file(state_dir: str) -> None:
    window = FenceWindow(
        start_iso=_utc(-1).isoformat(),
        end_iso=_utc(1).isoformat(),
    )
    save_fence(state_dir, "pipe_a", window)
    clear_fence(state_dir, "pipe_a")
    assert load_fence(state_dir, "pipe_a") is None


def test_active_fences_returns_fenced_pipelines(state_dir: str) -> None:
    save_fence(state_dir, "active", FenceWindow(
        start_iso=_utc(-1).isoformat(), end_iso=_utc(1).isoformat()
    ))
    save_fence(state_dir, "expired", FenceWindow(
        start_iso=_utc(-3).isoformat(), end_iso=_utc(-2).isoformat()
    ))
    result = active_fences(state_dir, ["active", "expired", "none"])
    assert result == ["active"]
