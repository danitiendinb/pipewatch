"""Integration tests for fencer: exercises save/load/is_fenced together."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.fencer import (
    FenceWindow,
    active_fences,
    clear_fence,
    is_fenced,
    save_fence,
)


def _utc(offset_hours: float = 0.0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=offset_hours)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_fence_blocks_during_window_then_expires(state_dir: str) -> None:
    now = _utc()
    window = FenceWindow(
        start_iso=(now - timedelta(minutes=30)).isoformat(),
        end_iso=(now + timedelta(minutes=30)).isoformat(),
    )
    save_fence(state_dir, "etl", window)
    assert is_fenced(state_dir, "etl") is True

    past_end = now + timedelta(hours=1)
    assert is_fenced(state_dir, "etl", at=past_end) is False


def test_multiple_pipelines_independent(state_dir: str) -> None:
    save_fence(state_dir, "pipe_a", FenceWindow(
        start_iso=_utc(-1).isoformat(),
        end_iso=_utc(1).isoformat(),
    ))
    # pipe_b has no fence
    assert is_fenced(state_dir, "pipe_a") is True
    assert is_fenced(state_dir, "pipe_b") is False


def test_clear_fence_makes_pipeline_not_fenced(state_dir: str) -> None:
    save_fence(state_dir, "pipe_a", FenceWindow(
        start_iso=_utc(-1).isoformat(),
        end_iso=_utc(1).isoformat(),
    ))
    assert is_fenced(state_dir, "pipe_a") is True
    clear_fence(state_dir, "pipe_a")
    assert is_fenced(state_dir, "pipe_a") is False


def test_active_fences_only_current(state_dir: str) -> None:
    save_fence(state_dir, "live", FenceWindow(
        start_iso=_utc(-1).isoformat(),
        end_iso=_utc(2).isoformat(),
    ))
    save_fence(state_dir, "done", FenceWindow(
        start_iso=_utc(-5).isoformat(),
        end_iso=_utc(-3).isoformat(),
    ))
    result = active_fences(state_dir, ["live", "done", "unfenced"])
    assert "live" in result
    assert "done" not in result
    assert "unfenced" not in result
