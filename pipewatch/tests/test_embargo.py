"""Tests for pipewatch.embargo."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from pathlib import Path

from pipewatch.embargo import (
    EmbargoWindow,
    load_embargo,
    save_embargo,
    clear_embargo,
    is_embargoed,
    embargoed_pipelines,
)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _utc(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 6, 15, hour, minute, 0, tzinfo=timezone.utc)


# --- load / save / clear ---

def test_load_embargo_none_for_unknown(state_dir):
    assert load_embargo(state_dir, "pipe_a") is None


def test_save_and_load_embargo(state_dir):
    w = EmbargoWindow(start_time="02:00", end_time="04:00", reason="maint")
    save_embargo(state_dir, "pipe_a", w)
    loaded = load_embargo(state_dir, "pipe_a")
    assert loaded is not None
    assert loaded.start_time == "02:00"
    assert loaded.end_time == "04:00"
    assert loaded.reason == "maint"


def test_clear_embargo_removes_file(state_dir):
    w = EmbargoWindow(start_time="01:00", end_time="03:00")
    save_embargo(state_dir, "pipe_a", w)
    clear_embargo(state_dir, "pipe_a")
    assert load_embargo(state_dir, "pipe_a") is None


def test_clear_embargo_noop_when_missing(state_dir):
    # Should not raise
    clear_embargo(state_dir, "ghost")


# --- is_embargoed ---

def test_is_embargoed_false_when_no_window(state_dir):
    assert is_embargoed(state_dir, "pipe_a", at=_utc(3)) is False


def test_is_embargoed_true_within_window(state_dir):
    w = EmbargoWindow(start_time="02:00", end_time="04:00")
    save_embargo(state_dir, "pipe_a", w)
    assert is_embargoed(state_dir, "pipe_a", at=_utc(3)) is True


def test_is_embargoed_false_before_window(state_dir):
    w = EmbargoWindow(start_time="02:00", end_time="04:00")
    save_embargo(state_dir, "pipe_a", w)
    assert is_embargoed(state_dir, "pipe_a", at=_utc(1, 59)) is False


def test_is_embargoed_false_at_end_boundary(state_dir):
    w = EmbargoWindow(start_time="02:00", end_time="04:00")
    save_embargo(state_dir, "pipe_a", w)
    assert is_embargoed(state_dir, "pipe_a", at=_utc(4, 0)) is False


def test_is_embargoed_overnight_window_before_midnight(state_dir):
    w = EmbargoWindow(start_time="23:00", end_time="01:00")
    save_embargo(state_dir, "pipe_a", w)
    assert is_embargoed(state_dir, "pipe_a", at=_utc(23, 30)) is True


def test_is_embargoed_overnight_window_after_midnight(state_dir):
    w = EmbargoWindow(start_time="23:00", end_time="01:00")
    save_embargo(state_dir, "pipe_a", w)
    assert is_embargoed(state_dir, "pipe_a", at=_utc(0, 30)) is True


# --- embargoed_pipelines ---

def test_embargoed_pipelines_returns_only_active(state_dir):
    save_embargo(state_dir, "pipe_a", EmbargoWindow("02:00", "04:00"))
    save_embargo(state_dir, "pipe_b", EmbargoWindow("10:00", "12:00"))
    result = embargoed_pipelines(state_dir, ["pipe_a", "pipe_b", "pipe_c"], at=_utc(3))
    assert result == ["pipe_a"]


def test_embargoed_pipelines_empty_when_none_active(state_dir):
    result = embargoed_pipelines(state_dir, ["pipe_a", "pipe_b"], at=_utc(12))
    assert result == []
