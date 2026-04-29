"""Tests for pipewatch.capper."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.capper import (
    CapPolicy,
    clear_cap_policy,
    evaluate_cap,
    load_cap_policy,
    save_cap_policy,
)
from pipewatch.state import PipelineState
from pipewatch.tests.test_state import store  # reuse fixture


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _utc(offset_hours: float = 0.0) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=offset_hours)).isoformat()


# ── policy persistence ────────────────────────────────────────────────────────

def test_load_cap_policy_none_for_unknown(state_dir: str) -> None:
    assert load_cap_policy(state_dir, "pipe") is None


def test_save_and_load_cap_policy(state_dir: str) -> None:
    policy = CapPolicy(max_runs=50, window_hours=12)
    save_cap_policy(state_dir, "pipe", policy)
    loaded = load_cap_policy(state_dir, "pipe")
    assert loaded is not None
    assert loaded.max_runs == 50
    assert loaded.window_hours == 12


def test_clear_cap_policy_removes_file(state_dir: str) -> None:
    save_cap_policy(state_dir, "pipe", CapPolicy())
    clear_cap_policy(state_dir, "pipe")
    assert load_cap_policy(state_dir, "pipe") is None


# ── evaluate_cap ──────────────────────────────────────────────────────────────

def test_evaluate_cap_no_runs_not_exceeded(tmp_path: Path) -> None:
    state = PipelineState(str(tmp_path))
    policy = CapPolicy(max_runs=5, window_hours=1)
    result = evaluate_cap(state, "pipe", policy)
    assert result.cap_exceeded is False
    assert result.run_count == 0


def test_evaluate_cap_recent_runs_counted(tmp_path: Path) -> None:
    state = PipelineState(str(tmp_path))
    for _ in range(3):
        state.record_success("pipe")
    policy = CapPolicy(max_runs=5, window_hours=24)
    result = evaluate_cap(state, "pipe", policy)
    assert result.run_count == 3
    assert result.cap_exceeded is False


def test_evaluate_cap_exceeded_at_threshold(tmp_path: Path) -> None:
    state = PipelineState(str(tmp_path))
    for _ in range(5):
        state.record_success("pipe")
    policy = CapPolicy(max_runs=5, window_hours=24)
    result = evaluate_cap(state, "pipe", policy)
    assert result.cap_exceeded is True


def test_evaluate_cap_old_runs_excluded(tmp_path: Path) -> None:
    """Runs older than the window should not count."""
    state = PipelineState(str(tmp_path))
    # inject a run with a timestamp well outside the window
    from pipewatch.state import PipelineRun
    old_ts = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    old_run = PipelineRun(run_id="old", status="ok", started_at=old_ts, finished_at=old_ts)
    state.runs.setdefault("pipe", []).append(old_run)

    policy = CapPolicy(max_runs=1, window_hours=24)
    result = evaluate_cap(state, "pipe", policy)
    assert result.run_count == 0
    assert result.cap_exceeded is False
