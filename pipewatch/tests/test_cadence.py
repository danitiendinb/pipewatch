"""Tests for pipewatch.cadence."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.cadence import (
    CadencePolicy,
    CadenceReport,
    clear_cadence_policy,
    evaluate_cadence,
    load_cadence_policy,
    save_cadence_policy,
)
from pipewatch.state import PipelineState, PipelineRun


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _utc(offset_minutes: int = 0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)


def _run(started_offset: int = -30, finished_offset: int = -29) -> PipelineRun:
    return PipelineRun(
        run_id="r1",
        pipeline="pipe",
        status="ok",
        started_at=_utc(started_offset).isoformat(),
        finished_at=_utc(finished_offset).isoformat(),
        message=None,
    )


# ── persistence ──────────────────────────────────────────────────────────────

def test_load_cadence_policy_none_for_unknown(state_dir: str) -> None:
    assert load_cadence_policy(state_dir, "pipe") is None


def test_save_and_load_cadence_policy(state_dir: str) -> None:
    policy = CadencePolicy(expected_interval_minutes=30, tolerance_minutes=3)
    save_cadence_policy(state_dir, "pipe", policy)
    loaded = load_cadence_policy(state_dir, "pipe")
    assert loaded is not None
    assert loaded.expected_interval_minutes == 30
    assert loaded.tolerance_minutes == 3


def test_clear_cadence_policy_removes_file(state_dir: str) -> None:
    save_cadence_policy(state_dir, "pipe", CadencePolicy(60))
    clear_cadence_policy(state_dir, "pipe")
    assert load_cadence_policy(state_dir, "pipe") is None


# ── evaluate_cadence ─────────────────────────────────────────────────────────

def test_evaluate_cadence_no_runs_is_off_cadence(state_dir: str) -> None:
    state = PipelineState(pipeline="pipe", runs=[])
    policy = CadencePolicy(expected_interval_minutes=60)
    report = evaluate_cadence("pipe", state, policy)
    assert report.on_cadence is False
    assert report.last_run_at is None


def test_evaluate_cadence_recent_run_is_on_cadence(state_dir: str) -> None:
    run = _run(started_offset=-10)  # ran 10 minutes ago
    state = PipelineState(pipeline="pipe", runs=[run])
    policy = CadencePolicy(expected_interval_minutes=60, tolerance_minutes=5)
    report = evaluate_cadence("pipe", state, policy)
    assert report.on_cadence is True
    assert report.minutes_overdue == 0.0


def test_evaluate_cadence_old_run_is_off_cadence(state_dir: str) -> None:
    run = _run(started_offset=-120)  # ran 2 hours ago
    state = PipelineState(pipeline="pipe", runs=[run])
    policy = CadencePolicy(expected_interval_minutes=60, tolerance_minutes=5)
    report = evaluate_cadence("pipe", state, policy)
    assert report.on_cadence is False
    assert report.minutes_overdue is not None
    assert report.minutes_overdue > 0


def test_evaluate_cadence_picks_most_recent_run() -> None:
    old_run = _run(started_offset=-200)
    recent_run = _run(started_offset=-5)
    recent_run = PipelineRun(
        run_id="r2", pipeline="pipe", status="ok",
        started_at=_utc(-5).isoformat(),
        finished_at=_utc(-4).isoformat(),
        message=None,
    )
    state = PipelineState(pipeline="pipe", runs=[old_run, recent_run])
    policy = CadencePolicy(expected_interval_minutes=60, tolerance_minutes=5)
    report = evaluate_cadence("pipe", state, policy)
    assert report.on_cadence is True
