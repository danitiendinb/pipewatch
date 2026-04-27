"""cadence.py — track and evaluate pipeline run cadence (expected vs actual frequency)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from pipewatch.state import PipelineState


@dataclass
class CadencePolicy:
    expected_interval_minutes: int  # how often the pipeline should run
    tolerance_minutes: int = 5      # grace window before marking as off-cadence


@dataclass
class CadenceReport:
    pipeline: str
    on_cadence: bool
    last_run_at: Optional[str]
    expected_by: Optional[str]
    minutes_overdue: Optional[float]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _cadence_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.cadence.json"


def save_cadence_policy(state_dir: str, pipeline: str, policy: CadencePolicy) -> None:
    path = _cadence_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "expected_interval_minutes": policy.expected_interval_minutes,
        "tolerance_minutes": policy.tolerance_minutes,
    }))


def load_cadence_policy(state_dir: str, pipeline: str) -> Optional[CadencePolicy]:
    path = _cadence_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return CadencePolicy(
        expected_interval_minutes=data["expected_interval_minutes"],
        tolerance_minutes=data.get("tolerance_minutes", 5),
    )


def clear_cadence_policy(state_dir: str, pipeline: str) -> None:
    path = _cadence_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def evaluate_cadence(
    pipeline: str,
    state: PipelineState,
    policy: CadencePolicy,
) -> CadenceReport:
    runs = state.runs
    if not runs:
        return CadenceReport(
            pipeline=pipeline,
            on_cadence=False,
            last_run_at=None,
            expected_by=None,
            minutes_overdue=None,
        )

    last = max(runs, key=lambda r: r.started_at)
    last_dt = datetime.fromisoformat(last.started_at)
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=timezone.utc)

    expected_by = last_dt + timedelta(minutes=policy.expected_interval_minutes)
    deadline = expected_by + timedelta(minutes=policy.tolerance_minutes)
    now = _now()
    overdue = max(0.0, (now - deadline).total_seconds() / 60)
    on_cadence = now <= deadline

    return CadenceReport(
        pipeline=pipeline,
        on_cadence=on_cadence,
        last_run_at=last.started_at,
        expected_by=expected_by.isoformat(),
        minutes_overdue=round(overdue, 2) if not on_cadence else 0.0,
    )
