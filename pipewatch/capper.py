"""capper.py – per-pipeline run-count cap enforcement.

Prevents a pipeline from registering more than a configured maximum number
of runs within a rolling time window (in hours).  Useful for runaway
schedulers that might flood the state store.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pipewatch.state import PipelineState


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _cap_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.cap.json"


@dataclass
class CapPolicy:
    max_runs: int = 100
    window_hours: int = 24


@dataclass
class CapResult:
    pipeline: str
    run_count: int
    cap_exceeded: bool
    policy: CapPolicy


def save_cap_policy(state_dir: str, pipeline: str, policy: CapPolicy) -> None:
    path = _cap_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"max_runs": policy.max_runs, "window_hours": policy.window_hours}))


def load_cap_policy(state_dir: str, pipeline: str) -> Optional[CapPolicy]:
    path = _cap_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return CapPolicy(max_runs=data["max_runs"], window_hours=data["window_hours"])


def clear_cap_policy(state_dir: str, pipeline: str) -> None:
    _cap_path(state_dir, pipeline).unlink(missing_ok=True)


def evaluate_cap(state: PipelineState, pipeline: str, policy: CapPolicy) -> CapResult:
    """Count runs within the rolling window and check against the cap."""
    from datetime import timedelta

    cutoff = _now() - timedelta(hours=policy.window_hours)
    runs = state.runs.get(pipeline, [])
    recent = [
        r for r in runs
        if datetime.fromisoformat(r.started_at) >= cutoff
    ]
    exceeded = len(recent) >= policy.max_runs
    return CapResult(
        pipeline=pipeline,
        run_count=len(recent),
        cap_exceeded=exceeded,
        policy=policy,
    )
