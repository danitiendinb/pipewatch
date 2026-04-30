"""Run-count limiter: cap how many times a pipeline may run within a time window."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _limiter_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.limiter.json"


@dataclass
class LimiterPolicy:
    max_runs: int = 10
    window_hours: float = 24.0


@dataclass
class LimiterState:
    policy: LimiterPolicy
    run_timestamps: List[str] = field(default_factory=list)


@dataclass
class LimiterResult:
    allowed: bool
    current_count: int
    max_runs: int
    window_hours: float


def save_limiter_policy(state_dir: str, pipeline: str, policy: LimiterPolicy) -> None:
    path = _limiter_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {"max_runs": policy.max_runs, "window_hours": policy.window_hours, "run_timestamps": []}
    if path.exists():
        existing = json.loads(path.read_text())
        data["run_timestamps"] = existing.get("run_timestamps", [])
    path.write_text(json.dumps(data))


def load_limiter_state(state_dir: str, pipeline: str) -> Optional[LimiterState]:
    path = _limiter_path(state_dir, pipeline)
    if not path.exists():
        return None
    raw = json.loads(path.read_text())
    policy = LimiterPolicy(
        max_runs=raw.get("max_runs", 10),
        window_hours=raw.get("window_hours", 24.0),
    )
    return LimiterState(policy=policy, run_timestamps=raw.get("run_timestamps", []))


def _prune_timestamps(timestamps: List[str], window_hours: float) -> List[str]:
    cutoff = _now() - timedelta(hours=window_hours)
    return [ts for ts in timestamps if datetime.fromisoformat(ts) >= cutoff]


def check_and_record(state_dir: str, pipeline: str) -> LimiterResult:
    """Check whether a new run is allowed; if so, record it."""
    state = load_limiter_state(state_dir, pipeline)
    if state is None:
        return LimiterResult(allowed=True, current_count=0, max_runs=0, window_hours=24.0)

    pruned = _prune_timestamps(state.run_timestamps, state.policy.window_hours)
    allowed = len(pruned) < state.policy.max_runs
    if allowed:
        pruned.append(_now().isoformat())
        path = _limiter_path(state_dir, pipeline)
        data = {
            "max_runs": state.policy.max_runs,
            "window_hours": state.policy.window_hours,
            "run_timestamps": pruned,
        }
        path.write_text(json.dumps(data))
    return LimiterResult(
        allowed=allowed,
        current_count=len(pruned) if allowed else len(pruned),
        max_runs=state.policy.max_runs,
        window_hours=state.policy.window_hours,
    )


def clear_limiter(state_dir: str, pipeline: str) -> None:
    _limiter_path(state_dir, pipeline).unlink(missing_ok=True)
