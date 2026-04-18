"""Track consecutive success/failure streaks for pipelines."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

from pipewatch.state import PipelineState


@dataclass
class StreakInfo:
    pipeline: str
    current_streak: int        # positive = successes, negative = failures
    longest_success_streak: int
    longest_failure_streak: int


def _streak_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.streak.json"


def load_streak(state_dir: str, pipeline: str) -> StreakInfo:
    p = _streak_path(state_dir, pipeline)
    if not p.exists():
        return StreakInfo(
            pipeline=pipeline,
            current_streak=0,
            longest_success_streak=0,
            longest_failure_streak=0,
        )
    data = json.loads(p.read_text())
    return StreakInfo(**data)


def save_streak(state_dir: str, info: StreakInfo) -> None:
    p = _streak_path(state_dir, info.pipeline)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(asdict(info)))


def update_streak(state_dir: str, pipeline: str, success: bool) -> StreakInfo:
    info = load_streak(state_dir, pipeline)
    if success:
        info.current_streak = max(info.current_streak, 0) + 1
        if info.current_streak > info.longest_success_streak:
            info.longest_success_streak = info.current_streak
    else:
        info.current_streak = min(info.current_streak, 0) - 1
        depth = abs(info.current_streak)
        if depth > info.longest_failure_streak:
            info.longest_failure_streak = depth
    save_streak(state_dir, info)
    return info


def compute_streak(state_dir: str, pipeline: str, ps: Optional[PipelineState] = None) -> StreakInfo:
    """Recompute streak from stored runs (useful for backfill)."""
    from pipewatch.state import load as load_state
    if ps is None:
        ps = load_state(state_dir, pipeline)
    info = StreakInfo(
        pipeline=pipeline,
        current_streak=0,
        longest_success_streak=0,
        longest_failure_streak=0,
    )
    for run in sorted(ps.runs, key=lambda r: r.started_at):
        success = run.status == "ok"
        if success:
            info.current_streak = max(info.current_streak, 0) + 1
            if info.current_streak > info.longest_success_streak:
                info.longest_success_streak = info.current_streak
        else:
            info.current_streak = min(info.current_streak, 0) - 1
            depth = abs(info.current_streak)
            if depth > info.longest_failure_streak:
                info.longest_failure_streak = depth
    save_streak(state_dir, info)
    return info
