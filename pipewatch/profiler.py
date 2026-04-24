"""Pipeline run duration profiling — tracks percentile statistics over a rolling window."""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from pipewatch.state import PipelineState

_WINDOW = 50  # maximum number of durations to retain


@dataclass
class DurationProfile:
    pipeline: str
    sample_size: int
    mean_seconds: float
    median_seconds: float
    p95_seconds: float
    p99_seconds: float
    min_seconds: float
    max_seconds: float


def _profile_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.profile.json"


def _finished_durations(state: PipelineState) -> List[float]:
    durations = []
    for run in state.runs:
        if run.finished_at and run.started_at:
            try:
                from datetime import datetime
                fmt = "%Y-%m-%dT%H:%M:%S"
                start = datetime.strptime(run.started_at[:19], fmt)
                end = datetime.strptime(run.finished_at[:19], fmt)
                diff = (end - start).total_seconds()
                if diff >= 0:
                    durations.append(diff)
            except ValueError:
                continue
    return durations


def compute_profile(pipeline: str, state: PipelineState) -> Optional[DurationProfile]:
    durations = _finished_durations(state)
    if len(durations) < 2:
        return None
    window = durations[-_WINDOW:]
    sorted_d = sorted(window)
    n = len(sorted_d)

    def _percentile(data: List[float], pct: float) -> float:
        idx = int(len(data) * pct / 100)
        return data[min(idx, len(data) - 1)]

    return DurationProfile(
        pipeline=pipeline,
        sample_size=n,
        mean_seconds=statistics.mean(window),
        median_seconds=statistics.median(window),
        p95_seconds=_percentile(sorted_d, 95),
        p99_seconds=_percentile(sorted_d, 99),
        min_seconds=sorted_d[0],
        max_seconds=sorted_d[-1],
    )


def save_profile(state_dir: str, profile: DurationProfile) -> None:
    path = _profile_path(state_dir, profile.pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(profile.__dict__, indent=2))


def load_profile(state_dir: str, pipeline: str) -> Optional[DurationProfile]:
    path = _profile_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return DurationProfile(**data)


def clear_profile(state_dir: str, pipeline: str) -> None:
    path = _profile_path(state_dir, pipeline)
    if path.exists():
        path.unlink()
