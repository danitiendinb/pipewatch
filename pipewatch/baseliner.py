"""Baseline management: record and compare pipeline run durations against a stored baseline."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pipewatch.state import PipelineState


@dataclass
class Baseline:
    pipeline: str
    mean_duration: float
    sample_count: int
    recorded_at: str


def _baseline_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.baseline.json"


def load_baseline(state_dir: str, pipeline: str) -> Optional[Baseline]:
    path = _baseline_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return Baseline(**data)


def save_baseline(state_dir: str, baseline: Baseline) -> None:
    path = _baseline_path(state_dir, baseline.pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "pipeline": baseline.pipeline,
        "mean_duration": baseline.mean_duration,
        "sample_count": baseline.sample_count,
        "recorded_at": baseline.recorded_at,
    }))


def clear_baseline(state_dir: str, pipeline: str) -> None:
    _baseline_path(state_dir, pipeline).unlink(missing_ok=True)


def compute_baseline(state: PipelineState, pipeline: str) -> Optional[Baseline]:
    from pipewatch.state import now_iso
    finished = [
        r for r in state.runs
        if r.duration_seconds is not None and r.status == "ok"
    ]
    if not finished:
        return None
    mean = sum(r.duration_seconds for r in finished) / len(finished)
    return Baseline(
        pipeline=pipeline,
        mean_duration=round(mean, 3),
        sample_count=len(finished),
        recorded_at=now_iso(),
    )


def exceeds_baseline(duration: float, baseline: Baseline, factor: float = 2.0) -> bool:
    """Return True if duration exceeds baseline mean by more than `factor` times."""
    return duration > baseline.mean_duration * factor
