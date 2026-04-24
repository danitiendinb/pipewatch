"""Detect configuration or behaviour drift between pipeline runs."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from pipewatch.state import PipelineState


@dataclass
class DriftReport:
    pipeline: str
    previous_avg_duration: Optional[float]  # seconds
    current_avg_duration: Optional[float]   # seconds
    drift_seconds: Optional[float]
    drift_pct: Optional[float]
    has_drift: bool
    window: int = 10


def _drift_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.drift.json"


def _recent_durations(state: PipelineState, window: int) -> list[float]:
    finished = [
        r for r in state.runs
        if r.status in ("ok", "fail") and r.started_at and r.finished_at
    ]
    finished = finished[-window:]
    durations: list[float] = []
    for r in finished:
        from datetime import datetime
        fmt = "%Y-%m-%dT%H:%M:%S"
        try:
            s = datetime.fromisoformat(r.started_at)
            f = datetime.fromisoformat(r.finished_at)
            durations.append((f - s).total_seconds())
        except Exception:
            pass
    return durations


def save_drift_baseline(state_dir: str, pipeline: str, avg_duration: float) -> None:
    path = _drift_path(state_dir, pipeline)
    path.write_text(json.dumps({"avg_duration": avg_duration}))


def load_drift_baseline(state_dir: str, pipeline: str) -> Optional[float]:
    path = _drift_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return data.get("avg_duration")


def clear_drift_baseline(state_dir: str, pipeline: str) -> None:
    path = _drift_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def detect_drift(
    state: PipelineState,
    state_dir: str,
    pipeline: str,
    threshold_pct: float = 20.0,
    window: int = 10,
) -> DriftReport:
    durations = _recent_durations(state, window)
    current_avg = sum(durations) / len(durations) if durations else None
    previous_avg = load_drift_baseline(state_dir, pipeline)

    drift_seconds: Optional[float] = None
    drift_pct: Optional[float] = None
    has_drift = False

    if current_avg is not None and previous_avg is not None and previous_avg > 0:
        drift_seconds = current_avg - previous_avg
        drift_pct = abs(drift_seconds) / previous_avg * 100.0
        has_drift = drift_pct >= threshold_pct

    return DriftReport(
        pipeline=pipeline,
        previous_avg_duration=previous_avg,
        current_avg_duration=current_avg,
        drift_seconds=drift_seconds,
        drift_pct=drift_pct,
        has_drift=has_drift,
        window=window,
    )
