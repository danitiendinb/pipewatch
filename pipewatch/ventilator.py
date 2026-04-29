"""ventilator.py – pipeline backpressure / concurrency-pressure monitor.

Tracks how many pipeline runs are queued vs. active and emits a
VentilatorReport when the queue depth exceeds a configured threshold.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _vent_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.vent.json"


@dataclass
class VentilatorState:
    pipeline: str
    queued: int = 0
    active: int = 0
    last_updated: str = ""


@dataclass
class VentilatorReport:
    pipeline: str
    queued: int
    active: int
    threshold: int
    pressure: float          # queued / threshold, clamped to [0, 1]
    overloaded: bool
    last_updated: str


def load_ventilator(state_dir: str, pipeline: str) -> VentilatorState:
    p = _vent_path(state_dir, pipeline)
    if not p.exists():
        return VentilatorState(pipeline=pipeline)
    data = json.loads(p.read_text())
    return VentilatorState(
        pipeline=data.get("pipeline", pipeline),
        queued=data.get("queued", 0),
        active=data.get("active", 0),
        last_updated=data.get("last_updated", ""),
    )


def save_ventilator(state_dir: str, state: VentilatorState) -> None:
    Path(state_dir).mkdir(parents=True, exist_ok=True)
    p = _vent_path(state_dir, state.pipeline)
    state.last_updated = _now().isoformat()
    p.write_text(json.dumps({
        "pipeline": state.pipeline,
        "queued": state.queued,
        "active": state.active,
        "last_updated": state.last_updated,
    }, indent=2))


def update_ventilator(state_dir: str, pipeline: str,
                      queued: int, active: int) -> VentilatorState:
    """Persist current queue/active counts and return updated state."""
    state = VentilatorState(pipeline=pipeline, queued=queued, active=active)
    save_ventilator(state_dir, state)
    return state


def clear_ventilator(state_dir: str, pipeline: str) -> None:
    p = _vent_path(state_dir, pipeline)
    if p.exists():
        p.unlink()


def evaluate_pressure(state: VentilatorState, threshold: int) -> VentilatorReport:
    """Compute a pressure report for *state* against *threshold*."""
    if threshold <= 0:
        raise ValueError("threshold must be > 0")
    pressure = min(state.queued / threshold, 1.0)
    return VentilatorReport(
        pipeline=state.pipeline,
        queued=state.queued,
        active=state.active,
        threshold=threshold,
        pressure=round(pressure, 4),
        overloaded=state.queued >= threshold,
        last_updated=state.last_updated,
    )


def overloaded_pipelines(
    state_dir: str,
    pipeline_names: List[str],
    threshold: int,
) -> List[VentilatorReport]:
    """Return reports for all pipelines whose queue depth meets/exceeds threshold."""
    results = []
    for name in pipeline_names:
        state = load_ventilator(state_dir, name)
        report = evaluate_pressure(state, threshold)
        if report.overloaded:
            results.append(report)
    return results
