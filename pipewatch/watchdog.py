"""Watchdog: detect pipelines that stopped reporting entirely."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

from pipewatch.config import PipewatchConfig
from pipewatch.state import PipelineState


@dataclass
class StaleReport:
    pipeline: str
    last_seen: str | None  # ISO timestamp or None
    hours_silent: float | None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def hours_since(iso: str) -> float:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (_utcnow() - dt).total_seconds() / 3600


def is_stale(state: PipelineState, threshold_hours: float) -> bool:
    """Return True if the pipeline has not reported within threshold_hours."""
    runs = state.runs
    if not runs:
        return True
    latest = max(runs, key=lambda r: r.started_at)
    ts = latest.finished_at or latest.started_at
    return hours_since(ts) >= threshold_hours


def stale_pipelines(
    config: PipewatchConfig,
    store,
    threshold_hours: float = 24.0,
) -> List[StaleReport]:
    """Return a StaleReport for every pipeline that is stale."""
    reports: List[StaleReport] = []
    for pc in config.pipelines:
        state = store.load(pc.name)
        runs = state.runs
        if not runs:
            reports.append(StaleReport(pipeline=pc.name, last_seen=None, hours_silent=None))
            continue
        latest = max(runs, key=lambda r: r.started_at)
        ts = latest.finished_at or latest.started_at
        h = hours_since(ts)
        if h >= threshold_hours:
            reports.append(StaleReport(pipeline=pc.name, last_seen=ts, hours_silent=round(h, 2)))
    return reports
