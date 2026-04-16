"""Prune old pipeline run history from state storage."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

from pipewatch.state import PipelineState, PipelineRun


def cutoff_datetime(days: int, now: Optional[datetime] = None) -> datetime:
    """Return a UTC datetime *days* before *now*."""
    if now is None:
        now = datetime.now(timezone.utc)
    return now - timedelta(days=days)


def prune_runs(runs: list[PipelineRun], days: int, now: Optional[datetime] = None) -> list[PipelineRun]:
    """Return only runs whose *started_at* is within the retention window."""
    cutoff = cutoff_datetime(days, now)
    return [
        r for r in runs
        if datetime.fromisoformat(r.started_at) >= cutoff
    ]


def prune_pipeline(store: PipelineState, pipeline: str, days: int, now: Optional[datetime] = None) -> int:
    """Prune runs for a single pipeline. Returns number of runs removed."""
    before = store.load(pipeline)
    kept = prune_runs(before.runs, days, now)
    removed = len(before.runs) - len(kept)
    if removed:
        before.runs = kept
        store.save(pipeline, before)
    return removed


def prune_all(store: PipelineState, pipeline_names: list[str], days: int, now: Optional[datetime] = None) -> dict[str, int]:
    """Prune runs for all listed pipelines. Returns mapping of pipeline -> removed count."""
    return {name: prune_pipeline(store, name, days, now) for name in pipeline_names}
