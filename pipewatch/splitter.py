"""splitter.py – split pipeline run history into time-based windows.

Allows callers to partition stored runs into fixed-width time buckets
(e.g. hourly, daily) for trend analysis or reporting.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from pipewatch.state import PipelineRun, PipelineState


@dataclass
class SplitBucket:
    label: str                   # ISO date or hour string
    start: datetime
    end: datetime
    runs: List[PipelineRun] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.runs)

    @property
    def failures(self) -> int:
        return sum(1 for r in self.runs if r.status == "failure")

    @property
    def success_rate(self) -> Optional[float]:
        if not self.runs:
            return None
        return (self.total - self.failures) / self.total


def _bucket_label(dt: datetime, granularity: str) -> str:
    if granularity == "hour":
        return dt.strftime("%Y-%m-%dT%H:00Z")
    return dt.strftime("%Y-%m-%d")


def split_runs(
    state: PipelineState,
    pipeline: str,
    days: int = 7,
    granularity: str = "day",
) -> List[SplitBucket]:
    """Return a list of SplitBucket objects covering the last *days* days."""
    if granularity not in ("hour", "day"):
        raise ValueError(f"granularity must be 'hour' or 'day', got {granularity!r}")

    now = datetime.now(timezone.utc)
    delta = timedelta(hours=1) if granularity == "hour" else timedelta(days=1)
    total_steps = days * 24 if granularity == "hour" else days
    cutoff = now - delta * total_steps

    # Build ordered buckets
    buckets: Dict[str, SplitBucket] = {}
    for i in range(total_steps):
        start = cutoff + delta * i
        end = start + delta
        label = _bucket_label(start, granularity)
        buckets[label] = SplitBucket(label=label, start=start, end=end)

    ps = state.load(pipeline)
    for run in ps.runs:
        if run.started_at is None:
            continue
        try:
            ts = datetime.fromisoformat(run.started_at.replace("Z", "+00:00"))
        except ValueError:
            continue
        if ts < cutoff:
            continue
        label = _bucket_label(ts, granularity)
        if label in buckets:
            buckets[label].runs.append(run)

    return list(buckets.values())


def format_split_row(bucket: SplitBucket) -> str:
    rate = bucket.success_rate
    rate_str = f"{rate * 100:.0f}%" if rate is not None else "n/a"
    return f"{bucket.label}  total={bucket.total:3d}  failures={bucket.failures:3d}  ok={rate_str}"
