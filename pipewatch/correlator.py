"""correlator.py – detect co-failure patterns between pipelines.

When multiple pipelines fail around the same time it often indicates a shared
root cause (e.g. a broken data source, infrastructure outage).  The correlator
scans recent run history across all pipelines and groups those whose failures
overlap within a configurable time window.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from pipewatch.state import PipelineState, PipelineRun


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class CorrelationGroup:
    """A set of pipelines that failed within *window_minutes* of each other."""
    pipelines: List[str]
    earliest_failure: datetime
    latest_failure: datetime

    @property
    def span_minutes(self) -> float:
        delta = self.latest_failure - self.earliest_failure
        return delta.total_seconds() / 60


@dataclass
class CorrelationReport:
    """Full correlation analysis result."""
    window_minutes: int
    groups: List[CorrelationGroup] = field(default_factory=list)

    @property
    def correlated_pipelines(self) -> List[str]:
        """Flat list of every pipeline that appears in at least one group."""
        seen: List[str] = []
        for g in self.groups:
            for p in g.pipelines:
                if p not in seen:
                    seen.append(p)
        return seen


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _most_recent_failure(runs: Sequence[PipelineRun]) -> Optional[datetime]:
    """Return the timestamp of the most recent failed run, or None."""
    failures = [
        r for r in runs
        if r.status == "failure" and r.finished_at is not None
    ]
    if not failures:
        return None
    return max(
        datetime.fromisoformat(r.finished_at)  # type: ignore[arg-type]
        for r in failures
    )


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def find_correlations(
    states: Dict[str, PipelineState],
    window_minutes: int = 15,
) -> CorrelationReport:
    """Analyse *states* and return pipelines whose most-recent failures cluster
    within *window_minutes* of each other.

    Args:
        states: Mapping of pipeline name → PipelineState.
        window_minutes: Maximum gap (in minutes) between failure timestamps
            for two pipelines to be considered co-failing.

    Returns:
        A :class:`CorrelationReport` containing one
        :class:`CorrelationGroup` per detected cluster.
    """
    window = timedelta(minutes=window_minutes)

    # Collect (pipeline_name, failure_time) pairs
    failure_times: List[tuple[str, datetime]] = []
    for name, state in states.items():
        ts = _most_recent_failure(state.runs)
        if ts is not None:
            failure_times.append((name, _ensure_utc(ts)))

    # Sort by failure time so we can do a single-pass sweep
    failure_times.sort(key=lambda x: x[1])

    report = CorrelationReport(window_minutes=window_minutes)
    if len(failure_times) < 2:
        return report

    # Greedy grouping: extend the current group while the next failure falls
    # within *window* of the group's earliest failure.
    group_start_idx = 0
    while group_start_idx < len(failure_times):
        anchor_time = failure_times[group_start_idx][1]
        members = [failure_times[group_start_idx]]
        idx = group_start_idx + 1
        while idx < len(failure_times):
            if failure_times[idx][1] - anchor_time <= window:
                members.append(failure_times[idx])
                idx += 1
            else:
                break

        if len(members) >= 2:
            report.groups.append(
                CorrelationGroup(
                    pipelines=[m[0] for m in members],
                    earliest_failure=members[0][1],
                    latest_failure=members[-1][1],
                )
            )
            group_start_idx = idx  # skip past consumed members
        else:
            group_start_idx += 1

    return report
