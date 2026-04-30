"""Trendline: compute linear trend over recent pipeline run durations."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.state import PipelineState


@dataclass
class TrendlineReport:
    pipeline: str
    sample_size: int
    slope: float          # seconds per run (positive = getting slower)
    intercept: float
    direction: str        # 'improving', 'degrading', 'stable'
    latest_predicted: float  # predicted duration for next run (seconds)


def _finished_durations(state: PipelineState, window: int) -> List[float]:
    runs = [
        r for r in state.runs
        if r.status in ("ok", "failed") and r.duration_seconds is not None
    ]
    runs = sorted(runs, key=lambda r: r.finished_at or "")[-window:]
    return [r.duration_seconds for r in runs]  # type: ignore[misc]


def _linear_regression(values: List[float]):
    """Return (slope, intercept) via ordinary least squares."""
    n = len(values)
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(values) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, values))
    den = sum((x - mean_x) ** 2 for x in xs)
    slope = num / den if den != 0 else 0.0
    intercept = mean_y - slope * mean_x
    return slope, intercept


def compute_trendline(
    pipeline: str,
    state: PipelineState,
    window: int = 20,
    stable_threshold: float = 1.0,
) -> Optional[TrendlineReport]:
    """Compute a linear trendline over the last *window* finished runs.

    Returns None when fewer than 2 data points are available.
    """
    durations = _finished_durations(state, window)
    if len(durations) < 2:
        return None

    slope, intercept = _linear_regression(durations)
    predicted = intercept + slope * len(durations)

    if slope > stable_threshold:
        direction = "degrading"
    elif slope < -stable_threshold:
        direction = "improving"
    else:
        direction = "stable"

    return TrendlineReport(
        pipeline=pipeline,
        sample_size=len(durations),
        slope=round(slope, 4),
        intercept=round(intercept, 4),
        direction=direction,
        latest_predicted=round(max(predicted, 0.0), 2),
    )


def compute_all(
    states: dict,
    window: int = 20,
    stable_threshold: float = 1.0,
) -> List[TrendlineReport]:
    """Compute trendlines for every pipeline in *states*."""
    results = []
    for name, state in states.items():
        report = compute_trendline(name, state, window, stable_threshold)
        if report is not None:
            results.append(report)
    return sorted(results, key=lambda r: r.pipeline)
