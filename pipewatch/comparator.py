"""Compare pipeline run durations against historical baselines."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.state import PipelineState, PipelineRun


@dataclass
class DurationStats:
    pipeline: str
    mean_seconds: float
    stddev_seconds: float
    sample_size: int


@dataclass
class DurationAnomaly:
    pipeline: str
    run_id: str
    duration_seconds: float
    mean_seconds: float
    z_score: float


def _finished_runs(state: PipelineState, pipeline: str) -> List[PipelineRun]:
    return [
        r for r in state.load(pipeline).runs
        if r.finished_at is not None and r.duration_seconds is not None
    ]


def compute_stats(state: PipelineState, pipeline: str) -> Optional[DurationStats]:
    runs = _finished_runs(state, pipeline)
    if len(runs) < 2:
        return None
    durations = [r.duration_seconds for r in runs]
    mean = sum(durations) / len(durations)
    variance = sum((d - mean) ** 2 for d in durations) / len(durations)
    stddev = variance ** 0.5
    return DurationStats(
        pipeline=pipeline,
        mean_seconds=mean,
        stddev_seconds=stddev,
        sample_size=len(runs),
    )


def detect_anomaly(
    state: PipelineState,
    pipeline: str,
    z_threshold: float = 2.5,
) -> Optional[DurationAnomaly]:
    stats = compute_stats(state, pipeline)
    if stats is None or stats.stddev_seconds == 0:
        return None
    runs = _finished_runs(state, pipeline)
    latest = runs[-1]
    z = (latest.duration_seconds - stats.mean_seconds) / stats.stddev_seconds
    if abs(z) >= z_threshold:
        return DurationAnomaly(
            pipeline=pipeline,
            run_id=latest.run_id,
            duration_seconds=latest.duration_seconds,
            mean_seconds=stats.mean_seconds,
            z_score=z,
        )
    return None


def check_all_pipelines(
    state: PipelineState,
    pipelines: List[str],
    z_threshold: float = 2.5,
) -> List[DurationAnomaly]:
    anomalies = []
    for name in pipelines:
        anomaly = detect_anomaly(state, name, z_threshold)
        if anomaly:
            anomalies.append(anomaly)
    return anomalies
