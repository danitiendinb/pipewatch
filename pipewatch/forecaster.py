"""Simple next-run success forecaster based on recent history."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.state import PipelineState, PipelineRun


@dataclass
class Forecast:
    pipeline: str
    total_runs: int
    success_rate: float          # 0.0 – 1.0
    predicted_success: bool
    confidence: str              # "high" | "medium" | "low"


def _recent_runs(state: PipelineState, pipeline: str, window: int) -> List[PipelineRun]:
    runs = state.runs.get(pipeline, [])
    finished = [r for r in runs if r.status in ("ok", "failed")]
    return finished[-window:]


def success_rate(runs: List[PipelineRun]) -> float:
    if not runs:
        return 0.0
    ok = sum(1 for r in runs if r.status == "ok")
    return ok / len(runs)


def _confidence(n: int) -> str:
    if n >= 20:
        return "high"
    if n >= 5:
        return "medium"
    return "low"


def forecast_pipeline(
    state: PipelineState,
    pipeline: str,
    window: int = 30,
) -> Optional[Forecast]:
    runs = _recent_runs(state, pipeline, window)
    if not runs:
        return None
    rate = success_rate(runs)
    return Forecast(
        pipeline=pipeline,
        total_runs=len(runs),
        success_rate=rate,
        predicted_success=rate >= 0.5,
        confidence=_confidence(len(runs)),
    )


def forecast_all(
    state: PipelineState,
    window: int = 30,
) -> List[Forecast]:
    results = []
    for pipeline in state.runs:
        f = forecast_pipeline(state, pipeline, window)
        if f is not None:
            results.append(f)
    results.sort(key=lambda f: f.success_rate)
    return results
