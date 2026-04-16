"""Render pipeline status reports to stdout."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from pipewatch.state import PipelineState, PipelineRun


STATUS_OK = "OK"
STATUS_FAILING = "FAILING"
STATUS_UNKNOWN = "UNKNOWN"


def pipeline_status(state: PipelineState) -> str:
    if state.last_run is None:
        return STATUS_UNKNOWN
    if state.consecutive_failures > 0:
        return STATUS_FAILING
    return STATUS_OK


def format_run(run: PipelineRun) -> str:
    duration = ""
    if run.started_at and run.finished_at:
        start = datetime.fromisoformat(run.started_at)
        end = datetime.fromisoformat(run.finished_at)
        secs = (end - start).total_seconds()
        duration = f"  duration={secs:.1f}s"
    result = "success" if run.success else "failure"
    return f"  [{run.started_at}] {result}{duration}"


def render_pipeline(name: str, state: PipelineState, history: int = 5) -> str:
    lines: List[str] = []
    status = pipeline_status(state)
    lines.append(f"Pipeline: {name}  status={status}  failures={state.consecutive_failures}")
    if state.runs:
        lines.append("Recent runs:")
        for run in state.runs[-history:][::-1]:
            lines.append(format_run(run))
    else:
        lines.append("  No runs recorded.")
    return "\n".join(lines)


def render_summary(pipeline_states: dict[str, PipelineState]) -> str:
    if not pipeline_states:
        return "No pipelines tracked."
    lines = []
    for name, state in sorted(pipeline_states.items()):
        status = pipeline_status(state)
        lines.append(f"{name:<30} {status:<10} failures={state.consecutive_failures}")
    return "\n".join(lines)
