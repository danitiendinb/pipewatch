"""Summarize pipeline health across all pipelines."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.state import PipelineState
from pipewatch.reporter import pipeline_status


@dataclass
class PipelineSummary:
    name: str
    status: str
    consecutive_failures: int
    last_run: str | None


@dataclass
class HealthReport:
    total: int
    ok: int
    failing: int
    unknown: int
    pipelines: List[PipelineSummary]

    @property
    def healthy(self) -> bool:
        return self.failing == 0

    @property
    def summary_line(self) -> str:
        """Return a one-line summary string suitable for logging or notifications."""
        return (
            f"{self.total} pipelines — "
            f"{self.ok} ok, {self.failing} failing, {self.unknown} unknown"
        )


def summarize_pipeline(name: str, store) -> PipelineSummary:
    """Load state for a single pipeline and return a PipelineSummary."""
    state = store.load(name)
    status = pipeline_status(state)
    last_run = state.runs[-1].finished_at if state.runs else None
    return PipelineSummary(
        name=name,
        status=status,
        consecutive_failures=state.consecutive_failures,
        last_run=last_run,
    )


def build_health_report(pipeline_names: List[str], store) -> HealthReport:
    """Build a HealthReport by summarizing all named pipelines."""
    summaries = [summarize_pipeline(n, store) for n in pipeline_names]
    ok = sum(1 for s in summaries if s.status == "ok")
    failing = sum(1 for s in summaries if s.status == "failing")
    unknown = sum(1 for s in summaries if s.status == "unknown")
    return HealthReport(
        total=len(summaries),
        ok=ok,
        failing=failing,
        unknown=unknown,
        pipelines=summaries,
    )


def render_health_report(report: HealthReport) -> str:
    """Render a HealthReport as a human-readable string."""
    lines = [
        f"Health Report: {report.summary_line}",
        "-" * 60,
    ]
    for s in report.pipelines:
        last = s.last_run or "never"
        lines.append(f"  {s.name:<30} {s.status:<10} last={last}")
    return "\n".join(lines)
