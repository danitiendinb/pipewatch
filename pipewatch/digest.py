"""Weekly/daily digest report generation for pipeline health."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List

from pipewatch.summarizer import PipelineSummary, build_health_report
from pipewatch.state import PipelineState


@dataclass
class DigestReport:
    generated_at: str
    period_days: int
    total_pipelines: int
    healthy: int
    failing: int
    unknown: int
    summaries: List[PipelineSummary]


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def build_digest(store: PipelineState, pipeline_names: List[str], period_days: int = 7) -> DigestReport:
    """Build a digest report covering the last *period_days* days."""
    cutoff = _utcnow() - timedelta(days=period_days)
    report = build_health_report(store, pipeline_names)

    filtered: List[PipelineSummary] = []
    for s in report.summaries:
        if s.last_run is None or s.last_run >= cutoff.isoformat():
            filtered.append(s)

    healthy = sum(1 for s in filtered if s.status == "ok")
    failing = sum(1 for s in filtered if s.status == "failing")
    unknown = sum(1 for s in filtered if s.status == "unknown")

    return DigestReport(
        generated_at=_utcnow().isoformat(),
        period_days=period_days,
        total_pipelines=len(filtered),
        healthy=healthy,
        failing=failing,
        unknown=unknown,
        summaries=filtered,
    )


def format_digest(report: DigestReport) -> str:
    """Render a digest report as plain text."""
    lines = [
        f"Pipewatch Digest — last {report.period_days} day(s)",
        f"Generated : {report.generated_at}",
        f"Pipelines : {report.total_pipelines}  "
        f"OK={report.healthy}  Failing={report.failing}  Unknown={report.unknown}",
        "-" * 50,
    ]
    for s in report.summaries:
        overdue_tag = "  [OVERDUE]" if s.is_overdue else ""
        lines.append(f"  {s.status.upper():<8} {s.pipeline_name}{overdue_tag}")
        if s.last_run:
            lines.append(f"           last run : {s.last_run}")
        if s.last_error:
            lines.append(f"           error    : {s.last_error}")
    return "\n".join(lines)
