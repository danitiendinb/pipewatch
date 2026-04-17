"""Terminal dashboard: renders a full health overview to stdout."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from pipewatch.summarizer import HealthReport, PipelineSummary, build_health_report
from pipewatch.scheduler import is_overdue
from pipewatch.state import PipelineState
from pipewatch.config import PipewatchConfig

_RESET = "\033[0m"
_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_BOLD = "\033[1m"


def _colour(text: str, code: str) -> str:
    return f"{code}{text}{_RESET}"


def status_icon(summary: PipelineSummary) -> str:
    if summary.status == "ok":
        return _colour("✔", _GREEN)
    if summary.status == "failing":
        return _colour("✘", _RED)
    return _colour("?", _YELLOW)


def format_summary_row(summary: PipelineSummary, overdue: bool) -> str:
    icon = status_icon(summary)
    overdue_tag = _colour(" [OVERDUE]", _YELLOW) if overdue else ""
    last = summary.last_run or "never"
    failures = f"failures={summary.consecutive_failures}"
    return f"  {icon}  {summary.pipeline:<30} {failures:<20} last={last}{overdue_tag}"


def render_dashboard(report: HealthReport, overdue_names: List[str], now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    lines = [
        _colour(f"{_BOLD}PipeWatch Dashboard", _BOLD),
        f"Generated: {now.isoformat()}",
        f"Total: {report.total}  OK: {report.ok_count}  Failing: {report.failing_count}  Unknown: {report.unknown_count}",
        "-" * 70,
    ]
    for s in report.pipelines:
        lines.append(format_summary_row(s, s.pipeline in overdue_names))
    lines.append("-" * 70)
    return "\n".join(lines)


def run_dashboard(config: PipewatchConfig, store: PipelineState) -> str:
    report = build_health_report(config, store)
    now = datetime.now(timezone.utc)
    overdue_names = []
    for pc in config.pipelines:
        if pc.schedule and is_overdue(pc, store, now):
            overdue_names.append(pc.name)
    return render_dashboard(report, overdue_names, now)
