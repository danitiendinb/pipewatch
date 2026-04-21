"""Inspector: per-pipeline health inspection with structured findings."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.state import PipelineState, PipelineStore
from pipewatch.scheduler import is_overdue
from pipewatch.summarizer import summarize_pipeline
from pipewatch.baseliner import load_baseline
from pipewatch.silencer import is_silenced
from pipewatch.acknowledger import is_acknowledged


@dataclass
class Finding:
    code: str
    severity: str          # "info" | "warning" | "critical"
    message: str


@dataclass
class InspectionReport:
    pipeline: str
    findings: List[Finding] = field(default_factory=list)

    @property
    def has_critical(self) -> bool:
        return any(f.severity == "critical" for f in self.findings)

    @property
    def has_warnings(self) -> bool:
        return any(f.severity == "warning" for f in self.findings)


def inspect_pipeline(
    name: str,
    store: PipelineStore,
    state_dir: str,
    schedule_minutes: Optional[int] = None,
) -> InspectionReport:
    report = InspectionReport(pipeline=name)
    summary = summarize_pipeline(name, store)

    if summary.status == "unknown":
        report.findings.append(Finding("NO_RUNS", "info", "No runs recorded yet."))
        return report

    if summary.status == "failing":
        report.findings.append(
            Finding(
                "CONSECUTIVE_FAILURES",
                "critical",
                f"{summary.consecutive_failures} consecutive failure(s).",
            )
        )

    if schedule_minutes and is_overdue(name, store, schedule_minutes):
        report.findings.append(
            Finding("OVERDUE", "warning", f"Pipeline overdue by schedule ({schedule_minutes}m).")
        )

    baseline = load_baseline(name, state_dir)
    if baseline and summary.mean_duration_seconds is not None:
        if summary.mean_duration_seconds > baseline.mean_seconds * 1.5:
            report.findings.append(
                Finding(
                    "SLOW_VS_BASELINE",
                    "warning",
                    f"Mean duration {summary.mean_duration_seconds:.1f}s exceeds "
                    f"baseline {baseline.mean_seconds:.1f}s by >50%.",
                )
            )

    if is_silenced(name, state_dir):
        report.findings.append(Finding("SILENCED", "info", "Alerts are currently silenced."))

    if is_acknowledged(name, state_dir):
        report.findings.append(Finding("ACKNOWLEDGED", "info", "Failure has been acknowledged."))

    if not report.findings:
        report.findings.append(Finding("OK", "info", "All checks passed."))

    return report


def inspect_all(
    store: PipelineStore,
    state_dir: str,
    schedules: Optional[dict] = None,
) -> List[InspectionReport]:
    schedules = schedules or {}
    return [
        inspect_pipeline(name, store, state_dir, schedules.get(name))
        for name in store.pipelines()
    ]
