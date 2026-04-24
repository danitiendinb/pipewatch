"""Pipeline configuration linter — validates pipewatch.yml entries for common mistakes."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.config import PipewatchConfig, PipelineConfig


@dataclass
class LintIssue:
    pipeline: str
    severity: str  # "error" | "warning"
    message: str


@dataclass
class LintReport:
    issues: List[LintIssue] = field(default_factory=list)

    @property
    def errors(self) -> List[LintIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> List[LintIssue]:
        return [i for i in self.issues if i.severity == "warning"]

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0


def _lint_pipeline(cfg: PipelineConfig) -> List[LintIssue]:
    issues: List[LintIssue] = []
    name = cfg.name

    if not name or not name.strip():
        issues.append(LintIssue(pipeline="<unknown>", severity="error", message="Pipeline name is empty"))
        return issues

    if " " in name:
        issues.append(LintIssue(pipeline=name, severity="warning", message="Pipeline name contains spaces; prefer underscores"))

    if cfg.schedule:
        parts = cfg.schedule.strip().split()
        if len(parts) != 5:
            issues.append(LintIssue(pipeline=name, severity="error",
                                    message=f"Invalid cron expression '{cfg.schedule}' (expected 5 fields)"))

    if cfg.failure_threshold is not None and cfg.failure_threshold < 1:
        issues.append(LintIssue(pipeline=name, severity="error",
                                message="failure_threshold must be >= 1"))

    if cfg.failure_threshold is not None and cfg.failure_threshold > 100:
        issues.append(LintIssue(pipeline=name, severity="warning",
                                message="failure_threshold > 100 seems unusually high"))

    return issues


def lint_config(config: PipewatchConfig) -> LintReport:
    """Run all lint checks across every pipeline in *config*."""
    report = LintReport()
    seen: set = set()
    for pipeline in config.pipelines:
        if pipeline.name in seen:
            report.issues.append(LintIssue(pipeline=pipeline.name, severity="error",
                                           message="Duplicate pipeline name"))
        seen.add(pipeline.name)
        report.issues.extend(_lint_pipeline(pipeline))
    return report


def format_lint_report(report: LintReport) -> str:
    if not report.issues:
        return "✓ No issues found."
    lines = []
    for issue in report.issues:
        icon = "✗" if issue.severity == "error" else "⚠"
        lines.append(f"  {icon} [{issue.pipeline}] {issue.message}")
    summary = f"{len(report.errors)} error(s), {len(report.warnings)} warning(s)"
    return "\n".join(lines) + f"\n{summary}"
