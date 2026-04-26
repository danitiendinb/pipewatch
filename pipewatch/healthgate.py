"""healthgate.py — Pipeline health gate for CI/CD integration.

Evaluates whether a set of pipelines meets a minimum health threshold,
returning a pass/fail result suitable for use in deployment gates or
pre-promotion checks.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from pipewatch.scorer import score_pipeline, HealthScore
from pipewatch.state import PipelineState
from pipewatch.summarizer import summarize_pipeline


@dataclass
class GateResult:
    """Outcome of a health gate evaluation."""

    pipeline: str
    score: float
    grade: str
    passed: bool
    reason: str


@dataclass
class GateReport:
    """Aggregate report for a health gate check."""

    results: List[GateResult] = field(default_factory=list)
    threshold: float = 60.0

    @property
    def passed(self) -> bool:
        """True only when every pipeline in the report passed."""
        return all(r.passed for r in self.results)

    @property
    def failed_pipelines(self) -> List[str]:
        return [r.pipeline for r in self.results if not r.passed]


def evaluate_gate(
    pipeline_names: List[str],
    state_dir: Path,
    threshold: float = 60.0,
) -> GateReport:
    """Evaluate health gate for *pipeline_names* against *threshold*.

    A pipeline passes when its health score is >= *threshold*.
    Pipelines with no recorded runs are treated as failing (score 0).

    Args:
        pipeline_names: Names of pipelines to evaluate.
        state_dir: Directory where pipeline state files are stored.
        threshold: Minimum acceptable health score (0–100).

    Returns:
        A :class:`GateReport` containing per-pipeline results.
    """
    report = GateReport(threshold=threshold)

    for name in pipeline_names:
        store = PipelineState(state_dir)
        summary = summarize_pipeline(name, store)
        hs: HealthScore = score_pipeline(summary)

        passed = hs.score >= threshold
        if passed:
            reason = f"score {hs.score:.1f} >= threshold {threshold:.1f}"
        else:
            reason = f"score {hs.score:.1f} < threshold {threshold:.1f}"

        report.results.append(
            GateResult(
                pipeline=name,
                score=hs.score,
                grade=hs.grade,
                passed=passed,
                reason=reason,
            )
        )

    return report


def format_gate_report(report: GateReport, *, colour: bool = True) -> str:
    """Render a :class:`GateReport` as a human-readable string."""
    lines: List[str] = []
    icon_pass = "\u2705" if colour else "PASS"
    icon_fail = "\u274c" if colour else "FAIL"

    for r in report.results:
        icon = icon_pass if r.passed else icon_fail
        lines.append(f"  {icon}  {r.pipeline:<30}  {r.grade}  ({r.reason})")

    overall = "PASSED" if report.passed else "FAILED"
    lines.append("")
    lines.append(f"Gate: {overall}  (threshold={report.threshold:.1f})")
    if report.failed_pipelines:
        lines.append("Failing: " + ", ".join(report.failed_pipelines))

    return "\n".join(lines)


def export_gate_report_json(report: GateReport) -> str:
    """Serialise a :class:`GateReport` to a JSON string."""
    return json.dumps(
        {
            "passed": report.passed,
            "threshold": report.threshold,
            "failed_pipelines": report.failed_pipelines,
            "results": [
                {
                    "pipeline": r.pipeline,
                    "score": r.score,
                    "grade": r.grade,
                    "passed": r.passed,
                    "reason": r.reason,
                }
                for r in report.results
            ],
        },
        indent=2,
    )
