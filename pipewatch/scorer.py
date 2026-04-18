"""Pipeline health scorer: produces a 0-100 score from a PipelineSummary."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.summarizer import PipelineSummary


@dataclass
class HealthScore:
    pipeline: str
    score: int          # 0 (worst) – 100 (best)
    grade: str          # A / B / C / D / F
    reason: str


def _grade(score: int) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def score_pipeline(summary: PipelineSummary) -> HealthScore:
    """Derive a health score from a PipelineSummary."""
    if summary.status == "unknown":
        return HealthScore(
            pipeline=summary.pipeline,
            score=50,
            grade="C",
            reason="no runs recorded",
        )

    total = summary.total_runs
    if total == 0:
        raw = 50
        reason = "no runs recorded"
    else:
        success_ratio = summary.success_runs / total
        raw = int(success_ratio * 100)
        reason = f"{summary.success_runs}/{total} runs succeeded"

    # penalise overdue pipelines
    if summary.is_overdue:
        raw = max(0, raw - 15)
        reason += "; pipeline is overdue"

    # penalise consecutive failures
    penalty = min(summary.consecutive_failures * 5, 30)
    raw = max(0, raw - penalty)
    if summary.consecutive_failures:
        reason += f"; {summary.consecutive_failures} consecutive failure(s)"

    score = max(0, min(100, raw))
    return HealthScore(
        pipeline=summary.pipeline,
        score=score,
        grade=_grade(score),
        reason=reason,
    )


def score_all(summaries: list[PipelineSummary]) -> list[HealthScore]:
    return [score_pipeline(s) for s in summaries]
