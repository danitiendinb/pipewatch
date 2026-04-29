"""Scorecard: aggregate per-pipeline health metrics into a printable report card."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.scorer import HealthScore, score_pipeline
from pipewatch.summarizer import PipelineSummary, summarize_pipeline
from pipewatch.state import PipelineState


@dataclass
class ScorecardRow:
    name: str
    grade: str
    score: float
    status: str
    consecutive_failures: int
    overdue: bool
    note: str = ""


@dataclass
class Scorecard:
    rows: List[ScorecardRow] = field(default_factory=list)
    period_days: int = 7

    @property
    def average_score(self) -> float:
        if not self.rows:
            return 0.0
        return round(sum(r.score for r in self.rows) / len(self.rows), 2)

    @property
    def passing(self) -> List[ScorecardRow]:
        return [r for r in self.rows if r.grade not in ("D", "F")]

    @property
    def failing(self) -> List[ScorecardRow]:
        return [r for r in self.rows if r.grade in ("D", "F")]


def build_scorecard(
    pipeline_names: List[str],
    store: object,
    period_days: int = 7,
) -> Scorecard:
    rows: List[ScorecardRow] = []
    for name in sorted(pipeline_names):
        state: PipelineState = store.load(name)
        summary: PipelineSummary = summarize_pipeline(name, state, period_days=period_days)
        hs: HealthScore = score_pipeline(summary)
        rows.append(
            ScorecardRow(
                name=name,
                grade=hs.grade,
                score=hs.score,
                status=summary.status,
                consecutive_failures=summary.consecutive_failures,
                overdue=summary.overdue,
            )
        )
    return Scorecard(rows=rows, period_days=period_days)


def format_scorecard(sc: Scorecard) -> str:
    lines = [
        f"{'Pipeline':<30} {'Grade':>5} {'Score':>6} {'Status':<10} {'Failures':>8} {'Overdue':>7}",
        "-" * 72,
    ]
    for row in sc.rows:
        overdue_flag = "YES" if row.overdue else "no"
        lines.append(
            f"{row.name:<30} {row.grade:>5} {row.score:>6.1f} {row.status:<10}"
            f" {row.consecutive_failures:>8} {overdue_flag:>7}"
        )
    lines.append("-" * 72)
    lines.append(f"Average score: {sc.average_score:.1f}  "
                 f"Passing: {len(sc.passing)}  Failing: {len(sc.failing)}")
    return "\n".join(lines)
