"""Rank pipelines by health score and surface the worst offenders."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.scorer import HealthScore, score_all
from pipewatch.state import PipelineState


@dataclass
class RankedPipeline:
    name: str
    score: float
    grade: str
    rank: int


def rank_pipelines(
    names: List[str],
    store: PipelineState,
    *,
    overdue: set | None = None,
) -> List[RankedPipeline]:
    """Return pipelines sorted worst-first (ascending score)."""
    overdue = overdue or set()
    scores: List[HealthScore] = score_all(names, store, overdue=overdue)
    sorted_scores = sorted(scores, key=lambda s: s.score)
    return [
        RankedPipeline(
            name=hs.pipeline,
            score=hs.score,
            grade=hs.grade,
            rank=idx + 1,
        )
        for idx, hs in enumerate(sorted_scores)
    ]


def top_offenders(
    names: List[str],
    store: PipelineState,
    n: int = 5,
    *,
    overdue: set | None = None,
) -> List[RankedPipeline]:
    """Return the *n* worst-scoring pipelines."""
    ranked = rank_pipelines(names, store, overdue=overdue)
    return ranked[:n]


def format_ranked_row(rp: RankedPipeline) -> str:
    bar_len = max(0, int(rp.score / 5))
    bar = "█" * bar_len + "░" * (20 - bar_len)
    return f"#{rp.rank:<3} {rp.name:<30} {bar} {rp.score:5.1f}  [{rp.grade}]"
