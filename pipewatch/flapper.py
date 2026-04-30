"""Flap detection: identify pipelines that oscillate between success and failure."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from pipewatch.state import PipelineState, PipelineRun


@dataclass
class FlapReport:
    pipeline: str
    flap_count: int
    is_flapping: bool
    transitions: List[str] = field(default_factory=list)


def _flap_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.flap.json"


def _recent_statuses(state: PipelineState, window: int = 10) -> List[str]:
    runs: List[PipelineRun] = state.runs[-window:]
    return [r.status for r in runs]


def count_transitions(statuses: List[str]) -> int:
    """Count the number of status changes in a sequence."""
    if len(statuses) < 2:
        return 0
    return sum(1 for a, b in zip(statuses, statuses[1:]) if a != b)


def detect_flap(
    pipeline: str,
    state: PipelineState,
    threshold: int = 3,
    window: int = 10,
) -> FlapReport:
    statuses = _recent_statuses(state, window)
    transitions = count_transitions(statuses)
    return FlapReport(
        pipeline=pipeline,
        flap_count=transitions,
        is_flapping=transitions >= threshold,
        transitions=statuses,
    )


def detect_all(
    pipelines: List[str],
    load_state,
    threshold: int = 3,
    window: int = 10,
) -> List[FlapReport]:
    reports = []
    for name in pipelines:
        state = load_state(name)
        reports.append(detect_flap(name, state, threshold, window))
    return reports


def save_flap_report(state_dir: str, report: FlapReport) -> None:
    path = _flap_path(state_dir, report.pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "pipeline": report.pipeline,
        "flap_count": report.flap_count,
        "is_flapping": report.is_flapping,
        "transitions": report.transitions,
    }))


def load_flap_report(state_dir: str, pipeline: str) -> Optional[FlapReport]:
    path = _flap_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return FlapReport(**data)
