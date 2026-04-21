"""Heatmap: produce a day-of-week × hour-of-day failure frequency matrix."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipewatch.state import PipelineState, PipelineRun

# Matrix is keyed by (weekday 0-6, hour 0-23) -> count
HeatCell = Dict[tuple, int]


@dataclass
class Heatmap:
    pipeline: str
    # failures[weekday][hour] = count
    failures: List[List[int]] = field(
        default_factory=lambda: [[0] * 24 for _ in range(7)]
    )
    total_runs: int = 0
    total_failures: int = 0


def _failed_runs(state: PipelineState) -> List[PipelineRun]:
    return [
        r for r in state.runs
        if r.status == "failure" and r.finished_at is not None
    ]


def build_heatmap(pipeline: str, state: PipelineState) -> Heatmap:
    """Build a failure heatmap from a pipeline's run history."""
    from datetime import datetime, timezone

    hm = Heatmap(pipeline=pipeline)
    hm.total_runs = len(state.runs)

    for run in _failed_runs(state):
        try:
            dt = datetime.fromisoformat(run.finished_at).astimezone(timezone.utc)
        except (ValueError, TypeError):
            continue
        hm.failures[dt.weekday()][dt.hour] += 1
        hm.total_failures += 1

    return hm


def peak_cell(hm: Heatmap) -> tuple | None:
    """Return (weekday, hour) of the highest failure count, or None if no failures."""
    if hm.total_failures == 0:
        return None
    best = (-1, -1)
    best_count = -1
    for wd in range(7):
        for hr in range(24):
            if hm.failures[wd][hr] > best_count:
                best_count = hm.failures[wd][hr]
                best = (wd, hr)
    return best


def format_heatmap(hm: Heatmap) -> str:
    """Render a compact ASCII heatmap (days as rows, hours as columns)."""
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    symbols = " ░▒▓█"
    if hm.total_failures == 0:
        return f"{hm.pipeline}: no failures recorded\n"

    max_val = max(hm.failures[wd][hr] for wd in range(7) for hr in range(24))

    lines = [f"Heatmap for '{hm.pipeline}' ({hm.total_failures} failures)"]
    lines.append("     " + "".join(f"{h:02d}" for h in range(0, 24, 2)))
    for wd in range(7):
        row = ""
        for hr in range(24):
            v = hm.failures[wd][hr]
            idx = 0 if v == 0 else max(1, round(v / max_val * (len(symbols) - 1)))
            row += symbols[idx] * (1 if hr % 2 else 1)
        # only show every-other hour label; row has 24 chars
        lines.append(f"{days[wd]}  {row}")
    return "\n".join(lines) + "\n"
