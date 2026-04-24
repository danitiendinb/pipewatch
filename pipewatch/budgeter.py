"""budgeter.py – track and enforce per-pipeline failure-budget consumption.

A failure budget is defined as the maximum allowed failure rate over a rolling
window of N runs.  When the budget is exhausted the pipeline is considered
"budget-burned".
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pipewatch.state import PipelineState


@dataclass
class BudgetPolicy:
    max_failure_rate: float  # 0.0 – 1.0, e.g. 0.10 means 10 %
    window: int = 20         # number of recent runs to consider


@dataclass
class BudgetStatus:
    pipeline: str
    total_runs: int
    failures: int
    failure_rate: float
    budget: float            # max_failure_rate
    burned: bool
    remaining_failures: int  # how many more failures are allowed


def _budget_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.budget.json"


def save_budget_policy(state_dir: str, pipeline: str, policy: BudgetPolicy) -> None:
    path = _budget_path(state_dir, pipeline)
    path.write_text(json.dumps({"max_failure_rate": policy.max_failure_rate,
                                 "window": policy.window}))


def load_budget_policy(state_dir: str, pipeline: str) -> Optional[BudgetPolicy]:
    path = _budget_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return BudgetPolicy(max_failure_rate=data["max_failure_rate"],
                        window=data.get("window", 20))


def clear_budget_policy(state_dir: str, pipeline: str) -> None:
    path = _budget_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def evaluate_budget(state: PipelineState, policy: BudgetPolicy) -> BudgetStatus:
    """Compute the current budget status for *pipeline* given *state*."""
    runs = state.runs[-policy.window:] if state.runs else []
    total = len(runs)
    failures = sum(1 for r in runs if r.status != "ok")
    rate = failures / total if total > 0 else 0.0
    burned = rate > policy.max_failure_rate
    allowed = int(policy.max_failure_rate * total)
    remaining = max(0, allowed - failures)
    return BudgetStatus(
        pipeline=state.pipeline,
        total_runs=total,
        failures=failures,
        failure_rate=round(rate, 4),
        budget=policy.max_failure_rate,
        burned=burned,
        remaining_failures=remaining,
    )
