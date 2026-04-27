"""Gatekeeper: block pipeline execution based on health conditions."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from pipewatch.state import PipelineState
from pipewatch.scorer import score_pipeline
from pipewatch.summarizer import summarize_pipeline


@dataclass
class GatePolicy:
    min_score: float = 0.0
    max_consecutive_failures: int = 0  # 0 = disabled
    require_status: Optional[str] = None  # "ok", "failing", or None


@dataclass
class GateDecision:
    pipeline: str
    allowed: bool
    reasons: List[str] = field(default_factory=list)


def _gate_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.gate.json"


def save_gate_policy(state_dir: str, pipeline: str, policy: GatePolicy) -> None:
    path = _gate_path(state_dir, pipeline)
    path.write_text(
        json.dumps(
            {
                "min_score": policy.min_score,
                "max_consecutive_failures": policy.max_consecutive_failures,
                "require_status": policy.require_status,
            }
        )
    )


def load_gate_policy(state_dir: str, pipeline: str) -> Optional[GatePolicy]:
    path = _gate_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return GatePolicy(
        min_score=data.get("min_score", 0.0),
        max_consecutive_failures=data.get("max_consecutive_failures", 0),
        require_status=data.get("require_status"),
    )


def clear_gate_policy(state_dir: str, pipeline: str) -> None:
    path = _gate_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def evaluate_gate(state_dir: str, pipeline: str, state: PipelineState) -> GateDecision:
    """Return a GateDecision for the given pipeline state."""
    policy = load_gate_policy(state_dir, pipeline)
    if policy is None:
        return GateDecision(pipeline=pipeline, allowed=True)

    reasons: List[str] = []

    summary = summarize_pipeline(pipeline, state)
    scored = score_pipeline(summary)

    if policy.min_score > 0.0 and scored.score < policy.min_score:
        reasons.append(
            f"score {scored.score:.1f} below minimum {policy.min_score:.1f}"
        )

    if (
        policy.max_consecutive_failures > 0
        and state.consecutive_failures >= policy.max_consecutive_failures
    ):
        reasons.append(
            f"consecutive failures {state.consecutive_failures} "
            f">= limit {policy.max_consecutive_failures}"
        )

    if policy.require_status and summary.status != policy.require_status:
        reasons.append(
            f"status '{summary.status}' does not match required '{policy.require_status}'"
        )

    return GateDecision(pipeline=pipeline, allowed=len(reasons) == 0, reasons=reasons)
