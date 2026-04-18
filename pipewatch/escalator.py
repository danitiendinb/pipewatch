"""Escalation policy: track how long a pipeline has been failing and
return an escalation level based on configured thresholds."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Optional


@dataclass
class EscalationPolicy:
    levels: list[int] = field(default_factory=lambda: [1, 3, 10])  # failure counts


@dataclass
class EscalationState:
    pipeline: str
    level: int = 0
    last_escalated_at: Optional[str] = None


def _esc_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.escalation.json"


def load_escalation(state_dir: str, pipeline: str) -> EscalationState:
    p = _esc_path(state_dir, pipeline)
    if not p.exists():
        return EscalationState(pipeline=pipeline)
    data = json.loads(p.read_text())
    return EscalationState(
        pipeline=data["pipeline"],
        level=data.get("level", 0),
        last_escalated_at=data.get("last_escalated_at"),
    )


def save_escalation(state_dir: str, state: EscalationState) -> None:
    p = _esc_path(state_dir, state.pipeline)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({
        "pipeline": state.pipeline,
        "level": state.level,
        "last_escalated_at": state.last_escalated_at,
    }))


def clear_escalation(state_dir: str, pipeline: str) -> None:
    p = _esc_path(state_dir, pipeline)
    if p.exists():
        p.unlink()


def evaluate_escalation(
    state_dir: str,
    pipeline: str,
    consecutive_failures: int,
    policy: EscalationPolicy,
) -> int:
    """Return the current escalation level (0-based index into policy.levels).
    Persists updated state. Returns -1 if no escalation applies."""
    esc = load_escalation(state_dir, pipeline)
    new_level = -1
    for idx, threshold in enumerate(policy.levels):
        if consecutive_failures >= threshold:
            new_level = idx
    if new_level != esc.level:
        esc.level = new_level
        esc.last_escalated_at = datetime.now(timezone.utc).isoformat()
        save_escalation(state_dir, esc)
    return new_level
