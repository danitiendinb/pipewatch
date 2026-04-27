"""Sentinel: mark pipelines as critical and enforce zero-tolerance failure policy."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

from pipewatch.state import PipelineState


@dataclass
class SentinelPolicy:
    enabled: bool = False
    max_failures: int = 0  # 0 = zero tolerance
    notify_on_first: bool = True


@dataclass
class SentinelViolation:
    pipeline: str
    consecutive_failures: int
    max_allowed: int

    def __str__(self) -> str:
        return (
            f"{self.pipeline}: {self.consecutive_failures} consecutive failure(s) "
            f"(max allowed: {self.max_allowed})"
        )


def _sentinel_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / pipeline / "sentinel.json"


def load_sentinel_policy(state_dir: str, pipeline: str) -> Optional[SentinelPolicy]:
    path = _sentinel_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return SentinelPolicy(**data)


def save_sentinel_policy(state_dir: str, pipeline: str, policy: SentinelPolicy) -> None:
    path = _sentinel_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(policy), indent=2))


def clear_sentinel_policy(state_dir: str, pipeline: str) -> None:
    path = _sentinel_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def evaluate_sentinel(
    pipeline: str, state: PipelineState, policy: SentinelPolicy
) -> Optional[SentinelViolation]:
    """Return a SentinelViolation if the pipeline breaches the policy, else None."""
    if not policy.enabled:
        return None
    failures = state.consecutive_failures
    if failures > policy.max_failures:
        return SentinelViolation(
            pipeline=pipeline,
            consecutive_failures=failures,
            max_allowed=policy.max_failures,
        )
    return None


def sentinel_violations(
    pipelines: list[str], state_dir: str, states: dict[str, PipelineState]
) -> list[SentinelViolation]:
    """Return all sentinel violations across the given pipelines."""
    violations: list[SentinelViolation] = []
    for name in pipelines:
        policy = load_sentinel_policy(state_dir, name)
        if policy is None:
            continue
        state = states.get(name)
        if state is None:
            continue
        v = evaluate_sentinel(name, state, policy)
        if v is not None:
            violations.append(v)
    return violations
