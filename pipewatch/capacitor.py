"""capacitor.py – tracks and enforces concurrent run limits per pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _cap_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / pipeline / "capacitor.json"


@dataclass
class ActiveRun:
    run_id: str
    started_at: str


@dataclass
class CapacitorState:
    pipeline: str
    max_concurrent: int
    active_runs: List[ActiveRun] = field(default_factory=list)

    @property
    def current_count(self) -> int:
        return len(self.active_runs)

    @property
    def is_at_capacity(self) -> bool:
        return self.current_count >= self.max_concurrent


def load_capacitor(state_dir: str, pipeline: str, max_concurrent: int = 1) -> CapacitorState:
    path = _cap_path(state_dir, pipeline)
    if not path.exists():
        return CapacitorState(pipeline=pipeline, max_concurrent=max_concurrent)
    data = json.loads(path.read_text())
    runs = [ActiveRun(**r) for r in data.get("active_runs", [])]
    return CapacitorState(
        pipeline=data.get("pipeline", pipeline),
        max_concurrent=data.get("max_concurrent", max_concurrent),
        active_runs=runs,
    )


def _save_capacitor(state_dir: str, state: CapacitorState) -> None:
    path = _cap_path(state_dir, state.pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "pipeline": state.pipeline,
        "max_concurrent": state.max_concurrent,
        "active_runs": [{"run_id": r.run_id, "started_at": r.started_at} for r in state.active_runs],
    }, indent=2))


def acquire_slot(state_dir: str, pipeline: str, run_id: str, max_concurrent: int = 1) -> bool:
    """Attempt to register a new active run. Returns True if slot acquired."""
    state = load_capacitor(state_dir, pipeline, max_concurrent)
    if state.is_at_capacity:
        return False
    state.active_runs.append(ActiveRun(run_id=run_id, started_at=_now().isoformat()))
    _save_capacitor(state_dir, state)
    return True


def release_slot(state_dir: str, pipeline: str, run_id: str) -> bool:
    """Remove a run from the active set. Returns True if run was found."""
    state = load_capacitor(state_dir, pipeline)
    before = len(state.active_runs)
    state.active_runs = [r for r in state.active_runs if r.run_id != run_id]
    if len(state.active_runs) == before:
        return False
    _save_capacitor(state_dir, state)
    return True


def clear_capacitor(state_dir: str, pipeline: str) -> None:
    path = _cap_path(state_dir, pipeline)
    if path.exists():
        path.unlink()
