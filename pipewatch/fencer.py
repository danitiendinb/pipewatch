"""fencer.py — time-window based execution fencing for pipelines.

A fence prevents a pipeline from being checked or alerted during
defined time windows (e.g. maintenance windows, known downtime).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _fence_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.fence.json"


@dataclass
class FenceWindow:
    start_iso: str   # ISO-8601 UTC
    end_iso: str     # ISO-8601 UTC
    reason: str = ""

    @property
    def start(self) -> datetime:
        return datetime.fromisoformat(self.start_iso)

    @property
    def end(self) -> datetime:
        return datetime.fromisoformat(self.end_iso)


def load_fence(state_dir: str, pipeline: str) -> Optional[FenceWindow]:
    path = _fence_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return FenceWindow(**data)


def save_fence(state_dir: str, pipeline: str, window: FenceWindow) -> None:
    path = _fence_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(window)))


def clear_fence(state_dir: str, pipeline: str) -> None:
    path = _fence_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def is_fenced(state_dir: str, pipeline: str, at: Optional[datetime] = None) -> bool:
    """Return True if *pipeline* is inside an active fence window."""
    window = load_fence(state_dir, pipeline)
    if window is None:
        return False
    now = at if at is not None else _now()
    return window.start <= now <= window.end


def active_fences(state_dir: str, pipelines: list[str]) -> list[str]:
    """Return names of pipelines that are currently fenced."""
    return [p for p in pipelines if is_fenced(state_dir, p)]
