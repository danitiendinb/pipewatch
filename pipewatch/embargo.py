"""embargo.py — Time-window based run suppression for pipelines.

An embargo blocks a pipeline from being checked or alerted during
a defined recurring time window (e.g. maintenance windows).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _embargo_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.embargo.json"


@dataclass
class EmbargoWindow:
    """A recurring daily embargo window expressed in UTC HH:MM strings."""
    start_time: str   # e.g. "02:00"
    end_time: str     # e.g. "04:00"
    reason: str = ""


def load_embargo(state_dir: str, pipeline: str) -> Optional[EmbargoWindow]:
    path = _embargo_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return EmbargoWindow(**data)


def save_embargo(state_dir: str, pipeline: str, window: EmbargoWindow) -> None:
    path = _embargo_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(window)))


def clear_embargo(state_dir: str, pipeline: str) -> None:
    path = _embargo_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def is_embargoed(state_dir: str, pipeline: str, at: Optional[datetime] = None) -> bool:
    """Return True if *pipeline* is within its embargo window at *at* (UTC)."""
    window = load_embargo(state_dir, pipeline)
    if window is None:
        return False
    now = at or _now()
    current = now.strftime("%H:%M")
    start = window.start_time
    end = window.end_time
    if start <= end:
        return start <= current < end
    # Overnight window: e.g. 23:00 – 01:00
    return current >= start or current < end


def embargoed_pipelines(state_dir: str, pipelines: list[str],
                        at: Optional[datetime] = None) -> list[str]:
    """Return the subset of *pipelines* currently under embargo."""
    return [p for p in pipelines if is_embargoed(state_dir, p, at=at)]
