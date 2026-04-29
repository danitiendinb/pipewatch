"""Pauser — temporarily pause a pipeline from being checked or alerted."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _pause_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.pause.json"


def load_pause(state_dir: str, pipeline: str) -> Optional[datetime]:
    """Return the pause expiry datetime, or None if not paused."""
    p = _pause_path(state_dir, pipeline)
    if not p.exists():
        return None
    data = json.loads(p.read_text())
    return datetime.fromisoformat(data["expires_at"])


def pause_pipeline(state_dir: str, pipeline: str, hours: float) -> datetime:
    """Pause a pipeline for *hours* hours. Returns the expiry datetime."""
    from datetime import timedelta

    expires_at = _now() + timedelta(hours=hours)
    p = _pause_path(state_dir, pipeline)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({
        "pipeline": pipeline,
        "paused_at": _now().isoformat(),
        "expires_at": expires_at.isoformat(),
        "hours": hours,
    }))
    return expires_at


def clear_pause(state_dir: str, pipeline: str) -> None:
    """Remove the pause record for a pipeline."""
    p = _pause_path(state_dir, pipeline)
    if p.exists():
        p.unlink()


def is_paused(state_dir: str, pipeline: str) -> bool:
    """Return True if the pipeline is currently paused."""
    expiry = load_pause(state_dir, pipeline)
    if expiry is None:
        return False
    if _now() >= expiry:
        clear_pause(state_dir, pipeline)
        return False
    return True


def paused_pipelines(state_dir: str, pipelines: list[str]) -> list[str]:
    """Return the subset of *pipelines* that are currently paused."""
    return [p for p in pipelines if is_paused(state_dir, p)]
