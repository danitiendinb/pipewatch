"""Lockout: temporarily block a pipeline from being checked after repeated rapid failures."""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _lockout_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.lockout.json"


def load_lockout(state_dir: str, pipeline: str) -> Optional[datetime]:
    """Return the lockout expiry datetime, or None if not locked out."""
    path = _lockout_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return datetime.fromisoformat(data["expires_at"])


def set_lockout(state_dir: str, pipeline: str, duration_minutes: int = 30) -> datetime:
    """Lock out a pipeline for *duration_minutes* minutes. Returns expiry time."""
    expires_at = _now() + timedelta(minutes=duration_minutes)
    path = _lockout_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "pipeline": pipeline,
        "locked_at": _now().isoformat(),
        "expires_at": expires_at.isoformat(),
        "duration_minutes": duration_minutes,
    }))
    return expires_at


def clear_lockout(state_dir: str, pipeline: str) -> None:
    """Remove the lockout file for a pipeline."""
    path = _lockout_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def is_locked_out(state_dir: str, pipeline: str) -> bool:
    """Return True if the pipeline is currently locked out."""
    expires_at = load_lockout(state_dir, pipeline)
    if expires_at is None:
        return False
    if _now() < expires_at:
        return True
    # Expired — clean up automatically
    clear_lockout(state_dir, pipeline)
    return False


def locked_out_pipelines(state_dir: str, pipelines: list[str]) -> list[str]:
    """Return subset of *pipelines* that are currently locked out."""
    return [p for p in pipelines if is_locked_out(state_dir, p)]
