"""Suspender: temporarily suspend a pipeline from being checked or alerted.

A suspended pipeline is excluded from checker runs and alert dispatch
until the suspension expires or is explicitly lifted.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _suspend_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / pipeline / "suspend.json"


def load_suspension(state_dir: str, pipeline: str) -> Optional[datetime]:
    """Return the suspension expiry datetime, or None if not suspended."""
    path = _suspend_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return datetime.fromisoformat(data["until"])


def suspend_pipeline(
    state_dir: str, pipeline: str, hours: float
) -> datetime:
    """Suspend *pipeline* for *hours* hours. Returns the expiry datetime."""
    from datetime import timedelta

    expiry = _now() + timedelta(hours=hours)
    path = _suspend_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "until": expiry.isoformat(),
                "suspended_at": _now().isoformat(),
            }
        )
    )
    return expiry


def clear_suspension(state_dir: str, pipeline: str) -> None:
    """Remove any active suspension for *pipeline*."""
    path = _suspend_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def is_suspended(state_dir: str, pipeline: str) -> bool:
    """Return True if *pipeline* is currently suspended."""
    expiry = load_suspension(state_dir, pipeline)
    if expiry is None:
        return False
    if _now() >= expiry:
        # Expired — clean up automatically
        clear_suspension(state_dir, pipeline)
        return False
    return True


def active_suspensions(state_dir: str, pipelines: list[str]) -> list[str]:
    """Return the subset of *pipelines* that are currently suspended."""
    return [p for p in pipelines if is_suspended(state_dir, p)]
