"""Cooldown tracker — prevents repeated alerts within a configurable window."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _cooldown_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.cooldown.json"


def load_cooldown(state_dir: str, pipeline: str) -> Optional[datetime]:
    """Return the datetime when the cooldown expires, or None if not set."""
    path = _cooldown_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return datetime.fromisoformat(data["expires_at"])


def set_cooldown(state_dir: str, pipeline: str, minutes: int) -> datetime:
    """Set a cooldown that expires *minutes* from now. Returns the expiry time."""
    expires_at = _now() + timedelta(minutes=minutes)
    path = _cooldown_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"expires_at": expires_at.isoformat()}))
    return expires_at


def clear_cooldown(state_dir: str, pipeline: str) -> None:
    """Remove any active cooldown for *pipeline*."""
    path = _cooldown_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def is_cooling_down(state_dir: str, pipeline: str) -> bool:
    """Return True if the pipeline is still within its cooldown window."""
    expires_at = load_cooldown(state_dir, pipeline)
    if expires_at is None:
        return False
    return _now() < expires_at


def active_cooldowns(state_dir: str) -> dict[str, datetime]:
    """Return a mapping of pipeline name -> expiry for all active cooldowns."""
    result: dict[str, datetime] = []
    result = {}
    for path in Path(state_dir).glob("*.cooldown.json"):
        pipeline = path.name.replace(".cooldown.json", "")
        expires_at = load_cooldown(state_dir, pipeline)
        if expires_at is not None and _now() < expires_at:
            result[pipeline] = expires_at
    return result
