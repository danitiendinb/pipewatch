"""Silence (suppress) alerts for a pipeline for a given duration."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def silence_path(state_dir: str, pipeline: str) -> str:
    safe = pipeline.replace("/", "_")
    return os.path.join(state_dir, f"{safe}.silence.json")


def set_silence(state_dir: str, pipeline: str, until: datetime) -> None:
    """Write a silence record for *pipeline* expiring at *until*."""
    path = silence_path(state_dir, pipeline)
    os.makedirs(state_dir, exist_ok=True)
    with open(path, "w") as fh:
        json.dump({"until": until.isoformat()}, fh)


def clear_silence(state_dir: str, pipeline: str) -> None:
    """Remove any existing silence record for *pipeline*."""
    path = silence_path(state_dir, pipeline)
    if os.path.exists(path):
        os.remove(path)


def is_silenced(state_dir: str, pipeline: str) -> bool:
    """Return True if *pipeline* is currently silenced."""
    path = silence_path(state_dir, pipeline)
    if not os.path.exists(path):
        return False
    with open(path) as fh:
        data = json.load(fh)
    until = datetime.fromisoformat(data["until"])
    if _now() < until:
        return True
    os.remove(path)
    return False


def silence_until(state_dir: str, pipeline: str) -> Optional[datetime]:
    """Return the expiry datetime if silenced, else None."""
    path = silence_path(state_dir, pipeline)
    if not os.path.exists(path):
        return None
    with open(path) as fh:
        data = json.load(fh)
    until = datetime.fromisoformat(data["until"])
    return until if _now() < until else None
