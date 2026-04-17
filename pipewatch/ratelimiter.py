"""Rate-limiting for alerts: suppress repeated alerts within a cooldown window."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _rl_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.ratelimit.json"


def load_last_alert(state_dir: str, pipeline: str) -> Optional[datetime]:
    path = _rl_path(state_dir, pipeline)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return datetime.fromisoformat(data["last_alert"])
    except (KeyError, ValueError, json.JSONDecodeError):
        return None


def record_alert(state_dir: str, pipeline: str) -> None:
    os.makedirs(state_dir, exist_ok=True)
    path = _rl_path(state_dir, pipeline)
    path.write_text(json.dumps({"last_alert": _now().isoformat()}))


def clear_ratelimit(state_dir: str, pipeline: str) -> None:
    path = _rl_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def is_rate_limited(state_dir: str, pipeline: str, cooldown_minutes: int) -> bool:
    """Return True if an alert was already sent within the cooldown window."""
    last = load_last_alert(state_dir, pipeline)
    if last is None:
        return False
    elapsed = (_now() - last).total_seconds() / 60
    return elapsed < cooldown_minutes
