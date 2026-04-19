"""Deduplicator: suppress duplicate alerts within a time window."""
from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _dedup_path(state_dir: str, pipeline: str) -> Path:
    safe = pipeline.replace("/", "_")
    return Path(state_dir) / f"{safe}.dedup.json"


def _fingerprint(pipeline: str, message: str) -> str:
    raw = f"{pipeline}:{message}"
    return hashlib.sha1(raw.encode()).hexdigest()


def load_dedup(state_dir: str, pipeline: str) -> dict:
    p = _dedup_path(state_dir, pipeline)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_dedup(state_dir: str, pipeline: str, data: dict) -> None:
    p = _dedup_path(state_dir, pipeline)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2))


def is_duplicate(state_dir: str, pipeline: str, message: str, window_minutes: int = 60) -> bool:
    """Return True if an identical alert was already sent within the window."""
    fp = _fingerprint(pipeline, message)
    data = load_dedup(state_dir, pipeline)
    entry = data.get(fp)
    if entry is None:
        return False
    sent_at = datetime.fromisoformat(entry["sent_at"])
    return _now() - sent_at < timedelta(minutes=window_minutes)


def record_sent(state_dir: str, pipeline: str, message: str) -> str:
    """Record that an alert was sent; return the fingerprint."""
    fp = _fingerprint(pipeline, message)
    data = load_dedup(state_dir, pipeline)
    data[fp] = {"message": message, "sent_at": _now().isoformat()}
    _save_dedup(state_dir, pipeline, data)
    return fp


def clear_dedup(state_dir: str, pipeline: str) -> None:
    _dedup_path(state_dir, pipeline).unlink(missing_ok=True)
