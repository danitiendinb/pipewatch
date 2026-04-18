"""Acknowledgement of pipeline alerts — suppress alerts until next failure."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ack_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.ack.json"


def acknowledge(state_dir: str, pipeline: str, message: str = "") -> None:
    """Acknowledge a pipeline, suppressing alerts until the next failure."""
    path = _ack_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {"acknowledged_at": _now_iso(), "message": message}
    path.write_text(json.dumps(data))


def clear_acknowledgement(state_dir: str, pipeline: str) -> None:
    """Remove acknowledgement so alerts fire again."""
    path = _ack_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def is_acknowledged(state_dir: str, pipeline: str) -> bool:
    """Return True if the pipeline is currently acknowledged."""
    return _ack_path(state_dir, pipeline).exists()


def load_acknowledgement(state_dir: str, pipeline: str) -> dict | None:
    """Return acknowledgement metadata or None if not acknowledged."""
    path = _ack_path(state_dir, pipeline)
    if not path.exists():
        return None
    return json.loads(path.read_text())
