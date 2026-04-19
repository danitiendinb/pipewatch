"""Throttler: limit how often a pipeline can be checked."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _throttle_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.throttle.json"


def load_last_check(state_dir: str, pipeline: str) -> Optional[datetime]:
    p = _throttle_path(state_dir, pipeline)
    if not p.exists():
        return None
    data = json.loads(p.read_text())
    return datetime.fromisoformat(data["last_check"])


def record_check(state_dir: str, pipeline: str) -> datetime:
    p = _throttle_path(state_dir, pipeline)
    p.parent.mkdir(parents=True, exist_ok=True)
    ts = _now()
    p.write_text(json.dumps({"last_check": ts.isoformat()}))
    return ts


def clear_throttle(state_dir: str, pipeline: str) -> None:
    p = _throttle_path(state_dir, pipeline)
    if p.exists():
        p.unlink()


def is_throttled(state_dir: str, pipeline: str, min_interval_seconds: int) -> bool:
    """Return True if the pipeline was checked too recently."""
    last = load_last_check(state_dir, pipeline)
    if last is None:
        return False
    elapsed = (_now() - last).total_seconds()
    return elapsed < min_interval_seconds


def throttled_pipelines(
    state_dir: str, pipeline_names: list[str], min_interval_seconds: int
) -> list[str]:
    """Return names of pipelines currently throttled."""
    return [
        name
        for name in pipeline_names
        if is_throttled(state_dir, name, min_interval_seconds)
    ]
