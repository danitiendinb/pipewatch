"""Run quota enforcement: limit how many runs are recorded per pipeline per day."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _quota_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / pipeline / "quota.json"


def load_quota(state_dir: str, pipeline: str) -> dict:
    path = _quota_path(state_dir, pipeline)
    if not path.exists():
        return {"date": _today_utc(), "count": 0}
    with path.open() as f:
        data = json.load(f)
    if data.get("date") != _today_utc():
        return {"date": _today_utc(), "count": 0}
    return data


def _save_quota(state_dir: str, pipeline: str, data: dict) -> None:
    path = _quota_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(data, f)


def increment_quota(state_dir: str, pipeline: str) -> int:
    """Increment today's run count and return the new count."""
    data = load_quota(state_dir, pipeline)
    data["count"] += 1
    _save_quota(state_dir, pipeline, data)
    return data["count"]


def is_over_quota(state_dir: str, pipeline: str, max_runs: int) -> bool:
    """Return True if today's run count has reached or exceeded max_runs."""
    data = load_quota(state_dir, pipeline)
    return data["count"] >= max_runs


def reset_quota(state_dir: str, pipeline: str) -> None:
    path = _quota_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def quota_status(state_dir: str, pipeline: str, max_runs: int) -> dict:
    data = load_quota(state_dir, pipeline)
    return {
        "pipeline": pipeline,
        "date": data["date"],
        "count": data["count"],
        "max_runs": max_runs,
        "over_quota": data["count"] >= max_runs,
    }
