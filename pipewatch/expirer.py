"""expirer.py – mark pipelines as expired when they exceed a TTL without a
successful run.  An expired pipeline is flagged so dashboards and alerts can
distinguish "never ran" from "ran but is now stale beyond its declared TTL".
"""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pipewatch.state import PipelineState


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _expiry_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.expiry.json"


@dataclass
class ExpiryRecord:
    pipeline: str
    ttl_hours: float
    recorded_at: str
    expires_at: str
    expired: bool


def load_expiry(state_dir: str, pipeline: str) -> Optional[ExpiryRecord]:
    path = _expiry_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return ExpiryRecord(**data)


def save_expiry(state_dir: str, pipeline: str, ttl_hours: float) -> ExpiryRecord:
    now = _now()
    from datetime import timedelta
    expires_at = now + timedelta(hours=ttl_hours)
    record = ExpiryRecord(
        pipeline=pipeline,
        ttl_hours=ttl_hours,
        recorded_at=now.isoformat(),
        expires_at=expires_at.isoformat(),
        expired=False,
    )
    path = _expiry_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(record)))
    return record


def clear_expiry(state_dir: str, pipeline: str) -> None:
    _expiry_path(state_dir, pipeline).unlink(missing_ok=True)


def is_expired(state_dir: str, pipeline: str, store: PipelineState) -> bool:
    """Return True when the pipeline has an expiry policy and the last
    successful run occurred before *expires_at* (or there is no success at
    all)."""
    record = load_expiry(state_dir, pipeline)
    if record is None:
        return False
    expires_at = datetime.fromisoformat(record.expires_at)
    runs = store.load(pipeline).runs
    successful = [r for r in runs if r.status == "ok"]
    if not successful:
        return True
    last_ok = max(datetime.fromisoformat(r.finished_at) for r in successful if r.finished_at)
    return last_ok < expires_at and _now() > expires_at


def expired_pipelines(state_dir: str, pipelines: list[str], store: PipelineState) -> list[str]:
    return [p for p in pipelines if is_expired(state_dir, p, store)]
