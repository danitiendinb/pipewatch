"""Audit log: record significant pipewatch events to a append-only JSONL file."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _audit_path(state_dir: str) -> Path:
    return Path(state_dir) / "audit.jsonl"


def record_event(
    state_dir: str,
    event: str,
    pipeline: Optional[str] = None,
    detail: Optional[str] = None,
) -> dict:
    """Append an audit event and return the entry."""
    entry = {
        "ts": _now_iso(),
        "event": event,
    }
    if pipeline is not None:
        entry["pipeline"] = pipeline
    if detail is not None:
        entry["detail"] = detail

    path = _audit_path(state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        fh.write(json.dumps(entry) + "\n")
    return entry


def load_audit(state_dir: str, pipeline: Optional[str] = None) -> List[dict]:
    """Load audit entries, optionally filtered by pipeline."""
    path = _audit_path(state_dir)
    if not path.exists():
        return []
    entries = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if pipeline is None or entry.get("pipeline") == pipeline:
                entries.append(entry)
    return entries


def clear_audit(state_dir: str) -> None:
    """Remove the audit log."""
    path = _audit_path(state_dir)
    if path.exists():
        path.unlink()
