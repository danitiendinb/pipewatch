"""Point-in-time snapshot of pipeline health for trend analysis."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import List, Dict, Any

from pipewatch.summarizer import build_health_report, HealthReport
from pipewatch.state import PipelineState


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _snapshot_path(state_dir: str) -> str:
    return os.path.join(state_dir, "snapshots.jsonl")


def take_snapshot(state_dir: str, pipelines: List[str]) -> Dict[str, Any]:
    """Build a health report snapshot and append it to the snapshot log."""
    store = PipelineState(state_dir)
    report = build_health_report(pipelines, store)
    snapshot = {
        "ts": _now_iso(),
        "total": report.total,
        "ok": report.ok,
        "failing": report.failing,
        "unknown": report.unknown,
        "pipelines": [
            {
                "name": s.name,
                "status": s.status,
                "consecutive_failures": s.consecutive_failures,
            }
            for s in report.summaries
        ],
    }
    path = _snapshot_path(state_dir)
    with open(path, "a") as fh:
        fh.write(json.dumps(snapshot) + "\n")
    return snapshot


def load_snapshots(state_dir: str) -> List[Dict[str, Any]]:
    """Return all stored snapshots in chronological order."""
    path = _snapshot_path(state_dir)
    if not os.path.exists(path):
        return []
    snapshots = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                snapshots.append(json.loads(line))
    return snapshots


def clear_snapshots(state_dir: str) -> None:
    """Remove the snapshot log."""
    path = _snapshot_path(state_dir)
    if os.path.exists(path):
        os.remove(path)
