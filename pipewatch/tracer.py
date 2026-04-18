"""Execution trace log: record and retrieve per-run structured events."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _trace_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.trace.json"


def load_traces(state_dir: str, pipeline: str) -> List[Dict[str, Any]]:
    p = _trace_path(state_dir, pipeline)
    if not p.exists():
        return []
    return json.loads(p.read_text())


def _save_traces(state_dir: str, pipeline: str, traces: List[Dict[str, Any]]) -> None:
    p = _trace_path(state_dir, pipeline)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(traces, indent=2))


def add_event(
    state_dir: str,
    pipeline: str,
    run_id: str,
    event: str,
    detail: str = "",
) -> Dict[str, Any]:
    """Append a trace event for a specific run and return it."""
    traces = load_traces(state_dir, pipeline)
    entry: Dict[str, Any] = {
        "timestamp": _now_iso(),
        "run_id": run_id,
        "event": event,
        "detail": detail,
    }
    traces.append(entry)
    _save_traces(state_dir, pipeline, traces)
    return entry


def get_run_traces(state_dir: str, pipeline: str, run_id: str) -> List[Dict[str, Any]]:
    """Return all trace events for a given run_id."""
    return [t for t in load_traces(state_dir, pipeline) if t["run_id"] == run_id]


def clear_traces(state_dir: str, pipeline: str) -> None:
    p = _trace_path(state_dir, pipeline)
    if p.exists():
        p.unlink()
