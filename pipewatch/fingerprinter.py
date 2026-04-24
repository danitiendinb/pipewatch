"""Fingerprinter: generate and compare run signature hashes for change detection."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pipewatch.state import PipelineRun, PipelineState


@dataclass
class FingerprintRecord:
    pipeline: str
    run_id: str
    fingerprint: str
    changed: bool  # True when fingerprint differs from previous run


def _fp_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.fingerprint.json"


def _hash_run(run: PipelineRun) -> str:
    """Produce a stable SHA-256 hex digest for a run's observable fields."""
    payload = json.dumps(
        {
            "status": run.status,
            "message": run.message or "",
            "duration_seconds": run.duration_seconds,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def load_fingerprint(state_dir: str, pipeline: str) -> Optional[str]:
    """Return the stored fingerprint for the most recent run, or None."""
    path = _fp_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return data.get("fingerprint")


def save_fingerprint(state_dir: str, pipeline: str, fingerprint: str) -> None:
    path = _fp_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"pipeline": pipeline, "fingerprint": fingerprint}))


def clear_fingerprint(state_dir: str, pipeline: str) -> None:
    path = _fp_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def fingerprint_latest(state: PipelineState, state_dir: str) -> Optional[FingerprintRecord]:
    """Compute fingerprint for the most recent run and detect changes."""
    runs = state.runs
    if not runs:
        return None
    latest = runs[-1]
    current = _hash_run(latest)
    previous = load_fingerprint(state_dir, state.pipeline)
    changed = previous is None or current != previous
    save_fingerprint(state_dir, state.pipeline, current)
    return FingerprintRecord(
        pipeline=state.pipeline,
        run_id=latest.run_id,
        fingerprint=current,
        changed=changed,
    )


def fingerprint_all(
    states: list[PipelineState], state_dir: str
) -> list[FingerprintRecord]:
    """Return fingerprint records for all pipelines that have runs."""
    results = []
    for state in states:
        record = fingerprint_latest(state, state_dir)
        if record is not None:
            results.append(record)
    return results
