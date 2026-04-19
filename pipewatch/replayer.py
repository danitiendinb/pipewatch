"""Replayer: re-emit historical pipeline run events for testing or backfill."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional

from pipewatch.state import PipelineState, PipelineRun


@dataclass
class ReplayResult:
    pipeline: str
    replayed: int
    skipped: int


def _replay_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.replay.json"


def load_replayed_ids(state_dir: str, pipeline: str) -> set:
    p = _replay_path(state_dir, pipeline)
    if not p.exists():
        return set()
    return set(json.loads(p.read_text()))


def _save_replayed_ids(state_dir: str, pipeline: str, ids: set) -> None:
    p = _replay_path(state_dir, pipeline)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(sorted(ids)))


def replay_runs(
    store: PipelineState,
    state_dir: str,
    pipeline: str,
    handler,
    since: Optional[str] = None,
    dry_run: bool = False,
) -> ReplayResult:
    """Call handler(run) for each run not yet replayed."""
    state = store.load(pipeline)
    seen = load_replayed_ids(state_dir, pipeline)
    replayed = 0
    skipped = 0
    new_ids = set(seen)
    for run in state.runs:
        if since and run.finished_at and run.finished_at < since:
            skipped += 1
            continue
        if run.run_id in seen:
            skipped += 1
            continue
        handler(run)
        new_ids.add(run.run_id)
        replayed += 1
    if not dry_run:
        _save_replayed_ids(state_dir, pipeline, new_ids)
    return ReplayResult(pipeline=pipeline, replayed=replayed, skipped=skipped)


def clear_replay(state_dir: str, pipeline: str) -> None:
    p = _replay_path(state_dir, pipeline)
    if p.exists():
        p.unlink()
