"""merger.py – merge run histories from two pipeline state stores.

Useful when consolidating data from multiple pipewatch deployments or
reconstructing a state directory after a partial loss.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from .state import PipelineState, PipelineRun


@dataclass
class MergeResult:
    """Summary of a completed merge operation."""

    pipeline: str
    runs_added: int
    runs_skipped: int  # already present (duplicate run_id)
    total_after: int

    def __str__(self) -> str:
        return (
            f"{self.pipeline}: +{self.runs_added} added, "
            f"{self.runs_skipped} skipped, {self.total_after} total"
        )


def _run_ids(state: PipelineState) -> set:
    """Return the set of run_ids already present in *state*."""
    return {r.run_id for r in state.runs}


def merge_pipeline(
    pipeline: str,
    source_dir: Path,
    dest_dir: Path,
    *,
    keep_newest: Optional[int] = None,
) -> MergeResult:
    """Merge runs for *pipeline* from *source_dir* into *dest_dir*.

    Runs that already exist in the destination (matched by ``run_id``) are
    skipped so the operation is idempotent.  Optionally cap the merged history
    to the *keep_newest* most-recent runs after merging.

    Returns a :class:`MergeResult` describing what changed.
    """
    src_state = PipelineState(pipeline, source_dir)
    dst_state = PipelineState(pipeline, dest_dir)

    existing_ids = _run_ids(dst_state)
    added = 0
    skipped = 0

    for run in src_state.runs:
        if run.run_id in existing_ids:
            skipped += 1
            continue
        dst_state.runs.append(run)
        existing_ids.add(run.run_id)
        added += 1

    # Sort chronologically so the store stays ordered.
    dst_state.runs.sort(key=lambda r: r.started_at or "")

    if keep_newest is not None and keep_newest > 0:
        dst_state.runs = dst_state.runs[-keep_newest:]

    dst_state.save()

    return MergeResult(
        pipeline=pipeline,
        runs_added=added,
        runs_skipped=skipped,
        total_after=len(dst_state.runs),
    )


def merge_all(
    source_dir: Path,
    dest_dir: Path,
    *,
    keep_newest: Optional[int] = None,
) -> List[MergeResult]:
    """Merge every pipeline found in *source_dir* into *dest_dir*.

    Pipelines are discovered by scanning for ``*.json`` state files in
    *source_dir*.  The destination directory is created if it does not exist.

    Returns one :class:`MergeResult` per pipeline processed.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    results: List[MergeResult] = []

    for state_file in sorted(source_dir.glob("*.json")):
        # Skip non-pipeline files (e.g. silence, tags, …) – pipeline state
        # files contain a top-level "runs" key.
        try:
            data = json.loads(state_file.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        if "runs" not in data:
            continue

        pipeline_name = state_file.stem
        result = merge_pipeline(
            pipeline_name,
            source_dir,
            dest_dir,
            keep_newest=keep_newest,
        )
        results.append(result)

    return results
