"""Archive pipeline run history to a compressed JSON file."""
from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

from pipewatch.state import PipelineState
from pipewatch.exporter import state_to_records


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _archive_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / pipeline / "archive.jsonl.gz"


def archive_pipeline(state_dir: str, pipeline: str, store: PipelineState) -> Path:
    """Append current run records to the pipeline's gzip archive."""
    path = _archive_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    records = state_to_records(pipeline, store)
    with gzip.open(path, "at", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")
    return path


def load_archive(state_dir: str, pipeline: str) -> List[Dict[str, Any]]:
    """Load all archived records for a pipeline."""
    path = _archive_path(state_dir, pipeline)
    if not path.exists():
        return []
    records: List[Dict[str, Any]] = []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def clear_archive(state_dir: str, pipeline: str) -> None:
    """Remove the archive file for a pipeline."""
    path = _archive_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def archive_all(state_dir: str, pipelines: List[str], stores: Dict[str, PipelineState]) -> Dict[str, int]:
    """Archive all pipelines; return mapping of pipeline -> records written."""
    result: Dict[str, int] = {}
    for name in pipelines:
        if name in stores:
            archive_pipeline(state_dir, name, stores[name])
            result[name] = len(stores[name].runs)
    return result
