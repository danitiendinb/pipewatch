"""Mirror module: replicate pipeline state snapshots to a remote destination."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

from pipewatch.snapshotter import load_snapshots

log = logging.getLogger(__name__)


@dataclass
class MirrorRecord:
    pipeline: str
    destination: str
    last_mirrored: Optional[str] = None
    snapshot_count: int = 0


def _mirror_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.mirror.json"


def load_mirror(state_dir: str, pipeline: str) -> Optional[MirrorRecord]:
    path = _mirror_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return MirrorRecord(**data)


def save_mirror(state_dir: str, record: MirrorRecord) -> None:
    path = _mirror_path(state_dir, record.pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(record), indent=2))


def clear_mirror(state_dir: str, pipeline: str) -> None:
    path = _mirror_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def mirror_pipeline(
    state_dir: str,
    pipeline: str,
    destination: str,
    now_iso: str,
) -> MirrorRecord:
    """Record a mirror operation for a pipeline and return updated record."""
    snapshots = load_snapshots(state_dir, pipeline)
    record = MirrorRecord(
        pipeline=pipeline,
        destination=destination,
        last_mirrored=now_iso,
        snapshot_count=len(snapshots),
    )
    save_mirror(state_dir, record)
    log.debug("Mirrored %s -> %s (%d snapshots)", pipeline, destination, len(snapshots))
    return record


def mirror_all(
    state_dir: str,
    pipelines: list[str],
    destination: str,
    now_iso: str,
) -> list[MirrorRecord]:
    """Mirror all listed pipelines to the destination."""
    return [mirror_pipeline(state_dir, p, destination, now_iso) for p in pipelines]
