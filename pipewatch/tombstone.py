"""Tombstone: mark a pipeline as permanently decommissioned."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

from pipewatch.state import PipelineState


@dataclass
class TombstoneRecord:
    pipeline: str
    reason: str
    tombstoned_at: str
    tombstoned_by: Optional[str] = None


def _tombstone_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.tombstone.json"


def load_tombstone(state_dir: str, pipeline: str) -> Optional[TombstoneRecord]:
    """Return the TombstoneRecord for *pipeline*, or None if not tombstoned."""
    path = _tombstone_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return TombstoneRecord(**data)


def set_tombstone(
    state_dir: str,
    pipeline: str,
    reason: str,
    tombstoned_at: str,
    tombstoned_by: Optional[str] = None,
) -> TombstoneRecord:
    """Write a tombstone file for *pipeline* and return the record."""
    record = TombstoneRecord(
        pipeline=pipeline,
        reason=reason,
        tombstoned_at=tombstoned_at,
        tombstoned_by=tombstoned_by,
    )
    path = _tombstone_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(record), indent=2))
    return record


def clear_tombstone(state_dir: str, pipeline: str) -> bool:
    """Remove the tombstone for *pipeline*. Returns True if a file was removed."""
    path = _tombstone_path(state_dir, pipeline)
    if path.exists():
        path.unlink()
        return True
    return False


def is_tombstoned(state_dir: str, pipeline: str) -> bool:
    """Return True if *pipeline* has an active tombstone."""
    return _tombstone_path(state_dir, pipeline).exists()


def list_tombstoned(state_dir: str) -> list[str]:
    """Return sorted names of all tombstoned pipelines under *state_dir*."""
    root = Path(state_dir)
    if not root.exists():
        return []
    return sorted(
        p.name.replace(".tombstone.json", "")
        for p in root.glob("*.tombstone.json")
    )
