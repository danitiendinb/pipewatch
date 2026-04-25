"""Checkpoint tracking — record and retrieve named progress markers for pipelines."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class Checkpoint:
    name: str
    recorded_at: str
    metadata: dict


def _checkpoint_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.checkpoints.json"


def load_checkpoints(state_dir: str, pipeline: str) -> dict[str, Checkpoint]:
    """Return all checkpoints for *pipeline* keyed by checkpoint name."""
    path = _checkpoint_path(state_dir, pipeline)
    if not path.exists():
        return {}
    raw = json.loads(path.read_text())
    return {k: Checkpoint(**v) for k, v in raw.items()}


def _save_checkpoints(state_dir: str, pipeline: str, data: dict[str, Checkpoint]) -> None:
    path = _checkpoint_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({k: asdict(v) for k, v in data.items()}, indent=2))


def set_checkpoint(
    state_dir: str,
    pipeline: str,
    name: str,
    metadata: Optional[dict] = None,
) -> Checkpoint:
    """Create or overwrite a named checkpoint for *pipeline*."""
    cp = Checkpoint(
        name=name,
        recorded_at=datetime.now(timezone.utc).isoformat(),
        metadata=metadata or {},
    )
    data = load_checkpoints(state_dir, pipeline)
    data[name] = cp
    _save_checkpoints(state_dir, pipeline, data)
    return cp


def get_checkpoint(state_dir: str, pipeline: str, name: str) -> Optional[Checkpoint]:
    """Return a single checkpoint by name, or *None* if not found."""
    return load_checkpoints(state_dir, pipeline).get(name)


def remove_checkpoint(state_dir: str, pipeline: str, name: str) -> bool:
    """Delete a named checkpoint.  Returns True if it existed."""
    data = load_checkpoints(state_dir, pipeline)
    if name not in data:
        return False
    del data[name]
    _save_checkpoints(state_dir, pipeline, data)
    return True


def clear_checkpoints(state_dir: str, pipeline: str) -> None:
    """Remove all checkpoints for *pipeline*."""
    _checkpoint_path(state_dir, pipeline).unlink(missing_ok=True)
