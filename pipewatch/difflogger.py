"""difflogger.py – track config-level changes between pipewatch runs.

Records a snapshot of each pipeline's configuration hash on every check
and exposes a list of pipelines whose config has changed since the last
recorded snapshot.  Useful for auditing unexpected schedule or threshold
changes in long-running deployments.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from pipewatch.config import PipewatchConfig, PipelineConfig


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class ConfigDiff:
    """Describes a detected configuration change for a single pipeline."""

    pipeline: str
    previous_hash: str
    current_hash: str

    def __str__(self) -> str:  # pragma: no cover
        return (
            f"{self.pipeline}: {self.previous_hash[:8]} → {self.current_hash[:8]}"
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _difflog_path(state_dir: str) -> Path:
    """Return the path to the persisted config-hash log."""
    return Path(state_dir) / "_difflog.json"


def _hash_pipeline(cfg: PipelineConfig) -> str:
    """Produce a stable SHA-256 fingerprint for a pipeline's config fields."""
    payload = json.dumps(
        {
            "name": cfg.name,
            "schedule": cfg.schedule,
            "max_failures": cfg.max_failures,
        },
        sort_keys=True,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def load_difflog(state_dir: str) -> Dict[str, str]:
    """Load the persisted mapping of pipeline → config hash.

    Returns an empty dict when no log file exists yet.
    """
    path = _difflog_path(state_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def save_difflog(state_dir: str, hashes: Dict[str, str]) -> None:
    """Persist the mapping of pipeline → config hash to disk."""
    path = _difflog_path(state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(hashes, indent=2))


def clear_difflog(state_dir: str) -> None:
    """Remove the persisted diff log (useful for tests and resets)."""
    path = _difflog_path(state_dir)
    if path.exists():
        path.unlink()


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def detect_config_diffs(
    config: PipewatchConfig,
    state_dir: str,
) -> List[ConfigDiff]:
    """Compare current pipeline configs against the last saved hashes.

    Returns a list of :class:`ConfigDiff` objects – one per pipeline whose
    configuration has changed (or that is brand-new in the log).
    """
    previous = load_difflog(state_dir)
    diffs: List[ConfigDiff] = []

    for pipeline in config.pipelines:
        current_hash = _hash_pipeline(pipeline)
        prev_hash: Optional[str] = previous.get(pipeline.name)

        if prev_hash is None:
            # First time we've seen this pipeline – not a diff, just record it.
            continue

        if prev_hash != current_hash:
            diffs.append(
                ConfigDiff(
                    pipeline=pipeline.name,
                    previous_hash=prev_hash,
                    current_hash=current_hash,
                )
            )

    return diffs


def record_config_snapshot(config: PipewatchConfig, state_dir: str) -> None:
    """Persist the current config hashes so future runs can detect changes."""
    hashes = {p.name: _hash_pipeline(p) for p in config.pipelines}
    save_difflog(state_dir, hashes)
