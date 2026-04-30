"""Random sampling of pipeline runs for spot-check auditing."""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional

from pipewatch.state import PipelineState, PipelineRun


@dataclass
class SampleResult:
    pipeline: str
    run_id: str
    started_at: str
    finished_at: Optional[str]
    status: str
    message: str


def _sample_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / pipeline / "sample.json"


def sample_runs(
    state: PipelineState,
    pipeline: str,
    n: int = 5,
    seed: Optional[int] = None,
) -> List[SampleResult]:
    """Return up to *n* randomly sampled runs from *pipeline*."""
    runs: List[PipelineRun] = state.load(pipeline).runs
    if not runs:
        return []
    rng = random.Random(seed)
    chosen = rng.sample(runs, min(n, len(runs)))
    return [
        SampleResult(
            pipeline=pipeline,
            run_id=r.run_id,
            started_at=r.started_at,
            finished_at=r.finished_at,
            status=r.status,
            message=r.message,
        )
        for r in chosen
    ]


def save_sample(
    state_dir: str,
    pipeline: str,
    results: List[SampleResult],
) -> Path:
    """Persist a sample to disk and return the path."""
    path = _sample_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([asdict(r) for r in results], indent=2))
    return path


def load_sample(state_dir: str, pipeline: str) -> List[SampleResult]:
    """Load a previously saved sample; returns empty list if none exists."""
    path = _sample_path(state_dir, pipeline)
    if not path.exists():
        return []
    raw = json.loads(path.read_text())
    return [SampleResult(**item) for item in raw]


def clear_sample(state_dir: str, pipeline: str) -> None:
    """Remove a saved sample file if it exists."""
    path = _sample_path(state_dir, pipeline)
    if path.exists():
        path.unlink()
