"""Pipeline dependency tracking — record and query upstream/downstream relationships."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List


def _dep_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / pipeline / "dependencies.json"


def load_dependencies(state_dir: str, pipeline: str) -> Dict[str, List[str]]:
    """Return {upstream: [...], downstream: [...]} for a pipeline."""
    path = _dep_path(state_dir, pipeline)
    if not path.exists():
        return {"upstream": [], "downstream": []}
    return json.loads(path.read_text())


def _save(state_dir: str, pipeline: str, data: Dict[str, List[str]]) -> None:
    path = _dep_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


def add_upstream(state_dir: str, pipeline: str, upstream: str) -> None:
    """Declare that *upstream* must succeed before *pipeline* runs."""
    data = load_dependencies(state_dir, pipeline)
    if upstream not in data["upstream"]:
        data["upstream"].append(upstream)
    _save(state_dir, pipeline, data)
    # mirror: pipeline is downstream of upstream
    up_data = load_dependencies(state_dir, upstream)
    if pipeline not in up_data["downstream"]:
        up_data["downstream"].append(pipeline)
    _save(state_dir, upstream, up_data)


def remove_upstream(state_dir: str, pipeline: str, upstream: str) -> None:
    data = load_dependencies(state_dir, pipeline)
    data["upstream"] = [u for u in data["upstream"] if u != upstream]
    _save(state_dir, pipeline, data)
    up_data = load_dependencies(state_dir, upstream)
    up_data["downstream"] = [d for d in up_data["downstream"] if d != pipeline]
    _save(state_dir, upstream, up_data)


def clear_dependencies(state_dir: str, pipeline: str) -> None:
    path = _dep_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def blocked_by_failures(
    state_dir: str, pipeline: str, failing_pipelines: List[str]
) -> List[str]:
    """Return the subset of upstream pipelines that are currently failing."""
    data = load_dependencies(state_dir, pipeline)
    return [u for u in data["upstream"] if u in failing_pipelines]
