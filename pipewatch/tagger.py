"""Pipeline run tagging — attach arbitrary key/value metadata to runs."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional


def _tag_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.tags.json"


def load_tags(state_dir: str, pipeline: str) -> Dict[str, str]:
    """Return the current tag dict for a pipeline (empty if none set)."""
    p = _tag_path(state_dir, pipeline)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def set_tag(state_dir: str, pipeline: str, key: str, value: str) -> Dict[str, str]:
    """Set a single tag and persist; returns updated tag dict."""
    tags = load_tags(state_dir, pipeline)
    tags[key] = value
    _write(state_dir, pipeline, tags)
    return tags


def remove_tag(state_dir: str, pipeline: str, key: str) -> Dict[str, str]:
    """Remove a tag by key (no-op if absent); returns updated tag dict."""
    tags = load_tags(state_dir, pipeline)
    tags.pop(key, None)
    _write(state_dir, pipeline, tags)
    return tags


def clear_tags(state_dir: str, pipeline: str) -> None:
    """Delete all tags for a pipeline."""
    p = _tag_path(state_dir, pipeline)
    if p.exists():
        p.unlink()


def get_tag(state_dir: str, pipeline: str, key: str) -> Optional[str]:
    """Return a single tag value or None."""
    return load_tags(state_dir, pipeline).get(key)


def _write(state_dir: str, pipeline: str, tags: Dict[str, str]) -> None:
    p = _tag_path(state_dir, pipeline)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(tags, indent=2))
