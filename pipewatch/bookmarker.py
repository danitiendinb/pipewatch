"""Bookmarker – persist a named cursor position for incremental pipeline runs.

Each pipeline can store a single bookmark (e.g. a timestamp, offset, or
sequence number) so that the next run knows where to resume from.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class Bookmark:
    pipeline: str
    value: str
    updated_at: str  # ISO-8601


def _bookmark_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.bookmark.json"


def load_bookmark(state_dir: str, pipeline: str) -> Optional[Bookmark]:
    """Return the stored bookmark, or *None* if none has been set."""
    path = _bookmark_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return Bookmark(
        pipeline=data["pipeline"],
        value=data["value"],
        updated_at=data["updated_at"],
    )


def set_bookmark(state_dir: str, pipeline: str, value: str) -> Bookmark:
    """Persist *value* as the current bookmark for *pipeline*."""
    updated_at = datetime.now(timezone.utc).isoformat()
    bm = Bookmark(pipeline=pipeline, value=value, updated_at=updated_at)
    path = _bookmark_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"pipeline": bm.pipeline, "value": bm.value, "updated_at": bm.updated_at})
    )
    return bm


def clear_bookmark(state_dir: str, pipeline: str) -> None:
    """Remove any stored bookmark for *pipeline*."""
    path = _bookmark_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def all_bookmarks(state_dir: str) -> list[Bookmark]:
    """Return every bookmark found in *state_dir*."""
    results: list[Bookmark] = []
    for p in Path(state_dir).glob("*.bookmark.json"):
        data = json.loads(p.read_text())
        results.append(
            Bookmark(
                pipeline=data["pipeline"],
                value=data["value"],
                updated_at=data["updated_at"],
            )
        )
    return sorted(results, key=lambda b: b.pipeline)
