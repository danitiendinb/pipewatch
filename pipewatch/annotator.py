"""Pipeline run annotation: attach free-text notes to pipeline runs."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional


def _note_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.notes.json"


def load_notes(state_dir: str, pipeline: str) -> Dict[str, str]:
    """Return mapping of run_id -> note text."""
    p = _note_path(state_dir, pipeline)
    if not p.exists():
        return {}
    with p.open() as fh:
        return json.load(fh)


def _save_notes(state_dir: str, pipeline: str, notes: Dict[str, str]) -> None:
    p = _note_path(state_dir, pipeline)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as fh:
        json.dump(notes, fh, indent=2)


def set_note(state_dir: str, pipeline: str, run_id: str, text: str) -> None:
    """Attach *text* to *run_id* for *pipeline*."""
    notes = load_notes(state_dir, pipeline)
    notes[run_id] = text
    _save_notes(state_dir, pipeline, notes)


def get_note(state_dir: str, pipeline: str, run_id: str) -> Optional[str]:
    """Return note for *run_id*, or None."""
    return load_notes(state_dir, pipeline).get(run_id)


def remove_note(state_dir: str, pipeline: str, run_id: str) -> bool:
    """Delete note for *run_id*. Returns True if a note existed."""
    notes = load_notes(state_dir, pipeline)
    if run_id not in notes:
        return False
    del notes[run_id]
    _save_notes(state_dir, pipeline, notes)
    return True


def annotated_runs(state_dir: str, pipeline: str) -> Dict[str, str]:
    """Alias for load_notes — returns all annotations for a pipeline."""
    return load_notes(state_dir, pipeline)
