"""Pipeline renamer — rename a pipeline across state files."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List

_RENAME_LOG = "_rename_log.json"


def _rename_log_path(state_dir: str) -> Path:
    return Path(state_dir) / _RENAME_LOG


def _state_files_for(state_dir: str, old_name: str) -> List[Path]:
    """Return every state file whose stem matches old_name."""
    root = Path(state_dir)
    return [p for p in root.iterdir() if p.is_file() and p.stem == old_name]


def rename_pipeline(state_dir: str, old_name: str, new_name: str) -> List[str]:
    """Rename all state files from old_name to new_name.

    Returns a list of renamed file paths (new names).
    Raises ValueError if new_name already has any state files.
    """
    if not old_name or not new_name:
        raise ValueError("Pipeline names must be non-empty strings.")
    if old_name == new_name:
        return []

    root = Path(state_dir)
    root.mkdir(parents=True, exist_ok=True)

    conflicts = _state_files_for(state_dir, new_name)
    if conflicts:
        raise ValueError(
            f"Cannot rename: target '{new_name}' already has state files: "
            + ", ".join(str(c) for c in conflicts)
        )

    sources = _state_files_for(state_dir, old_name)
    renamed: List[str] = []
    for src in sources:
        dst = src.with_name(new_name + src.suffix)
        src.rename(dst)
        renamed.append(str(dst))

    _record_rename(state_dir, old_name, new_name, renamed)
    return renamed


def _record_rename(state_dir: str, old_name: str, new_name: str, files: List[str]) -> None:
    log_path = _rename_log_path(state_dir)
    entries: list = []
    if log_path.exists():
        with open(log_path) as fh:
            entries = json.load(fh)
    from pipewatch.state import now_iso
    entries.append({"from": old_name, "to": new_name, "files": files, "at": now_iso()})
    with open(log_path, "w") as fh:
        json.dump(entries, fh, indent=2)


def load_rename_log(state_dir: str) -> list:
    log_path = _rename_log_path(state_dir)
    if not log_path.exists():
        return []
    with open(log_path) as fh:
        return json.load(fh)


def clear_rename_log(state_dir: str) -> None:
    log_path = _rename_log_path(state_dir)
    if log_path.exists():
        os.remove(log_path)
