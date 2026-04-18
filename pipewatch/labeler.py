"""Attach and query environment labels (e.g. prod/staging) for pipelines."""
from __future__ import annotations

import json
from pathlib import Path

_DEFAULT_LABEL = "default"


def _label_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / pipeline / "label.json"


def load_label(state_dir: str, pipeline: str) -> str:
    path = _label_path(state_dir, pipeline)
    if not path.exists():
        return _DEFAULT_LABEL
    try:
        data = json.loads(path.read_text())
        return data.get("label", _DEFAULT_LABEL)
    except (json.JSONDecodeError, OSError):
        return _DEFAULT_LABEL


def set_label(state_dir: str, pipeline: str, label: str) -> None:
    path = _label_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"label": label}))


def clear_label(state_dir: str, pipeline: str) -> None:
    path = _label_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def pipelines_by_label(state_dir: str, pipelines: list[str], label: str) -> list[str]:
    """Return pipelines whose stored label matches *label*."""
    return [p for p in pipelines if load_label(state_dir, p) == label]
