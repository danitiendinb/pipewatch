"""pinner.py – pin a pipeline version/tag for deployment tracking."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional


@dataclass
class PinRecord:
    pipeline: str
    version: str
    pinned_at: str
    pinned_by: str = "unknown"
    note: str = ""


def _pin_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.pin.json"


def load_pin(state_dir: str, pipeline: str) -> Optional[PinRecord]:
    path = _pin_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return PinRecord(**data)


def set_pin(
    state_dir: str,
    pipeline: str,
    version: str,
    pinned_by: str = "unknown",
    note: str = "",
) -> PinRecord:
    from pipewatch.state import now_iso

    record = PinRecord(
        pipeline=pipeline,
        version=version,
        pinned_at=now_iso(),
        pinned_by=pinned_by,
        note=note,
    )
    _pin_path(state_dir, pipeline).write_text(json.dumps(asdict(record), indent=2))
    return record


def clear_pin(state_dir: str, pipeline: str) -> bool:
    path = _pin_path(state_dir, pipeline)
    if path.exists():
        path.unlink()
        return True
    return False


def all_pins(state_dir: str) -> list[PinRecord]:
    return [
        PinRecord(**json.loads(p.read_text()))
        for p in sorted(Path(state_dir).glob("*.pin.json"))
    ]
