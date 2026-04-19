"""Manual and conditional pipeline trigger tracking."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

from pipewatch.state import PipelineState


@dataclass
class TriggerRecord:
    pipeline: str
    reason: str
    triggered_by: str
    timestamp: str


def _trigger_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.trigger.json"


def load_trigger(state_dir: str, pipeline: str) -> Optional[TriggerRecord]:
    path = _trigger_path(state_dir, pipeline)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return TriggerRecord(**data)


def set_trigger(
    state_dir: str,
    pipeline: str,
    reason: str,
    triggered_by: str = "user",
    timestamp: Optional[str] = None,
) -> TriggerRecord:
    from pipewatch.state import now_iso
    record = TriggerRecord(
        pipeline=pipeline,
        reason=reason,
        triggered_by=triggered_by,
        timestamp=timestamp or now_iso(),
    )
    path = _trigger_path(state_dir, pipeline)
    path.write_text(json.dumps(asdict(record)))
    return record


def clear_trigger(state_dir: str, pipeline: str) -> None:
    path = _trigger_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def pending_triggers(state_dir: str, pipelines: list[str]) -> list[TriggerRecord]:
    return [r for p in pipelines if (r := load_trigger(state_dir, p)) is not None]
