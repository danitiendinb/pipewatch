"""Event sink: collect and persist structured pipeline events for downstream consumers."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional

from pipewatch.state import PipelineState


@dataclass
class SinkEvent:
    pipeline: str
    event_type: str  # 'success' | 'failure' | 'overdue' | 'custom'
    timestamp: str
    message: Optional[str] = None
    metadata: Optional[dict] = None


def _sink_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / pipeline / "events.json"


def load_events(state_dir: str, pipeline: str) -> List[SinkEvent]:
    path = _sink_path(state_dir, pipeline)
    if not path.exists():
        return []
    data = json.loads(path.read_text())
    return [SinkEvent(**e) for e in data]


def _save_events(state_dir: str, pipeline: str, events: List[SinkEvent]) -> None:
    path = _sink_path(state_dir, pipeline)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([asdict(e) for e in events], indent=2))


def push_event(
    state_dir: str,
    pipeline: str,
    event_type: str,
    timestamp: str,
    message: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> SinkEvent:
    events = load_events(state_dir, pipeline)
    ev = SinkEvent(
        pipeline=pipeline,
        event_type=event_type,
        timestamp=timestamp,
        message=message,
        metadata=metadata,
    )
    events.append(ev)
    _save_events(state_dir, pipeline, events)
    return ev


def clear_events(state_dir: str, pipeline: str) -> None:
    path = _sink_path(state_dir, pipeline)
    if path.exists():
        path.unlink()


def drain_events(
    state_dir: str, pipeline: str
) -> List[SinkEvent]:
    """Return all events and clear the sink."""
    events = load_events(state_dir, pipeline)
    clear_events(state_dir, pipeline)
    return events


def flush_from_state(
    state_dir: str, pipeline: str, state: PipelineState, timestamp: str
) -> Optional[SinkEvent]:
    """Push a success/failure event derived from the latest run."""
    if not state.runs:
        return None
    last = state.runs[-1]
    event_type = "success" if last.status == "ok" else "failure"
    return push_event(
        state_dir, pipeline, event_type, timestamp, message=last.message
    )
