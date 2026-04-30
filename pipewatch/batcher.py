"""Batch execution tracker — groups pipeline runs into named batches and
reports aggregate pass/fail status for the batch."""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from pipewatch.state import PipelineState


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _batch_path(state_dir: str, batch_id: str) -> Path:
    return Path(state_dir) / f"batch_{batch_id}.json"


@dataclass
class BatchEntry:
    pipeline: str
    status: str  # "ok" | "fail" | "pending"
    recorded_at: Optional[str] = None


@dataclass
class BatchRecord:
    batch_id: str
    created_at: str
    entries: List[BatchEntry] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.entries)

    @property
    def passed(self) -> int:
        return sum(1 for e in self.entries if e.status == "ok")

    @property
    def failed(self) -> int:
        return sum(1 for e in self.entries if e.status == "fail")

    @property
    def pending(self) -> int:
        return sum(1 for e in self.entries if e.status == "pending")

    @property
    def complete(self) -> bool:
        return self.pending == 0

    @property
    def healthy(self) -> bool:
        return self.complete and self.failed == 0


def load_batch(state_dir: str, batch_id: str) -> Optional[BatchRecord]:
    path = _batch_path(state_dir, batch_id)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    entries = [BatchEntry(**e) for e in data.get("entries", [])]
    return BatchRecord(
        batch_id=data["batch_id"],
        created_at=data["created_at"],
        entries=entries,
    )


def save_batch(state_dir: str, record: BatchRecord) -> None:
    path = _batch_path(state_dir, record.batch_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = asdict(record)
    path.write_text(json.dumps(data, indent=2))


def create_batch(state_dir: str, batch_id: str, pipelines: List[str]) -> BatchRecord:
    record = BatchRecord(
        batch_id=batch_id,
        created_at=_now_iso(),
        entries=[BatchEntry(pipeline=p, status="pending") for p in pipelines],
    )
    save_batch(state_dir, record)
    return record


def record_batch_result(
    state_dir: str, batch_id: str, pipeline: str, status: str
) -> Optional[BatchRecord]:
    record = load_batch(state_dir, batch_id)
    if record is None:
        return None
    for entry in record.entries:
        if entry.pipeline == pipeline:
            entry.status = status
            entry.recorded_at = _now_iso()
            break
    save_batch(state_dir, record)
    return record


def clear_batch(state_dir: str, batch_id: str) -> None:
    path = _batch_path(state_dir, batch_id)
    if path.exists():
        path.unlink()
