"""Retry policy tracking for pipeline runs."""
from __future__ import annotations
import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
from pipewatch.state import PipelineState


@dataclass
class RetryPolicy:
    max_retries: int = 3
    backoff_seconds: int = 60


@dataclass
class RetryRecord:
    pipeline: str
    attempt: int = 0
    last_retry_at: Optional[str] = None


def _retry_path(state_dir: str, pipeline: str) -> Path:
    return Path(state_dir) / f"{pipeline}.retry.json"


def load_retry(state_dir: str, pipeline: str) -> RetryRecord:
    p = _retry_path(state_dir, pipeline)
    if not p.exists():
        return RetryRecord(pipeline=pipeline)
    data = json.loads(p.read_text())
    return RetryRecord(**data)


def save_retry(state_dir: str, record: RetryRecord) -> None:
    p = _retry_path(state_dir, record.pipeline)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({
        "pipeline": record.pipeline,
        "attempt": record.attempt,
        "last_retry_at": record.last_retry_at,
    }))


def clear_retry(state_dir: str, pipeline: str) -> None:
    p = _retry_path(state_dir, pipeline)
    if p.exists():
        p.unlink()


def should_retry(record: RetryRecord, policy: RetryPolicy) -> bool:
    return record.attempt < policy.max_retries


def increment_retry(state_dir: str, pipeline: str, now_iso: str) -> RetryRecord:
    record = load_retry(state_dir, pipeline)
    record.attempt += 1
    record.last_retry_at = now_iso
    save_retry(state_dir, record)
    return record
