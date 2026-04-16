"""Pipeline state tracking — persists run history to disk."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class PipelineRun:
    pipeline: str
    status: str  # "success" | "failure" | "running"
    started_at: str
    finished_at: Optional[str] = None
    exit_code: Optional[int] = None
    error_message: Optional[str] = None

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @classmethod
    def start(cls, pipeline: str) -> "PipelineRun":
        return cls(pipeline=pipeline, status="running", started_at=cls.now_iso())

    def finish(self, exit_code: int, error_message: Optional[str] = None) -> None:
        self.exit_code = exit_code
        self.status = "success" if exit_code == 0 else "failure"
        self.finished_at = self.now_iso()
        self.error_message = error_message


@dataclass
class PipelineState:
    pipeline: str
    last_run: Optional[PipelineRun] = None
    consecutive_failures: int = 0
    history: list = field(default_factory=list)


class StateStore:
    def __init__(self, state_dir: str) -> None:
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, pipeline: str) -> Path:
        safe = pipeline.replace("/", "__")
        return self.state_dir / f"{safe}.json"

    def load(self, pipeline: str) -> PipelineState:
        path = self._path(pipeline)
        if not path.exists():
            return PipelineState(pipeline=pipeline)
        with path.open() as fh:
            data = json.load(fh)
        last = data.get("last_run")
        return PipelineState(
            pipeline=pipeline,
            last_run=PipelineRun(**last) if last else None,
            consecutive_failures=data.get("consecutive_failures", 0),
            history=data.get("history", []),
        )

    def save(self, state: PipelineState) -> None:
        path = self._path(state.pipeline)
        data = {
            "pipeline": state.pipeline,
            "last_run": asdict(state.last_run) if state.last_run else None,
            "consecutive_failures": state.consecutive_failures,
            "history": state.history[-50:],  # keep last 50
        }
        with path.open("w") as fh:
            json.dump(data, fh, indent=2)

    def record_run(self, run: PipelineRun) -> PipelineState:
        state = self.load(run.pipeline)
        state.last_run = run
        if run.status == "failure":
            state.consecutive_failures += 1
        else:
            state.consecutive_failures = 0
        state.history.append(asdict(run))
        self.save(state)
        return state
