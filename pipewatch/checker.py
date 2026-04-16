"""Pipeline health checker — runs commands and records results."""

from __future__ import annotations

import logging
import subprocess
from typing import Optional

from pipewatch.config import PipelineConfig, PipewatchConfig
from pipewatch.state import PipelineRun, PipelineState, StateStore

logger = logging.getLogger(__name__)


class PipelineChecker:
    def __init__(self, config: PipewatchConfig) -> None:
        self.config = config
        self.store = StateStore(config.state_dir)

    def check(self, pipeline: PipelineConfig) -> PipelineState:
        logger.info("Checking pipeline: %s", pipeline.name)
        run = PipelineRun.start(pipeline.name)

        try:
            result = subprocess.run(
                pipeline.command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=pipeline.timeout,
            )
            error_msg: Optional[str] = None
            if result.returncode != 0:
                error_msg = (result.stderr or result.stdout or "").strip()[:500]
            run.finish(exit_code=result.returncode, error_message=error_msg)
        except subprocess.TimeoutExpired:
            run.finish(exit_code=1, error_message=f"Timed out after {pipeline.timeout}s")
        except Exception as exc:  # noqa: BLE001
            run.finish(exit_code=1, error_message=str(exc))

        state = self.store.record_run(run)
        logger.debug(
            "Pipeline %s finished: status=%s exit_code=%s",
            pipeline.name,
            run.status,
            run.exit_code,
        )
        return state

    def check_all(self) -> list[PipelineState]:
        return [self.check(p) for p in self.config.pipelines]
