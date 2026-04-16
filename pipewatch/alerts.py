"""Alert dispatchers for pipeline failures."""

from __future__ import annotations

import logging
import urllib.request
import json
from typing import Optional

from pipewatch.config import AlertConfig
from pipewatch.state import PipelineRun, PipelineState

logger = logging.getLogger(__name__)


def should_alert(state: PipelineState, alert_config: AlertConfig) -> bool:
    """Return True when the state warrants sending an alert."""
    if state.last_run is None or state.last_run.status != "failure":
        return False
    return state.consecutive_failures >= alert_config.min_failures


def _post_json(url: str, payload: dict, timeout: int = 10) -> None:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        logger.debug("Webhook response: %s", resp.status)


def send_webhook(run: PipelineRun, state: PipelineState, webhook_url: str) -> None:
    payload = {
        "pipeline": run.pipeline,
        "status": run.status,
        "exit_code": run.exit_code,
        "error_message": run.error_message,
        "consecutive_failures": state.consecutive_failures,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
    }
    try:
        _post_json(webhook_url, payload)
        logger.info("Alert sent for pipeline %s", run.pipeline)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send webhook alert: %s", exc)


def dispatch_alerts(state: PipelineState, alert_config: AlertConfig) -> None:
    if not should_alert(state, alert_config):
        return
    assert state.last_run is not None  # noqa: S101
    if alert_config.webhook_url:
        send_webhook(state.last_run, state, alert_config.webhook_url)
