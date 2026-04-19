"""Integration tests: silencer interacts with alerts dispatch."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.alerts import dispatch_alerts
from pipewatch.silencer import is_silenced, set_silence
from pipewatch.state import PipelineState, PipelineRun


def _utc(**kw) -> datetime:
    return datetime.now(timezone.utc) + timedelta(**kw)


def _failing_state(pipeline: str) -> PipelineState:
    run = PipelineRun(
        pipeline=pipeline,
        started_at=_utc(hours=-1).isoformat(),
        finished_at=_utc(hours=-1).isoformat(),
        success=False,
        message="boom",
    )
    return PipelineState(pipeline=pipeline, runs=[run], consecutive_failures=3)


def _dispatch(state_dir, pipeline, state):
    """Helper to dispatch alerts with standard test parameters."""
    with patch("pipewatch.alerts.send_webhook") as mock_wh:
        dispatch_alerts(
            {pipeline: state},
            webhook_url="http://hook",
            threshold=1,
            silence_check=lambda name: is_silenced(state_dir, name),
        )
    return mock_wh


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def test_silenced_pipeline_skips_webhook(state_dir):
    set_silence(state_dir, "pipe-a", _utc(hours=1))
    state = _failing_state("pipe-a")
    mock_wh = _dispatch(state_dir, "pipe-a", state)
    mock_wh.assert_not_called()


def test_unsilenced_pipeline_triggers_webhook(state_dir):
    state = _failing_state("pipe-b")
    mock_wh = _dispatch(state_dir, "pipe-b", state)
    mock_wh.assert_called_once()


def test_expired_silence_triggers_webhook(state_dir):
    set_silence(state_dir, "pipe-c", _utc(hours=-1))  # already expired
    state = _failing_state("pipe-c")
    mock_wh = _dispatch(state_dir, "pipe-c", state)
    mock_wh.assert_called_once()
