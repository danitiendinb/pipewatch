"""Tests for pipewatch.alerts."""

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts import dispatch_alerts, should_alert, send_webhook
from pipewatch.config import AlertConfig
from pipewatch.state import PipelineRun, PipelineState


def _make_state(status: str, consecutive: int) -> PipelineState:
    run = PipelineRun.start("test_pipe")
    run.finish(exit_code=0 if status == "success" else 1)
    state = PipelineState(
        pipeline="test_pipe",
        last_run=run,
        consecutive_failures=consecutive,
    )
    return state


def test_should_alert_false_on_success() -> None:
    cfg = AlertConfig(min_failures=1, webhook_url="http://x")
    state = _make_state("success", 0)
    assert should_alert(state, cfg) is False


def test_should_alert_false_below_threshold() -> None:
    cfg = AlertConfig(min_failures=3, webhook_url="http://x")
    state = _make_state("failure", 2)
    assert should_alert(state, cfg) is False


def test_should_alert_true_at_threshold() -> None:
    cfg = AlertConfig(min_failures=2, webhook_url="http://x")
    state = _make_state("failure", 2)
    assert should_alert(state, cfg) is True


def test_dispatch_alerts_calls_webhook() -> None:
    cfg = AlertConfig(min_failures=1, webhook_url="http://hook.example/")
    state = _make_state("failure", 1)
    with patch("pipewatch.alerts._post_json") as mock_post:
        dispatch_alerts(state, cfg)
        mock_post.assert_called_once()
        args = mock_post.call_args[0]
        assert args[0] == "http://hook.example/"
        assert args[1]["pipeline"] == "test_pipe"


def test_dispatch_alerts_skips_when_no_webhook() -> None:
    cfg = AlertConfig(min_failures=1, webhook_url=None)
    state = _make_state("failure", 1)
    with patch("pipewatch.alerts._post_json") as mock_post:
        dispatch_alerts(state, cfg)
        mock_post.assert_not_called()
