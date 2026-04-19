"""Tests for pipewatch.router."""
from unittest.mock import patch, MagicMock
import pytest

from pipewatch.router import RouteRule, RouterConfig, _url_for_pipeline, route_alert, route_all
from pipewatch.state import PipelineState, PipelineRun


def _make_state(failures: int = 0) -> PipelineState:
    run = MagicMock(spec=PipelineRun)
    run.finished_at = "2024-01-01T00:00:00"
    state = PipelineState(runs=[run], consecutive_failures=failures)
    return state


@pytest.fixture
def config() -> RouterConfig:
    return RouterConfig(
        rules=[RouteRule(label="critical", webhook_url="http://critical.example/hook")],
        default_url="http://default.example/hook",
    )


def test_url_for_pipeline_matches_label(config, tmp_path):
    with patch("pipewatch.router.load_label", return_value="critical"):
        url = _url_for_pipeline("pipe_a", str(tmp_path), config)
    assert url == "http://critical.example/hook"


def test_url_for_pipeline_falls_back_to_default(config, tmp_path):
    with patch("pipewatch.router.load_label", return_value="low"):
        url = _url_for_pipeline("pipe_b", str(tmp_path), config)
    assert url == "http://default.example/hook"


def test_url_for_pipeline_none_when_no_default(tmp_path):
    cfg = RouterConfig(rules=[], default_url=None)
    with patch("pipewatch.router.load_label", return_value="anything"):
        url = _url_for_pipeline("pipe_c", str(tmp_path), cfg)
    assert url is None


def test_route_alert_returns_false_when_no_url(tmp_path):
    cfg = RouterConfig(rules=[], default_url=None)
    with patch("pipewatch.router.load_label", return_value="x"):
        result = route_alert("pipe", _make_state(2), str(tmp_path), cfg)
    assert result is False


def test_route_alert_calls_send_webhook(config, tmp_path):
    with patch("pipewatch.router.load_label", return_value="critical"), \
         patch("pipewatch.router.send_webhook", return_value=True) as mock_send:
        result = route_alert("pipe_a", _make_state(3), str(tmp_path), config)
    assert result is True
    mock_send.assert_called_once()
    args = mock_send.call_args[0]
    assert args[0] == "http://critical.example/hook"
    assert args[1]["pipeline"] == "pipe_a"


def test_route_all_returns_alerted_pipelines(config, tmp_path):
    states = {
        "ok_pipe": _make_state(0),
        "bad_pipe": _make_state(3),
    }
    with patch("pipewatch.router.load_label", return_value="default_label"), \
         patch("pipewatch.router.send_webhook", return_value=True):
        alerted = route_all(states, str(tmp_path), config, threshold=1)
    assert "bad_pipe" in alerted
    assert "ok_pipe" not in alerted


def test_route_all_empty_when_no_failures(config, tmp_path):
    states = {"pipe": _make_state(0)}
    with patch("pipewatch.router.load_label", return_value="critical"), \
         patch("pipewatch.router.send_webhook", return_value=True):
        alerted = route_all(states, str(tmp_path), config, threshold=1)
    assert alerted == []
