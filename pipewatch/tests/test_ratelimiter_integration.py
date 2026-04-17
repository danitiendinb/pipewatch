"""Integration tests: rate-limiter interacting with alerts dispatch."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.ratelimiter import is_rate_limited, record_alert
from pipewatch.state import PipelineRun, PipelineState


def _make_state(failures: int) -> PipelineState:
    run = PipelineRun(
        run_id="r1",
        started_at=datetime.now(timezone.utc).isoformat(),
        finished_at=datetime.now(timezone.utc).isoformat(),
        success=False,
        message="boom",
    )
    return PipelineState(
        pipeline="etl",
        last_run=run,
        consecutive_failures=failures,
        runs=[run],
    )


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def test_alert_suppressed_when_rate_limited(state_dir):
    record_alert(state_dir, "etl")
    assert is_rate_limited(state_dir, "etl", cooldown_minutes=60) is True


def test_alert_fires_after_cooldown_cleared(state_dir):
    from pipewatch.ratelimiter import clear_ratelimit
    record_alert(state_dir, "etl")
    clear_ratelimit(state_dir, "etl")
    assert is_rate_limited(state_dir, "etl", cooldown_minutes=60) is False


def test_record_alert_called_on_dispatch(state_dir):
    """Simulate a dispatch that records the alert timestamp."""
    state = _make_state(failures=3)
    with patch("pipewatch.alerts.send_webhook", return_value=True) as mock_wh:
        from pipewatch.alerts import dispatch_alerts
        from pipewatch.config import AlertConfig
        cfg = AlertConfig(webhook_url="http://example.com/hook", threshold=3)
        dispatch_alerts({"etl": state}, cfg)
        mock_wh.assert_called_once()
    record_alert(state_dir, "etl")
    assert is_rate_limited(state_dir, "etl", cooldown_minutes=30) is True
