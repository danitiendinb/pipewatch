"""Tests for pipewatch.notifier."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.notifier import EmailConfig, build_message, send_email, dispatch_email_alerts
from pipewatch.state import PipelineState


@pytest.fixture()
def cfg() -> EmailConfig:
    return EmailConfig(
        to=["ops@example.com"],
        from_addr="pipewatch@example.com",
        smtp_host="mail.example.com",
        smtp_port=587,
    )


def _make_state(failures: int) -> PipelineState:
    st = PipelineState(pipeline="pipe1")
    st.consecutive_failures = failures
    return st


def test_build_message_subject(cfg):
    msg = build_message(cfg.from_addr, cfg.to, "pipe1", 3)
    assert "pipe1" in msg["Subject"]
    assert "3" in msg["Subject"]


def test_build_message_to(cfg):
    msg = build_message(cfg.from_addr, cfg.to, "pipe1", 1)
    assert "ops@example.com" in msg["To"]


def test_build_message_body(cfg):
    msg = build_message(cfg.from_addr, cfg.to, "pipe1", 2)
    body = msg.get_content()
    assert "pipe1" in body
    assert "2" in body


def test_send_email_returns_true_on_success(cfg):
    with patch("smtplib.SMTP") as mock_smtp:
        instance = MagicMock()
        mock_smtp.return_value.__enter__ = lambda s: instance
        mock_smtp.return_value.__exit__ = MagicMock(return_value=False)
        result = send_email(cfg, "pipe1", 3)
    assert result is True


def test_send_email_returns_false_on_error(cfg):
    with patch("smtplib.SMTP", side_effect=OSError("connection refused")):
        result = send_email(cfg, "pipe1", 3)
    assert result is False


def test_dispatch_email_alerts_sends_when_threshold_met(cfg):
    states = {"pipe1": _make_state(3)}
    with patch("pipewatch.notifier.send_email", return_value=True) as mock_send:
        dispatch_email_alerts(cfg, states, threshold=3)
    mock_send.assert_called_once_with(cfg, "pipe1", 3)


def test_dispatch_email_alerts_skips_below_threshold(cfg):
    states = {"pipe1": _make_state(2)}
    with patch("pipewatch.notifier.send_email", return_value=True) as mock_send:
        dispatch_email_alerts(cfg, states, threshold=3)
    mock_send.assert_not_called()
