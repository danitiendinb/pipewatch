"""Integration tests for email notifier using a local SMTP stub."""
from __future__ import annotations

import smtplib
import threading
from email.message import EmailMessage
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.notifier import EmailConfig, send_email, dispatch_email_alerts
from pipewatch.state import PipelineState


def _make_state(failures: int, pipeline: str = "pipe") -> PipelineState:
    st = PipelineState(pipeline=pipeline)
    st.consecutive_failures = failures
    return st


@pytest.fixture()
def email_cfg() -> EmailConfig:
    return EmailConfig(
        to=["team@example.com"],
        from_addr="pipewatch@example.com",
        smtp_host="127.0.0.1",
        smtp_port=10025,
    )


def test_send_email_connection_refused_returns_false(email_cfg):
    """No SMTP server running — should fail gracefully."""
    result = send_email(email_cfg, "pipe1", 5)
    assert result is False


def test_dispatch_calls_send_for_each_failing_pipeline(email_cfg):
    states = {
        "pipe_ok": _make_state(0),
        "pipe_bad": _make_state(4),
        "pipe_also_bad": _make_state(3),
    }
    sent = []
    with patch("pipewatch.notifier.send_email", side_effect=lambda c, p, f: sent.append(p) or True):
        dispatch_email_alerts(email_cfg, states, threshold=3)
    assert sorted(sent) == ["pipe_also_bad", "pipe_bad"]


def test_dispatch_sends_nothing_when_all_ok(email_cfg):
    states = {"pipe1": _make_state(1), "pipe2": _make_state(0)}
    with patch("pipewatch.notifier.send_email", return_value=True) as mock_send:
        dispatch_email_alerts(email_cfg, states, threshold=3)
    mock_send.assert_not_called()


def test_send_email_with_tls_uses_smtp_ssl():
    cfg = EmailConfig(
        to=["a@b.com"],
        from_addr="pw@b.com",
        smtp_host="mail.b.com",
        smtp_port=465,
        use_tls=True,
    )
    with patch("smtplib.SMTP_SSL") as mock_ssl:
        instance = MagicMock()
        mock_ssl.return_value.__enter__ = lambda s: instance
        mock_ssl.return_value.__exit__ = MagicMock(return_value=False)
        result = send_email(cfg, "pipe1", 2)
    mock_ssl.assert_called_once_with("mail.b.com", 465)
    assert result is True
