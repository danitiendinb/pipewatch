"""Email notification support for pipewatch alerts."""
from __future__ import annotations

import smtplib
import logging
from email.message import EmailMessage
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class EmailConfig:
    to: list[str]
    from_addr: str
    smtp_host: str = "localhost"
    smtp_port: int = 25
    username: Optional[str] = None
    password: Optional[str] = None
    use_tls: bool = False


def build_message(from_addr: str, to: list[str], pipeline: str, failures: int) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = f"[pipewatch] Pipeline '{pipeline}' has {failures} consecutive failure(s)"
    msg["From"] = from_addr
    msg["To"] = ", ".join(to)
    msg.set_content(
        f"Pipeline: {pipeline}\n"
        f"Consecutive failures: {failures}\n\n"
        "Check pipewatch status for details."
    )
    return msg


def send_email(cfg: EmailConfig, pipeline: str, failures: int) -> bool:
    """Send an alert email. Returns True on success."""
    msg = build_message(cfg.from_addr, cfg.to, pipeline, failures)
    try:
        if cfg.use_tls:
            server = smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port)
        else:
            server = smtplib.SMTP(cfg.smtp_host, cfg.smtp_port)
        with server:
            if cfg.username and cfg.password:
                server.login(cfg.username, cfg.password)
            server.send_message(msg)
        log.info("Email alert sent for pipeline '%s'", pipeline)
        return True
    except Exception as exc:  # noqa: BLE001
        log.error("Failed to send email for pipeline '%s': %s", pipeline, exc)
        return False


def dispatch_email_alerts(cfg: EmailConfig, states: dict[str, object], threshold: int) -> None:
    """Send email alerts for any pipeline exceeding the failure threshold."""
    from pipewatch.alerts import should_alert  # avoid circular at module level

    for pipeline, state in states.items():
        if should_alert(state, threshold):
            send_email(cfg, pipeline, state.consecutive_failures)
