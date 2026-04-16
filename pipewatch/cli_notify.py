"""CLI subcommand: notify — send email alerts for overdue/failing pipelines."""
from __future__ import annotations

import argparse
import sys
import logging

from pipewatch.config import load_config
from pipewatch.state import PipelineState
from pipewatch.notifier import EmailConfig, dispatch_email_alerts

log = logging.getLogger(__name__)


def add_notify_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    p = subparsers.add_parser("notify", help="Send email alerts for failing pipelines")
    p.add_argument("--config", default="pipewatch.yml", help="Path to config file")
    p.add_argument(
        "--smtp-host", default=None, help="Override SMTP host from config"
    )
    p.add_argument("--dry-run", action="store_true", help="Print alerts without sending")


def cmd_notify(args: argparse.Namespace) -> int:
    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        print(f"Config file not found: {args.config}", file=sys.stderr)
        return 1

    email_cfg_data = getattr(cfg, "email", None)
    if email_cfg_data is None:
        print("No email configuration found in config.", file=sys.stderr)
        return 1

    smtp_host = args.smtp_host or email_cfg_data.get("smtp_host", "localhost")
    email_cfg = EmailConfig(
        to=email_cfg_data.get("to", []),
        from_addr=email_cfg_data.get("from", "pipewatch@localhost"),
        smtp_host=smtp_host,
        smtp_port=email_cfg_data.get("smtp_port", 25),
        username=email_cfg_data.get("username"),
        password=email_cfg_data.get("password"),
        use_tls=email_cfg_data.get("use_tls", False),
    )

    from pipewatch.state import load as load_state

    threshold = cfg.alert.failure_threshold
    states: dict[str, PipelineState] = {
        p.name: load_state(cfg.state_dir, p.name) for p in cfg.pipelines
    }

    if args.dry_run:
        from pipewatch.alerts import should_alert
        for name, state in states.items():
            if should_alert(state, threshold):
                print(f"[dry-run] Would email alert for '{name}' ({state.consecutive_failures} failures)")
        return 0

    dispatch_email_alerts(email_cfg, states, threshold)
    return 0
