"""CLI sub-commands: silence / unsilence."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from pipewatch.config import load_config
from pipewatch.silencer import clear_silence, is_silenced, set_silence, silence_until


def add_silence_subparser(sub: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = sub.add_parser("silence", help="Suppress alerts for a pipeline")
    p.add_argument("pipeline", help="Pipeline name")
    p.add_argument("--hours", type=float, default=1.0, help="Duration in hours (default 1)")
    p.add_argument("--config", default="pipewatch.yml")

    u = sub.add_parser("unsilence", help="Remove silence for a pipeline")
    u.add_argument("pipeline", help="Pipeline name")
    u.add_argument("--config", default="pipewatch.yml")


def cmd_silence(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("Config not found.")
        return 1
    until = datetime.now(timezone.utc) + timedelta(hours=args.hours)
    set_silence(cfg.state_dir, args.pipeline, until)
    print(f"Silenced '{args.pipeline}' until {until.isoformat()}")
    return 0


def cmd_unsilence(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("Config not found.")
        return 1
    if not is_silenced(cfg.state_dir, args.pipeline):
        print(f"'{args.pipeline}' is not currently silenced.")
        return 0
    clear_silence(cfg.state_dir, args.pipeline)
    print(f"Silence cleared for '{args.pipeline}'.")
    return 0
