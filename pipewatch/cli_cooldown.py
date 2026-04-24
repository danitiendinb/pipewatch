"""CLI subcommands for managing pipeline alert cooldowns."""

from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.cooldown import (
    active_cooldowns,
    clear_cooldown,
    is_cooling_down,
    set_cooldown,
)


def add_cooldown_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    p = subparsers.add_parser("cooldown", help="Manage alert cooldowns for pipelines")
    sub = p.add_subparsers(dest="cooldown_cmd")

    # set
    ps = sub.add_parser("set", help="Set a cooldown window for a pipeline")
    ps.add_argument("pipeline", help="Pipeline name")
    ps.add_argument(
        "--minutes",
        type=int,
        default=60,
        help="Cooldown duration in minutes (default: 60)",
    )

    # clear
    pc = sub.add_parser("clear", help="Clear the cooldown for a pipeline")
    pc.add_argument("pipeline", help="Pipeline name")

    # status
    sub.add_parser("status", help="List all active cooldowns")


def cmd_cooldown(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config file not found", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir
    cooldown_cmd = getattr(args, "cooldown_cmd", None)

    if cooldown_cmd == "set":
        expires_at = set_cooldown(state_dir, args.pipeline, args.minutes)
        print(f"Cooldown set for '{args.pipeline}' until {expires_at.isoformat()}")
        return 0

    if cooldown_cmd == "clear":
        clear_cooldown(state_dir, args.pipeline)
        print(f"Cooldown cleared for '{args.pipeline}'")
        return 0

    if cooldown_cmd == "status":
        active = active_cooldowns(state_dir)
        if not active:
            print("No active cooldowns.")
        else:
            for name, exp in sorted(active.items()):
                print(f"  {name}: cooling down until {exp.isoformat()}")
        return 0

    print("error: specify a subcommand (set|clear|status)", file=sys.stderr)
    return 1
