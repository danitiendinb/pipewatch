"""CLI subcommands for throttle management."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.throttler import clear_throttle, is_throttled, load_last_check


def add_throttler_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("throttle", help="Manage pipeline check throttling")
    sub = p.add_subparsers(dest="throttle_cmd")

    status_p = sub.add_parser("status", help="Show throttle status for a pipeline")
    status_p.add_argument("pipeline", help="Pipeline name")
    status_p.add_argument("--interval", type=int, default=60, help="Min interval seconds")

    clear_p = sub.add_parser("clear", help="Clear throttle record for a pipeline")
    clear_p.add_argument("pipeline", help="Pipeline name")


def cmd_throttler(args: argparse.Namespace) -> int:
    cfg = load_config(getattr(args, "config", "pipewatch.yml"))
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir

    if args.throttle_cmd == "status":
        last = load_last_check(state_dir, args.pipeline)
        throttled = is_throttled(state_dir, args.pipeline, args.interval)
        if last is None:
            print(f"{args.pipeline}: never checked")
        else:
            status = "THROTTLED" if throttled else "ok"
            print(f"{args.pipeline}: last_check={last.isoformat()}  [{status}]")
        return 0

    if args.throttle_cmd == "clear":
        clear_throttle(state_dir, args.pipeline)
        print(f"Throttle cleared for {args.pipeline}")
        return 0

    print("error: specify a throttle subcommand", file=sys.stderr)
    return 1
