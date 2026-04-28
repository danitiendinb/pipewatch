"""CLI subcommands for managing pipeline fence windows."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

from pipewatch.config import load_config
from pipewatch.fencer import FenceWindow, save_fence, clear_fence, load_fence, is_fenced


def add_fencer_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("fence", help="Manage pipeline fence windows")
    sub = p.add_subparsers(dest="fence_cmd")

    # set
    s = sub.add_parser("set", help="Set a fence window")
    s.add_argument("pipeline")
    s.add_argument("--hours", type=float, default=1.0, help="Duration in hours (default: 1)")
    s.add_argument("--reason", default="", help="Optional reason")

    # clear
    c = sub.add_parser("clear", help="Remove a fence window")
    c.add_argument("pipeline")

    # status
    st = sub.add_parser("status", help="Show fence status for a pipeline")
    st.add_argument("pipeline")


def cmd_fencer(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1

    if not hasattr(args, "fence_cmd") or args.fence_cmd is None:
        print("error: fence subcommand required", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir
    pipeline = args.pipeline

    if args.fence_cmd == "set":
        now = datetime.now(timezone.utc)
        end = now + timedelta(hours=args.hours)
        window = FenceWindow(
            start_iso=now.isoformat(),
            end_iso=end.isoformat(),
            reason=args.reason,
        )
        save_fence(state_dir, pipeline, window)
        print(f"Fence set for '{pipeline}' until {end.isoformat()} (reason: {args.reason or 'none'})")
        return 0

    if args.fence_cmd == "clear":
        clear_fence(state_dir, pipeline)
        print(f"Fence cleared for '{pipeline}'")
        return 0

    if args.fence_cmd == "status":
        window = load_fence(state_dir, pipeline)
        if window is None:
            print(f"'{pipeline}' has no fence window")
        elif is_fenced(state_dir, pipeline):
            print(f"'{pipeline}' is FENCED until {window.end_iso} ({window.reason or 'no reason'})")
        else:
            print(f"'{pipeline}' fence window expired at {window.end_iso}")
        return 0

    return 1
