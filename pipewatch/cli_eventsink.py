"""CLI sub-commands for the event sink."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict

from pipewatch.config import load_config
from pipewatch.eventsink import drain_events, load_events, clear_events


def add_eventsink_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("events", help="Inspect or drain the event sink")
    p.add_argument("pipeline", help="Pipeline name")
    p.add_argument(
        "--drain",
        action="store_true",
        default=False,
        help="Consume and clear events after printing",
    )
    p.add_argument(
        "--clear",
        action="store_true",
        default=False,
        help="Clear events without printing",
    )
    p.add_argument(
        "--config",
        default="pipewatch.yml",
        metavar="FILE",
    )


def cmd_eventsink(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print(f"Config not found: {args.config}", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir
    pipeline = args.pipeline

    if args.clear:
        clear_events(state_dir, pipeline)
        print(f"Events cleared for '{pipeline}'.")
        return 0

    if args.drain:
        events = drain_events(state_dir, pipeline)
    else:
        events = load_events(state_dir, pipeline)

    if not events:
        print(f"No events for '{pipeline}'.")
        return 0

    print(json.dumps([asdict(e) for e in events], indent=2))
    return 0
