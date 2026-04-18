"""CLI sub-command: pipewatch watchdog"""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.state import PipelineState
from pipewatch.watchdog import stale_pipelines


def add_watchdog_subparser(subparsers) -> None:
    p = subparsers.add_parser("watchdog", help="List pipelines that have gone silent")
    p.add_argument(
        "--threshold",
        type=float,
        default=24.0,
        metavar="HOURS",
        help="Hours of silence before a pipeline is considered stale (default: 24)",
    )
    p.add_argument("--config", default="pipewatch.yml", metavar="FILE")


def cmd_watchdog(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    if config is None:
        print(f"Error: config file '{args.config}' not found.", file=sys.stderr)
        return 1

    from pipewatch.state import PipelineStore  # local import to mirror existing pattern

    store = PipelineStore(config.state_dir)
    reports = stale_pipelines(config, store, threshold_hours=args.threshold)

    if not reports:
        print("All pipelines reported within the threshold period.")
        return 0

    print(f"{'PIPELINE':<30} {'LAST SEEN':<30} {'HOURS SILENT'}")
    print("-" * 72)
    for r in reports:
        last = r.last_seen or "never"
        silent = str(r.hours_silent) if r.hours_silent is not None else "n/a"
        print(f"{r.pipeline:<30} {last:<30} {silent}")
    return 0
