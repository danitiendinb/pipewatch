"""cli_splitter.py – CLI sub-command for run splitting / windowed analysis."""
from __future__ import annotations

import sys
from argparse import ArgumentParser, Namespace

from pipewatch.config import load_config
from pipewatch.splitter import format_split_row, split_runs
from pipewatch.state import PipelineState


def add_splitter_subparser(sub) -> None:  # type: ignore[no-untyped-def]
    p: ArgumentParser = sub.add_parser(
        "split",
        help="Show run history split into time windows",
    )
    p.add_argument("pipeline", help="Pipeline name to inspect")
    p.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days of history to include (default: 7)",
    )
    p.add_argument(
        "--granularity",
        choices=["day", "hour"],
        default="day",
        help="Bucket size: 'day' or 'hour' (default: day)",
    )
    p.add_argument(
        "--config",
        default="pipewatch.yml",
        help="Path to config file",
    )
    p.set_defaults(func=cmd_split)


def cmd_split(args: Namespace) -> int:
    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        print(f"Config file not found: {args.config}", file=sys.stderr)
        return 1

    store = PipelineState(cfg.state_dir)
    try:
        buckets = split_runs(
            store,
            args.pipeline,
            days=args.days,
            granularity=args.granularity,
        )
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if not any(b.total > 0 for b in buckets):
        print(f"No runs found for '{args.pipeline}' in the last {args.days} day(s).")
        return 0

    print(f"Run split for '{args.pipeline}' ({args.granularity}, last {args.days}d):")
    for bucket in buckets:
        if bucket.total > 0:
            print(" ", format_split_row(bucket))
    return 0
