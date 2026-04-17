"""CLI sub-command: pipewatch digest."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.state import PipelineState
from pipewatch.digest import build_digest, format_digest


def add_digest_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("digest", help="Print a digest report of pipeline health")
    p.add_argument("--config", default="pipewatch.yml", help="Path to config file")
    p.add_argument(
        "--days",
        type=int,
        default=7,
        metavar="N",
        help="Report window in days (default: 7)",
    )
    p.add_argument("--pipeline", metavar="NAME", help="Limit digest to a single pipeline")


def cmd_digest(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    if config is None:
        print(f"Error: config file not found: {args.config}", file=sys.stderr)
        return 1

    store = PipelineState(config.state_dir)

    if args.pipeline:
        names = [args.pipeline]
    else:
        names = [p.name for p in config.pipelines]

    report = build_digest(store, names, period_days=args.days)
    print(format_digest(report))
    return 0
