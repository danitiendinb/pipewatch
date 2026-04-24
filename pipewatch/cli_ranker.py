"""CLI sub-command: pipewatch rank"""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.ranker import format_ranked_row, top_offenders
from pipewatch.scheduler import overdue_pipelines
from pipewatch.state import PipelineState


def add_ranker_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("rank", help="Rank pipelines by health score")
    p.add_argument(
        "-n",
        "--top",
        type=int,
        default=10,
        help="Number of pipelines to show (default: 10)",
    )
    p.add_argument(
        "--worst-first",
        action="store_true",
        default=True,
        help="Show worst-scoring pipelines first (default)",
    )
    p.add_argument(
        "--config",
        default="pipewatch.yml",
        help="Path to config file",
    )


def cmd_rank(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print(f"Error: config file not found: {args.config}", file=sys.stderr)
        return 1

    store = PipelineState(cfg.state_dir)
    names = [p.name for p in cfg.pipelines]

    try:
        overdue = set(overdue_pipelines(cfg.pipelines, store))
    except Exception:
        overdue = set()

    ranked = top_offenders(names, store, n=args.top, overdue=overdue)

    if not ranked:
        print("No pipelines configured.")
        return 0

    header = f"{'#':<4} {'Pipeline':<30} {'Score Bar':<20}  Score  Grade"
    print(header)
    print("-" * len(header))
    for rp in ranked:
        print(format_ranked_row(rp))

    return 0
