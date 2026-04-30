"""CLI sub-command: pipewatch trendline"""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.state import PipelineState
from pipewatch.trendline import compute_trendline, compute_all


def add_trendline_subparser(subparsers) -> None:
    p = subparsers.add_parser("trendline", help="Show duration trend for pipelines")
    p.add_argument("--config", default="pipewatch.yml", help="Config file path")
    p.add_argument("--pipeline", default=None, help="Limit to a single pipeline")
    p.add_argument(
        "--window",
        type=int,
        default=20,
        help="Number of recent runs to include (default: 20)",
    )
    p.add_argument(
        "--stable-threshold",
        type=float,
        default=1.0,
        dest="stable_threshold",
        help="Slope (s/run) below which trend is considered stable (default: 1.0)",
    )
    p.set_defaults(func=cmd_trendline)


def cmd_trendline(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print(f"error: config not found: {args.config}", file=sys.stderr)
        return 1

    if args.pipeline:
        state = PipelineState(cfg.state_dir).load(args.pipeline)
        report = compute_trendline(
            args.pipeline, state, args.window, args.stable_threshold
        )
        if report is None:
            print(f"{args.pipeline}: insufficient data (need >=2 finished runs)")
            return 0
        reports = [report]
    else:
        states = {
            p.name: PipelineState(cfg.state_dir).load(p.name)
            for p in cfg.pipelines
        }
        reports = compute_all(states, args.window, args.stable_threshold)

    if not reports:
        print("No trendline data available.")
        return 0

    header = f"{'PIPELINE':<30} {'DIRECTION':<12} {'SLOPE (s/run)':>14} {'PREDICTED (s)':>14} {'SAMPLES':>8}"
    print(header)
    print("-" * len(header))
    for r in reports:
        print(
            f"{r.pipeline:<30} {r.direction:<12} {r.slope:>14.4f} "
            f"{r.latest_predicted:>14.2f} {r.sample_size:>8}"
        )
    return 0
