"""CLI subcommand: pipewatch drift — detect duration drift for pipelines."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.drifter import detect_drift, save_drift_baseline
from pipewatch.state import PipelineState


def add_drifter_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("drift", help="Detect duration drift for pipelines")
    p.add_argument("--pipeline", help="Limit to a single pipeline")
    p.add_argument(
        "--threshold",
        type=float,
        default=20.0,
        help="Drift threshold in percent (default: 20)",
    )
    p.add_argument(
        "--window",
        type=int,
        default=10,
        help="Number of recent runs to average (default: 10)",
    )
    p.add_argument(
        "--record",
        action="store_true",
        help="Save current averages as the new baseline",
    )
    p.add_argument("-c", "--config", default="pipewatch.yml")
    p.set_defaults(func=cmd_drift)


def cmd_drift(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print(f"Config not found: {args.config}", file=sys.stderr)
        return 1

    pipelines = (
        [p for p in cfg.pipelines if p.name == args.pipeline]
        if args.pipeline
        else cfg.pipelines
    )

    if not pipelines:
        print(f"Pipeline not found: {args.pipeline}", file=sys.stderr)
        return 1

    found_drift = False
    for pcfg in pipelines:
        state = PipelineState.load(cfg.state_dir, pcfg.name)
        report = detect_drift(
            state, cfg.state_dir, pcfg.name,
            threshold_pct=args.threshold,
            window=args.window,
        )
        if args.record and report.current_avg_duration is not None:
            save_drift_baseline(cfg.state_dir, pcfg.name, report.current_avg_duration)
            print(f"[{pcfg.name}] baseline recorded: {report.current_avg_duration:.1f}s")
            continue

        if report.current_avg_duration is None:
            print(f"[{pcfg.name}] insufficient data")
            continue

        if report.previous_avg_duration is None:
            print(f"[{pcfg.name}] no baseline — run with --record to set one")
            continue

        symbol = "⚠" if report.has_drift else "✓"
        print(
            f"{symbol} [{pcfg.name}] prev={report.previous_avg_duration:.1f}s "
            f"curr={report.current_avg_duration:.1f}s "
            f"drift={report.drift_pct:.1f}%"
        )
        if report.has_drift:
            found_drift = True

    return 1 if found_drift else 0
