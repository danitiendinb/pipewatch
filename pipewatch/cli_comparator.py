"""CLI subcommand for duration anomaly detection."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.state import PipelineState
from pipewatch.comparator import check_all_pipelines, compute_stats


def add_comparator_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("compare", help="Detect duration anomalies")
    p.add_argument("--config", default="pipewatch.yml")
    p.add_argument(
        "--z-threshold",
        type=float,
        default=2.5,
        help="Z-score threshold for anomaly detection (default: 2.5)",
    )
    p.add_argument("--stats", action="store_true", help="Print baseline stats")


def cmd_compare(args: argparse.Namespace) -> int:
    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        print(f"Config not found: {args.config}", file=sys.stderr)
        return 1

    state = PipelineState(cfg.state_dir)
    pipeline_names = [p.name for p in cfg.pipelines]

    if args.stats:
        for name in pipeline_names:
            s = compute_stats(state, name)
            if s:
                print(
                    f"{name}: mean={s.mean_seconds:.1f}s "
                    f"stddev={s.stddev_seconds:.1f}s "
                    f"n={s.sample_size}"
                )
            else:
                print(f"{name}: insufficient data")
        return 0

    anomalies = check_all_pipelines(state, pipeline_names, args.z_threshold)
    if not anomalies:
        print("No duration anomalies detected.")
        return 0

    for a in anomalies:
        print(
            f"ANOMALY {a.pipeline} run={a.run_id} "
            f"duration={a.duration_seconds:.1f}s "
            f"mean={a.mean_seconds:.1f}s z={a.z_score:.2f}"
        )
    return 0
