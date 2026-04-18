"""CLI subcommands for baseline management."""

from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.state import PipelineState
from pipewatch.baseliner import compute_baseline, load_baseline, save_baseline, clear_baseline


def add_baseliner_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("baseline", help="Manage pipeline duration baselines")
    sub = p.add_subparsers(dest="baseline_cmd")

    rec = sub.add_parser("record", help="Record baseline from current state")
    rec.add_argument("pipeline", help="Pipeline name")

    show = sub.add_parser("show", help="Show stored baseline")
    show.add_argument("pipeline", help="Pipeline name")

    clr = sub.add_parser("clear", help="Clear stored baseline")
    clr.add_argument("pipeline", help="Pipeline name")

    p.set_defaults(func=cmd_baseline)


def cmd_baseline(args: argparse.Namespace, config_path: str = "pipewatch.yml") -> int:
    cfg = load_config(config_path)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir
    pipeline = args.pipeline

    if args.baseline_cmd == "record":
        state = PipelineState.load(state_dir, pipeline)
        baseline = compute_baseline(state, pipeline)
        if baseline is None:
            print(f"No successful runs found for '{pipeline}'.", file=sys.stderr)
            return 1
        save_baseline(state_dir, baseline)
        print(f"Baseline recorded for '{pipeline}': mean={baseline.mean_duration}s over {baseline.sample_count} runs.")
        return 0

    if args.baseline_cmd == "show":
        baseline = load_baseline(state_dir, pipeline)
        if baseline is None:
            print(f"No baseline stored for '{pipeline}'.")
            return 0
        print(f"Pipeline : {baseline.pipeline}")
        print(f"Mean     : {baseline.mean_duration}s")
        print(f"Samples  : {baseline.sample_count}")
        print(f"Recorded : {baseline.recorded_at}")
        return 0

    if args.baseline_cmd == "clear":
        clear_baseline(state_dir, pipeline)
        print(f"Baseline cleared for '{pipeline}'.")
        return 0

    print("error: specify a baseline subcommand (record|show|clear)", file=sys.stderr)
    return 1
