"""CLI subcommand for pipeline duration profiling."""

from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.state import PipelineState
from pipewatch.profiler import compute_profile, save_profile, load_profile, clear_profile


def add_profiler_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("profile", help="Show duration profile for a pipeline")
    p.add_argument("pipeline", help="Pipeline name")
    p.add_argument("--config", default="pipewatch.yml")
    p.add_argument("--save", action="store_true", help="Persist computed profile to disk")
    p.add_argument("--clear", action="store_true", help="Remove stored profile")


def cmd_profile(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print(f"Error: config not found: {args.config}", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir
    pipeline = args.pipeline

    if args.clear:
        clear_profile(state_dir, pipeline)
        print(f"Profile cleared for {pipeline}")
        return 0

    state = PipelineState.load(state_dir, pipeline)
    profile = compute_profile(pipeline, state)

    if profile is None:
        print(f"Insufficient data to profile '{pipeline}' (need at least 2 finished runs).")
        return 1

    if args.save:
        save_profile(state_dir, profile)

    print(f"Duration profile — {profile.pipeline} ({profile.sample_size} runs)")
    print(f"  mean   : {profile.mean_seconds:.1f}s")
    print(f"  median : {profile.median_seconds:.1f}s")
    print(f"  p95    : {profile.p95_seconds:.1f}s")
    print(f"  p99    : {profile.p99_seconds:.1f}s")
    print(f"  min    : {profile.min_seconds:.1f}s")
    print(f"  max    : {profile.max_seconds:.1f}s")
    return 0
