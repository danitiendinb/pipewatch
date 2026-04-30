"""CLI sub-commands for the pipeline run sampler."""

from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.state import PipelineState
from pipewatch.sampler import sample_runs, save_sample, load_sample, clear_sample


def add_sampler_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("sample", help="Spot-sample pipeline runs for auditing")
    p.add_argument("pipeline", help="Pipeline name to sample")
    p.add_argument(
        "--n",
        type=int,
        default=5,
        metavar="N",
        help="Number of runs to sample (default: 5)",
    )
    p.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    p.add_argument("--save", action="store_true", help="Persist sample to state directory")
    p.add_argument("--show", action="store_true", help="Print previously saved sample")
    p.add_argument("--clear", action="store_true", help="Remove saved sample")
    p.set_defaults(func=cmd_sampler)


def cmd_sampler(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config file not found", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir
    pipeline = args.pipeline

    if args.clear:
        clear_sample(state_dir, pipeline)
        print(f"Sample cleared for '{pipeline}'.")
        return 0

    if args.show:
        results = load_sample(state_dir, pipeline)
        if not results:
            print(f"No saved sample for '{pipeline}'.")
            return 0
        _print_results(results)
        return 0

    store = PipelineState(state_dir)
    results = sample_runs(store, pipeline, n=args.n, seed=args.seed)
    if not results:
        print(f"No runs recorded for '{pipeline}'.")
        return 0

    _print_results(results)

    if args.save:
        path = save_sample(state_dir, pipeline, results)
        print(f"Sample saved to {path}")

    return 0


def _print_results(results) -> None:  # type: ignore[no-untyped-def]
    for r in results:
        status_tag = f"[{r.status.upper()}]"
        print(f"{status_tag:10s} {r.run_id}  started={r.started_at}  msg={r.message!r}")
