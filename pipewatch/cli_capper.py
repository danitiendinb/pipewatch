"""cli_capper.py – CLI sub-commands for the run-count capper."""
from __future__ import annotations

import argparse
import sys

from pipewatch.capper import CapPolicy, clear_cap_policy, evaluate_cap, load_cap_policy, save_cap_policy
from pipewatch.config import load_config
from pipewatch.state import PipelineState


def add_capper_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("cap", help="Manage per-pipeline run-count caps")
    sub = p.add_subparsers(dest="cap_cmd")

    s = sub.add_parser("set", help="Set a cap policy for a pipeline")
    s.add_argument("pipeline")
    s.add_argument("--max-runs", type=int, default=100)
    s.add_argument("--window-hours", type=int, default=24)

    c = sub.add_parser("clear", help="Remove cap policy for a pipeline")
    c.add_argument("pipeline")

    v = sub.add_parser("check", help="Check whether a pipeline has exceeded its cap")
    v.add_argument("pipeline")


def cmd_capper(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir

    if args.cap_cmd == "set":
        policy = CapPolicy(max_runs=args.max_runs, window_hours=args.window_hours)
        save_cap_policy(state_dir, args.pipeline, policy)
        print(f"Cap set for '{args.pipeline}': max {policy.max_runs} runs per {policy.window_hours}h")
        return 0

    if args.cap_cmd == "clear":
        clear_cap_policy(state_dir, args.pipeline)
        print(f"Cap policy cleared for '{args.pipeline}'")
        return 0

    if args.cap_cmd == "check":
        policy = load_cap_policy(state_dir, args.pipeline)
        if policy is None:
            print(f"No cap policy set for '{args.pipeline}'")
            return 0
        state = PipelineState(state_dir)
        result = evaluate_cap(state, args.pipeline, policy)
        status = "EXCEEDED" if result.cap_exceeded else "ok"
        print(f"{args.pipeline}: {result.run_count} runs in last {policy.window_hours}h (cap={policy.max_runs}) [{status}]")
        return 1 if result.cap_exceeded else 0

    print("error: no sub-command given", file=sys.stderr)
    return 1
