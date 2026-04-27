"""CLI subcommands for managing sentinel policies."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.sentinel import (
    SentinelPolicy,
    clear_sentinel_policy,
    load_sentinel_policy,
    save_sentinel_policy,
)


def add_sentinel_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("sentinel", help="Manage sentinel policies for pipelines")
    sub = p.add_subparsers(dest="sentinel_cmd")

    # set
    s = sub.add_parser("set", help="Mark a pipeline as a sentinel")
    s.add_argument("pipeline", help="Pipeline name")
    s.add_argument("--max-failures", type=int, default=0, help="Max consecutive failures (default 0)")
    s.add_argument("--no-notify-first", action="store_true", help="Suppress first-failure notification")

    # show
    sh = sub.add_parser("show", help="Show sentinel policy for a pipeline")
    sh.add_argument("pipeline", help="Pipeline name")

    # clear
    cl = sub.add_parser("clear", help="Remove sentinel policy")
    cl.add_argument("pipeline", help="Pipeline name")

    p.set_defaults(func=cmd_sentinel)


def cmd_sentinel(args: argparse.Namespace) -> int:
    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        print("error: config file not found", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir

    if args.sentinel_cmd == "set":
        policy = SentinelPolicy(
            enabled=True,
            max_failures=args.max_failures,
            notify_on_first=not args.no_notify_first,
        )
        save_sentinel_policy(state_dir, args.pipeline, policy)
        print(f"Sentinel policy set for '{args.pipeline}' (max_failures={args.max_failures}).")
        return 0

    if args.sentinel_cmd == "show":
        policy = load_sentinel_policy(state_dir, args.pipeline)
        if policy is None:
            print(f"No sentinel policy for '{args.pipeline}'.")
            return 0
        print(f"pipeline:        {args.pipeline}")
        print(f"enabled:         {policy.enabled}")
        print(f"max_failures:    {policy.max_failures}")
        print(f"notify_on_first: {policy.notify_on_first}")
        return 0

    if args.sentinel_cmd == "clear":
        clear_sentinel_policy(state_dir, args.pipeline)
        print(f"Sentinel policy cleared for '{args.pipeline}'.")
        return 0

    print("error: no subcommand given", file=sys.stderr)
    return 1
