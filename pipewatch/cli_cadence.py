"""cli_cadence.py — CLI subcommand for managing pipeline cadence policies."""
from __future__ import annotations

import argparse
import sys

from pipewatch.cadence import (
    CadencePolicy,
    clear_cadence_policy,
    evaluate_cadence,
    load_cadence_policy,
    save_cadence_policy,
)
from pipewatch.config import load_config
from pipewatch.state import load as load_state


def add_cadence_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("cadence", help="Manage pipeline cadence policies")
    sub = p.add_subparsers(dest="cadence_cmd")

    s = sub.add_parser("set", help="Set cadence policy for a pipeline")
    s.add_argument("pipeline", help="Pipeline name")
    s.add_argument("--interval", type=int, default=60,
                   help="Expected interval in minutes (default: 60)")
    s.add_argument("--tolerance", type=int, default=5,
                   help="Grace window in minutes (default: 5)")

    sub.add_parser("clear", help="Clear cadence policy").add_argument("pipeline")

    chk = sub.add_parser("check", help="Check cadence status")
    chk.add_argument("pipeline", help="Pipeline name")

    p.set_defaults(func=cmd_cadence)


def cmd_cadence(args: argparse.Namespace) -> int:
    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        print(f"Config not found: {args.config}", file=sys.stderr)
        return 1

    if not hasattr(args, "cadence_cmd") or args.cadence_cmd is None:
        print("No cadence subcommand given. Use set, clear, or check.", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir

    if args.cadence_cmd == "set":
        policy = CadencePolicy(
            expected_interval_minutes=args.interval,
            tolerance_minutes=args.tolerance,
        )
        save_cadence_policy(state_dir, args.pipeline, policy)
        print(f"Cadence policy set for '{args.pipeline}': "
              f"every {args.interval}m ±{args.tolerance}m")
        return 0

    if args.cadence_cmd == "clear":
        clear_cadence_policy(state_dir, args.pipeline)
        print(f"Cadence policy cleared for '{args.pipeline}'")
        return 0

    if args.cadence_cmd == "check":
        policy = load_cadence_policy(state_dir, args.pipeline)
        if policy is None:
            print(f"No cadence policy defined for '{args.pipeline}'")
            return 1
        state = load_state(state_dir, args.pipeline)
        report = evaluate_cadence(args.pipeline, state, policy)
        status = "ON CADENCE" if report.on_cadence else "OFF CADENCE"
        print(f"[{status}] {args.pipeline}")
        print(f"  Last run : {report.last_run_at or 'never'}")
        print(f"  Expected by: {report.expected_by or 'N/A'}")
        if not report.on_cadence:
            print(f"  Overdue  : {report.minutes_overdue}m")
        return 0 if report.on_cadence else 2

    print(f"Unknown cadence subcommand: {args.cadence_cmd}", file=sys.stderr)
    return 1
