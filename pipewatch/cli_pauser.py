"""CLI sub-commands for the pauser feature."""

from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.pauser import clear_pause, is_paused, load_pause, pause_pipeline


def add_pauser_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    pause_p = subparsers.add_parser("pause", help="Pause a pipeline temporarily")
    pause_p.add_argument("pipeline", help="Pipeline name")
    pause_p.add_argument(
        "--hours",
        type=float,
        default=1.0,
        help="Duration to pause in hours (default: 1.0)",
    )
    pause_p.set_defaults(pauser_cmd="pause")

    unpause_p = subparsers.add_parser("unpause", help="Remove a pipeline pause")
    unpause_p.add_argument("pipeline", help="Pipeline name")
    unpause_p.set_defaults(pauser_cmd="unpause")

    status_p = subparsers.add_parser("pause-status", help="Show pause status for a pipeline")
    status_p.add_argument("pipeline", help="Pipeline name")
    status_p.set_defaults(pauser_cmd="status")


def cmd_pauser(args: argparse.Namespace) -> int:
    cfg = load_config(getattr(args, "config", "pipewatch.yml"))
    if cfg is None:
        print("error: config file not found", file=sys.stderr)
        return 1

    pauser_cmd = getattr(args, "pauser_cmd", None)
    if pauser_cmd is None:
        print("error: no pauser sub-command given", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir
    pipeline = args.pipeline

    if pauser_cmd == "pause":
        expiry = pause_pipeline(state_dir, pipeline, args.hours)
        print(f"Paused '{pipeline}' until {expiry.isoformat()}")
        return 0

    if pauser_cmd == "unpause":
        clear_pause(state_dir, pipeline)
        print(f"Pause cleared for '{pipeline}'")
        return 0

    if pauser_cmd == "status":
        if is_paused(state_dir, pipeline):
            expiry = load_pause(state_dir, pipeline)
            print(f"'{pipeline}' is PAUSED until {expiry.isoformat()}")
        else:
            print(f"'{pipeline}' is not paused")
        return 0

    print(f"error: unknown pauser sub-command '{pauser_cmd}'", file=sys.stderr)
    return 1
