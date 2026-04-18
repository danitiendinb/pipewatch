"""CLI subcommands for acknowledging / unacknowledging pipeline alerts."""

from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.acknowledger import acknowledge, clear_acknowledgement, is_acknowledged


def add_acknowledge_subparser(subparsers: argparse._SubParsersAction) -> None:
    ack = subparsers.add_parser("ack", help="Acknowledge a pipeline alert")
    ack.add_argument("pipeline", help="Pipeline name")
    ack.add_argument("-m", "--message", default="", help="Optional note")

    unack = subparsers.add_parser("unack", help="Remove acknowledgement for a pipeline")
    unack.add_argument("pipeline", help="Pipeline name")


def cmd_acknowledge(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1
    acknowledge(cfg.state_dir, args.pipeline, getattr(args, "message", ""))
    print(f"Acknowledged '{args.pipeline}'.")
    return 0


def cmd_unacknowledge(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1
    if not is_acknowledged(cfg.state_dir, args.pipeline):
        print(f"'{args.pipeline}' is not currently acknowledged.")
        return 0
    clear_acknowledgement(cfg.state_dir, args.pipeline)
    print(f"Acknowledgement cleared for '{args.pipeline}'.")
    return 0
