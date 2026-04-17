"""CLI sub-command: pipewatch dashboard"""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.state import PipelineState
from pipewatch.dashboard import run_dashboard


def add_dashboard_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("dashboard", help="Print a live health dashboard")
    p.add_argument("--config", default="pipewatch.yml", help="Path to config file")
    p.add_argument("--no-colour", action="store_true", help="Disable ANSI colour output")


def cmd_dashboard(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    if config is None:
        print(f"Error: config file '{args.config}' not found.", file=sys.stderr)
        return 1

    store = PipelineState(config.state_dir)
    output = run_dashboard(config, store)

    if getattr(args, "no_colour", False):
        import re
        output = re.sub(r"\033\[[0-9;]*m", "", output)

    print(output)
    return 0
