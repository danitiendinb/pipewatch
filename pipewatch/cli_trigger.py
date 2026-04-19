"""CLI subcommands for pipeline trigger management."""
from __future__ import annotations

import argparse

from pipewatch.config import load_config
from pipewatch.trigger import set_trigger, clear_trigger, load_trigger, pending_triggers


def add_trigger_subparser(subparsers: argparse._SubParsersAction) -> None:
    t = subparsers.add_parser("trigger", help="Manage pipeline triggers")
    tsub = t.add_subparsers(dest="trigger_cmd")

    fire = tsub.add_parser("fire", help="Set a trigger for a pipeline")
    fire.add_argument("pipeline")
    fire.add_argument("--reason", default="manual", help="Trigger reason")
    fire.add_argument("--by", default="user", dest="triggered_by")

    clr = tsub.add_parser("clear", help="Clear a pipeline trigger")
    clr.add_argument("pipeline")

    tsub.add_parser("list", help="List all pending triggers")


def cmd_trigger(args: argparse.Namespace, config_path: str = "pipewatch.yml") -> int:
    cfg = load_config(config_path)
    if cfg is None:
        print("Error: config not found")
        return 1

    state_dir = cfg.state_dir

    if args.trigger_cmd == "fire":
        rec = set_trigger(state_dir, args.pipeline, args.reason, args.triggered_by)
        print(f"Trigger set for '{rec.pipeline}': {rec.reason} (by {rec.triggered_by})")
        return 0

    if args.trigger_cmd == "clear":
        clear_trigger(state_dir, args.pipeline)
        print(f"Trigger cleared for '{args.pipeline}'")
        return 0

    if args.trigger_cmd == "list":
        names = [p.name for p in cfg.pipelines]
        pending = pending_triggers(state_dir, names)
        if not pending:
            print("No pending triggers.")
        for r in pending:
            print(f"  {r.pipeline}: {r.reason} (by {r.triggered_by} at {r.timestamp})")
        return 0

    print("No trigger subcommand given. Use fire, clear, or list.")
    return 1
