"""cli_expirer.py – CLI subcommands for pipeline expiry management."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.expirer import (
    load_expiry,
    save_expiry,
    clear_expiry,
    expired_pipelines,
)
from pipewatch.state import PipelineState


def add_expirer_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("expiry", help="manage pipeline expiry TTLs")
    sp = p.add_subparsers(dest="expiry_cmd")

    set_p = sp.add_parser("set", help="set a TTL for a pipeline")
    set_p.add_argument("pipeline", help="pipeline name")
    set_p.add_argument("--hours", type=float, default=24.0, help="TTL in hours (default: 24)")

    sp.add_parser("list", help="list expired pipelines")

    clr = sp.add_parser("clear", help="remove expiry policy")
    clr.add_argument("pipeline", help="pipeline name")

    p.add_argument("--config", default="pipewatch.yml")
    p.set_defaults(func=cmd_expirer)


def cmd_expirer(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print(f"error: config not found: {args.config}", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir
    cmd = getattr(args, "expiry_cmd", None)

    if cmd == "set":
        record = save_expiry(state_dir, args.pipeline, args.hours)
        print(f"Expiry set for '{args.pipeline}': {args.hours}h → expires {record.expires_at}")
        return 0

    if cmd == "clear":
        clear_expiry(state_dir, args.pipeline)
        print(f"Expiry cleared for '{args.pipeline}'.")
        return 0

    if cmd == "list":
        store = PipelineState(state_dir)
        names = [p.name for p in cfg.pipelines]
        expired = expired_pipelines(state_dir, names, store)
        if not expired:
            print("No expired pipelines.")
        else:
            for name in expired:
                rec = load_expiry(state_dir, name)
                print(f"  EXPIRED  {name}  (ttl={rec.ttl_hours}h, expires_at={rec.expires_at})")
        return 0

    print("error: specify a subcommand: set | list | clear", file=sys.stderr)
    return 1
