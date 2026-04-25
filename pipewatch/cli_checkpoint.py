"""CLI sub-commands for checkpoint management."""
from __future__ import annotations

import argparse
import json
import sys

from pipewatch.checkpoint import (
    clear_checkpoints,
    get_checkpoint,
    load_checkpoints,
    remove_checkpoint,
    set_checkpoint,
)
from pipewatch.config import load_config


def add_checkpoint_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    p = subparsers.add_parser("checkpoint", help="Manage pipeline checkpoints")
    sub = p.add_subparsers(dest="checkpoint_cmd")

    # set
    ps = sub.add_parser("set", help="Record a named checkpoint")
    ps.add_argument("pipeline")
    ps.add_argument("name")
    ps.add_argument("--meta", default="{}", help="JSON metadata string")

    # get
    pg = sub.add_parser("get", help="Show a checkpoint")
    pg.add_argument("pipeline")
    pg.add_argument("name")

    # list
    pl = sub.add_parser("list", help="List all checkpoints for a pipeline")
    pl.add_argument("pipeline")

    # remove
    pr = sub.add_parser("remove", help="Remove a named checkpoint")
    pr.add_argument("pipeline")
    pr.add_argument("name")

    # clear
    pc = sub.add_parser("clear", help="Clear all checkpoints for a pipeline")
    pc.add_argument("pipeline")


def cmd_checkpoint(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir
    sub = args.checkpoint_cmd

    if sub == "set":
        try:
            meta = json.loads(args.meta)
        except json.JSONDecodeError:
            print("error: --meta must be valid JSON", file=sys.stderr)
            return 1
        cp = set_checkpoint(state_dir, args.pipeline, args.name, meta)
        print(f"checkpoint '{cp.name}' recorded at {cp.recorded_at}")
        return 0

    if sub == "get":
        cp = get_checkpoint(state_dir, args.pipeline, args.name)
        if cp is None:
            print(f"no checkpoint '{args.name}' for {args.pipeline}")
            return 1
        print(json.dumps({"name": cp.name, "recorded_at": cp.recorded_at, "metadata": cp.metadata}, indent=2))
        return 0

    if sub == "list":
        data = load_checkpoints(state_dir, args.pipeline)
        if not data:
            print(f"no checkpoints for {args.pipeline}")
            return 0
        for name, cp in data.items():
            print(f"{name}  {cp.recorded_at}")
        return 0

    if sub == "remove":
        removed = remove_checkpoint(state_dir, args.pipeline, args.name)
        if not removed:
            print(f"checkpoint '{args.name}' not found", file=sys.stderr)
            return 1
        print(f"removed '{args.name}'")
        return 0

    if sub == "clear":
        clear_checkpoints(state_dir, args.pipeline)
        print(f"cleared all checkpoints for {args.pipeline}")
        return 0

    print("error: no sub-command given", file=sys.stderr)
    return 1
