"""CLI sub-commands for tombstone management."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from pipewatch.config import load_config
from pipewatch.tombstone import (
    clear_tombstone,
    is_tombstoned,
    list_tombstoned,
    load_tombstone,
    set_tombstone,
)


def add_tombstone_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("tombstone", help="Mark pipelines as decommissioned")
    sp = p.add_subparsers(dest="tombstone_cmd")

    mark = sp.add_parser("mark", help="Tombstone a pipeline")
    mark.add_argument("pipeline", help="Pipeline name")
    mark.add_argument("--reason", default="decommissioned", help="Reason for tombstoning")
    mark.add_argument("--by", dest="tombstoned_by", default=None, help="Who is tombstoning")

    sp.add_parser("list", help="List all tombstoned pipelines")

    show = sp.add_parser("show", help="Show tombstone details")
    show.add_argument("pipeline", help="Pipeline name")

    restore = sp.add_parser("restore", help="Remove tombstone from a pipeline")
    restore.add_argument("pipeline", help="Pipeline name")

    p.set_defaults(func=cmd_tombstone)


def cmd_tombstone(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config file not found", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir
    sub = getattr(args, "tombstone_cmd", None)

    if sub == "mark":
        now = datetime.now(timezone.utc).isoformat()
        rec = set_tombstone(
            state_dir,
            args.pipeline,
            reason=args.reason,
            tombstoned_at=now,
            tombstoned_by=args.tombstoned_by,
        )
        print(f"Tombstoned '{rec.pipeline}' at {rec.tombstoned_at}: {rec.reason}")
        return 0

    if sub == "list":
        names = list_tombstoned(state_dir)
        if not names:
            print("No tombstoned pipelines.")
        else:
            for name in names:
                print(name)
        return 0

    if sub == "show":
        rec = load_tombstone(state_dir, args.pipeline)
        if rec is None:
            print(f"'{args.pipeline}' is not tombstoned.")
            return 1
        print(f"Pipeline : {rec.pipeline}")
        print(f"Reason   : {rec.reason}")
        print(f"At       : {rec.tombstoned_at}")
        if rec.tombstoned_by:
            print(f"By       : {rec.tombstoned_by}")
        return 0

    if sub == "restore":
        removed = clear_tombstone(state_dir, args.pipeline)
        if removed:
            print(f"Tombstone cleared for '{args.pipeline}'.")
            return 0
        print(f"'{args.pipeline}' was not tombstoned.")
        return 1

    print("error: no sub-command given", file=sys.stderr)
    return 1
