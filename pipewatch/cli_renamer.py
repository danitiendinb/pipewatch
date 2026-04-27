"""CLI sub-commands for pipeline renaming."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.renamer import load_rename_log, rename_pipeline


def add_renamer_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("rename", help="Rename a pipeline across all state files")
    sub = p.add_subparsers(dest="rename_cmd")

    do = sub.add_parser("pipeline", help="Perform the rename")
    do.add_argument("old_name", help="Current pipeline name")
    do.add_argument("new_name", help="New pipeline name")

    sub.add_parser("log", help="Show rename history")
    sub.add_parser("clear-log", help="Clear rename history")

    p.set_defaults(func=cmd_rename)


def cmd_rename(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1

    rename_cmd = getattr(args, "rename_cmd", None)
    if rename_cmd is None:
        print("error: specify a sub-command (pipeline | log | clear-log)", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir

    if rename_cmd == "pipeline":
        try:
            renamed = rename_pipeline(state_dir, args.old_name, args.new_name)
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        if not renamed:
            print(f"No state files found for '{args.old_name}' (or names are identical).")
        else:
            print(f"Renamed '{args.old_name}' -> '{args.new_name}': {len(renamed)} file(s)")
            for f in renamed:
                print(f"  {f}")
        return 0

    if rename_cmd == "log":
        entries = load_rename_log(state_dir)
        if not entries:
            print("No rename history.")
        for e in entries:
            print(f"{e['at']}  {e['from']} -> {e['to']}  ({len(e['files'])} file(s))")
        return 0

    if rename_cmd == "clear-log":
        from pipewatch.renamer import clear_rename_log
        clear_rename_log(state_dir)
        print("Rename log cleared.")
        return 0

    print(f"Unknown rename sub-command: {rename_cmd}", file=sys.stderr)
    return 1
