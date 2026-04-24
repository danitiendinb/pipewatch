"""CLI sub-commands for managing pipeline bookmarks."""
from __future__ import annotations

import argparse
import sys

from pipewatch.bookmarker import all_bookmarks, clear_bookmark, load_bookmark, set_bookmark
from pipewatch.config import load_config


def add_bookmarker_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("bookmark", help="Manage pipeline bookmarks")
    sub = p.add_subparsers(dest="bookmark_cmd")

    # get
    g = sub.add_parser("get", help="Show the current bookmark for a pipeline")
    g.add_argument("pipeline", help="Pipeline name")

    # set
    s = sub.add_parser("set", help="Set a bookmark value for a pipeline")
    s.add_argument("pipeline", help="Pipeline name")
    s.add_argument("value", help="Bookmark value (e.g. timestamp or offset)")

    # clear
    c = sub.add_parser("clear", help="Remove the bookmark for a pipeline")
    c.add_argument("pipeline", help="Pipeline name")

    # list
    sub.add_parser("list", help="List all bookmarks")


def cmd_bookmark(args: argparse.Namespace) -> int:
    cfg = load_config(getattr(args, "config", "pipewatch.yml"))
    if cfg is None:
        print("error: config file not found", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir

    if args.bookmark_cmd == "get":
        bm = load_bookmark(state_dir, args.pipeline)
        if bm is None:
            print(f"{args.pipeline}: no bookmark set")
        else:
            print(f"{bm.pipeline}: {bm.value}  (updated {bm.updated_at})")
        return 0

    if args.bookmark_cmd == "set":
        bm = set_bookmark(state_dir, args.pipeline, args.value)
        print(f"bookmark set: {bm.pipeline} -> {bm.value}")
        return 0

    if args.bookmark_cmd == "clear":
        clear_bookmark(state_dir, args.pipeline)
        print(f"bookmark cleared: {args.pipeline}")
        return 0

    if args.bookmark_cmd == "list":
        bookmarks = all_bookmarks(state_dir)
        if not bookmarks:
            print("no bookmarks stored")
        for bm in bookmarks:
            print(f"{bm.pipeline}: {bm.value}  (updated {bm.updated_at})")
        return 0

    print("error: no sub-command specified", file=sys.stderr)
    return 1
