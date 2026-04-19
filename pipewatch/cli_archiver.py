"""CLI subcommand for archiving pipeline run history."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.state import load as load_state
from pipewatch.archiver import archive_pipeline, load_archive, clear_archive


def add_archiver_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("archive", help="Archive pipeline run history")
    p.add_argument("pipeline", help="Pipeline name")
    p.add_argument("--config", default="pipewatch.yml")
    p.add_argument("--show", action="store_true", help="Print archived record count")
    p.add_argument("--clear", action="store_true", help="Remove archive file")


def cmd_archive(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print(f"Config not found: {args.config}", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir
    pipeline = args.pipeline

    if args.clear:
        clear_archive(state_dir, pipeline)
        print(f"Archive cleared for '{pipeline}'.")
        return 0

    if args.show:
        records = load_archive(state_dir, pipeline)
        print(f"'{pipeline}' archive contains {len(records)} record(s).")
        return 0

    store = load_state(state_dir, pipeline)
    path = archive_pipeline(state_dir, pipeline, store)
    print(f"Archived {len(store.runs)} run(s) for '{pipeline}' -> {path}")
    return 0
