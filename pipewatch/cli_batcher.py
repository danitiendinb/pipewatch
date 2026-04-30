"""CLI subcommands for batch tracking."""
from __future__ import annotations

import argparse
import sys

from pipewatch.batcher import (
    create_batch,
    load_batch,
    record_batch_result,
    clear_batch,
)
from pipewatch.config import load_config


def add_batcher_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("batch", help="Manage pipeline batch runs")
    sp = p.add_subparsers(dest="batch_cmd")

    create = sp.add_parser("create", help="Create a new batch")
    create.add_argument("batch_id")
    create.add_argument("pipelines", nargs="+")

    record = sp.add_parser("record", help="Record a pipeline result in a batch")
    record.add_argument("batch_id")
    record.add_argument("pipeline")
    record.add_argument("status", choices=["ok", "fail"])

    show = sp.add_parser("show", help="Show batch status")
    show.add_argument("batch_id")

    sp.add_parser("clear", help="Clear a batch").add_argument("batch_id")

    p.set_defaults(func=cmd_batcher)


def cmd_batcher(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1

    if args.batch_cmd is None:
        print("error: batch subcommand required", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir

    if args.batch_cmd == "create":
        record = create_batch(state_dir, args.batch_id, args.pipelines)
        print(f"Created batch '{record.batch_id}' with {record.total} pipeline(s).")
        return 0

    if args.batch_cmd == "record":
        result = record_batch_result(state_dir, args.batch_id, args.pipeline, args.status)
        if result is None:
            print(f"error: batch '{args.batch_id}' not found", file=sys.stderr)
            return 1
        print(
            f"Batch '{args.batch_id}': {result.passed}/{result.total} passed, "
            f"{result.failed} failed, {result.pending} pending."
        )
        return 0

    if args.batch_cmd == "show":
        record = load_batch(state_dir, args.batch_id)
        if record is None:
            print(f"error: batch '{args.batch_id}' not found", file=sys.stderr)
            return 1
        status = "HEALTHY" if record.healthy else ("INCOMPLETE" if not record.complete else "FAILED")
        print(f"Batch : {record.batch_id}")
        print(f"Status: {status} ({record.passed}/{record.total} passed)")
        for e in record.entries:
            print(f"  {e.pipeline}: {e.status}")
        return 0

    if args.batch_cmd == "clear":
        clear_batch(state_dir, args.batch_id)
        print(f"Cleared batch '{args.batch_id}'.")
        return 0

    print("error: unknown batch subcommand", file=sys.stderr)
    return 1
