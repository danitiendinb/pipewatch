"""CLI subcommands for pipeline execution tracing."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.tracer import load_traces, get_run_traces, clear_traces


def add_tracer_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("trace", help="Show execution trace events for a pipeline")
    p.add_argument("pipeline", help="Pipeline name")
    p.add_argument("--run-id", default=None, help="Filter by run ID")
    p.add_argument("--clear", action="store_true", help="Clear all traces for the pipeline")
    p.add_argument("--config", default="pipewatch.yml")


def cmd_trace(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir

    if args.clear:
        clear_traces(state_dir, args.pipeline)
        print(f"Traces cleared for {args.pipeline}.")
        return 0

    if args.run_id:
        events = get_run_traces(state_dir, args.pipeline, args.run_id)
    else:
        events = load_traces(state_dir, args.pipeline)

    if not events:
        print(f"No trace events found for {args.pipeline}.")
        return 0

    for e in events:
        detail = f" — {e['detail']}" if e.get("detail") else ""
        print(f"[{e['timestamp']}] run={e['run_id']} {e['event']}{detail}")

    return 0
