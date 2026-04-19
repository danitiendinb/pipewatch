"""CLI subcommand: pipewatch replay."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.state import PipelineState
from pipewatch.replayer import replay_runs, clear_replay


def add_replayer_subparser(subparsers) -> None:
    p = subparsers.add_parser("replay", help="Re-emit historical run events")
    p.add_argument("pipeline", help="Pipeline name to replay")
    p.add_argument("--since", default=None, help="ISO timestamp lower bound")
    p.add_argument("--dry-run", action="store_true", help="Do not mark runs as replayed")
    p.add_argument("--clear", action="store_true", help="Clear replay tracking for pipeline")
    p.add_argument("--config", default="pipewatch.yml")


def cmd_replay(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1

    store = PipelineState(cfg.state_dir)

    if args.clear:
        clear_replay(cfg.state_dir, args.pipeline)
        print(f"Replay tracking cleared for {args.pipeline}")
        return 0

    collected = []

    def _handler(run):
        collected.append(run)

    result = replay_runs(
        store=store,
        state_dir=cfg.state_dir,
        pipeline=args.pipeline,
        handler=_handler,
        since=args.since,
        dry_run=args.dry_run,
    )

    for run in collected:
        status = "ok" if run.success else "fail"
        print(f"  [{status}] {run.run_id} finished={run.finished_at}")

    print(f"Replayed {result.replayed}, skipped {result.skipped}")
    return 0
