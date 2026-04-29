"""CLI subcommand for the mirror feature."""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from pipewatch.config import load_config
from pipewatch.mirror import (
    load_mirror,
    clear_mirror,
    mirror_all,
)


def add_mirror_subparser(subparsers) -> None:
    p = subparsers.add_parser("mirror", help="Replicate pipeline state snapshots")
    p.add_argument("--config", default="pipewatch.yml")
    p.add_argument(
        "--destination",
        default="remote",
        help="Destination label for the mirror (default: remote)",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        help="Mirror a single pipeline (default: all)",
    )
    p.add_argument(
        "--show",
        action="store_true",
        help="Show current mirror records without mirroring",
    )
    p.add_argument(
        "--clear",
        action="store_true",
        help="Clear mirror record for --pipeline",
    )
    p.set_defaults(func=cmd_mirror)


def cmd_mirror(args) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print(f"Config not found: {args.config}", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir
    pipelines = (
        [args.pipeline]
        if args.pipeline
        else [p.name for p in cfg.pipelines]
    )

    if args.clear:
        if not args.pipeline:
            print("--clear requires --pipeline", file=sys.stderr)
            return 1
        clear_mirror(state_dir, args.pipeline)
        print(f"Cleared mirror record for {args.pipeline}")
        return 0

    if args.show:
        for name in pipelines:
            rec = load_mirror(state_dir, name)
            if rec is None:
                print(f"{name}: no mirror record")
            else:
                print(
                    f"{name}: dest={rec.destination} "
                    f"last={rec.last_mirrored} snapshots={rec.snapshot_count}"
                )
        return 0

    now = datetime.now(timezone.utc).isoformat()
    records = mirror_all(state_dir, pipelines, args.destination, now)
    for rec in records:
        print(
            f"Mirrored {rec.pipeline} -> {rec.destination} "
            f"({rec.snapshot_count} snapshots)"
        )
    return 0
