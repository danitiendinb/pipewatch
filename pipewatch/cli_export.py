"""CLI sub-command: export pipeline state to JSON or CSV."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

from pipewatch.config import PipewatchConfig
from pipewatch.state import StateStore
from pipewatch.exporter import export_json, export_csv


FORMATS = ("json", "csv")


def add_export_subparser(subparsers) -> None:
    p = subparsers.add_parser("export", help="Export pipeline state snapshot")
    p.add_argument(
        "--format",
        choices=FORMATS,
        default="json",
        help="Output format (default: json)",
    )
    p.add_argument(
        "--output",
        metavar="FILE",
        default=None,
        help="Write output to FILE instead of stdout",
    )
    p.add_argument(
        "--pipeline",
        metavar="NAME",
        default=None,
        help="Limit export to a single pipeline",
    )


def cmd_export(args, config: PipewatchConfig) -> int:
    store = StateStore(config.state_dir)
    pipelines = (
        [args.pipeline]
        if args.pipeline
        else [p.name for p in config.pipelines]
    )

    states = {name: store.load(name) for name in pipelines}

    if args.format == "json":
        output = export_json(states)
    else:
        output = export_csv(states)

    if args.output:
        Path(args.output).write_text(output)
    else:
        sys.stdout.write(output)
        if not output.endswith("\n"):
            sys.stdout.write("\n")

    return 0
