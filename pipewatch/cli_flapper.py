"""CLI subcommand for flap detection."""

from __future__ import annotations

import sys
from argparse import ArgumentParser, Namespace
from typing import Optional

from pipewatch.config import load_config
from pipewatch.state import PipelineState
from pipewatch.flapper import detect_all, save_flap_report


def add_flapper_subparser(subparsers) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "flap", help="Detect pipelines oscillating between success and failure"
    )
    p.add_argument("--config", default="pipewatch.yml", help="Config file path")
    p.add_argument(
        "--threshold",
        type=int,
        default=3,
        help="Number of transitions to consider flapping (default: 3)",
    )
    p.add_argument(
        "--window",
        type=int,
        default=10,
        help="Number of recent runs to inspect (default: 10)",
    )
    p.add_argument(
        "--save",
        action="store_true",
        default=False,
        help="Persist flap reports to state directory",
    )
    p.set_defaults(func=cmd_flapper)


def cmd_flapper(args: Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print(f"ERROR: config not found: {args.config}", file=sys.stderr)
        return 1

    pipeline_names = [p.name for p in cfg.pipelines]

    def _load(name: str) -> PipelineState:
        from pipewatch.state import load as load_state
        return load_state(cfg.state_dir, name)

    reports = detect_all(
        pipeline_names,
        _load,
        threshold=args.threshold,
        window=args.window,
    )

    flapping = [r for r in reports if r.is_flapping]

    if not flapping:
        print("OK  No flapping pipelines detected.")
        return 0

    for r in flapping:
        print(f"FLAP  {r.pipeline}  transitions={r.flap_count}  history={r.transitions}")
        if args.save:
            save_flap_report(cfg.state_dir, r)

    return 1
