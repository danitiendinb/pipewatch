"""cli_ventilator.py – CLI subcommands for the ventilator (backpressure) feature."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.ventilator import (
    clear_ventilator,
    evaluate_pressure,
    load_ventilator,
    overloaded_pipelines,
    update_ventilator,
)


def add_ventilator_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("ventilator", help="Monitor pipeline backpressure")
    sub = p.add_subparsers(dest="vent_cmd")

    # set
    s = sub.add_parser("set", help="Update queue/active counts for a pipeline")
    s.add_argument("pipeline")
    s.add_argument("--queued", type=int, default=0)
    s.add_argument("--active", type=int, default=0)

    # show
    sh = sub.add_parser("show", help="Show current ventilator state")
    sh.add_argument("pipeline")
    sh.add_argument("--threshold", type=int, default=10)

    # scan
    sc = sub.add_parser("scan", help="List overloaded pipelines")
    sc.add_argument("--threshold", type=int, default=10)

    # clear
    cl = sub.add_parser("clear", help="Remove ventilator state for a pipeline")
    cl.add_argument("pipeline")


def cmd_ventilator(args: argparse.Namespace) -> int:
    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        print(f"Config not found: {args.config}", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir

    if args.vent_cmd == "set":
        state = update_ventilator(state_dir, args.pipeline, args.queued, args.active)
        print(f"Updated {args.pipeline}: queued={state.queued} active={state.active}")
        return 0

    if args.vent_cmd == "show":
        state = load_ventilator(state_dir, args.pipeline)
        report = evaluate_pressure(state, args.threshold)
        status = "OVERLOADED" if report.overloaded else "ok"
        print(
            f"{report.pipeline}: queued={report.queued} active={report.active} "
            f"pressure={report.pressure:.0%} threshold={report.threshold} [{status}]"
        )
        return 0

    if args.vent_cmd == "scan":
        names = [p.name for p in cfg.pipelines]
        reports = overloaded_pipelines(state_dir, names, args.threshold)
        if not reports:
            print("No overloaded pipelines.")
            return 0
        for r in reports:
            print(f"  {r.pipeline}: queued={r.queued} pressure={r.pressure:.0%}")
        return 0

    if args.vent_cmd == "clear":
        clear_ventilator(state_dir, args.pipeline)
        print(f"Cleared ventilator state for {args.pipeline}")
        return 0

    print("No subcommand given. Use --help.", file=sys.stderr)
    return 1
