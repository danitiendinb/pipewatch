"""CLI sub-command: scorecard — print a graded health report for all pipelines."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.scorecard import build_scorecard, format_scorecard
from pipewatch.state import PipelineState


def add_scorecard_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    p = subparsers.add_parser("scorecard", help="Print graded health scorecard")
    p.add_argument(
        "--config", default="pipewatch.yml", metavar="FILE", help="Config file path"
    )
    p.add_argument(
        "--period",
        type=int,
        default=7,
        metavar="DAYS",
        help="Look-back window in days (default: 7)",
    )
    p.add_argument(
        "--min-grade",
        default=None,
        metavar="GRADE",
        help="Only show pipelines at or below this grade (e.g. C)",
    )
    p.set_defaults(func=cmd_scorecard)


_GRADE_ORDER = ["A", "B", "C", "D", "F"]


def cmd_scorecard(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print(f"error: config not found: {args.config}", file=sys.stderr)
        return 1

    from pipewatch.state import PipelineState as _PS  # local import to mirror cli.py style

    store = _PS(cfg.state_dir)
    names = [p.name for p in cfg.pipelines]
    sc = build_scorecard(names, store, period_days=args.period)

    if args.min_grade and args.min_grade.upper() in _GRADE_ORDER:
        cutoff = _GRADE_ORDER.index(args.min_grade.upper())
        sc.rows = [r for r in sc.rows if _GRADE_ORDER.index(r.grade) >= cutoff]

    print(format_scorecard(sc))
    return 0
