"""CLI sub-command: pipewatch tagfilter — list pipelines matching tag criteria."""
from __future__ import annotations

import argparse
import sys
from typing import List

from pipewatch.config import load_config
from pipewatch.tagfilter import filter_by_tags, format_filter_row


def add_tagfilter_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser(
        "tagfilter",
        help="filter pipelines by tag key=value pairs",
    )
    p.add_argument(
        "tags",
        nargs="+",
        metavar="KEY=VALUE",
        help="one or more tag criteria (all must match)",
    )
    p.add_argument(
        "--config",
        default="pipewatch.yml",
        help="path to config file (default: pipewatch.yml)",
    )
    p.add_argument(
        "--all",
        dest="show_all",
        action="store_true",
        default=False,
        help="show all pipelines, not only those that match",
    )
    p.set_defaults(func=cmd_tagfilter)


def _parse_criteria(raw: List[str]) -> dict:
    criteria: dict = {}
    for item in raw:
        if "=" not in item:
            print(f"error: invalid tag criterion '{item}' — expected KEY=VALUE", file=sys.stderr)
            sys.exit(1)
        k, _, v = item.partition("=")
        criteria[k.strip()] = v.strip()
    return criteria


def cmd_tagfilter(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print(f"error: config file not found: {args.config}", file=sys.stderr)
        return 1

    criteria = _parse_criteria(args.tags)
    pipeline_names = [p.name for p in cfg.pipelines]
    results = filter_by_tags(pipeline_names, cfg.state_dir, criteria)

    shown = results if args.show_all else [r for r in results if r.matched]
    if not shown:
        print("no pipelines match the given tag criteria.")
        return 0

    for row in shown:
        print(format_filter_row(row))
    return 0
