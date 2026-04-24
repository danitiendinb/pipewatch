"""CLI sub-command: pipewatch lint"""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.linter import lint_config, format_lint_report


def add_linter_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    p = subparsers.add_parser("lint", help="Validate pipewatch configuration")
    p.add_argument(
        "--config", "-c",
        default="pipewatch.yml",
        help="Path to config file (default: pipewatch.yml)",
    )
    p.add_argument(
        "--strict",
        action="store_true",
        default=False,
        help="Exit non-zero on warnings as well as errors",
    )


def cmd_lint(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    if config is None:
        print(f"Error: config file not found: {args.config}", file=sys.stderr)
        return 1

    report = lint_config(config)
    print(format_lint_report(report))

    if not report.ok:
        return 2
    if args.strict and report.warnings:
        return 3
    return 0
