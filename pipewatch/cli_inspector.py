"""CLI sub-command: pipewatch inspect"""
from __future__ import annotations

import sys
from argparse import ArgumentParser, Namespace

from pipewatch.config import load_config
from pipewatch.state import PipelineStore
from pipewatch.inspector import inspect_all, inspect_pipeline

_SEVERITY_COLOUR = {
    "critical": "\033[31m",
    "warning": "\033[33m",
    "info": "\033[36m",
}
_RESET = "\033[0m"


def add_inspector_subparser(sub) -> None:
    p: ArgumentParser = sub.add_parser("inspect", help="Inspect pipeline health in detail")
    p.add_argument("--pipeline", "-p", default=None, help="Inspect a single pipeline")
    p.add_argument("--no-colour", action="store_true", help="Disable ANSI colour output")
    p.add_argument("--config", "-c", default="pipewatch.yml")
    p.set_defaults(func=cmd_inspect)


def _render(report, no_colour: bool) -> str:
    lines = [f"=== {report.pipeline} ==="]
    for finding in report.findings:
        prefix = f"[{finding.severity.upper()}]"
        if not no_colour:
            colour = _SEVERITY_COLOUR.get(finding.severity, "")
            prefix = f"{colour}{prefix}{_RESET}"
        lines.append(f"  {prefix} ({finding.code}) {finding.message}")
    return "\n".join(lines)


def cmd_inspect(args: Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print(f"Config not found: {args.config}", file=sys.stderr)
        return 1

    store = PipelineStore(cfg.state_dir)
    schedules = {
        p.name: p.schedule_minutes
        for p in cfg.pipelines
        if p.schedule_minutes is not None
    }

    if args.pipeline:
        schedule_minutes = schedules.get(args.pipeline)
        reports = [inspect_pipeline(args.pipeline, store, cfg.state_dir, schedule_minutes)]
    else:
        reports = inspect_all(store, cfg.state_dir, schedules)

    if not reports:
        print("No pipelines found.")
        return 0

    for report in reports:
        print(_render(report, getattr(args, "no_colour", False)))

    critical_count = sum(1 for r in reports if r.has_critical)
    return 1 if critical_count else 0
