"""CLI subcommands for managing pipeline dependencies."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.dependency import (
    add_upstream,
    load_dependencies,
    remove_upstream,
    clear_dependencies,
)


def add_dependency_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("dependency", help="Manage pipeline dependencies")
    sub = p.add_subparsers(dest="dep_cmd", required=True)

    add_p = sub.add_parser("add", help="Add an upstream dependency")
    add_p.add_argument("pipeline")
    add_p.add_argument("upstream")

    rm_p = sub.add_parser("remove", help="Remove an upstream dependency")
    rm_p.add_argument("pipeline")
    rm_p.add_argument("upstream")

    show_p = sub.add_parser("show", help="Show dependencies for a pipeline")
    show_p.add_argument("pipeline")

    clear_p = sub.add_parser("clear", help="Clear all dependencies for a pipeline")
    clear_p.add_argument("pipeline")


def cmd_dependency(args: argparse.Namespace, config_path: str = "pipewatch.yml") -> int:
    cfg = load_config(config_path)
    if cfg is None:
        print("ERROR: config not found", file=sys.stderr)
        return 1
    state_dir = cfg.state_dir

    if args.dep_cmd == "add":
        add_upstream(state_dir, args.pipeline, args.upstream)
        print(f"Added upstream '{args.upstream}' -> '{args.pipeline}'")

    elif args.dep_cmd == "remove":
        remove_upstream(state_dir, args.pipeline, args.upstream)
        print(f"Removed upstream '{args.upstream}' from '{args.pipeline}'")

    elif args.dep_cmd == "show":
        data = load_dependencies(state_dir, args.pipeline)
        print(f"Pipeline : {args.pipeline}")
        print(f"  upstream  : {', '.join(data['upstream']) or '(none)'}")
        print(f"  downstream: {', '.join(data['downstream']) or '(none)'}")

    elif args.dep_cmd == "clear":
        clear_dependencies(state_dir, args.pipeline)
        print(f"Cleared dependencies for '{args.pipeline}'")

    return 0
