"""CLI subcommand: pipewatch graph"""
from __future__ import annotations

import argparse
import sys
from typing import List

from pipewatch.config import load_config
from pipewatch.grapher import build_graph, critical_path, reachable_from, root_nodes


def add_grapher_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("graph", help="Analyse the pipeline dependency graph")
    p.add_argument("--config", default="pipewatch.yml", help="Config file path")
    p.add_argument("--roots", action="store_true", help="List root (source) pipelines")
    p.add_argument(
        "--reachable",
        metavar="PIPELINE",
        help="List all pipelines reachable downstream from PIPELINE",
    )
    p.add_argument(
        "--critical-path",
        metavar="PIPELINE",
        dest="critical_path",
        help="Show the longest downstream chain from PIPELINE",
    )


def cmd_graph(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print(f"ERROR: config not found: {args.config}", file=sys.stderr)
        return 1

    names: List[str] = [p.name for p in cfg.pipelines]
    graph = build_graph(names, cfg.state_dir)

    if args.roots:
        roots = root_nodes(graph)
        if roots:
            print("Root pipelines (no upstreams):")
            for r in roots:
                print(f"  {r}")
        else:
            print("No root pipelines found.")
        return 0

    if args.reachable:
        reached = reachable_from(graph, args.reachable, direction="downstream")
        if not reached:
            print(f"No downstream pipelines reachable from '{args.reachable}'.")
        else:
            print(f"Downstream from '{args.reachable}':")
            for name in sorted(reached):
                print(f"  {name}")
        return 0

    if args.critical_path:
        path = critical_path(graph, args.critical_path)
        if not path:
            print(f"Pipeline '{args.critical_path}' not found in graph.")
            return 1
        print("Critical path: " + " -> ".join(path))
        return 0

    # Default: print full adjacency summary
    print(f"{'Pipeline':<30}  {'Upstreams':<25}  Downstreams")
    print("-" * 75)
    for name in graph.pipeline_names():
        node = graph.nodes[name]
        up = ", ".join(node.upstreams) or "-"
        down = ", ".join(node.downstreams) or "-"
        print(f"{name:<30}  {up:<25}  {down}")
    return 0
