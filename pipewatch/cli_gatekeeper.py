"""CLI sub-commands for the gatekeeper feature."""
from __future__ import annotations

import argparse
import sys

from pipewatch.config import load_config
from pipewatch.gatekeeper import (
    GatePolicy,
    clear_gate_policy,
    evaluate_gate,
    load_gate_policy,
    save_gate_policy,
)
from pipewatch.state import PipelineState


def add_gatekeeper_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("gate", help="manage pipeline gate policies")
    sp = p.add_subparsers(dest="gate_cmd")

    # set
    s = sp.add_parser("set", help="set gate policy for a pipeline")
    s.add_argument("pipeline")
    s.add_argument("--min-score", type=float, default=0.0)
    s.add_argument("--max-failures", type=int, default=0)
    s.add_argument("--require-status", choices=["ok", "failing"], default=None)

    # check
    c = sp.add_parser("check", help="evaluate gate for a pipeline")
    c.add_argument("pipeline")

    # clear
    cl = sp.add_parser("clear", help="remove gate policy for a pipeline")
    cl.add_argument("pipeline")


def cmd_gatekeeper(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir

    if args.gate_cmd == "set":
        policy = GatePolicy(
            min_score=args.min_score,
            max_consecutive_failures=args.max_failures,
            require_status=args.require_status,
        )
        save_gate_policy(state_dir, args.pipeline, policy)
        print(f"gate policy saved for '{args.pipeline}'")
        return 0

    if args.gate_cmd == "check":
        from pipewatch.state import PipelineState as _PS
        store = _PS.load(state_dir, args.pipeline)
        decision = evaluate_gate(state_dir, args.pipeline, store)
        if decision.allowed:
            print(f"{args.pipeline}: ALLOWED")
        else:
            print(f"{args.pipeline}: BLOCKED")
            for r in decision.reasons:
                print(f"  - {r}")
        return 0 if decision.allowed else 2

    if args.gate_cmd == "clear":
        clear_gate_policy(state_dir, args.pipeline)
        print(f"gate policy cleared for '{args.pipeline}'")
        return 0

    print("error: no gate sub-command specified", file=sys.stderr)
    return 1
