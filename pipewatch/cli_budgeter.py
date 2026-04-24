"""cli_budgeter.py – CLI sub-commands for the failure-budget feature."""
from __future__ import annotations

import argparse
import sys

from pipewatch.budgeter import (
    BudgetPolicy,
    clear_budget_policy,
    evaluate_budget,
    load_budget_policy,
    save_budget_policy,
)
from pipewatch.config import load_config
from pipewatch.state import PipelineState


def add_budgeter_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("budget", help="manage failure budgets")
    sub = p.add_subparsers(dest="budget_cmd")

    # budget set <pipeline> --rate 0.1 --window 20
    ps = sub.add_parser("set", help="set a failure-budget policy")
    ps.add_argument("pipeline")
    ps.add_argument("--rate", type=float, default=0.10,
                    help="max allowed failure rate (default: 0.10)")
    ps.add_argument("--window", type=int, default=20,
                    help="rolling window of runs (default: 20)")

    # budget show <pipeline>
    psh = sub.add_parser("show", help="show current budget status")
    psh.add_argument("pipeline")

    # budget clear <pipeline>
    pc = sub.add_parser("clear", help="remove budget policy")
    pc.add_argument("pipeline")


def cmd_budget(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if cfg is None:
        print("error: config not found", file=sys.stderr)
        return 1

    state_dir = cfg.state_dir

    if args.budget_cmd == "set":
        policy = BudgetPolicy(max_failure_rate=args.rate, window=args.window)
        save_budget_policy(state_dir, args.pipeline, policy)
        print(f"Budget set for '{args.pipeline}': rate={args.rate}, window={args.window}")
        return 0

    if args.budget_cmd == "clear":
        clear_budget_policy(state_dir, args.pipeline)
        print(f"Budget policy cleared for '{args.pipeline}'")
        return 0

    if args.budget_cmd == "show":
        policy = load_budget_policy(state_dir, args.pipeline)
        if policy is None:
            print(f"No budget policy set for '{args.pipeline}'")
            return 0
        state = PipelineState.load(state_dir, args.pipeline)
        status = evaluate_budget(state, policy)
        burned_tag = " [BURNED]" if status.burned else ""
        print(f"{args.pipeline}{burned_tag}")
        print(f"  runs={status.total_runs}  failures={status.failures}  "
              f"rate={status.failure_rate:.1%}  budget={status.budget:.1%}  "
              f"remaining={status.remaining_failures}")
        return 0

    print("error: no budget sub-command given", file=sys.stderr)
    return 1
