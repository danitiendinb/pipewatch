"""CLI entry point for pipewatch."""
import sys
import argparse
from pathlib import Path

from pipewatch.config import load_config
from pipewatch.checker import PipelineChecker
from pipewatch.alerts import dispatch_alerts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Lightweight CLI monitor for ETL pipeline health.",
    )
    parser.add_argument(
        "-c", "--config",
        default="pipewatch.yml",
        help="Path to config file (default: pipewatch.yml)",
    )
    subparsers = parser.add_subparsers(dest="command")

    check_parser = subparsers.add_parser("check", help="Run health checks for all pipelines.")
    check_parser.add_argument(
        "--pipeline",
        default=None,
        help="Check a single pipeline by name.",
    )

    subparsers.add_parser("status", help="Print current state of all pipelines.")

    return parser


def cmd_check(args, config):
    checker = PipelineChecker(config)
    if args.pipeline:
        pipelines = [p for p in config.pipelines if p.name == args.pipeline]
        if not pipelines:
            print(f"[pipewatch] Unknown pipeline: {args.pipeline}", file=sys.stderr)
            sys.exit(1)
        results = {pipelines[0].name: checker.check(pipelines[0])}
    else:
        results = checker.check_all()

    alerts_sent = 0
    for name, state in results.items():
        status = "OK" if state.consecutive_failures == 0 else f"FAILING ({state.consecutive_failures}x)"
        print(f"  {name}: {status}")
        sent = dispatch_alerts(state, config.alert)
        alerts_sent += len(sent)

    if alerts_sent:
        print(f"[pipewatch] {alerts_sent} alert(s) dispatched.")


def cmd_status(args, config):
    from pipewatch.state import PipelineState
    for pipeline in config.pipelines:
        store = PipelineState(config.state_dir)
        state = store.load(pipeline.name)
        last = state.last_run_at or "never"
        failures = state.consecutive_failures
        print(f"  {pipeline.name}: last_run={last}, consecutive_failures={failures}")


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"[pipewatch] Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    config = load_config(config_path)

    if args.command == "check":
        cmd_check(args, config)
    elif args.command == "status":
        cmd_status(args, config)
    else:
        parser.print_help()
        sys.exit(0)


if __name__ == "__main__":
    main()
