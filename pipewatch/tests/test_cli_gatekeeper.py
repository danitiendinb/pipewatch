"""Unit tests for pipewatch.cli_gatekeeper."""
from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_gatekeeper import add_gatekeeper_subparser, cmd_gatekeeper
from pipewatch.gatekeeper import GateDecision, GatePolicy


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="pipewatch.yml")
    sp = p.add_subparsers(dest="command")
    add_gatekeeper_subparser(sp)
    return p


def test_add_gatekeeper_subparser_registers_command(parser):
    args = parser.parse_args(["gate", "set", "my_pipe"])
    assert args.command == "gate"


def test_add_gatekeeper_subparser_min_score_default(parser):
    args = parser.parse_args(["gate", "set", "my_pipe"])
    assert args.min_score == 0.0


def test_add_gatekeeper_subparser_max_failures_default(parser):
    args = parser.parse_args(["gate", "set", "my_pipe"])
    assert args.max_failures == 0


def test_cmd_gatekeeper_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(
        config=str(tmp_path / "missing.yml"),
        gate_cmd="set",
        pipeline="p",
        min_score=0.0,
        max_failures=0,
        require_status=None,
    )
    assert cmd_gatekeeper(args) == 1


def test_cmd_gatekeeper_set_saves_policy(tmp_path):
    from pipewatch.config import PipewatchConfig, PipelineConfig
    cfg = PipewatchConfig(state_dir=str(tmp_path), pipelines=[], log_level="INFO")
    args = argparse.Namespace(
        config="pw.yml",
        gate_cmd="set",
        pipeline="pipe1",
        min_score=55.0,
        max_failures=3,
        require_status=None,
    )
    with patch("pipewatch.cli_gatekeeper.load_config", return_value=cfg):
        rc = cmd_gatekeeper(args)
    assert rc == 0
    from pipewatch.gatekeeper import load_gate_policy
    policy = load_gate_policy(str(tmp_path), "pipe1")
    assert policy is not None
    assert policy.min_score == 55.0


def test_cmd_gatekeeper_clear_returns_0(tmp_path):
    from pipewatch.config import PipewatchConfig
    from pipewatch.gatekeeper import save_gate_policy
    save_gate_policy(str(tmp_path), "pipe1", GatePolicy(min_score=10.0))
    cfg = PipewatchConfig(state_dir=str(tmp_path), pipelines=[], log_level="INFO")
    args = argparse.Namespace(config="pw.yml", gate_cmd="clear", pipeline="pipe1")
    with patch("pipewatch.cli_gatekeeper.load_config", return_value=cfg):
        rc = cmd_gatekeeper(args)
    assert rc == 0


def test_cmd_gatekeeper_no_subcommand_returns_1(tmp_path):
    from pipewatch.config import PipewatchConfig
    cfg = PipewatchConfig(state_dir=str(tmp_path), pipelines=[], log_level="INFO")
    args = argparse.Namespace(config="pw.yml", gate_cmd=None)
    with patch("pipewatch.cli_gatekeeper.load_config", return_value=cfg):
        rc = cmd_gatekeeper(args)
    assert rc == 1
