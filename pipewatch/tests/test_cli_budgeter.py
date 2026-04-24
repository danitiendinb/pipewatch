"""Tests for pipewatch.cli_budgeter."""
from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_budgeter import add_budgeter_subparser, cmd_budget


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_budgeter_subparser(sub)
    return p


def _args(extra: list[str], config: str = "pipewatch.yml") -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=config)
    sub = p.add_subparsers(dest="command")
    add_budgeter_subparser(sub)
    return p.parse_args(["budget"] + extra)


# ---------------------------------------------------------------------------
# parser registration
# ---------------------------------------------------------------------------

def test_add_budgeter_subparser_registers_command(parser):
    ns = parser.parse_args(["budget", "show", "my_pipe"])
    assert ns.command == "budget"


def test_add_budgeter_subparser_set_defaults(parser):
    ns = parser.parse_args(["budget", "set", "my_pipe"])
    assert ns.rate == 0.10
    assert ns.window == 20


def test_add_budgeter_subparser_custom_rate(parser):
    ns = parser.parse_args(["budget", "set", "my_pipe", "--rate", "0.25"])
    assert ns.rate == pytest.approx(0.25)


# ---------------------------------------------------------------------------
# cmd_budget missing config
# ---------------------------------------------------------------------------

def test_cmd_budget_missing_config_returns_1(tmp_path):
    args = _args(["show", "p"], config=str(tmp_path / "missing.yml"))
    with patch("pipewatch.cli_budgeter.load_config", return_value=None):
        assert cmd_budget(args) == 1


# ---------------------------------------------------------------------------
# cmd_budget set
# ---------------------------------------------------------------------------

def test_cmd_budget_set_saves_policy(tmp_path):
    from pipewatch.budgeter import load_budget_policy

    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = _args(["set", "my_pipe", "--rate", "0.05", "--window", "15"])

    with patch("pipewatch.cli_budgeter.load_config", return_value=cfg):
        rc = cmd_budget(args)

    assert rc == 0
    policy = load_budget_policy(str(tmp_path), "my_pipe")
    assert policy is not None
    assert policy.max_failure_rate == pytest.approx(0.05)
    assert policy.window == 15


# ---------------------------------------------------------------------------
# cmd_budget show
# ---------------------------------------------------------------------------

def test_cmd_budget_show_no_policy(tmp_path, capsys):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = _args(["show", "unknown_pipe"])

    with patch("pipewatch.cli_budgeter.load_config", return_value=cfg):
        rc = cmd_budget(args)

    assert rc == 0
    out = capsys.readouterr().out
    assert "No budget policy" in out


# ---------------------------------------------------------------------------
# cmd_budget no sub-command
# ---------------------------------------------------------------------------

def test_cmd_budget_no_subcommand_returns_1(tmp_path):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = _args([])
    args.budget_cmd = None

    with patch("pipewatch.cli_budgeter.load_config", return_value=cfg):
        assert cmd_budget(args) == 1
