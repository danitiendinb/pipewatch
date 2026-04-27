"""Unit tests for pipewatch.cli_sentinel."""
from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_sentinel import add_sentinel_subparser, cmd_sentinel
from pipewatch.sentinel import SentinelPolicy


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="pipewatch.yml")
    sub = p.add_subparsers(dest="command")
    add_sentinel_subparser(sub)
    return p


def test_add_sentinel_subparser_registers_command(parser):
    args = parser.parse_args(["sentinel", "set", "my_pipe"])
    assert args.command == "sentinel"


def test_add_sentinel_subparser_default_max_failures(parser):
    args = parser.parse_args(["sentinel", "set", "my_pipe"])
    assert args.max_failures == 0


def test_add_sentinel_subparser_custom_max_failures(parser):
    args = parser.parse_args(["sentinel", "set", "my_pipe", "--max-failures", "3"])
    assert args.max_failures == 3


def test_cmd_sentinel_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(
        config=str(tmp_path / "missing.yml"),
        sentinel_cmd="set",
        pipeline="pipe_a",
        max_failures=0,
        no_notify_first=False,
    )
    assert cmd_sentinel(args) == 1


def test_cmd_sentinel_set_saves_policy(tmp_path):
    from pipewatch.sentinel import load_sentinel_policy

    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(
        config="pipewatch.yml",
        sentinel_cmd="set",
        pipeline="pipe_a",
        max_failures=1,
        no_notify_first=False,
    )
    with patch("pipewatch.cli_sentinel.load_config", return_value=cfg):
        rc = cmd_sentinel(args)
    assert rc == 0
    policy = load_sentinel_policy(str(tmp_path), "pipe_a")
    assert policy is not None
    assert policy.max_failures == 1


def test_cmd_sentinel_show_no_policy(tmp_path, capsys):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(
        config="pipewatch.yml",
        sentinel_cmd="show",
        pipeline="pipe_a",
    )
    with patch("pipewatch.cli_sentinel.load_config", return_value=cfg):
        rc = cmd_sentinel(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "No sentinel policy" in out


def test_cmd_sentinel_no_subcommand_returns_1(tmp_path):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(config="pipewatch.yml", sentinel_cmd=None)
    with patch("pipewatch.cli_sentinel.load_config", return_value=cfg):
        rc = cmd_sentinel(args)
    assert rc == 1
