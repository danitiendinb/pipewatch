"""Unit tests for pipewatch.cli_trigger."""
from __future__ import annotations

import argparse
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from pipewatch.cli_trigger import add_trigger_subparser, cmd_trigger
from pipewatch.trigger import set_trigger


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_trigger_subparser(sub)
    return p


def test_add_trigger_subparser_registers_command(parser):
    args = parser.parse_args(["trigger", "list"])
    assert args.command == "trigger"


def test_add_trigger_subparser_fire_defaults(parser):
    args = parser.parse_args(["trigger", "fire", "my-pipe"])
    assert args.pipeline == "my-pipe"
    assert args.reason == "manual"
    assert args.triggered_by == "user"


def test_cmd_trigger_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(trigger_cmd="list")
    result = cmd_trigger(args, config_path=str(tmp_path / "missing.yml"))
    assert result == 1


def test_cmd_trigger_no_subcommand_returns_1(tmp_path):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    cfg.pipelines = []
    args = argparse.Namespace(trigger_cmd=None)
    with patch("pipewatch.cli_trigger.load_config", return_value=cfg):
        result = cmd_trigger(args)
    assert result == 1


def test_cmd_trigger_fire_prints_confirmation(tmp_path, capsys):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(trigger_cmd="fire", pipeline="etl", reason="retry", triggered_by="ops")
    with patch("pipewatch.cli_trigger.load_config", return_value=cfg):
        result = cmd_trigger(args)
    assert result == 0
    out = capsys.readouterr().out
    assert "etl" in out
    assert "retry" in out


def test_cmd_trigger_list_shows_pending(tmp_path, capsys):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    cfg.pipelines = [MagicMock(name="p")]
    cfg.pipelines[0].name = "pipe-x"
    set_trigger(str(tmp_path), "pipe-x", "overnight")
    args = argparse.Namespace(trigger_cmd="list")
    with patch("pipewatch.cli_trigger.load_config", return_value=cfg):
        result = cmd_trigger(args)
    assert result == 0
    out = capsys.readouterr().out
    assert "pipe-x" in out
    assert "overnight" in out
