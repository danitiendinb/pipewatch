"""Unit tests for pipewatch.cli_renamer."""
from __future__ import annotations

import argparse
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_renamer import add_renamer_subparser, cmd_rename


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_renamer_subparser(sub)
    return p


def test_add_renamer_subparser_registers_command(parser):
    args = parser.parse_args(["rename", "log"])
    assert args.command == "rename"


def test_add_renamer_subparser_pipeline_subcommand(parser):
    args = parser.parse_args(["rename", "pipeline", "old", "new"])
    assert args.old_name == "old"
    assert args.new_name == "new"


def test_cmd_rename_missing_config_returns_1():
    args = argparse.Namespace(config="/nonexistent/pipewatch.yml", rename_cmd="log")
    with patch("pipewatch.cli_renamer.load_config", return_value=None):
        assert cmd_rename(args) == 1


def test_cmd_rename_no_subcommand_returns_1():
    cfg = MagicMock(state_dir="/tmp")
    args = argparse.Namespace(config="pw.yml", rename_cmd=None)
    with patch("pipewatch.cli_renamer.load_config", return_value=cfg):
        assert cmd_rename(args) == 1


def test_cmd_rename_log_empty(capsys):
    cfg = MagicMock()
    with tempfile.TemporaryDirectory() as d:
        cfg.state_dir = d
        args = argparse.Namespace(config="pw.yml", rename_cmd="log")
        with patch("pipewatch.cli_renamer.load_config", return_value=cfg):
            rc = cmd_rename(args)
    assert rc == 0
    captured = capsys.readouterr()
    assert "No rename history" in captured.out


def test_cmd_rename_pipeline_success(capsys):
    cfg = MagicMock()
    with tempfile.TemporaryDirectory() as d:
        Path(d, "oldpipe.json").write_text("{}")
        cfg.state_dir = d
        args = argparse.Namespace(
            config="pw.yml", rename_cmd="pipeline", old_name="oldpipe", new_name="newpipe"
        )
        with patch("pipewatch.cli_renamer.load_config", return_value=cfg):
            rc = cmd_rename(args)
    assert rc == 0
    assert "newpipe" in capsys.readouterr().out


def test_cmd_rename_pipeline_conflict_returns_1(capsys):
    cfg = MagicMock()
    with tempfile.TemporaryDirectory() as d:
        Path(d, "a.json").write_text("{}")
        Path(d, "b.json").write_text("{}")
        cfg.state_dir = d
        args = argparse.Namespace(
            config="pw.yml", rename_cmd="pipeline", old_name="a", new_name="b"
        )
        with patch("pipewatch.cli_renamer.load_config", return_value=cfg):
            rc = cmd_rename(args)
    assert rc == 1
