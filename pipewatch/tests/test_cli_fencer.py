"""Unit tests for pipewatch.cli_fencer."""
from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_fencer import add_fencer_subparser, cmd_fencer


@pytest.fixture()
def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_fencer_subparser(sub)
    return p


def test_add_fencer_subparser_registers_command(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args(["fence", "set", "my_pipe"])
    assert args.command == "fence"


def test_add_fencer_subparser_default_hours(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args(["fence", "set", "my_pipe"])
    assert args.hours == 1.0


def test_add_fencer_subparser_custom_hours(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args(["fence", "set", "my_pipe", "--hours", "4"])
    assert args.hours == 4.0


def test_cmd_fencer_missing_config_returns_1(tmp_path: Path) -> None:
    args = argparse.Namespace(
        config=str(tmp_path / "missing.yml"),
        fence_cmd="set",
        pipeline="p",
        hours=1.0,
        reason="",
    )
    with patch("pipewatch.cli_fencer.load_config", return_value=None):
        assert cmd_fencer(args) == 1


def test_cmd_fencer_no_subcommand_returns_1(tmp_path: Path) -> None:
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(config="x.yml", fence_cmd=None, pipeline="p")
    with patch("pipewatch.cli_fencer.load_config", return_value=cfg):
        assert cmd_fencer(args) == 1


def test_cmd_fencer_set_returns_0(tmp_path: Path) -> None:
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(
        config="x.yml",
        fence_cmd="set",
        pipeline="my_pipe",
        hours=2.0,
        reason="deploy",
    )
    with patch("pipewatch.cli_fencer.load_config", return_value=cfg):
        assert cmd_fencer(args) == 0


def test_cmd_fencer_clear_returns_0(tmp_path: Path) -> None:
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(config="x.yml", fence_cmd="clear", pipeline="my_pipe")
    with patch("pipewatch.cli_fencer.load_config", return_value=cfg):
        assert cmd_fencer(args) == 0


def test_cmd_fencer_status_no_fence(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(config="x.yml", fence_cmd="status", pipeline="my_pipe")
    with patch("pipewatch.cli_fencer.load_config", return_value=cfg):
        result = cmd_fencer(args)
    out = capsys.readouterr().out
    assert result == 0
    assert "no fence" in out
