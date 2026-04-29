"""Unit tests for pipewatch.cli_pauser."""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_pauser import add_pauser_subparser, cmd_pauser


@pytest.fixture()
def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers()
    add_pauser_subparser(sub)
    return p


def _args(**kwargs):
    ns = argparse.Namespace(config="pipewatch.yml", **kwargs)
    return ns


def test_add_pauser_subparser_registers_pause(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args(["pause", "my_pipe"])
    assert args.pauser_cmd == "pause"
    assert args.pipeline == "my_pipe"


def test_add_pauser_subparser_default_hours(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args(["pause", "my_pipe"])
    assert args.hours == 1.0


def test_add_pauser_subparser_custom_hours(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args(["pause", "my_pipe", "--hours", "4"])
    assert args.hours == 4.0


def test_add_pauser_subparser_registers_unpause(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args(["unpause", "my_pipe"])
    assert args.pauser_cmd == "unpause"


def test_cmd_pauser_missing_config_returns_1(tmp_path: Path) -> None:
    with patch("pipewatch.cli_pauser.load_config", return_value=None):
        result = cmd_pauser(_args(pipeline="p", pauser_cmd="pause", hours=1.0))
    assert result == 1


def test_cmd_pauser_no_subcommand_returns_1(tmp_path: Path) -> None:
    mock_cfg = MagicMock(state_dir=str(tmp_path))
    with patch("pipewatch.cli_pauser.load_config", return_value=mock_cfg):
        result = cmd_pauser(_args(pipeline="p"))
    assert result == 1


def test_cmd_pauser_pause_prints_expiry(tmp_path: Path, capsys) -> None:
    mock_cfg = MagicMock(state_dir=str(tmp_path))
    with patch("pipewatch.cli_pauser.load_config", return_value=mock_cfg):
        result = cmd_pauser(_args(pipeline="pipe_a", pauser_cmd="pause", hours=2.0))
    assert result == 0
    captured = capsys.readouterr()
    assert "pipe_a" in captured.out
    assert "Paused" in captured.out


def test_cmd_pauser_unpause_clears(tmp_path: Path, capsys) -> None:
    from pipewatch.pauser import pause_pipeline
    pause_pipeline(str(tmp_path), "pipe_a", hours=1.0)
    mock_cfg = MagicMock(state_dir=str(tmp_path))
    with patch("pipewatch.cli_pauser.load_config", return_value=mock_cfg):
        result = cmd_pauser(_args(pipeline="pipe_a", pauser_cmd="unpause"))
    assert result == 0
    from pipewatch.pauser import is_paused
    assert not is_paused(str(tmp_path), "pipe_a")


def test_cmd_pauser_status_not_paused(tmp_path: Path, capsys) -> None:
    mock_cfg = MagicMock(state_dir=str(tmp_path))
    with patch("pipewatch.cli_pauser.load_config", return_value=mock_cfg):
        result = cmd_pauser(_args(pipeline="pipe_a", pauser_cmd="status"))
    assert result == 0
    captured = capsys.readouterr()
    assert "not paused" in captured.out
