"""Tests for pipewatch.cli_throttler."""
from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_throttler import add_throttler_subparser, cmd_throttler


@pytest.fixture()
def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_throttler_subparser(sub)
    return p


def test_add_throttler_subparser_registers_command(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args(["throttle", "status", "my_pipe"])
    assert args.command == "throttle"


def test_add_throttler_subparser_default_interval(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args(["throttle", "status", "my_pipe"])
    assert args.interval == 60


def test_cmd_throttler_missing_config_returns_1(tmp_path: Path) -> None:
    args = argparse.Namespace(
        config=str(tmp_path / "missing.yml"),
        throttle_cmd="status",
        pipeline="pipe_a",
        interval=60,
    )
    with patch("pipewatch.cli_throttler.load_config", return_value=None):
        assert cmd_throttler(args) == 1


def test_cmd_throttler_status_never_checked(tmp_path: Path, capsys) -> None:
    cfg = MagicMock(state_dir=str(tmp_path))
    args = argparse.Namespace(
        config="pipewatch.yml",
        throttle_cmd="status",
        pipeline="pipe_a",
        interval=60,
    )
    with patch("pipewatch.cli_throttler.load_config", return_value=cfg):
        rc = cmd_throttler(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "never checked" in out


def test_cmd_throttler_clear_prints_confirmation(tmp_path: Path, capsys) -> None:
    cfg = MagicMock(state_dir=str(tmp_path))
    args = argparse.Namespace(
        config="pipewatch.yml",
        throttle_cmd="clear",
        pipeline="pipe_a",
    )
    with patch("pipewatch.cli_throttler.load_config", return_value=cfg):
        rc = cmd_throttler(args)
    assert rc == 0
    assert "cleared" in capsys.readouterr().out
