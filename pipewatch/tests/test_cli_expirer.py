"""Unit tests for pipewatch.cli_expirer."""
from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_expirer import add_expirer_subparser, cmd_expirer


@pytest.fixture()
def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    sp = p.add_subparsers(dest="command")
    add_expirer_subparser(sp)
    return p


def test_add_expirer_subparser_registers_command(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args(["expiry", "list"])
    assert args.command == "expiry"


def test_add_expirer_subparser_default_hours(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args(["expiry", "set", "my_pipe"])
    assert args.hours == 24.0


def test_add_expirer_subparser_custom_hours(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args(["expiry", "set", "my_pipe", "--hours", "48"])
    assert args.hours == 48.0


def test_cmd_expirer_missing_config_returns_1(tmp_path: Path) -> None:
    args = argparse.Namespace(
        config=str(tmp_path / "missing.yml"),
        expiry_cmd="list",
    )
    with patch("pipewatch.cli_expirer.load_config", return_value=None):
        assert cmd_expirer(args) == 1


def test_cmd_expirer_no_subcommand_returns_1(tmp_path: Path) -> None:
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    cfg.pipelines = []
    args = argparse.Namespace(config="pipewatch.yml", expiry_cmd=None)
    with patch("pipewatch.cli_expirer.load_config", return_value=cfg):
        assert cmd_expirer(args) == 1


def test_cmd_expirer_set_prints_record(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(config="pipewatch.yml", expiry_cmd="set", pipeline="p1", hours=6.0)
    with patch("pipewatch.cli_expirer.load_config", return_value=cfg):
        rc = cmd_expirer(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "p1" in out
    assert "6.0h" in out


def test_cmd_expirer_clear_prints_confirmation(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(config="pipewatch.yml", expiry_cmd="clear", pipeline="p1")
    with patch("pipewatch.cli_expirer.load_config", return_value=cfg):
        rc = cmd_expirer(args)
    assert rc == 0
    assert "cleared" in capsys.readouterr().out
