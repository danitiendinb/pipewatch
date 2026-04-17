"""Tests for pipewatch.cli_dashboard"""
from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_dashboard import add_dashboard_subparser, cmd_dashboard


@pytest.fixture()
def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_dashboard_subparser(sub)
    return p


def test_add_dashboard_subparser_registers_command(parser):
    args = parser.parse_args(["dashboard"])
    assert args.command == "dashboard"


def test_add_dashboard_subparser_no_colour_flag(parser):
    args = parser.parse_args(["dashboard", "--no-colour"])
    assert args.no_colour is True


def test_cmd_dashboard_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(config=str(tmp_path / "missing.yml"), no_colour=False)
    with patch("pipewatch.cli_dashboard.load_config", return_value=None):
        assert cmd_dashboard(args) == 1


def test_cmd_dashboard_prints_output(capsys):
    fake_config = MagicMock()
    fake_config.state_dir = "/tmp"
    fake_config.pipelines = []
    args = argparse.Namespace(config="pipewatch.yml", no_colour=False)
    with patch("pipewatch.cli_dashboard.load_config", return_value=fake_config), \
         patch("pipewatch.cli_dashboard.PipelineState"), \
         patch("pipewatch.cli_dashboard.run_dashboard", return_value="DASHBOARD OUTPUT"):
        result = cmd_dashboard(args)
    assert result == 0
    captured = capsys.readouterr()
    assert "DASHBOARD OUTPUT" in captured.out


def test_cmd_dashboard_strips_colour_when_flag_set(capsys):
    fake_config = MagicMock()
    fake_config.state_dir = "/tmp"
    fake_config.pipelines = []
    args = argparse.Namespace(config="pipewatch.yml", no_colour=True)
    coloured = "\033[32mHello\033[0m World"
    with patch("pipewatch.cli_dashboard.load_config", return_value=fake_config), \
         patch("pipewatch.cli_dashboard.PipelineState"), \
         patch("pipewatch.cli_dashboard.run_dashboard", return_value=coloured):
        cmd_dashboard(args)
    captured = capsys.readouterr()
    assert "\033[" not in captured.out
    assert "Hello World" in captured.out
