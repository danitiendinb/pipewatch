"""Tests for pipewatch.cli_watchdog"""
from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_watchdog import add_watchdog_subparser, cmd_watchdog
from pipewatch.watchdog import StaleReport


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_watchdog_subparser(sub)
    return p


def test_add_watchdog_subparser_registers_command(parser):
    args = parser.parse_args(["watchdog"])
    assert args.command == "watchdog"


def test_add_watchdog_subparser_default_threshold(parser):
    args = parser.parse_args(["watchdog"])
    assert args.threshold == 24.0


def test_add_watchdog_subparser_custom_threshold(parser):
    args = parser.parse_args(["watchdog", "--threshold", "6"])
    assert args.threshold == 6.0


def test_cmd_watchdog_missing_config_returns_1(tmp_path):
    args = MagicMock()
    args.config = str(tmp_path / "missing.yml")
    args.threshold = 24.0
    with patch("pipewatch.cli_watchdog.load_config", return_value=None):
        assert cmd_watchdog(args) == 1


def test_cmd_watchdog_no_stale_returns_0(capsys):
    args = MagicMock()
    args.config = "pipewatch.yml"
    args.threshold = 24.0
    config = MagicMock()
    config.state_dir = "/tmp"
    config.pipelines = []
    with patch("pipewatch.cli_watchdog.load_config", return_value=config), \
         patch("pipewatch.cli_watchdog.stale_pipelines", return_value=[]):
        rc = cmd_watchdog(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "All pipelines" in out


def test_cmd_watchdog_stale_prints_table(capsys):
    args = MagicMock()
    args.config = "pipewatch.yml"
    args.threshold = 24.0
    config = MagicMock()
    config.state_dir = "/tmp"
    report = StaleReport(pipeline="my_pipe", last_seen="2024-01-01T00:00:00", hours_silent=30.0)
    with patch("pipewatch.cli_watchdog.load_config", return_value=config), \
         patch("pipewatch.cli_watchdog.stale_pipelines", return_value=[report]):
        rc = cmd_watchdog(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "my_pipe" in out
    assert "30.0" in out
