"""Tests for pipewatch.cli_notify."""
from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_notify import add_notify_subparser, cmd_notify


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_notify_subparser(sub)
    return p


def test_add_notify_subparser_registers_command(parser):
    args = parser.parse_args(["notify"])
    assert args.command == "notify"


def test_add_notify_subparser_dry_run_flag(parser):
    args = parser.parse_args(["notify", "--dry-run"])
    assert args.dry_run is True


def test_cmd_notify_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(config=str(tmp_path / "missing.yml"), smtp_host=None, dry_run=False)
    assert cmd_notify(args) == 1


def test_cmd_notify_no_email_config_returns_1(tmp_path):
    cfg_mock = MagicMock()
    cfg_mock.email = None
    with patch("pipewatch.cli_notify.load_config", return_value=cfg_mock):
        args = argparse.Namespace(config="pipewatch.yml", smtp_host=None, dry_run=False)
        assert cmd_notify(args) == 1


def test_cmd_notify_dry_run_prints_alert(tmp_path, capsys):
    cfg_mock = MagicMock()
    cfg_mock.email = {
        "to": ["ops@example.com"],
        "from": "pw@example.com",
        "smtp_host": "localhost",
        "smtp_port": 25,
        "use_tls": False,
    }
    cfg_mock.alert.failure_threshold = 2
    pipeline = MagicMock()
    pipeline.name = "etl"
    cfg_mock.pipelines = [pipeline]
    cfg_mock.state_dir = str(tmp_path)

    state_mock = MagicMock()
    state_mock.consecutive_failures = 3

    with patch("pipewatch.cli_notify.load_config", return_value=cfg_mock), \
         patch("pipewatch.cli_notify.load_state", return_value=state_mock), \
         patch("pipewatch.alerts.should_alert", return_value=True):
        args = argparse.Namespace(config="pipewatch.yml", smtp_host=None, dry_run=True)
        result = cmd_notify(args)

    assert result == 0
    captured = capsys.readouterr()
    assert "dry-run" in captured.out
    assert "etl" in captured.out
