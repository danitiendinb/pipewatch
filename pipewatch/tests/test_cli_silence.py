"""Unit tests for pipewatch.cli_silence."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_silence import add_silence_subparser, cmd_silence, cmd_unsilence


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_silence_subparser(sub)
    return p


def test_add_silence_subparser_registers_silence(parser):
    args = parser.parse_args(["silence", "my-pipe"])
    assert args.pipeline == "my-pipe"


def test_add_silence_subparser_default_hours(parser):
    args = parser.parse_args(["silence", "my-pipe"])
    assert args.hours == 1.0


def test_add_silence_subparser_registers_unsilence(parser):
    args = parser.parse_args(["unsilence", "my-pipe"])
    assert args.pipeline == "my-pipe"


def test_cmd_silence_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(pipeline="p", hours=1.0, config=str(tmp_path / "missing.yml"))
    with patch("pipewatch.cli_silence.load_config", return_value=None):
        assert cmd_silence(args) == 1


def test_cmd_silence_writes_silence(tmp_path):
    cfg = MagicMock(state_dir=str(tmp_path))
    args = argparse.Namespace(pipeline="pipe-a", hours=2.0, config="pipewatch.yml")
    with patch("pipewatch.cli_silence.load_config", return_value=cfg), \
         patch("pipewatch.cli_silence.set_silence") as mock_set:
        result = cmd_silence(args)
    assert result == 0
    mock_set.assert_called_once()
    _, pipeline, until = mock_set.call_args[0]
    assert pipeline == "pipe-a"
    assert until > datetime.now(timezone.utc) + timedelta(hours=1.9)


def test_cmd_unsilence_not_silenced_returns_0(tmp_path):
    cfg = MagicMock(state_dir=str(tmp_path))
    args = argparse.Namespace(pipeline="pipe-a", config="pipewatch.yml")
    with patch("pipewatch.cli_silence.load_config", return_value=cfg), \
         patch("pipewatch.cli_silence.is_silenced", return_value=False):
        result = cmd_unsilence(args)
    assert result == 0


def test_cmd_unsilence_clears_silence(tmp_path):
    cfg = MagicMock(state_dir=str(tmp_path))
    args = argparse.Namespace(pipeline="pipe-a", config="pipewatch.yml")
    with patch("pipewatch.cli_silence.load_config", return_value=cfg), \
         patch("pipewatch.cli_silence.is_silenced", return_value=True), \
         patch("pipewatch.cli_silence.clear_silence") as mock_clear:
        result = cmd_unsilence(args)
    assert result == 0
    mock_clear.assert_called_once_with(str(tmp_path), "pipe-a")
