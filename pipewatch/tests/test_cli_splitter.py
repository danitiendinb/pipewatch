"""Unit tests for pipewatch.cli_splitter."""
from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_splitter import add_splitter_subparser, cmd_split


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_splitter_subparser(sub)
    return p


def test_add_splitter_subparser_registers_command(parser):
    args = parser.parse_args(["split", "my_pipe"])
    assert args.command == "split"


def test_add_splitter_subparser_default_days(parser):
    args = parser.parse_args(["split", "my_pipe"])
    assert args.days == 7


def test_add_splitter_subparser_custom_days(parser):
    args = parser.parse_args(["split", "my_pipe", "--days", "14"])
    assert args.days == 14


def test_add_splitter_subparser_default_granularity(parser):
    args = parser.parse_args(["split", "my_pipe"])
    assert args.granularity == "day"


def test_add_splitter_subparser_hour_granularity(parser):
    args = parser.parse_args(["split", "my_pipe", "--granularity", "hour"])
    assert args.granularity == "hour"


def test_cmd_split_missing_config_returns_1(tmp_path):
    args = MagicMock()
    args.config = str(tmp_path / "missing.yml")
    assert cmd_split(args) == 1


def test_cmd_split_no_runs_prints_message(tmp_path, capsys):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = MagicMock()
    args.config = "pipewatch.yml"
    args.pipeline = "nopipe"
    args.days = 3
    args.granularity = "day"

    with patch("pipewatch.cli_splitter.load_config", return_value=cfg), \
         patch("pipewatch.cli_splitter.PipelineState"), \
         patch(
             "pipewatch.cli_splitter.split_runs",
             return_value=[MagicMock(total=0, runs=[])],
         ):
        rc = cmd_split(args)

    assert rc == 0
    out = capsys.readouterr().out
    assert "No runs" in out


def test_cmd_split_invalid_granularity_returns_1(tmp_path):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = MagicMock()
    args.config = "pipewatch.yml"
    args.pipeline = "p"
    args.days = 1
    args.granularity = "week"

    with patch("pipewatch.cli_splitter.load_config", return_value=cfg), \
         patch("pipewatch.cli_splitter.PipelineState"), \
         patch(
             "pipewatch.cli_splitter.split_runs",
             side_effect=ValueError("bad granularity"),
         ):
        rc = cmd_split(args)

    assert rc == 1
