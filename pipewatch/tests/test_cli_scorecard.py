"""Unit tests for pipewatch.cli_scorecard."""
from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_scorecard import add_scorecard_subparser, cmd_scorecard


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_scorecard_subparser(sub)
    return p


def test_add_scorecard_subparser_registers_command(parser):
    args = parser.parse_args(["scorecard"])
    assert args.command == "scorecard"


def test_add_scorecard_subparser_default_period(parser):
    args = parser.parse_args(["scorecard"])
    assert args.period == 7


def test_add_scorecard_subparser_custom_period(parser):
    args = parser.parse_args(["scorecard", "--period", "14"])
    assert args.period == 14


def test_add_scorecard_subparser_min_grade_default(parser):
    args = parser.parse_args(["scorecard"])
    assert args.min_grade is None


def test_add_scorecard_subparser_min_grade_custom(parser):
    args = parser.parse_args(["scorecard", "--min-grade", "C"])
    assert args.min_grade == "C"


def test_cmd_scorecard_missing_config_returns_1(tmp_path):
    ns = argparse.Namespace(
        config=str(tmp_path / "missing.yml"),
        period=7,
        min_grade=None,
    )
    with patch("pipewatch.cli_scorecard.load_config", return_value=None):
        assert cmd_scorecard(ns) == 1


def test_cmd_scorecard_prints_output(tmp_path, capsys):
    from pipewatch.config import PipewatchConfig, PipelineConfig, AlertConfig

    cfg = MagicMock(spec=PipewatchConfig)
    cfg.state_dir = str(tmp_path)
    cfg.pipelines = []

    ns = argparse.Namespace(config="pipewatch.yml", period=7, min_grade=None)

    with patch("pipewatch.cli_scorecard.load_config", return_value=cfg), \
         patch("pipewatch.cli_scorecard.build_scorecard") as mock_build, \
         patch("pipewatch.cli_scorecard.format_scorecard", return_value="SCORECARD"):
        mock_build.return_value = MagicMock(rows=[])
        result = cmd_scorecard(ns)

    captured = capsys.readouterr()
    assert result == 0
    assert "SCORECARD" in captured.out
