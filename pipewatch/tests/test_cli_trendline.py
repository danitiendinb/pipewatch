"""Tests for pipewatch.cli_trendline"""
from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_trendline import add_trendline_subparser, cmd_trendline


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_trendline_subparser(sub)
    return p


def test_add_trendline_subparser_registers_command(parser):
    args = parser.parse_args(["trendline"])
    assert args.command == "trendline"


def test_add_trendline_subparser_default_window(parser):
    args = parser.parse_args(["trendline"])
    assert args.window == 20


def test_add_trendline_subparser_custom_window(parser):
    args = parser.parse_args(["trendline", "--window", "50"])
    assert args.window == 50


def test_add_trendline_subparser_default_stable_threshold(parser):
    args = parser.parse_args(["trendline"])
    assert args.stable_threshold == pytest.approx(1.0)


def test_add_trendline_subparser_pipeline_flag(parser):
    args = parser.parse_args(["trendline", "--pipeline", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_cmd_trendline_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(
        config=str(tmp_path / "missing.yml"),
        pipeline=None,
        window=20,
        stable_threshold=1.0,
    )
    with patch("pipewatch.cli_trendline.load_config", return_value=None):
        assert cmd_trendline(args) == 1


def test_cmd_trendline_no_data_prints_message(capsys, tmp_path):
    fake_cfg = MagicMock()
    fake_cfg.state_dir = str(tmp_path)
    fake_cfg.pipelines = []

    args = argparse.Namespace(
        config="pipewatch.yml",
        pipeline=None,
        window=20,
        stable_threshold=1.0,
    )
    with patch("pipewatch.cli_trendline.load_config", return_value=fake_cfg), \
         patch("pipewatch.cli_trendline.compute_all", return_value=[]):
        rc = cmd_trendline(args)

    captured = capsys.readouterr()
    assert rc == 0
    assert "No trendline data" in captured.out


def test_cmd_trendline_prints_table(capsys, tmp_path):
    from pipewatch.trendline import TrendlineReport

    fake_cfg = MagicMock()
    fake_cfg.state_dir = str(tmp_path)
    fake_cfg.pipelines = []

    report = TrendlineReport(
        pipeline="etl_daily",
        sample_size=10,
        slope=2.5,
        intercept=30.0,
        direction="degrading",
        latest_predicted=55.0,
    )

    args = argparse.Namespace(
        config="pipewatch.yml",
        pipeline=None,
        window=20,
        stable_threshold=1.0,
    )
    with patch("pipewatch.cli_trendline.load_config", return_value=fake_cfg), \
         patch("pipewatch.cli_trendline.compute_all", return_value=[report]):
        rc = cmd_trendline(args)

    captured = capsys.readouterr()
    assert rc == 0
    assert "etl_daily" in captured.out
    assert "degrading" in captured.out
