"""Unit tests for pipewatch.cli_flapper."""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_flapper import add_flapper_subparser, cmd_flapper


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_flapper_subparser(sub)
    return p


def test_add_flapper_subparser_registers_command(parser):
    args = parser.parse_args(["flap"])
    assert args.command == "flap"


def test_add_flapper_subparser_default_threshold(parser):
    args = parser.parse_args(["flap"])
    assert args.threshold == 3


def test_add_flapper_subparser_custom_threshold(parser):
    args = parser.parse_args(["flap", "--threshold", "5"])
    assert args.threshold == 5


def test_add_flapper_subparser_default_window(parser):
    args = parser.parse_args(["flap"])
    assert args.window == 10


def test_add_flapper_subparser_save_flag(parser):
    args = parser.parse_args(["flap", "--save"])
    assert args.save is True


def test_cmd_flapper_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(
        config=str(tmp_path / "missing.yml"),
        threshold=3,
        window=10,
        save=False,
    )
    with patch("pipewatch.cli_flapper.load_config", return_value=None):
        assert cmd_flapper(args) == 1


def test_cmd_flapper_no_flapping_returns_0(tmp_path, capsys):
    from pipewatch.flapper import FlapReport
    mock_cfg = MagicMock()
    mock_cfg.state_dir = str(tmp_path)
    mock_cfg.pipelines = [MagicMock(name="p1")]
    mock_cfg.pipelines[0].name = "p1"

    stable_report = FlapReport(pipeline="p1", flap_count=1, is_flapping=False, transitions=["ok", "ok"])

    args = argparse.Namespace(
        config="pipewatch.yml", threshold=3, window=10, save=False
    )
    with patch("pipewatch.cli_flapper.load_config", return_value=mock_cfg), \
         patch("pipewatch.cli_flapper.detect_all", return_value=[stable_report]):
        result = cmd_flapper(args)

    assert result == 0
    captured = capsys.readouterr()
    assert "No flapping" in captured.out


def test_cmd_flapper_flapping_returns_1(tmp_path, capsys):
    from pipewatch.flapper import FlapReport
    mock_cfg = MagicMock()
    mock_cfg.state_dir = str(tmp_path)
    mock_cfg.pipelines = [MagicMock()]
    mock_cfg.pipelines[0].name = "p1"

    flap_report = FlapReport(pipeline="p1", flap_count=4, is_flapping=True, transitions=["ok", "fail", "ok", "fail", "ok"])

    args = argparse.Namespace(
        config="pipewatch.yml", threshold=3, window=10, save=False
    )
    with patch("pipewatch.cli_flapper.load_config", return_value=mock_cfg), \
         patch("pipewatch.cli_flapper.detect_all", return_value=[flap_report]):
        result = cmd_flapper(args)

    assert result == 1
    captured = capsys.readouterr()
    assert "FLAP" in captured.out
    assert "p1" in captured.out
