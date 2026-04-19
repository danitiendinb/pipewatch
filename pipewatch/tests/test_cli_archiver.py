"""Unit tests for pipewatch.cli_archiver."""
from __future__ import annotations

import argparse
import pytest
from unittest.mock import patch, MagicMock

from pipewatch.cli_archiver import add_archiver_subparser, cmd_archive
from pipewatch.config import PipewatchConfig, PipelineConfig


@pytest.fixture
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_archiver_subparser(sub)
    return p


def _config(tmp_path):
    return PipewatchConfig(
        pipelines=[PipelineConfig(name="pipe_a", schedule="@hourly", alert_after=3)],
        state_dir=str(tmp_path),
        log_level="INFO",
        alert=None,
    )


def test_add_archiver_subparser_registers_command(parser):
    args = parser.parse_args(["archive", "pipe_a"])
    assert args.pipeline == "pipe_a"


def test_add_archiver_subparser_show_flag(parser):
    args = parser.parse_args(["archive", "pipe_a", "--show"])
    assert args.show is True


def test_add_archiver_subparser_clear_flag(parser):
    args = parser.parse_args(["archive", "pipe_a", "--clear"])
    assert args.clear is True


def test_cmd_archive_missing_config_returns_1(tmp_path):
    ns = argparse.Namespace(config="missing.yml", pipeline="pipe_a", show=False, clear=False)
    with patch("pipewatch.cli_archiver.load_config", return_value=None):
        assert cmd_archive(ns) == 1


def test_cmd_archive_show_prints_count(tmp_path, capsys):
    ns = argparse.Namespace(config="pw.yml", pipeline="pipe_a", show=True, clear=False)
    with patch("pipewatch.cli_archiver.load_config", return_value=_config(tmp_path)):
        with patch("pipewatch.cli_archiver.load_archive", return_value=[{}, {}]):
            result = cmd_archive(ns)
    assert result == 0
    out = capsys.readouterr().out
    assert "2" in out


def test_cmd_archive_clear_calls_clear(tmp_path, capsys):
    ns = argparse.Namespace(config="pw.yml", pipeline="pipe_a", show=False, clear=True)
    with patch("pipewatch.cli_archiver.load_config", return_value=_config(tmp_path)):
        with patch("pipewatch.cli_archiver.clear_archive") as mock_clear:
            result = cmd_archive(ns)
    assert result == 0
    mock_clear.assert_called_once()
