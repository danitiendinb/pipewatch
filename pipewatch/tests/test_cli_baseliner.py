"""Tests for cli_baseliner."""

from __future__ import annotations

import argparse
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from pipewatch.cli_baseliner import add_baseliner_subparser, cmd_baseline
from pipewatch.baseliner import Baseline


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_baseliner_subparser(sub)
    return p


def test_add_baseliner_subparser_registers_command(parser):
    args = parser.parse_args(["baseline", "show", "mypipe"])
    assert args.command == "baseline"


def test_cmd_baseline_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(pipeline="p", baseline_cmd="show")
    result = cmd_baseline(args, config_path=str(tmp_path / "missing.yml"))
    assert result == 1


def test_cmd_baseline_show_no_baseline(tmp_path, capsys):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    with patch("pipewatch.cli_baseliner.load_config", return_value=cfg):
        args = argparse.Namespace(pipeline="pipe", baseline_cmd="show")
        result = cmd_baseline(args, config_path="any.yml")
    assert result == 0
    assert "No baseline" in capsys.readouterr().out


def test_cmd_baseline_record_no_runs_returns_1(tmp_path):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    with patch("pipewatch.cli_baseliner.load_config", return_value=cfg):
        with patch("pipewatch.cli_baseliner.PipelineState.load") as mock_load:
            mock_load.return_value = MagicMock(runs=[])
            with patch("pipewatch.cli_baseliner.compute_baseline", return_value=None):
                args = argparse.Namespace(pipeline="pipe", baseline_cmd="record")
                result = cmd_baseline(args, config_path="any.yml")
    assert result == 1


def test_cmd_baseline_clear(tmp_path, capsys):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    with patch("pipewatch.cli_baseliner.load_config", return_value=cfg):
        args = argparse.Namespace(pipeline="pipe", baseline_cmd="clear")
        result = cmd_baseline(args, config_path="any.yml")
    assert result == 0
    assert "cleared" in capsys.readouterr().out
