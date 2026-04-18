"""Tests for pipewatch.cli_comparator."""
from __future__ import annotations

import argparse
from unittest.mock import patch, MagicMock

from pipewatch.cli_comparator import add_comparator_subparser, cmd_compare


def _args(**kwargs):
    defaults = {"config": "pipewatch.yml", "z_threshold": 2.5, "stats": False}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_add_comparator_subparser_registers_command():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    add_comparator_subparser(sub)
    args = parser.parse_args(["compare"])
    assert args.command == "compare"


def test_add_comparator_subparser_z_threshold_default():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    add_comparator_subparser(sub)
    args = parser.parse_args(["compare"])
    assert args.z_threshold == 2.5


def test_cmd_compare_missing_config_returns_1():
    result = cmd_compare(_args(config="nonexistent.yml"))
    assert result == 1


def test_cmd_compare_no_anomalies_prints_ok(tmp_path, capsys):
    from pipewatch.config import PipewatchConfig, PipelineConfig
    cfg = MagicMock(spec=PipewatchConfig)
    cfg.state_dir = str(tmp_path)
    cfg.pipelines = [MagicMock(name_attr="pipe")]
    cfg.pipelines[0].name = "pipe"

    with patch("pipewatch.cli_comparator.load_config", return_value=cfg), \
         patch("pipewatch.cli_comparator.check_all_pipelines", return_value=[]):
        result = cmd_compare(_args())

    assert result == 0
    captured = capsys.readouterr()
    assert "No duration anomalies" in captured.out


def test_cmd_compare_stats_flag(tmp_path, capsys):
    from pipewatch.config import PipewatchConfig
    from pipewatch.comparator import DurationStats
    cfg = MagicMock(spec=PipewatchConfig)
    cfg.state_dir = str(tmp_path)
    cfg.pipelines = [MagicMock()]
    cfg.pipelines[0].name = "pipe"
    stats = DurationStats("pipe", 15.0, 3.0, 10)

    with patch("pipewatch.cli_comparator.load_config", return_value=cfg), \
         patch("pipewatch.cli_comparator.compute_stats", return_value=stats):
        result = cmd_compare(_args(stats=True))

    assert result == 0
    captured = capsys.readouterr()
    assert "mean=15.0s" in captured.out
