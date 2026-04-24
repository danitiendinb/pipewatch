"""Unit tests for pipewatch.cli_profiler."""

from __future__ import annotations

import argparse
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_profiler import add_profiler_subparser, cmd_profile


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_profiler_subparser(sub)
    return p


def test_add_profiler_subparser_registers_command(parser):
    args = parser.parse_args(["profile", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_add_profiler_subparser_save_flag(parser):
    args = parser.parse_args(["profile", "my_pipe", "--save"])
    assert args.save is True


def test_add_profiler_subparser_clear_flag(parser):
    args = parser.parse_args(["profile", "my_pipe", "--clear"])
    assert args.clear is True


def test_cmd_profile_missing_config_returns_1(tmp_path):
    args = SimpleNamespace(
        pipeline="pipe",
        config=str(tmp_path / "missing.yml"),
        save=False,
        clear=False,
    )
    assert cmd_profile(args) == 1


def test_cmd_profile_insufficient_data_returns_1(tmp_path):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)

    fake_state = MagicMock()
    fake_state.runs = []

    with patch("pipewatch.cli_profiler.load_config", return_value=cfg), \
         patch("pipewatch.cli_profiler.PipelineState.load", return_value=fake_state), \
         patch("pipewatch.cli_profiler.compute_profile", return_value=None):
        args = SimpleNamespace(
            pipeline="pipe",
            config="pipewatch.yml",
            save=False,
            clear=False,
        )
        assert cmd_profile(args) == 1


def test_cmd_profile_prints_stats(tmp_path, capsys):
    from pipewatch.profiler import DurationProfile
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    fake_profile = DurationProfile(
        pipeline="pipe", sample_size=10,
        mean_seconds=120.0, median_seconds=115.0,
        p95_seconds=200.0, p99_seconds=250.0,
        min_seconds=60.0, max_seconds=300.0,
    )
    with patch("pipewatch.cli_profiler.load_config", return_value=cfg), \
         patch("pipewatch.cli_profiler.PipelineState.load", return_value=MagicMock()), \
         patch("pipewatch.cli_profiler.compute_profile", return_value=fake_profile):
        args = SimpleNamespace(
            pipeline="pipe",
            config="pipewatch.yml",
            save=False,
            clear=False,
        )
        rc = cmd_profile(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "p95" in out
    assert "120.0" in out
