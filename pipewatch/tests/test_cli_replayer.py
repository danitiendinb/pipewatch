"""Tests for pipewatch.cli_replayer."""
from __future__ import annotations

import argparse
import pytest
from unittest.mock import patch, MagicMock

from pipewatch.cli_replayer import add_replayer_subparser, cmd_replay


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_replayer_subparser(sub)
    return p


def test_add_replayer_subparser_registers_command(parser):
    args = parser.parse_args(["replay", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_add_replayer_subparser_dry_run_flag(parser):
    args = parser.parse_args(["replay", "my_pipe", "--dry-run"])
    assert args.dry_run is True


def test_add_replayer_subparser_clear_flag(parser):
    args = parser.parse_args(["replay", "my_pipe", "--clear"])
    assert args.clear is True


def test_add_replayer_subparser_defaults(parser):
    """Verify that dry_run and clear default to False when not provided."""
    args = parser.parse_args(["replay", "my_pipe"])
    assert args.dry_run is False
    assert args.clear is False


def test_cmd_replay_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(
        config=str(tmp_path / "missing.yml"),
        pipeline="p",
        since=None,
        dry_run=False,
        clear=False,
    )
    assert cmd_replay(args) == 1


def test_cmd_replay_clear_returns_0(tmp_path):
    cfg = MagicMock(state_dir=str(tmp_path))
    args = argparse.Namespace(
        config="pipewatch.yml",
        pipeline="p",
        since=None,
        dry_run=False,
        clear=True,
    )
    with patch("pipewatch.cli_replayer.load_config", return_value=cfg), \
         patch("pipewatch.cli_replayer.clear_replay") as mock_clear:
        rc = cmd_replay(args)
    assert rc == 0
    mock_clear.assert_called_once_with(str(tmp_path), "p")


def test_cmd_replay_prints_results(tmp_path, capsys):
    cfg = MagicMock(state_dir=str(tmp_path))
    fake_result = MagicMock(replayed=2, skipped=1)
    args = argparse.Namespace(
        config="pipewatch.yml",
        pipeline="p",
        since=None,
        dry_run=False,
        clear=False,
    )
    with patch("pipewatch.cli_replayer.load_config", return_value=cfg), \
         patch("pipewatch.cli_replayer.PipelineState"), \
         patch("pipewatch.cli_replayer.replay_runs", return_value=fake_result):
        rc = cmd_replay(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "Replayed 2" in out
    assert "skipped 1" in out


def test_cmd_replay_dry_run_does_not_clear(tmp_path):
    """Ensure --dry-run does not trigger clear_replay even when --clear is set."""
    cfg = MagicMock(state_dir=str(tmp_path))
    fake_result = MagicMock(replayed=0, skipped=0)
    args = argparse.Namespace(
        config="pipewatch.yml",
        pipeline="p",
        since=None,
        dry_run=True,
        clear=False,
    )
    with patch("pipewatch.cli_replayer.load_config", return_value=cfg), \
         patch("pipewatch.cli_replayer.PipelineState"), \
         patch("pipewatch.cli_replayer.replay_runs", return_value=fake_result), \
         patch("pipewatch.cli_replayer.clear_replay") as mock_clear:
        rc = cmd_replay(args)
    assert rc == 0
    mock_clear.assert_not_called()
