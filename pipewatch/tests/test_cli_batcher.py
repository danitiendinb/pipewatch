"""Unit tests for pipewatch.cli_batcher."""
from __future__ import annotations

import argparse
import types
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_batcher import add_batcher_subparser, cmd_batcher


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sp = p.add_subparsers(dest="command")
    add_batcher_subparser(sp)
    return p


def test_add_batcher_subparser_registers_command(parser):
    args = parser.parse_args(["batch", "show", "b1"])
    assert args.command == "batch"


def test_add_batcher_subparser_create_accepts_pipelines(parser):
    args = parser.parse_args(["batch", "create", "my_batch", "p1", "p2"])
    assert args.batch_id == "my_batch"
    assert args.pipelines == ["p1", "p2"]


def test_add_batcher_subparser_record_choices(parser):
    args = parser.parse_args(["batch", "record", "b1", "pipe_a", "ok"])
    assert args.status == "ok"


def _args(tmp_path, batch_cmd, **kwargs):
    ns = argparse.Namespace(
        config=str(tmp_path / "pipewatch.yml"),
        batch_cmd=batch_cmd,
        **kwargs,
    )
    return ns


def test_cmd_batcher_missing_config_returns_1(tmp_path):
    args = _args(tmp_path, "show", batch_id="b1")
    assert cmd_batcher(args) == 1


def _fake_config(tmp_path):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    return cfg


def test_cmd_batcher_no_subcommand_returns_1(tmp_path):
    args = _args(tmp_path, None)
    with patch("pipewatch.cli_batcher.load_config", return_value=_fake_config(tmp_path)):
        assert cmd_batcher(args) == 1


def test_cmd_batcher_show_missing_batch_returns_1(tmp_path):
    args = _args(tmp_path, "show", batch_id="ghost")
    with patch("pipewatch.cli_batcher.load_config", return_value=_fake_config(tmp_path)):
        assert cmd_batcher(args) == 1


def test_cmd_batcher_create_returns_0(tmp_path, capsys):
    args = _args(tmp_path, "create", batch_id="b1", pipelines=["p1", "p2"])
    with patch("pipewatch.cli_batcher.load_config", return_value=_fake_config(tmp_path)):
        rc = cmd_batcher(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "b1" in out


def test_cmd_batcher_clear_returns_0(tmp_path, capsys):
    from pipewatch.batcher import create_batch
    create_batch(str(tmp_path), "b2", ["p1"])
    args = _args(tmp_path, "clear", batch_id="b2")
    with patch("pipewatch.cli_batcher.load_config", return_value=_fake_config(tmp_path)):
        rc = cmd_batcher(args)
    assert rc == 0
