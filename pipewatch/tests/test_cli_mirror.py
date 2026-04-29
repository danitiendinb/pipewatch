"""Unit tests for pipewatch.cli_mirror."""

from __future__ import annotations

import argparse
import pytest
from unittest.mock import MagicMock, patch

from pipewatch.cli_mirror import add_mirror_subparser, cmd_mirror


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers()
    add_mirror_subparser(sub)
    return p


def test_add_mirror_subparser_registers_command(parser):
    args = parser.parse_args(["mirror"])
    assert hasattr(args, "func")


def test_add_mirror_subparser_default_destination(parser):
    args = parser.parse_args(["mirror"])
    assert args.destination == "remote"


def test_add_mirror_subparser_show_flag(parser):
    args = parser.parse_args(["mirror", "--show"])
    assert args.show is True


def test_add_mirror_subparser_clear_flag(parser):
    args = parser.parse_args(["mirror", "--clear", "--pipeline", "p1"])
    assert args.clear is True
    assert args.pipeline == "p1"


def test_cmd_mirror_missing_config_returns_1(tmp_path):
    args = MagicMock()
    args.config = str(tmp_path / "missing.yml")
    args.pipeline = None
    args.show = False
    args.clear = False
    args.destination = "remote"
    with patch("pipewatch.cli_mirror.load_config", return_value=None):
        assert cmd_mirror(args) == 1


def test_cmd_mirror_clear_without_pipeline_returns_1(tmp_path):
    args = MagicMock()
    args.config = "pipewatch.yml"
    args.pipeline = None
    args.clear = True
    args.show = False
    args.destination = "remote"
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    cfg.pipelines = []
    with patch("pipewatch.cli_mirror.load_config", return_value=cfg):
        assert cmd_mirror(args) == 1


def test_cmd_mirror_show_prints_no_record(tmp_path, capsys):
    args = MagicMock()
    args.config = "pipewatch.yml"
    args.pipeline = None
    args.show = True
    args.clear = False
    args.destination = "remote"
    pipe = MagicMock()
    pipe.name = "my_pipeline"
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    cfg.pipelines = [pipe]
    with patch("pipewatch.cli_mirror.load_config", return_value=cfg):
        rc = cmd_mirror(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "my_pipeline" in out
    assert "no mirror record" in out
