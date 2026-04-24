"""Unit tests for pipewatch.cli_ranker."""
from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_ranker import add_ranker_subparser, cmd_rank


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_ranker_subparser(sub)
    return p


def test_add_ranker_subparser_registers_command(parser):
    args = parser.parse_args(["rank"])
    assert args.command == "rank"


def test_add_ranker_subparser_default_top(parser):
    args = parser.parse_args(["rank"])
    assert args.top == 10


def test_add_ranker_subparser_custom_top(parser):
    args = parser.parse_args(["rank", "-n", "3"])
    assert args.top == 3


def test_cmd_rank_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(
        config=str(tmp_path / "missing.yml"),
        top=10,
        worst_first=True,
    )
    assert cmd_rank(args) == 1


def test_cmd_rank_no_pipelines_returns_0(tmp_path):
    cfg_path = tmp_path / "pipewatch.yml"
    cfg_path.write_text(
        "log_level: INFO\nstate_dir: " + str(tmp_path) + "\npipelines: []\n"
    )
    args = argparse.Namespace(
        config=str(cfg_path),
        top=10,
        worst_first=True,
    )
    assert cmd_rank(args) == 0


def test_cmd_rank_prints_header(tmp_path, capsys):
    cfg_path = tmp_path / "pipewatch.yml"
    cfg_path.write_text(
        "log_level: INFO\nstate_dir: "
        + str(tmp_path)
        + "\npipelines:\n  - name: mypipe\n    schedule: \"0 * * * *\"\n"
    )
    args = argparse.Namespace(
        config=str(cfg_path),
        top=10,
        worst_first=True,
    )
    rc = cmd_rank(args)
    out = capsys.readouterr().out
    assert rc == 0
    assert "Pipeline" in out
    assert "Score" in out
