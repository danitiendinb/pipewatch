"""Tests for pipewatch.tombstone and pipewatch.cli_tombstone."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.tombstone import (
    TombstoneRecord,
    clear_tombstone,
    is_tombstoned,
    list_tombstoned,
    load_tombstone,
    set_tombstone,
)
from pipewatch.cli_tombstone import add_tombstone_subparser, cmd_tombstone


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


# ---------------------------------------------------------------------------
# tombstone core
# ---------------------------------------------------------------------------

def test_load_tombstone_none_for_unknown(state_dir: str) -> None:
    assert load_tombstone(state_dir, "pipe_a") is None


def test_set_tombstone_returns_record(state_dir: str) -> None:
    rec = set_tombstone(state_dir, "pipe_a", reason="old", tombstoned_at="2024-01-01T00:00:00+00:00")
    assert isinstance(rec, TombstoneRecord)
    assert rec.pipeline == "pipe_a"
    assert rec.reason == "old"


def test_set_tombstone_persists(state_dir: str) -> None:
    set_tombstone(state_dir, "pipe_b", reason="sunset", tombstoned_at="2024-06-01T12:00:00+00:00", tombstoned_by="alice")
    rec = load_tombstone(state_dir, "pipe_b")
    assert rec is not None
    assert rec.tombstoned_by == "alice"


def test_is_tombstoned_false_when_no_file(state_dir: str) -> None:
    assert is_tombstoned(state_dir, "pipe_x") is False


def test_is_tombstoned_true_after_set(state_dir: str) -> None:
    set_tombstone(state_dir, "pipe_c", reason="r", tombstoned_at="2024-01-01T00:00:00+00:00")
    assert is_tombstoned(state_dir, "pipe_c") is True


def test_clear_tombstone_returns_true(state_dir: str) -> None:
    set_tombstone(state_dir, "pipe_d", reason="r", tombstoned_at="2024-01-01T00:00:00+00:00")
    assert clear_tombstone(state_dir, "pipe_d") is True
    assert is_tombstoned(state_dir, "pipe_d") is False


def test_clear_tombstone_returns_false_when_absent(state_dir: str) -> None:
    assert clear_tombstone(state_dir, "missing") is False


def test_list_tombstoned_empty_dir(state_dir: str) -> None:
    assert list_tombstoned(state_dir) == []


def test_list_tombstoned_returns_sorted_names(state_dir: str) -> None:
    for name in ("z_pipe", "a_pipe", "m_pipe"):
        set_tombstone(state_dir, name, reason="r", tombstoned_at="2024-01-01T00:00:00+00:00")
    assert list_tombstoned(state_dir) == ["a_pipe", "m_pipe", "z_pipe"]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@pytest.fixture()
def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    sp = p.add_subparsers(dest="command")
    add_tombstone_subparser(sp)
    return p


def test_add_tombstone_subparser_registers_command(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args(["tombstone", "list"])
    assert args.command == "tombstone"


def test_cmd_tombstone_missing_config_returns_1(state_dir: str) -> None:
    args = argparse.Namespace(config="/no/such/file.yml", tombstone_cmd="list")
    with patch("pipewatch.cli_tombstone.load_config", return_value=None):
        assert cmd_tombstone(args) == 1


def test_cmd_tombstone_mark_and_list(state_dir: str, capsys: pytest.CaptureFixture) -> None:
    cfg = MagicMock(state_dir=state_dir)
    with patch("pipewatch.cli_tombstone.load_config", return_value=cfg):
        args = argparse.Namespace(config="pw.yml", tombstone_cmd="mark",
                                  pipeline="etl_daily", reason="retired", tombstoned_by=None)
        assert cmd_tombstone(args) == 0
        args2 = argparse.Namespace(config="pw.yml", tombstone_cmd="list")
        assert cmd_tombstone(args2) == 0
    out = capsys.readouterr().out
    assert "etl_daily" in out


def test_cmd_tombstone_restore_missing_returns_1(state_dir: str) -> None:
    cfg = MagicMock(state_dir=state_dir)
    with patch("pipewatch.cli_tombstone.load_config", return_value=cfg):
        args = argparse.Namespace(config="pw.yml", tombstone_cmd="restore", pipeline="ghost")
        assert cmd_tombstone(args) == 1


def test_cmd_tombstone_no_subcommand_returns_1(state_dir: str) -> None:
    cfg = MagicMock(state_dir=state_dir)
    with patch("pipewatch.cli_tombstone.load_config", return_value=cfg):
        args = argparse.Namespace(config="pw.yml", tombstone_cmd=None)
        assert cmd_tombstone(args) == 1
