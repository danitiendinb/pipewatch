"""Tests for pipewatch.checkpoint and pipewatch.cli_checkpoint."""
from __future__ import annotations

import json
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.checkpoint import (
    Checkpoint,
    clear_checkpoints,
    get_checkpoint,
    load_checkpoints,
    remove_checkpoint,
    set_checkpoint,
)
from pipewatch.cli_checkpoint import add_checkpoint_subparser, cmd_checkpoint


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


# ---------------------------------------------------------------------------
# checkpoint module
# ---------------------------------------------------------------------------

def test_load_checkpoints_empty_for_new_pipeline(state_dir):
    assert load_checkpoints(state_dir, "pipe_a") == {}


def test_set_checkpoint_returns_checkpoint(state_dir):
    cp = set_checkpoint(state_dir, "pipe_a", "step_1")
    assert isinstance(cp, Checkpoint)
    assert cp.name == "step_1"
    assert cp.recorded_at != ""


def test_set_checkpoint_persists(state_dir):
    set_checkpoint(state_dir, "pipe_a", "step_1", {"rows": 42})
    data = load_checkpoints(state_dir, "pipe_a")
    assert "step_1" in data
    assert data["step_1"].metadata == {"rows": 42}


def test_set_checkpoint_overwrites_existing(state_dir):
    set_checkpoint(state_dir, "pipe_a", "step_1", {"rows": 1})
    set_checkpoint(state_dir, "pipe_a", "step_1", {"rows": 99})
    data = load_checkpoints(state_dir, "pipe_a")
    assert data["step_1"].metadata == {"rows": 99}


def test_get_checkpoint_returns_none_when_missing(state_dir):
    assert get_checkpoint(state_dir, "pipe_a", "missing") is None


def test_get_checkpoint_returns_correct_entry(state_dir):
    set_checkpoint(state_dir, "pipe_a", "step_2", {"ok": True})
    cp = get_checkpoint(state_dir, "pipe_a", "step_2")
    assert cp is not None
    assert cp.metadata == {"ok": True}


def test_remove_checkpoint_returns_false_when_missing(state_dir):
    assert remove_checkpoint(state_dir, "pipe_a", "ghost") is False


def test_remove_checkpoint_deletes_entry(state_dir):
    set_checkpoint(state_dir, "pipe_a", "step_1")
    set_checkpoint(state_dir, "pipe_a", "step_2")
    removed = remove_checkpoint(state_dir, "pipe_a", "step_1")
    assert removed is True
    assert get_checkpoint(state_dir, "pipe_a", "step_1") is None
    assert get_checkpoint(state_dir, "pipe_a", "step_2") is not None


def test_clear_checkpoints_removes_all(state_dir):
    set_checkpoint(state_dir, "pipe_a", "s1")
    set_checkpoint(state_dir, "pipe_a", "s2")
    clear_checkpoints(state_dir, "pipe_a")
    assert load_checkpoints(state_dir, "pipe_a") == {}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@pytest.fixture()
def parser():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="pipewatch.yml")
    sub = p.add_subparsers(dest="command")
    add_checkpoint_subparser(sub)
    return p


def _make_args(parser, state_dir, *argv):
    args = parser.parse_args(["checkpoint"] + list(argv))
    cfg = MagicMock()
    cfg.state_dir = state_dir
    with patch("pipewatch.cli_checkpoint.load_config", return_value=cfg):
        return args, cfg


def test_add_checkpoint_subparser_registers_command(parser):
    args = parser.parse_args(["checkpoint", "list", "pipe_a"])
    assert args.command == "checkpoint"


def test_cmd_checkpoint_missing_config_returns_1(parser, state_dir):
    args = parser.parse_args(["checkpoint", "list", "pipe_a"])
    with patch("pipewatch.cli_checkpoint.load_config", return_value=None):
        assert cmd_checkpoint(args) == 1


def test_cmd_checkpoint_set_and_list(parser, state_dir):
    args = parser.parse_args(["checkpoint", "set", "pipe_a", "step_1"])
    cfg = MagicMock(state_dir=state_dir)
    with patch("pipewatch.cli_checkpoint.load_config", return_value=cfg):
        rc = cmd_checkpoint(args)
    assert rc == 0
    assert get_checkpoint(state_dir, "pipe_a", "step_1") is not None


def test_cmd_checkpoint_no_subcommand_returns_1(parser, state_dir):
    args = parser.parse_args(["checkpoint"])
    cfg = MagicMock(state_dir=state_dir)
    with patch("pipewatch.cli_checkpoint.load_config", return_value=cfg):
        assert cmd_checkpoint(args) == 1
