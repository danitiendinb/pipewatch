"""Unit tests for pipewatch.cli_eventsink."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_eventsink import add_eventsink_subparser, cmd_eventsink
from pipewatch.eventsink import push_event


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_eventsink_subparser(sub)
    return p


def test_add_eventsink_subparser_registers_command(parser):
    args = parser.parse_args(["events", "my_pipe"])
    assert args.command == "events"


def test_add_eventsink_subparser_drain_flag(parser):
    args = parser.parse_args(["events", "my_pipe", "--drain"])
    assert args.drain is True


def test_add_eventsink_subparser_clear_flag(parser):
    args = parser.parse_args(["events", "my_pipe", "--clear"])
    assert args.clear is True


def test_cmd_eventsink_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(
        config=str(tmp_path / "missing.yml"),
        pipeline="pipe_a",
        drain=False,
        clear=False,
    )
    assert cmd_eventsink(args) == 1


def test_cmd_eventsink_no_events_prints_message(tmp_path, capsys):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(
        config="pipewatch.yml",
        pipeline="pipe_a",
        drain=False,
        clear=False,
    )
    with patch("pipewatch.cli_eventsink.load_config", return_value=cfg):
        rc = cmd_eventsink(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "No events" in out


def test_cmd_eventsink_prints_json(tmp_path, capsys):
    push_event(str(tmp_path), "pipe_a", "success", "2024-01-01T00:00:00")
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(
        config="pipewatch.yml",
        pipeline="pipe_a",
        drain=False,
        clear=False,
    )
    with patch("pipewatch.cli_eventsink.load_config", return_value=cfg):
        rc = cmd_eventsink(args)
    assert rc == 0
    out = capsys.readouterr().out
    data = json.loads(out)
    assert len(data) == 1
    assert data[0]["event_type"] == "success"


def test_cmd_eventsink_clear_removes_events(tmp_path, capsys):
    push_event(str(tmp_path), "pipe_a", "failure", "2024-01-01T00:00:00")
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(
        config="pipewatch.yml",
        pipeline="pipe_a",
        drain=False,
        clear=True,
    )
    with patch("pipewatch.cli_eventsink.load_config", return_value=cfg):
        rc = cmd_eventsink(args)
    assert rc == 0
    from pipewatch.eventsink import load_events
    assert load_events(str(tmp_path), "pipe_a") == []
