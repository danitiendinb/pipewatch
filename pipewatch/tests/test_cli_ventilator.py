"""Unit tests for pipewatch.cli_ventilator."""
from __future__ import annotations

import argparse

import pytest

from pipewatch.cli_ventilator import add_ventilator_subparser, cmd_ventilator
from pipewatch.ventilator import update_ventilator


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_ventilator_subparser(sub)
    return p


def test_add_ventilator_subparser_registers_command(parser):
    args = parser.parse_args(["ventilator", "scan"])
    assert args.command == "ventilator"


def test_add_ventilator_subparser_default_threshold(parser):
    args = parser.parse_args(["ventilator", "scan"])
    assert args.threshold == 10


def test_add_ventilator_subparser_custom_threshold(parser):
    args = parser.parse_args(["ventilator", "scan", "--threshold", "5"])
    assert args.threshold == 5


def test_cmd_ventilator_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(
        config=str(tmp_path / "missing.yml"),
        vent_cmd="scan",
        threshold=10,
    )
    assert cmd_ventilator(args) == 1


def test_cmd_ventilator_no_subcommand_returns_1(tmp_path):
    import yaml
    cfg_path = tmp_path / "pipewatch.yml"
    cfg_path.write_text(yaml.dump({
        "pipelines": [{"name": "p1", "schedule": "@hourly"}],
        "state_dir": str(tmp_path / "state"),
    }))
    args = argparse.Namespace(
        config=str(cfg_path),
        vent_cmd=None,
    )
    assert cmd_ventilator(args) == 1


def test_cmd_ventilator_set_and_show(tmp_path, capsys):
    import yaml
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    cfg_path = tmp_path / "pipewatch.yml"
    cfg_path.write_text(yaml.dump({
        "pipelines": [{"name": "p1", "schedule": "@hourly"}],
        "state_dir": str(state_dir),
    }))

    set_args = argparse.Namespace(
        config=str(cfg_path),
        vent_cmd="set",
        pipeline="p1",
        queued=8,
        active=2,
    )
    assert cmd_ventilator(set_args) == 0

    show_args = argparse.Namespace(
        config=str(cfg_path),
        vent_cmd="show",
        pipeline="p1",
        threshold=10,
    )
    assert cmd_ventilator(show_args) == 0
    out = capsys.readouterr().out
    assert "queued=8" in out
    assert "active=2" in out
