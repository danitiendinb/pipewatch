"""Tests for the sampler CLI sub-command."""

from __future__ import annotations

import argparse
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.cli_sampler import add_sampler_subparser, cmd_sampler


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers()
    add_sampler_subparser(sub)
    return p


def test_add_sampler_subparser_registers_command(parser):
    args = parser.parse_args(["sample", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_add_sampler_subparser_default_n(parser):
    args = parser.parse_args(["sample", "my_pipe"])
    assert args.n == 5


def test_add_sampler_subparser_custom_n(parser):
    args = parser.parse_args(["sample", "my_pipe", "--n", "10"])
    assert args.n == 10


def test_add_sampler_subparser_seed_flag(parser):
    args = parser.parse_args(["sample", "my_pipe", "--seed", "42"])
    assert args.seed == 42


def test_add_sampler_subparser_save_flag(parser):
    args = parser.parse_args(["sample", "my_pipe", "--save"])
    assert args.save is True


def test_cmd_sampler_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(
        config=str(tmp_path / "missing.yml"),
        pipeline="p",
        n=5,
        seed=None,
        save=False,
        show=False,
        clear=False,
    )
    with patch("pipewatch.cli_sampler.load_config", return_value=None):
        assert cmd_sampler(args) == 1


def test_cmd_sampler_no_runs_returns_0(tmp_path):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(
        config="pipewatch.yml",
        pipeline="empty",
        n=5,
        seed=None,
        save=False,
        show=False,
        clear=False,
    )
    with patch("pipewatch.cli_sampler.load_config", return_value=cfg):
        assert cmd_sampler(args) == 0


def test_cmd_sampler_clear_returns_0(tmp_path):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    args = argparse.Namespace(
        config="pipewatch.yml",
        pipeline="p",
        n=5,
        seed=None,
        save=False,
        show=False,
        clear=True,
    )
    with patch("pipewatch.cli_sampler.load_config", return_value=cfg):
        assert cmd_sampler(args) == 0
