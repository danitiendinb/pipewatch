"""Tests for pipewatch.cli_inspector"""
from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_inspector import add_inspector_subparser, cmd_inspect, _render
from pipewatch.inspector import InspectionReport, Finding


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers()
    add_inspector_subparser(sub)
    return p


def _args(**kwargs):
    base = dict(config="pipewatch.yml", pipeline=None, no_colour=False)
    base.update(kwargs)
    return argparse.Namespace(**base)


# ---------------------------------------------------------------------------
# subparser registration
# ---------------------------------------------------------------------------

def test_add_inspector_subparser_registers_command(parser):
    args = parser.parse_args(["inspect"])
    assert hasattr(args, "func")


def test_add_inspector_subparser_no_colour_flag(parser):
    args = parser.parse_args(["inspect", "--no-colour"])
    assert args.no_colour is True


def test_add_inspector_subparser_pipeline_flag(parser):
    args = parser.parse_args(["inspect", "--pipeline", "my-pipe"])
    assert args.pipeline == "my-pipe"


# ---------------------------------------------------------------------------
# _render
# ---------------------------------------------------------------------------

def test_render_contains_pipeline_name():
    r = InspectionReport("pipe-x", [Finding("OK", "info", "All good.")])
    out = _render(r, no_colour=True)
    assert "pipe-x" in out


def test_render_no_colour_omits_ansi():
    r = InspectionReport("p", [Finding("OK", "info", "msg")])
    out = _render(r, no_colour=True)
    assert "\033[" not in out


# ---------------------------------------------------------------------------
# cmd_inspect
# ---------------------------------------------------------------------------

def test_cmd_inspect_missing_config_returns_1(tmp_path):
    args = _args(config=str(tmp_path / "missing.yml"))
    assert cmd_inspect(args) == 1


def test_cmd_inspect_no_pipelines_returns_0(tmp_path):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    cfg.pipelines = []
    with patch("pipewatch.cli_inspector.load_config", return_value=cfg), \
         patch("pipewatch.cli_inspector.PipelineStore") as MockStore:
        MockStore.return_value.pipelines.return_value = []
        result = cmd_inspect(_args())
    assert result == 0


def test_cmd_inspect_critical_returns_1(tmp_path):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    cfg.pipelines = []
    critical_report = InspectionReport(
        "bad-pipe", [Finding("CONSECUTIVE_FAILURES", "critical", "3 failures.")]
    )
    with patch("pipewatch.cli_inspector.load_config", return_value=cfg), \
         patch("pipewatch.cli_inspector.PipelineStore"), \
         patch("pipewatch.cli_inspector.inspect_all", return_value=[critical_report]):
        result = cmd_inspect(_args())
    assert result == 1
