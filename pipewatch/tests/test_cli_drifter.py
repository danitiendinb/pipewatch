"""Unit tests for pipewatch.cli_drifter."""
from __future__ import annotations

import argparse
import pytest
from unittest.mock import MagicMock, patch

from pipewatch.cli_drifter import add_drifter_subparser, cmd_drift


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers()
    add_drifter_subparser(sub)
    return p


def test_add_drifter_subparser_registers_command(parser):
    args = parser.parse_args(["drift"])
    assert hasattr(args, "func")


def test_add_drifter_subparser_default_threshold(parser):
    args = parser.parse_args(["drift"])
    assert args.threshold == pytest.approx(20.0)


def test_add_drifter_subparser_default_window(parser):
    args = parser.parse_args(["drift"])
    assert args.window == 10


def test_add_drifter_subparser_record_flag(parser):
    args = parser.parse_args(["drift", "--record"])
    assert args.record is True


def test_cmd_drift_missing_config_returns_1():
    args = argparse.Namespace(
        config="nonexistent.yml",
        pipeline=None,
        threshold=20.0,
        window=10,
        record=False,
    )
    with patch("pipewatch.cli_drifter.load_config", return_value=None):
        assert cmd_drift(args) == 1


def test_cmd_drift_no_drift_returns_0(tmp_path):
    from pipewatch.config import PipewatchConfig, PipelineConfig
    pcfg = PipelineConfig(name="pipe", schedule="@hourly", max_failures=3)
    cfg = MagicMock(spec=PipewatchConfig)
    cfg.pipelines = [pcfg]
    cfg.state_dir = str(tmp_path)

    from pipewatch.drifter import save_drift_baseline
    from pipewatch.state import PipelineState, PipelineRun
    save_drift_baseline(str(tmp_path), "pipe", 60.0)
    state = PipelineState(
        pipeline="pipe",
        runs=[
            PipelineRun(
                run_id="r1",
                status="ok",
                started_at="2024-01-01T00:00:00",
                finished_at="2024-01-01T00:01:00",
                message="",
            )
        ],
    )

    args = argparse.Namespace(
        config="pw.yml",
        pipeline=None,
        threshold=20.0,
        window=10,
        record=False,
    )

    with patch("pipewatch.cli_drifter.load_config", return_value=cfg), \
         patch("pipewatch.cli_drifter.PipelineState") as MockState:
        MockState.load.return_value = state
        result = cmd_drift(args)

    assert result == 0
