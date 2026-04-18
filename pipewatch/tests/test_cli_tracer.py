"""Unit tests for pipewatch.cli_tracer."""
import argparse
import pytest
from unittest.mock import patch, MagicMock
from pipewatch.cli_tracer import add_tracer_subparser, cmd_trace


@pytest.fixture
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_tracer_subparser(sub)
    return p


def test_add_tracer_subparser_registers_command(parser):
    args = parser.parse_args(["trace", "my_pipe"])
    assert args.pipeline == "my_pipe"


def test_add_tracer_subparser_run_id_flag(parser):
    args = parser.parse_args(["trace", "my_pipe", "--run-id", "abc"])
    assert args.run_id == "abc"


def test_add_tracer_subparser_clear_flag(parser):
    args = parser.parse_args(["trace", "my_pipe", "--clear"])
    assert args.clear is True


def test_cmd_trace_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(
        pipeline="p", run_id=None, clear=False, config=str(tmp_path / "missing.yml")
    )
    with patch("pipewatch.cli_tracer.load_config", return_value=None):
        assert cmd_trace(args) == 1


def test_cmd_trace_no_events_prints_message(tmp_path, capsys):
    cfg = MagicMock(state_dir=str(tmp_path))
    args = argparse.Namespace(pipeline="p", run_id=None, clear=False, config="x.yml")
    with patch("pipewatch.cli_tracer.load_config", return_value=cfg):
        rc = cmd_trace(args)
    assert rc == 0
    assert "No trace events" in capsys.readouterr().out


def test_cmd_trace_clear_returns_0(tmp_path, capsys):
    cfg = MagicMock(state_dir=str(tmp_path))
    args = argparse.Namespace(pipeline="p", run_id=None, clear=True, config="x.yml")
    with patch("pipewatch.cli_tracer.load_config", return_value=cfg):
        rc = cmd_trace(args)
    assert rc == 0
    assert "cleared" in capsys.readouterr().out
