"""Tests for pipewatch.cli_export."""
import json
import sys
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_export import cmd_export, add_export_subparser
from pipewatch.state import PipelineState


def _config(tmp_path):
    cfg = MagicMock()
    cfg.state_dir = str(tmp_path)
    pipe = MagicMock()
    pipe.name = "demo"
    cfg.pipelines = [pipe]
    return cfg


def _args(fmt="json", output=None, pipeline=None):
    a = MagicMock()
    a.format = fmt
    a.output = output
    a.pipeline = pipeline
    return a


def test_cmd_export_json_stdout(tmp_path, capsys):
    cfg = _config(tmp_path)
    with patch("pipewatch.cli_export.StateStore") as MockStore:
        MockStore.return_value.load.return_value = PipelineState(runs=[], consecutive_failures=0)
        rc = cmd_export(_args(), cfg)
    assert rc == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert isinstance(data, list)


def test_cmd_export_csv_stdout(tmp_path, capsys):
    cfg = _config(tmp_path)
    with patch("pipewatch.cli_export.StateStore") as MockStore:
        MockStore.return_value.load.return_value = PipelineState(runs=[], consecutive_failures=0)
        rc = cmd_export(_args(fmt="csv"), cfg)
    assert rc == 0


def test_cmd_export_writes_file(tmp_path):
    cfg = _config(tmp_path)
    out_file = tmp_path / "out.json"
    with patch("pipewatch.cli_export.StateStore") as MockStore:
        MockStore.return_value.load.return_value = PipelineState(runs=[], consecutive_failures=0)
        cmd_export(_args(output=str(out_file)), cfg)
    assert out_file.exists()
    json.loads(out_file.read_text())


def test_cmd_export_single_pipeline(tmp_path, capsys):
    cfg = _config(tmp_path)
    with patch("pipewatch.cli_export.StateStore") as MockStore:
        MockStore.return_value.load.return_value = PipelineState(runs=[], consecutive_failures=0)
        cmd_export(_args(pipeline="demo"), cfg)
    MockStore.return_value.load.assert_called_once_with("demo")


def test_add_export_subparser():
    import argparse
    p = argparse.ArgumentParser()
    sub = p.add_subparsers()
    add_export_subparser(sub)
    args = p.parse_args(["export", "--format", "csv"])
    assert args.format == "csv"
