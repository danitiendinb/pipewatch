"""Unit tests for pipewatch.cli_grapher."""
from __future__ import annotations

import argparse
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cli_grapher import add_grapher_subparser, cmd_graph


@pytest.fixture()
def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_grapher_subparser(sub)
    return p


def test_add_grapher_subparser_registers_command(parser):
    args = parser.parse_args(["graph"])
    assert args.command == "graph"


def test_add_grapher_subparser_default_config(parser):
    args = parser.parse_args(["graph"])
    assert args.config == "pipewatch.yml"


def test_add_grapher_subparser_roots_flag(parser):
    args = parser.parse_args(["graph", "--roots"])
    assert args.roots is True


def test_add_grapher_subparser_reachable_flag(parser):
    args = parser.parse_args(["graph", "--reachable", "my_pipeline"])
    assert args.reachable == "my_pipeline"


def test_add_grapher_subparser_critical_path_flag(parser):
    args = parser.parse_args(["graph", "--critical-path", "etl_load"])
    assert args.critical_path == "etl_load"


def test_cmd_graph_missing_config_returns_1():
    args = SimpleNamespace(config="/nonexistent/pipewatch.yml", roots=False,
                           reachable=None, critical_path=None)
    with patch("pipewatch.cli_grapher.load_config", return_value=None):
        result = cmd_graph(args)
    assert result == 1


def test_cmd_graph_roots_prints_output(capsys):
    mock_cfg = MagicMock()
    mock_cfg.state_dir = "/tmp/state"
    mock_cfg.pipelines = [MagicMock(name_attr="A"), MagicMock(name_attr="B")]
    mock_cfg.pipelines[0].name = "A"
    mock_cfg.pipelines[1].name = "B"

    from pipewatch.grapher import GraphNode, GraphReport
    graph = GraphReport()
    graph.nodes["A"] = GraphNode(name="A", upstreams=[], downstreams=["B"])
    graph.nodes["B"] = GraphNode(name="B", upstreams=["A"], downstreams=[])

    args = SimpleNamespace(config="pipewatch.yml", roots=True,
                           reachable=None, critical_path=None)
    with patch("pipewatch.cli_grapher.load_config", return_value=mock_cfg), \
         patch("pipewatch.cli_grapher.build_graph", return_value=graph):
        result = cmd_graph(args)

    assert result == 0
    captured = capsys.readouterr()
    assert "A" in captured.out


def test_cmd_graph_critical_path_unknown_returns_1(capsys):
    mock_cfg = MagicMock()
    mock_cfg.state_dir = "/tmp/state"
    mock_cfg.pipelines = []

    from pipewatch.grapher import GraphReport
    graph = GraphReport()

    args = SimpleNamespace(config="pipewatch.yml", roots=False,
                           reachable=None, critical_path="ghost")
    with patch("pipewatch.cli_grapher.load_config", return_value=mock_cfg), \
         patch("pipewatch.cli_grapher.build_graph", return_value=graph):
        result = cmd_graph(args)

    assert result == 1
