"""Tests for pipewatch.cycledetector."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Dict, List

import pytest

from pipewatch.cycledetector import (
    CycleReport,
    detect_cycles,
    has_cycle,
    _build_graph,
)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _write_deps(state_dir: str, pipeline: str, upstream: List[str]) -> None:
    """Write a dependency file for *pipeline* with the given upstream list."""
    p = Path(state_dir) / f"{pipeline}.deps.json"
    p.write_text(json.dumps({"upstream": upstream, "downstream": []}))


# ---------------------------------------------------------------------------
# _build_graph
# ---------------------------------------------------------------------------

def test_build_graph_empty_when_no_deps(state_dir: str) -> None:
    graph = _build_graph(["a", "b"], state_dir)
    assert graph == {"a": [], "b": []}


def test_build_graph_reflects_upstream(state_dir: str) -> None:
    _write_deps(state_dir, "b", ["a"])
    graph = _build_graph(["a", "b"], state_dir)
    assert graph["b"] == ["a"]
    assert graph["a"] == []


# ---------------------------------------------------------------------------
# detect_cycles
# ---------------------------------------------------------------------------

def test_detect_cycles_empty_graph(state_dir: str) -> None:
    assert detect_cycles([], state_dir) == []


def test_detect_cycles_no_cycle_linear(state_dir: str) -> None:
    _write_deps(state_dir, "b", ["a"])
    _write_deps(state_dir, "c", ["b"])
    result = detect_cycles(["a", "b", "c"], state_dir)
    assert result == []


def test_detect_cycles_self_loop(state_dir: str) -> None:
    _write_deps(state_dir, "a", ["a"])
    result = detect_cycles(["a"], state_dir)
    assert len(result) == 1
    assert isinstance(result[0], CycleReport)
    assert "a" in result[0].cycle_path


def test_detect_cycles_two_node_cycle(state_dir: str) -> None:
    _write_deps(state_dir, "a", ["b"])
    _write_deps(state_dir, "b", ["a"])
    result = detect_cycles(["a", "b"], state_dir)
    assert len(result) >= 1


def test_detect_cycles_three_node_cycle(state_dir: str) -> None:
    _write_deps(state_dir, "b", ["a"])
    _write_deps(state_dir, "c", ["b"])
    _write_deps(state_dir, "a", ["c"])
    result = detect_cycles(["a", "b", "c"], state_dir)
    assert len(result) >= 1


# ---------------------------------------------------------------------------
# has_cycle
# ---------------------------------------------------------------------------

def test_has_cycle_false_for_dag(state_dir: str) -> None:
    _write_deps(state_dir, "b", ["a"])
    assert has_cycle(["a", "b"], state_dir) is False


def test_has_cycle_true_when_cycle_present(state_dir: str) -> None:
    _write_deps(state_dir, "a", ["b"])
    _write_deps(state_dir, "b", ["a"])
    assert has_cycle(["a", "b"], state_dir) is True


# ---------------------------------------------------------------------------
# CycleReport.__str__
# ---------------------------------------------------------------------------

def test_cycle_report_str() -> None:
    report = CycleReport(pipeline="a", cycle_path=["a", "b", "a"])
    assert str(report) == "a -> b -> a"
