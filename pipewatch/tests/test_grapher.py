"""Unit tests for pipewatch.grapher."""
from __future__ import annotations

import pytest

from pipewatch.grapher import (
    GraphNode,
    GraphReport,
    critical_path,
    reachable_from,
    root_nodes,
)


def _make_graph() -> GraphReport:
    """Build a simple graph: A -> B -> C, A -> D"""
    report = GraphReport()
    report.nodes["A"] = GraphNode(name="A", upstreams=[], downstreams=["B", "D"])
    report.nodes["B"] = GraphNode(name="B", upstreams=["A"], downstreams=["C"])
    report.nodes["C"] = GraphNode(name="C", upstreams=["B"], downstreams=[])
    report.nodes["D"] = GraphNode(name="D", upstreams=["A"], downstreams=[])
    return report


def test_pipeline_names_sorted():
    graph = _make_graph()
    assert graph.pipeline_names() == ["A", "B", "C", "D"]


def test_reachable_from_a_downstream():
    graph = _make_graph()
    reached = reachable_from(graph, "A", direction="downstream")
    assert reached == {"B", "C", "D"}


def test_reachable_from_b_downstream():
    graph = _make_graph()
    reached = reachable_from(graph, "B", direction="downstream")
    assert reached == {"C"}


def test_reachable_from_leaf_is_empty():
    graph = _make_graph()
    reached = reachable_from(graph, "C", direction="downstream")
    assert reached == set()


def test_reachable_upstream_from_c():
    graph = _make_graph()
    reached = reachable_from(graph, "C", direction="upstream")
    assert reached == {"A", "B"}


def test_reachable_unknown_node_returns_empty():
    graph = _make_graph()
    assert reachable_from(graph, "Z") == set()


def test_root_nodes_returns_a_only():
    graph = _make_graph()
    assert root_nodes(graph) == ["A"]


def test_root_nodes_all_roots_when_no_deps():
    report = GraphReport()
    report.nodes["X"] = GraphNode(name="X")
    report.nodes["Y"] = GraphNode(name="Y")
    assert root_nodes(report) == ["X", "Y"]


def test_root_nodes_empty_graph():
    """root_nodes on an empty graph should return an empty list."""
    report = GraphReport()
    assert root_nodes(report) == []


def test_critical_path_longest_chain():
    graph = _make_graph()
    path = critical_path(graph, "A")
    # Longest chain is A -> B -> C (length 3) vs A -> D (length 2)
    assert path == ["A", "B", "C"]


def test_critical_path_single_node():
    graph = _make_graph()
    path = critical_path(graph, "C")
    assert path == ["C"]


def test_critical_path_unknown_returns_empty():
    graph = _make_graph()
    assert critical_path(graph, "MISSING") == []


def test_critical_path_from_intermediate_node():
    """critical_path starting from B should return B -> C, not include A."""
    graph = _make_graph()
    path = critical_path(graph, "B")
    assert path == ["B", "C"]


def test_reachable_handles_cycle_gracefully():
    """Graph with a cycle must not loop forever."""
    report = GraphReport()
    report.nodes["P"] = GraphNode(name="P", downstreams=["Q"])
    report.nodes["Q"] = GraphNode(name="Q", downstreams=["P"])  # cycle
    reached = reachable_from(report, "P", direction="downstream")
    assert "Q" in reached
    assert len(reached) < 10  # no infinite expansion
