"""Dependency graph traversal and reachability analysis."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from pipewatch.dependency import load_dependencies


@dataclass
class GraphNode:
    name: str
    upstreams: List[str] = field(default_factory=list)
    downstreams: List[str] = field(default_factory=list)


@dataclass
class GraphReport:
    nodes: Dict[str, GraphNode] = field(default_factory=dict)

    def pipeline_names(self) -> List[str]:
        return sorted(self.nodes.keys())


def build_graph(pipeline_names: List[str], state_dir: str) -> GraphReport:
    """Build a full dependency graph from all known pipelines."""
    report = GraphReport()
    for name in pipeline_names:
        deps = load_dependencies(name, state_dir)
        report.nodes[name] = GraphNode(
            name=name,
            upstreams=list(deps.get("upstreams", [])),
            downstreams=list(deps.get("downstreams", [])),
        )
    return report


def reachable_from(graph: GraphReport, start: str, direction: str = "downstream") -> Set[str]:
    """Return all nodes reachable from *start* following *direction* edges.

    direction: 'downstream' or 'upstream'
    """
    if start not in graph.nodes:
        return set()
    visited: Set[str] = set()
    queue = [start]
    while queue:
        current = queue.pop()
        if current in visited:
            continue
        visited.add(current)
        node = graph.nodes.get(current)
        if node is None:
            continue
        neighbours = node.downstreams if direction == "downstream" else node.upstreams
        for neighbour in neighbours:
            if neighbour not in visited:
                queue.append(neighbour)
    visited.discard(start)
    return visited


def critical_path(graph: GraphReport, start: str) -> List[str]:
    """Return the longest downstream chain from *start* (greedy DFS)."""
    if start not in graph.nodes:
        return []

    def _dfs(name: str, visited: Set[str]) -> List[str]:
        if name in visited:
            return []
        visited = visited | {name}
        node = graph.nodes.get(name)
        if node is None or not node.downstreams:
            return [name]
        best: List[str] = []
        for child in node.downstreams:
            path = _dfs(child, visited)
            if len(path) > len(best):
                best = path
        return [name] + best

    return _dfs(start, set())


def root_nodes(graph: GraphReport) -> List[str]:
    """Return pipelines that have no upstream dependencies."""
    return sorted(name for name, node in graph.nodes.items() if not node.upstreams)
