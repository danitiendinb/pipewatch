"""Detect dependency cycles in the pipeline graph."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from pipewatch.dependency import load_dependencies


@dataclass
class CycleReport:
    pipeline: str
    cycle_path: List[str]

    def __str__(self) -> str:
        return " -> ".join(self.cycle_path)


def _build_graph(pipeline_names: List[str], state_dir: str) -> Dict[str, List[str]]:
    """Return adjacency list: pipeline -> list of upstream dependencies."""
    graph: Dict[str, List[str]] = {}
    for name in pipeline_names:
        deps = load_dependencies(state_dir, name)
        graph[name] = list(deps.get("upstream", []))
    return graph


def _dfs(
    node: str,
    graph: Dict[str, List[str]],
    visited: Set[str],
    stack: List[str],
    cycles: List[CycleReport],
) -> None:
    visited.add(node)
    stack.append(node)
    for neighbour in graph.get(node, []):
        if neighbour not in visited:
            _dfs(neighbour, graph, visited, stack, cycles)
        elif neighbour in stack:
            cycle_start = stack.index(neighbour)
            cycle_path = stack[cycle_start:] + [neighbour]
            cycles.append(CycleReport(pipeline=node, cycle_path=cycle_path))
    stack.pop()


def detect_cycles(
    pipeline_names: List[str],
    state_dir: str,
) -> List[CycleReport]:
    """Return a list of CycleReports for every cycle found in the dependency graph."""
    graph = _build_graph(pipeline_names, state_dir)
    visited: Set[str] = set()
    cycles: List[CycleReport] = []
    for node in graph:
        if node not in visited:
            _dfs(node, graph, visited, [], cycles)
    return cycles


def has_cycle(pipeline_names: List[str], state_dir: str) -> bool:
    return bool(detect_cycles(pipeline_names, state_dir))
