"""Tests for pipewatch.dependency."""
import pytest
from pathlib import Path
from pipewatch.dependency import (
    load_dependencies,
    add_upstream,
    remove_upstream,
    clear_dependencies,
    blocked_by_failures,
)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_load_dependencies_empty_for_new_pipeline(state_dir):
    data = load_dependencies(state_dir, "etl_a")
    assert data == {"upstream": [], "downstream": []}


def test_add_upstream_records_relationship(state_dir):
    add_upstream(state_dir, "etl_b", "etl_a")
    data = load_dependencies(state_dir, "etl_b")
    assert "etl_a" in data["upstream"]


def test_add_upstream_mirrors_downstream(state_dir):
    add_upstream(state_dir, "etl_b", "etl_a")
    up_data = load_dependencies(state_dir, "etl_a")
    assert "etl_b" in up_data["downstream"]


def test_add_upstream_idempotent(state_dir):
    add_upstream(state_dir, "etl_b", "etl_a")
    add_upstream(state_dir, "etl_b", "etl_a")
    data = load_dependencies(state_dir, "etl_b")
    assert data["upstream"].count("etl_a") == 1


def test_remove_upstream_clears_both_sides(state_dir):
    add_upstream(state_dir, "etl_b", "etl_a")
    remove_upstream(state_dir, "etl_b", "etl_a")
    assert "etl_a" not in load_dependencies(state_dir, "etl_b")["upstream"]
    assert "etl_b" not in load_dependencies(state_dir, "etl_a")["downstream"]


def test_clear_dependencies_removes_file(state_dir):
    add_upstream(state_dir, "etl_b", "etl_a")
    clear_dependencies(state_dir, "etl_b")
    data = load_dependencies(state_dir, "etl_b")
    assert data == {"upstream": [], "downstream": []}


def test_blocked_by_failures_returns_failing_upstreams(state_dir):
    add_upstream(state_dir, "etl_c", "etl_a")
    add_upstream(state_dir, "etl_c", "etl_b")
    blocked = blocked_by_failures(state_dir, "etl_c", ["etl_a"])
    assert blocked == ["etl_a"]


def test_blocked_by_failures_empty_when_all_ok(state_dir):
    add_upstream(state_dir, "etl_c", "etl_a")
    assert blocked_by_failures(state_dir, "etl_c", []) == []
