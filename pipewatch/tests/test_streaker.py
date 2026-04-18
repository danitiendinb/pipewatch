"""Tests for pipewatch.streaker."""
import pytest
from pathlib import Path

from pipewatch.streaker import (
    load_streak,
    update_streak,
    compute_streak,
    StreakInfo,
)
from pipewatch.state import PipelineStore


@pytest.fixture
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_load_streak_defaults_for_new_pipeline(state_dir):
    info = load_streak(state_dir, "pipe-a")
    assert info.current_streak == 0
    assert info.longest_success_streak == 0
    assert info.longest_failure_streak == 0


def test_update_streak_success_increments(state_dir):
    info = update_streak(state_dir, "pipe-a", success=True)
    assert info.current_streak == 1
    assert info.longest_success_streak == 1


def test_update_streak_failure_decrements(state_dir):
    info = update_streak(state_dir, "pipe-a", success=False)
    assert info.current_streak == -1
    assert info.longest_failure_streak == 1


def test_update_streak_resets_on_flip(state_dir):
    update_streak(state_dir, "pipe-a", success=True)
    update_streak(state_dir, "pipe-a", success=True)
    info = update_streak(state_dir, "pipe-a", success=False)
    assert info.current_streak == -1
    assert info.longest_success_streak == 2


def test_update_streak_longest_tracked(state_dir):
    for _ in range(4):
        update_streak(state_dir, "pipe-a", success=True)
    update_streak(state_dir, "pipe-a", success=False)
    info = update_streak(state_dir, "pipe-a", success=True)
    assert info.longest_success_streak == 4
    assert info.current_streak == 1


def test_compute_streak_from_runs(state_dir):
    store = PipelineStore(state_dir)
    store.record_success("pipe-b", "r1", duration=1.0)
    store.record_success("pipe-b", "r2", duration=1.0)
    store.record_failure("pipe-b", "r3", message="oops")
    info = compute_streak(state_dir, "pipe-b")
    assert info.current_streak == -1
    assert info.longest_success_streak == 2
    assert info.longest_failure_streak == 1


def test_compute_streak_all_success(state_dir):
    store = PipelineStore(state_dir)
    for i in range(3):
        store.record_success("pipe-c", f"r{i}", duration=0.5)
    info = compute_streak(state_dir, "pipe-c")
    assert info.current_streak == 3
    assert info.longest_success_streak == 3
    assert info.longest_failure_streak == 0
