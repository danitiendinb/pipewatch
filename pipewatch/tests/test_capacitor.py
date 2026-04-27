"""Tests for pipewatch.capacitor."""

import pytest
from pathlib import Path

from pipewatch.capacitor import (
    acquire_slot,
    release_slot,
    clear_capacitor,
    load_capacitor,
    CapacitorState,
)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_load_capacitor_defaults_for_new_pipeline(state_dir):
    state = load_capacitor(state_dir, "pipe_a", max_concurrent=2)
    assert isinstance(state, CapacitorState)
    assert state.current_count == 0
    assert state.max_concurrent == 2
    assert not state.is_at_capacity


def test_acquire_slot_returns_true_when_free(state_dir):
    result = acquire_slot(state_dir, "pipe_a", run_id="run-1", max_concurrent=2)
    assert result is True


def test_acquire_slot_persists_active_run(state_dir):
    acquire_slot(state_dir, "pipe_a", run_id="run-1", max_concurrent=2)
    state = load_capacitor(state_dir, "pipe_a", max_concurrent=2)
    assert state.current_count == 1
    assert state.active_runs[0].run_id == "run-1"


def test_acquire_slot_blocked_at_capacity(state_dir):
    acquire_slot(state_dir, "pipe_a", run_id="run-1", max_concurrent=1)
    result = acquire_slot(state_dir, "pipe_a", run_id="run-2", max_concurrent=1)
    assert result is False


def test_acquire_multiple_slots_within_limit(state_dir):
    acquire_slot(state_dir, "pipe_a", run_id="run-1", max_concurrent=3)
    acquire_slot(state_dir, "pipe_a", run_id="run-2", max_concurrent=3)
    state = load_capacitor(state_dir, "pipe_a", max_concurrent=3)
    assert state.current_count == 2
    assert not state.is_at_capacity


def test_release_slot_returns_true_when_found(state_dir):
    acquire_slot(state_dir, "pipe_a", run_id="run-1", max_concurrent=1)
    result = release_slot(state_dir, "pipe_a", run_id="run-1")
    assert result is True


def test_release_slot_removes_run(state_dir):
    acquire_slot(state_dir, "pipe_a", run_id="run-1", max_concurrent=1)
    release_slot(state_dir, "pipe_a", run_id="run-1")
    state = load_capacitor(state_dir, "pipe_a", max_concurrent=1)
    assert state.current_count == 0


def test_release_slot_returns_false_when_not_found(state_dir):
    result = release_slot(state_dir, "pipe_a", run_id="ghost-run")
    assert result is False


def test_release_frees_capacity_for_new_acquire(state_dir):
    acquire_slot(state_dir, "pipe_a", run_id="run-1", max_concurrent=1)
    release_slot(state_dir, "pipe_a", run_id="run-1")
    result = acquire_slot(state_dir, "pipe_a", run_id="run-2", max_concurrent=1)
    assert result is True


def test_clear_capacitor_removes_file(state_dir):
    acquire_slot(state_dir, "pipe_a", run_id="run-1", max_concurrent=1)
    clear_capacitor(state_dir, "pipe_a")
    state = load_capacitor(state_dir, "pipe_a", max_concurrent=1)
    assert state.current_count == 0


def test_pipelines_are_independent(state_dir):
    acquire_slot(state_dir, "pipe_a", run_id="run-1", max_concurrent=1)
    result = acquire_slot(state_dir, "pipe_b", run_id="run-1", max_concurrent=1)
    assert result is True
