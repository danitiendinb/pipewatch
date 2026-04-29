"""Integration tests for pipewatch.ventilator."""
from __future__ import annotations

import pytest

from pipewatch.ventilator import (
    clear_ventilator,
    evaluate_pressure,
    load_ventilator,
    overloaded_pipelines,
    update_ventilator,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def test_pressure_rises_with_queue_depth(state_dir):
    for depth in [0, 5, 10]:
        update_ventilator(state_dir, "pipe", queued=depth, active=0)
        state = load_ventilator(state_dir, "pipe")
        report = evaluate_pressure(state, threshold=10)
        assert report.pressure == min(depth / 10, 1.0)


def test_overloaded_flag_toggles_correctly(state_dir):
    update_ventilator(state_dir, "pipe", queued=9, active=0)
    state = load_ventilator(state_dir, "pipe")
    assert not evaluate_pressure(state, threshold=10).overloaded

    update_ventilator(state_dir, "pipe", queued=10, active=0)
    state = load_ventilator(state_dir, "pipe")
    assert evaluate_pressure(state, threshold=10).overloaded


def test_clear_resets_to_default(state_dir):
    update_ventilator(state_dir, "pipe", queued=20, active=5)
    clear_ventilator(state_dir, "pipe")
    state = load_ventilator(state_dir, "pipe")
    assert state.queued == 0
    assert state.active == 0


def test_multiple_pipelines_independent(state_dir):
    update_ventilator(state_dir, "pipe_a", queued=15, active=0)
    update_ventilator(state_dir, "pipe_b", queued=2, active=1)

    reports = overloaded_pipelines(state_dir, ["pipe_a", "pipe_b"], threshold=10)
    assert len(reports) == 1
    assert reports[0].pipeline == "pipe_a"


def test_last_updated_is_populated_after_save(state_dir):
    update_ventilator(state_dir, "pipe", queued=1, active=0)
    state = load_ventilator(state_dir, "pipe")
    assert state.last_updated != ""
