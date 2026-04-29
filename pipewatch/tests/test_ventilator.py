"""Unit tests for pipewatch.ventilator."""
from __future__ import annotations

import pytest

from pipewatch.ventilator import (
    VentilatorState,
    clear_ventilator,
    evaluate_pressure,
    load_ventilator,
    overloaded_pipelines,
    save_ventilator,
    update_ventilator,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def test_load_ventilator_defaults_for_new_pipeline(state_dir):
    s = load_ventilator(state_dir, "pipe_a")
    assert s.queued == 0
    assert s.active == 0
    assert s.pipeline == "pipe_a"


def test_save_and_load_ventilator(state_dir):
    state = VentilatorState(pipeline="pipe_a", queued=5, active=2)
    save_ventilator(state_dir, state)
    loaded = load_ventilator(state_dir, "pipe_a")
    assert loaded.queued == 5
    assert loaded.active == 2


def test_update_ventilator_returns_state(state_dir):
    state = update_ventilator(state_dir, "pipe_a", queued=3, active=1)
    assert state.queued == 3
    assert state.active == 1


def test_update_ventilator_persists(state_dir):
    update_ventilator(state_dir, "pipe_a", queued=7, active=2)
    loaded = load_ventilator(state_dir, "pipe_a")
    assert loaded.queued == 7


def test_clear_ventilator_removes_file(state_dir):
    update_ventilator(state_dir, "pipe_a", queued=3, active=0)
    clear_ventilator(state_dir, "pipe_a")
    s = load_ventilator(state_dir, "pipe_a")
    assert s.queued == 0  # defaults after clear


def test_clear_ventilator_noop_when_absent(state_dir):
    clear_ventilator(state_dir, "nonexistent")  # should not raise


def test_evaluate_pressure_not_overloaded(state_dir):
    state = VentilatorState(pipeline="p", queued=4, active=1)
    report = evaluate_pressure(state, threshold=10)
    assert not report.overloaded
    assert report.pressure == 0.4


def test_evaluate_pressure_overloaded_at_threshold(state_dir):
    state = VentilatorState(pipeline="p", queued=10, active=0)
    report = evaluate_pressure(state, threshold=10)
    assert report.overloaded
    assert report.pressure == 1.0


def test_evaluate_pressure_clamped_above_threshold(state_dir):
    state = VentilatorState(pipeline="p", queued=20, active=0)
    report = evaluate_pressure(state, threshold=10)
    assert report.pressure == 1.0


def test_evaluate_pressure_invalid_threshold():
    state = VentilatorState(pipeline="p", queued=1, active=0)
    with pytest.raises(ValueError):
        evaluate_pressure(state, threshold=0)


def test_overloaded_pipelines_returns_only_overloaded(state_dir):
    update_ventilator(state_dir, "pipe_ok", queued=2, active=1)
    update_ventilator(state_dir, "pipe_bad", queued=15, active=3)
    reports = overloaded_pipelines(state_dir, ["pipe_ok", "pipe_bad"], threshold=10)
    names = [r.pipeline for r in reports]
    assert "pipe_bad" in names
    assert "pipe_ok" not in names


def test_overloaded_pipelines_empty_when_all_ok(state_dir):
    update_ventilator(state_dir, "p1", queued=1, active=0)
    reports = overloaded_pipelines(state_dir, ["p1"], threshold=10)
    assert reports == []
