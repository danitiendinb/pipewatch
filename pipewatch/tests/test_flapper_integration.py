"""Integration tests for flap detection against a real PipelineState store."""

from __future__ import annotations

import pytest

from pipewatch.state import PipelineState, PipelineRun, start, finish, load as load_state
from pipewatch.flapper import detect_flap, detect_all, count_transitions


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def _record(state_dir: str, pipeline: str, status: str, msg: str = "") -> None:
    from pipewatch.state import record_success, record_failure
    if status == "ok":
        record_success(state_dir, pipeline)
    else:
        record_failure(state_dir, pipeline, msg or "err")


def test_stable_pipeline_not_flapping(state_dir):
    for _ in range(5):
        _record(state_dir, "stable", "ok")
    state = load_state(state_dir, "stable")
    report = detect_flap("stable", state, threshold=3)
    assert not report.is_flapping


def test_alternating_pipeline_is_flapping(state_dir):
    for i in range(6):
        status = "ok" if i % 2 == 0 else "fail"
        _record(state_dir, "flipper", status)
    state = load_state(state_dir, "flipper")
    report = detect_flap("flipper", state, threshold=3)
    assert report.is_flapping


def test_detect_all_returns_one_report_per_pipeline(state_dir):
    for name in ["a", "b", "c"]:
        _record(state_dir, name, "ok")

    def _load(name):
        return load_state(state_dir, name)

    reports = detect_all(["a", "b", "c"], _load)
    assert len(reports) == 3
    assert {r.pipeline for r in reports} == {"a", "b", "c"}


def test_flap_window_limits_history(state_dir):
    # 20 stable runs followed by 4 alternating — only last 10 seen
    for _ in range(20):
        _record(state_dir, "windowed", "ok")
    for i in range(4):
        status = "ok" if i % 2 == 0 else "fail"
        _record(state_dir, "windowed", status)
    state = load_state(state_dir, "windowed")
    report = detect_flap("windowed", state, threshold=3, window=10)
    # within window of 10: 6 ok + 2 ok/fail alternations = 2 transitions
    assert report.flap_count < 3
    assert not report.is_flapping
