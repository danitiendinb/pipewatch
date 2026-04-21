"""Integration tests: inspector works end-to-end with real store."""
from __future__ import annotations

import pytest

from pipewatch.state import PipelineStore
from pipewatch.inspector import inspect_pipeline, inspect_all
from pipewatch.silencer import set_silence
from pipewatch.acknowledger import acknowledge


@pytest.fixture()
def store(tmp_path):
    return PipelineStore(str(tmp_path))


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def test_fresh_pipeline_no_runs_finding(store, state_dir):
    report = inspect_pipeline("alpha", store, state_dir)
    codes = [f.code for f in report.findings]
    assert "NO_RUNS" in codes


def test_success_clears_critical(store, state_dir):
    store.record_failure("alpha", "err")
    store.record_success("alpha", 10.0)
    report = inspect_pipeline("alpha", store, state_dir)
    assert not report.has_critical


def test_repeated_failures_trigger_critical(store, state_dir):
    for _ in range(3):
        store.record_failure("alpha", "err")
    report = inspect_pipeline("alpha", store, state_dir)
    assert report.has_critical


def test_silenced_pipeline_shows_silenced_finding(store, state_dir):
    store.record_success("alpha", 5.0)
    set_silence("alpha", state_dir, hours=1)
    report = inspect_pipeline("alpha", store, state_dir)
    codes = [f.code for f in report.findings]
    assert "SILENCED" in codes


def test_acknowledged_pipeline_shows_acknowledged_finding(store, state_dir):
    store.record_failure("alpha", "oops")
    acknowledge("alpha", state_dir, message="known issue")
    report = inspect_pipeline("alpha", store, state_dir)
    codes = [f.code for f in report.findings]
    assert "ACKNOWLEDGED" in codes


def test_inspect_all_covers_multiple_pipelines(store, state_dir):
    store.record_success("pipe-1", 1.0)
    store.record_failure("pipe-2", "fail")
    reports = inspect_all(store, state_dir)
    names = {r.pipeline for r in reports}
    assert {"pipe-1", "pipe-2"} <= names
