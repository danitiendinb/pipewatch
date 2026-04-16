"""Tests for pipewatch.summarizer."""
import pytest
from unittest.mock import MagicMock

from pipewatch.state import PipelineState, PipelineRun
from pipewatch.summarizer import (
    summarize_pipeline,
    build_health_report,
    render_health_report,
    HealthReport,
)


def _make_store(runs=None, consecutive_failures=0):
    state = PipelineState(runs=runs or [], consecutive_failures=consecutive_failures)
    store = MagicMock()
    store.load.return_value = state
    return store


def _run(status="success", finished_at="2024-01-01T10:00:00"):
    r = MagicMock(spec=PipelineRun)
    r.status = status
    r.finished_at = finished_at
    return r


def test_summarize_pipeline_unknown_no_runs():
    store = _make_store()
    s = summarize_pipeline("pipe_a", store)
    assert s.status == "unknown"
    assert s.last_run is None


def test_summarize_pipeline_ok():
    store = _make_store(runs=[_run("success")], consecutive_failures=0)
    s = summarize_pipeline("pipe_a", store)
    assert s.status == "ok"
    assert s.last_run == "2024-01-01T10:00:00"


def test_summarize_pipeline_failing():
    store = _make_store(runs=[_run("failure")], consecutive_failures=3)
    s = summarize_pipeline("pipe_a", store)
    assert s.status == "failing"
    assert s.consecutive_failures == 3


def test_build_health_report_counts():
    stores = {
        "ok_pipe": _make_store(runs=[_run("success")]),
        "fail_pipe": _make_store(runs=[_run("failure")], consecutive_failures=1),
        "new_pipe": _make_store(),
    }

    def multi_store_load(name):
        return stores[name].load(name)

    store = MagicMock()
    store.load.side_effect = multi_store_load

    report = build_health_report(["ok_pipe", "fail_pipe", "new_pipe"], store)
    assert report.total == 3
    assert report.ok == 1
    assert report.failing == 1
    assert report.unknown == 1
    assert not report.healthy


def test_build_health_report_all_ok():
    store = _make_store(runs=[_run("success")])
    report = build_health_report(["a", "b"], store)
    assert report.healthy


def test_render_health_report_contains_summary_line():
    store = _make_store(runs=[_run("success")])
    report = build_health_report(["my_pipeline"], store)
    output = render_health_report(report)
    assert "1 pipelines" in output
    assert "my_pipeline" in output
    assert "ok" in output
