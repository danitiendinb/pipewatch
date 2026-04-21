"""Unit tests for pipewatch.inspector"""
from __future__ import annotations

import pytest
from unittest.mock import patch

from pipewatch.inspector import inspect_pipeline, inspect_all, Finding, InspectionReport
from pipewatch.tests.test_summarizer import _make_store, _run


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _store_with_failures(tmp_path, n: int):
    store = _make_store(tmp_path)
    for _ in range(n):
        store.record_failure("pipe-a", "boom")
    return store


def _store_with_success(tmp_path):
    store = _make_store(tmp_path)
    store.record_success("pipe-a", 30.0)
    return store


# ---------------------------------------------------------------------------
# InspectionReport properties
# ---------------------------------------------------------------------------

def test_has_critical_true():
    r = InspectionReport("x", [Finding("F", "critical", "bad")])
    assert r.has_critical


def test_has_critical_false():
    r = InspectionReport("x", [Finding("F", "info", "ok")])
    assert not r.has_critical


def test_has_warnings_true():
    r = InspectionReport("x", [Finding("W", "warning", "watch out")])
    assert r.has_warnings


# ---------------------------------------------------------------------------
# inspect_pipeline
# ---------------------------------------------------------------------------

def test_inspect_no_runs_returns_no_runs_finding(tmp_path):
    store = _make_store(tmp_path)
    report = inspect_pipeline("pipe-a", store, str(tmp_path))
    codes = [f.code for f in report.findings]
    assert "NO_RUNS" in codes


def test_inspect_consecutive_failures_critical(tmp_path):
    store = _store_with_failures(tmp_path, 3)
    report = inspect_pipeline("pipe-a", store, str(tmp_path))
    assert report.has_critical
    codes = [f.code for f in report.findings]
    assert "CONSECUTIVE_FAILURES" in codes


def test_inspect_ok_pipeline_returns_ok_finding(tmp_path):
    store = _store_with_success(tmp_path)
    report = inspect_pipeline("pipe-a", store, str(tmp_path))
    codes = [f.code for f in report.findings]
    assert "OK" in codes
    assert not report.has_critical


def test_inspect_silenced_adds_finding(tmp_path):
    store = _store_with_success(tmp_path)
    with patch("pipewatch.inspector.is_silenced", return_value=True):
        report = inspect_pipeline("pipe-a", store, str(tmp_path))
    codes = [f.code for f in report.findings]
    assert "SILENCED" in codes


def test_inspect_overdue_adds_warning(tmp_path):
    store = _store_with_success(tmp_path)
    with patch("pipewatch.inspector.is_overdue", return_value=True):
        report = inspect_pipeline("pipe-a", store, str(tmp_path), schedule_minutes=60)
    codes = [f.code for f in report.findings]
    assert "OVERDUE" in codes


# ---------------------------------------------------------------------------
# inspect_all
# ---------------------------------------------------------------------------

def test_inspect_all_returns_one_report_per_pipeline(tmp_path):
    store = _make_store(tmp_path)
    store.record_success("alpha", 10.0)
    store.record_success("beta", 20.0)
    reports = inspect_all(store, str(tmp_path))
    names = {r.pipeline for r in reports}
    assert names == {"alpha", "beta"}
