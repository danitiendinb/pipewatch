"""Unit tests for pipewatch.linter."""
from __future__ import annotations

import pytest

from pipewatch.config import PipelineConfig, PipewatchConfig, AlertConfig
from pipewatch.linter import (
    LintIssue,
    LintReport,
    lint_config,
    format_lint_report,
    _lint_pipeline,
)


def _cfg(**kwargs) -> PipelineConfig:
    defaults = {"name": "my_pipeline", "schedule": "0 * * * *", "failure_threshold": 3}
    defaults.update(kwargs)
    return PipelineConfig(**defaults)


def _config(*pipelines: PipelineConfig) -> PipewatchConfig:
    return PipewatchConfig(
        pipelines=list(pipelines),
        alert=AlertConfig(webhook_url=None),
        state_dir="/tmp/pw",
        log_level="INFO",
    )


# --- LintReport helpers ---

def test_lint_report_ok_when_no_issues():
    r = LintReport()
    assert r.ok is True


def test_lint_report_not_ok_with_error():
    r = LintReport(issues=[LintIssue(pipeline="p", severity="error", message="bad")])
    assert r.ok is False


def test_lint_report_ok_with_only_warning():
    r = LintReport(issues=[LintIssue(pipeline="p", severity="warning", message="meh")])
    assert r.ok is True


# --- _lint_pipeline ---

def test_no_issues_for_valid_pipeline():
    issues = _lint_pipeline(_cfg())
    assert issues == []


def test_space_in_name_gives_warning():
    issues = _lint_pipeline(_cfg(name="my pipeline"))
    severities = [i.severity for i in issues]
    assert "warning" in severities


def test_bad_cron_gives_error():
    issues = _lint_pipeline(_cfg(schedule="not-a-cron"))
    assert any(i.severity == "error" and "cron" in i.message.lower() for i in issues)


def test_threshold_zero_gives_error():
    issues = _lint_pipeline(_cfg(failure_threshold=0))
    assert any(i.severity == "error" for i in issues)


def test_threshold_high_gives_warning():
    issues = _lint_pipeline(_cfg(failure_threshold=200))
    assert any(i.severity == "warning" for i in issues)


# --- lint_config ---

def test_duplicate_pipeline_name_gives_error():
    p = _cfg(name="dup")
    report = lint_config(_config(p, _cfg(name="dup")))
    assert any("Duplicate" in i.message for i in report.errors)


def test_clean_config_no_issues():
    report = lint_config(_config(_cfg(name="a"), _cfg(name="b")))
    assert report.ok
    assert report.issues == []


# --- format_lint_report ---

def test_format_no_issues():
    assert "No issues" in format_lint_report(LintReport())


def test_format_shows_pipeline_name():
    r = LintReport(issues=[LintIssue(pipeline="myp", severity="error", message="oops")])
    assert "myp" in format_lint_report(r)
    assert "oops" in format_lint_report(r)
