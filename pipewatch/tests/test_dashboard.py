"""Unit tests for pipewatch.dashboard"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from pipewatch.dashboard import format_summary_row, render_dashboard, status_icon
from pipewatch.summarizer import HealthReport, PipelineSummary


def _summary(name: str, status: str, failures: int = 0) -> PipelineSummary:
    return PipelineSummary(
        pipeline=name,
        status=status,
        consecutive_failures=failures,
        last_run="2024-01-01T00:00:00+00:00" if status != "unknown" else None,
        total_runs=max(1, failures),
    )


def test_status_icon_ok():
    s = _summary("p", "ok")
    assert "✔" in status_icon(s)


def test_status_icon_failing():
    s = _summary("p", "failing", 3)
    assert "✘" in status_icon(s)


def test_status_icon_unknown():
    s = _summary("p", "unknown")
    assert "?" in status_icon(s)


def test_format_summary_row_no_overdue():
    s = _summary("my-pipeline", "ok")
    row = format_summary_row(s, overdue=False)
    assert "my-pipeline" in row
    assert "OVERDUE" not in row


def test_format_summary_row_overdue():
    s = _summary("my-pipeline", "ok")
    row = format_summary_row(s, overdue=True)
    assert "OVERDUE" in row


def test_render_dashboard_contains_pipeline_name():
    summaries = [_summary("etl-daily", "ok"), _summary("etl-hourly", "failing", 2)]
    report = HealthReport(pipelines=summaries, total=2, ok_count=1, failing_count=1, unknown_count=0)
    now = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
    output = render_dashboard(report, [], now)
    assert "etl-daily" in output
    assert "etl-hourly" in output


def test_render_dashboard_counts():
    summaries = [_summary("a", "ok"), _summary("b", "unknown")]
    report = HealthReport(pipelines=summaries, total=2, ok_count=1, failing_count=0, unknown_count=1)
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    output = render_dashboard(report, [], now)
    assert "OK: 1" in output
    assert "Unknown: 1" in output


def test_render_dashboard_marks_overdue():
    summaries = [_summary("slow-pipe", "ok")]
    report = HealthReport(pipelines=summaries, total=1, ok_count=1, failing_count=0, unknown_count=0)
    now = datetime(2024, 6, 1, tzinfo=timezone.utc)
    output = render_dashboard(report, ["slow-pipe"], now)
    assert "OVERDUE" in output
