"""Tests for pipewatch.digest and pipewatch.cli_digest."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.digest import DigestReport, build_digest, format_digest
from pipewatch.summarizer import PipelineSummary


def _make_store(summaries: list[PipelineSummary]):
    store = MagicMock()
    with patch("pipewatch.digest.build_health_report") as mock_hr:
        report = MagicMock()
        report.summaries = summaries
        mock_hr.return_value = report
        yield store, mock_hr


def _summary(name: str, status: str, last_run: str | None = None, overdue: bool = False) -> PipelineSummary:
    return PipelineSummary(
        pipeline_name=name,
        status=status,
        consecutive_failures=0 if status != "failing" else 2,
        last_run=last_run,
        last_error=None,
        is_overdue=overdue,
    )


def test_build_digest_counts_statuses():
    store = MagicMock()
    summaries = [
        _summary("a", "ok", last_run=datetime.now(tz=timezone.utc).isoformat()),
        _summary("b", "failing", last_run=datetime.now(tz=timezone.utc).isoformat()),
        _summary("c", "unknown"),
    ]
    with patch("pipewatch.digest.build_health_report") as mock_hr:
        report = MagicMock()
        report.summaries = summaries
        mock_hr.return_value = report
        digest = build_digest(store, ["a", "b", "c"], period_days=7)

    assert digest.healthy == 1
    assert digest.failing == 1
    assert digest.unknown == 1
    assert digest.total_pipelines == 3


def test_build_digest_period_days_stored():
    store = MagicMock()
    with patch("pipewatch.digest.build_health_report") as mock_hr:
        mock_hr.return_value = MagicMock(summaries=[])
        digest = build_digest(store, [], period_days=14)
    assert digest.period_days == 14


def test_format_digest_contains_pipeline_name():
    s = _summary("my-pipe", "ok", last_run=datetime.now(tz=timezone.utc).isoformat())
    report = DigestReport(
        generated_at="2024-01-01T00:00:00+00:00",
        period_days=7,
        total_pipelines=1,
        healthy=1,
        failing=0,
        unknown=0,
        summaries=[s],
    )
    text = format_digest(report)
    assert "my-pipe" in text
    assert "OK" in text


def test_format_digest_overdue_tag():
    s = _summary("late-pipe", "failing", last_run=datetime.now(tz=timezone.utc).isoformat(), overdue=True)
    report = DigestReport(
        generated_at="2024-01-01T00:00:00+00:00",
        period_days=7,
        total_pipelines=1,
        healthy=0,
        failing=1,
        unknown=0,
        summaries=[s],
    )
    text = format_digest(report)
    assert "OVERDUE" in text


def test_cmd_digest_missing_config_returns_1(tmp_path):
    from pipewatch.cli_digest import cmd_digest
    args = argparse.Namespace(config=str(tmp_path / "missing.yml"), days=7, pipeline=None)
    assert cmd_digest(args) == 1
