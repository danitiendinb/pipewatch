"""Tests for pipewatch.heatmap."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.heatmap import Heatmap, build_heatmap, peak_cell, format_heatmap
from pipewatch.state import PipelineState, PipelineRun


def _run(
    status: str,
    finished_at: str | None = None,
    run_id: str = "r1",
) -> PipelineRun:
    return PipelineRun(
        run_id=run_id,
        started_at="2024-01-01T00:00:00+00:00",
        finished_at=finished_at,
        status=status,
        message="",
    )


# Monday 2024-01-01 at 14:00 UTC  -> weekday=0, hour=14
_MON_14 = "2024-01-01T14:00:00+00:00"
# Wednesday 2024-01-03 at 03:00 UTC -> weekday=2, hour=3
_WED_03 = "2024-01-03T03:00:00+00:00"


def _state_with(runs):
    s = PipelineState(pipeline="pipe1")
    s.runs = runs
    return s


class TestBuildHeatmap:
    def test_no_runs_gives_zero_matrix(self):
        hm = build_heatmap("pipe1", _state_with([]))
        assert hm.total_runs == 0
        assert hm.total_failures == 0
        assert all(hm.failures[wd][hr] == 0 for wd in range(7) for hr in range(24))

    def test_success_run_not_counted(self):
        state = _state_with([_run("success", _MON_14)])
        hm = build_heatmap("pipe1", state)
        assert hm.total_failures == 0

    def test_failure_increments_correct_cell(self):
        state = _state_with([_run("failure", _MON_14)])
        hm = build_heatmap("pipe1", state)
        assert hm.failures[0][14] == 1
        assert hm.total_failures == 1

    def test_multiple_failures_accumulate(self):
        runs = [
            _run("failure", _MON_14, run_id="r1"),
            _run("failure", _MON_14, run_id="r2"),
            _run("failure", _WED_03, run_id="r3"),
        ]
        hm = build_heatmap("pipe1", _state_with(runs))
        assert hm.failures[0][14] == 2
        assert hm.failures[2][3] == 1
        assert hm.total_failures == 3

    def test_total_runs_includes_successes(self):
        runs = [_run("success", _MON_14, "r1"), _run("failure", _WED_03, "r2")]
        hm = build_heatmap("pipe1", _state_with(runs))
        assert hm.total_runs == 2

    def test_run_without_finished_at_skipped(self):
        state = _state_with([_run("failure", None)])
        hm = build_heatmap("pipe1", state)
        assert hm.total_failures == 0


class TestPeakCell:
    def test_returns_none_when_no_failures(self):
        hm = Heatmap(pipeline="p")
        assert peak_cell(hm) is None

    def test_returns_cell_with_highest_count(self):
        state = _state_with([
            _run("failure", _MON_14, "r1"),
            _run("failure", _MON_14, "r2"),
            _run("failure", _WED_03, "r3"),
        ])
        hm = build_heatmap("pipe1", state)
        assert peak_cell(hm) == (0, 14)


class TestFormatHeatmap:
    def test_no_failures_message(self):
        hm = Heatmap(pipeline="mypipe")
        out = format_heatmap(hm)
        assert "no failures" in out

    def test_contains_pipeline_name(self):
        state = _state_with([_run("failure", _MON_14)])
        hm = build_heatmap("pipe1", state)
        out = format_heatmap(hm)
        assert "pipe1" in out

    def test_contains_day_labels(self):
        state = _state_with([_run("failure", _MON_14)])
        hm = build_heatmap("pipe1", state)
        out = format_heatmap(hm)
        assert "Mon" in out
        assert "Sun" in out
