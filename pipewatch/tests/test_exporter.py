"""Tests for pipewatch.exporter."""
import json
import pytest
from unittest.mock import MagicMock

from pipewatch.exporter import export_json, export_csv, state_to_records
from pipewatch.state import PipelineState, PipelineRun


def _make_run(success: bool, run_id: str = "r1") -> PipelineRun:
    return PipelineRun(
        run_id=run_id,
        started_at="2024-01-01T00:00:00",
        finished_at="2024-01-01T00:01:00",
        success=success,
        duration_seconds=60.0,
        message=None,
    )


def _make_state(runs):
    state = PipelineState(runs=runs, consecutive_failures=0)
    return state


def test_export_json_empty():
    result = export_json({})
    assert json.loads(result) == []


def test_export_json_contains_pipeline_name():
    states = {"etl_daily": _make_state([_make_run(True)])}
    data = json.loads(export_json(states))
    assert data[0]["pipeline"] == "etl_daily"


def test_export_json_status_ok():
    states = {"etl_daily": _make_state([_make_run(True)])}
    data = json.loads(export_json(states))
    assert data[0]["status"] == "ok"


def test_export_json_run_fields():
    states = {"pipe": _make_state([_make_run(False, "abc")])}
    data = json.loads(export_json(states))
    run = data[0]["runs"][0]
    assert run["run_id"] == "abc"
    assert run["success"] is False


def test_export_csv_empty():
    assert export_csv({}) == ""


def test_export_csv_has_header():
    states = {"pipe": _make_state([_make_run(True)])}
    csv_out = export_csv(states)
    assert "pipeline" in csv_out
    assert "run_id" in csv_out


def test_export_csv_row_count():
    states = {
        "a": _make_state([_make_run(True, "r1"), _make_run(False, "r2")]),
        "b": _make_state([_make_run(True, "r3")]),
    }
    lines = [l for l in export_csv(states).strip().splitlines() if l]
    # 1 header + 3 data rows
    assert len(lines) == 4


def test_state_to_records_pipeline_field():
    states = {"mypipe": _make_state([_make_run(True)])}
    records = state_to_records(states)
    assert records[0]["pipeline"] == "mypipe"
