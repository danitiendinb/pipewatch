"""Tests for pipewatch.auditor."""
import pytest
from pathlib import Path
from pipewatch.auditor import record_event, load_audit, clear_audit


@pytest.fixture
def state_dir(tmp_path):
    return str(tmp_path)


def test_load_audit_empty_when_no_file(state_dir):
    assert load_audit(state_dir) == []


def test_record_event_returns_entry(state_dir):
    entry = record_event(state_dir, "check_run", pipeline="pipe_a")
    assert entry["event"] == "check_run"
    assert entry["pipeline"] == "pipe_a"
    assert "ts" in entry


def test_record_event_persists(state_dir):
    record_event(state_dir, "check_run", pipeline="pipe_a")
    entries = load_audit(state_dir)
    assert len(entries) == 1
    assert entries[0]["event"] == "check_run"


def test_record_multiple_events_appends(state_dir):
    record_event(state_dir, "check_run", pipeline="pipe_a")
    record_event(state_dir, "alert_sent", pipeline="pipe_a", detail="webhook")
    entries = load_audit(state_dir)
    assert len(entries) == 2


def test_load_audit_filters_by_pipeline(state_dir):
    record_event(state_dir, "check_run", pipeline="pipe_a")
    record_event(state_dir, "check_run", pipeline="pipe_b")
    entries = load_audit(state_dir, pipeline="pipe_a")
    assert len(entries) == 1
    assert entries[0]["pipeline"] == "pipe_a"


def test_record_event_without_pipeline(state_dir):
    entry = record_event(state_dir, "config_reload", detail="manual")
    assert "pipeline" not in entry
    assert entry["detail"] == "manual"
    entries = load_audit(state_dir)
    assert len(entries) == 1


def test_clear_audit_removes_file(state_dir):
    record_event(state_dir, "check_run", pipeline="pipe_a")
    clear_audit(state_dir)
    assert load_audit(state_dir) == []


def test_clear_audit_noop_when_no_file(state_dir):
    clear_audit(state_dir)  # should not raise


def test_audit_file_created_in_state_dir(state_dir):
    record_event(state_dir, "check_run")
    audit_file = Path(state_dir) / "audit.jsonl"
    assert audit_file.exists()
