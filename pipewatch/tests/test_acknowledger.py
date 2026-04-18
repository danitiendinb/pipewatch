"""Tests for pipewatch.acknowledger."""

import pytest
from pathlib import Path
from pipewatch.acknowledger import (
    acknowledge,
    clear_acknowledgement,
    is_acknowledged,
    load_acknowledgement,
)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_is_acknowledged_false_for_new_pipeline(state_dir):
    assert is_acknowledged(state_dir, "pipe_a") is False


def test_acknowledge_creates_file(state_dir):
    acknowledge(state_dir, "pipe_a")
    assert is_acknowledged(state_dir, "pipe_a") is True


def test_acknowledge_stores_message(state_dir):
    acknowledge(state_dir, "pipe_a", message="known issue")
    data = load_acknowledgement(state_dir, "pipe_a")
    assert data is not None
    assert data["message"] == "known issue"


def test_acknowledge_stores_timestamp(state_dir):
    acknowledge(state_dir, "pipe_a")
    data = load_acknowledgement(state_dir, "pipe_a")
    assert "acknowledged_at" in data


def test_load_acknowledgement_none_when_not_set(state_dir):
    assert load_acknowledgement(state_dir, "pipe_x") is None


def test_clear_acknowledgement_removes_flag(state_dir):
    acknowledge(state_dir, "pipe_a")
    clear_acknowledgement(state_dir, "pipe_a")
    assert is_acknowledged(state_dir, "pipe_a") is False


def test_clear_acknowledgement_noop_when_not_set(state_dir):
    # Should not raise
    clear_acknowledgement(state_dir, "pipe_missing")


def test_multiple_pipelines_independent(state_dir):
    acknowledge(state_dir, "pipe_a")
    assert is_acknowledged(state_dir, "pipe_a") is True
    assert is_acknowledged(state_dir, "pipe_b") is False
