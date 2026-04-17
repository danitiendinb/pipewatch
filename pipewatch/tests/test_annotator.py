"""Tests for pipewatch.annotator."""
import pytest
from pipewatch.annotator import (
    annotated_runs,
    get_note,
    load_notes,
    remove_note,
    set_note,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def test_load_notes_empty_for_new_pipeline(state_dir):
    assert load_notes(state_dir, "pipe_a") == {}


def test_set_note_persists(state_dir):
    set_note(state_dir, "pipe_a", "run-1", "looks suspicious")
    assert get_note(state_dir, "pipe_a", "run-1") == "looks suspicious"


def test_set_note_multiple_runs(state_dir):
    set_note(state_dir, "pipe_a", "run-1", "note one")
    set_note(state_dir, "pipe_a", "run-2", "note two")
    notes = load_notes(state_dir, "pipe_a")
    assert len(notes) == 2
    assert notes["run-2"] == "note two"


def test_set_note_overwrites_existing(state_dir):
    set_note(state_dir, "pipe_a", "run-1", "old")
    set_note(state_dir, "pipe_a", "run-1", "new")
    assert get_note(state_dir, "pipe_a", "run-1") == "new"


def test_get_note_returns_none_for_missing(state_dir):
    assert get_note(state_dir, "pipe_a", "nonexistent") is None


def test_remove_note_returns_true_when_exists(state_dir):
    set_note(state_dir, "pipe_a", "run-1", "hi")
    assert remove_note(state_dir, "pipe_a", "run-1") is True
    assert get_note(state_dir, "pipe_a", "run-1") is None


def test_remove_note_returns_false_when_missing(state_dir):
    assert remove_note(state_dir, "pipe_a", "ghost") is False


def test_annotated_runs_alias(state_dir):
    set_note(state_dir, "pipe_b", "r1", "x")
    assert annotated_runs(state_dir, "pipe_b") == {"r1": "x"}


def test_notes_isolated_per_pipeline(state_dir):
    set_note(state_dir, "pipe_a", "run-1", "alpha")
    set_note(state_dir, "pipe_b", "run-1", "beta")
    assert get_note(state_dir, "pipe_a", "run-1") == "alpha"
    assert get_note(state_dir, "pipe_b", "run-1") == "beta"
