"""Tests for pipewatch.labeler."""
import pytest
from pipewatch.labeler import (
    clear_label,
    load_label,
    pipelines_by_label,
    set_label,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def test_load_label_default_for_new_pipeline(state_dir):
    assert load_label(state_dir, "pipe_a") == "default"


def test_set_label_persists(state_dir):
    set_label(state_dir, "pipe_a", "prod")
    assert load_label(state_dir, "pipe_a") == "prod"


def test_set_label_overwrites_existing(state_dir):
    set_label(state_dir, "pipe_a", "staging")
    set_label(state_dir, "pipe_a", "prod")
    assert load_label(state_dir, "pipe_a") == "prod"


def test_clear_label_reverts_to_default(state_dir):
    set_label(state_dir, "pipe_a", "prod")
    clear_label(state_dir, "pipe_a")
    assert load_label(state_dir, "pipe_a") == "default"


def test_clear_label_noop_when_not_set(state_dir):
    clear_label(state_dir, "pipe_a")  # should not raise


def test_pipelines_by_label_filters_correctly(state_dir):
    set_label(state_dir, "pipe_a", "prod")
    set_label(state_dir, "pipe_b", "staging")
    set_label(state_dir, "pipe_c", "prod")
    result = pipelines_by_label(state_dir, ["pipe_a", "pipe_b", "pipe_c"], "prod")
    assert sorted(result) == ["pipe_a", "pipe_c"]


def test_pipelines_by_label_includes_default(state_dir):
    # pipe_x has no label set → default
    result = pipelines_by_label(state_dir, ["pipe_x"], "default")
    assert result == ["pipe_x"]


def test_pipelines_by_label_empty_when_no_match(state_dir):
    set_label(state_dir, "pipe_a", "staging")
    result = pipelines_by_label(state_dir, ["pipe_a"], "prod")
    assert result == []
