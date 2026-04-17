"""Tests for pipewatch.tagger."""
import pytest

from pipewatch.tagger import (
    clear_tags,
    get_tag,
    load_tags,
    remove_tag,
    set_tag,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def test_load_tags_empty_for_new_pipeline(state_dir):
    assert load_tags(state_dir, "pipe_a") == {}


def test_set_tag_persists(state_dir):
    result = set_tag(state_dir, "pipe_a", "env", "prod")
    assert result == {"env": "prod"}
    assert load_tags(state_dir, "pipe_a") == {"env": "prod"}


def test_set_tag_multiple_keys(state_dir):
    set_tag(state_dir, "pipe_a", "env", "prod")
    set_tag(state_dir, "pipe_a", "owner", "alice")
    tags = load_tags(state_dir, "pipe_a")
    assert tags["env"] == "prod"
    assert tags["owner"] == "alice"


def test_set_tag_overwrites_existing(state_dir):
    set_tag(state_dir, "pipe_a", "env", "prod")
    set_tag(state_dir, "pipe_a", "env", "staging")
    assert get_tag(state_dir, "pipe_a", "env") == "staging"


def test_get_tag_returns_none_for_missing_key(state_dir):
    assert get_tag(state_dir, "pipe_a", "missing") is None


def test_remove_tag_deletes_key(state_dir):
    set_tag(state_dir, "pipe_a", "env", "prod")
    remove_tag(state_dir, "pipe_a", "env")
    assert get_tag(state_dir, "pipe_a", "env") is None


def test_remove_tag_noop_when_absent(state_dir):
    # Should not raise
    result = remove_tag(state_dir, "pipe_a", "nonexistent")
    assert result == {}


def test_clear_tags_removes_all(state_dir):
    set_tag(state_dir, "pipe_a", "env", "prod")
    set_tag(state_dir, "pipe_a", "owner", "alice")
    clear_tags(state_dir, "pipe_a")
    assert load_tags(state_dir, "pipe_a") == {}


def test_tags_are_isolated_per_pipeline(state_dir):
    set_tag(state_dir, "pipe_a", "env", "prod")
    assert load_tags(state_dir, "pipe_b") == {}
