"""Tests for pipewatch.bookmarker."""
from __future__ import annotations

import pytest

from pipewatch.bookmarker import (
    Bookmark,
    all_bookmarks,
    clear_bookmark,
    load_bookmark,
    set_bookmark,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


# ---------------------------------------------------------------------------
# load_bookmark
# ---------------------------------------------------------------------------

def test_load_bookmark_none_for_unknown(state_dir):
    assert load_bookmark(state_dir, "pipe_a") is None


# ---------------------------------------------------------------------------
# set_bookmark
# ---------------------------------------------------------------------------

def test_set_bookmark_returns_bookmark(state_dir):
    bm = set_bookmark(state_dir, "pipe_a", "2024-01-01T00:00:00")
    assert isinstance(bm, Bookmark)
    assert bm.pipeline == "pipe_a"
    assert bm.value == "2024-01-01T00:00:00"


def test_set_bookmark_persists(state_dir):
    set_bookmark(state_dir, "pipe_a", "offset=42")
    bm = load_bookmark(state_dir, "pipe_a")
    assert bm is not None
    assert bm.value == "offset=42"


def test_set_bookmark_overwrites_existing(state_dir):
    set_bookmark(state_dir, "pipe_a", "first")
    set_bookmark(state_dir, "pipe_a", "second")
    bm = load_bookmark(state_dir, "pipe_a")
    assert bm is not None
    assert bm.value == "second"


def test_set_bookmark_stores_updated_at(state_dir):
    bm = set_bookmark(state_dir, "pipe_a", "v1")
    assert bm.updated_at  # non-empty ISO string
    assert "T" in bm.updated_at


# ---------------------------------------------------------------------------
# clear_bookmark
# ---------------------------------------------------------------------------

def test_clear_bookmark_removes_file(state_dir):
    set_bookmark(state_dir, "pipe_a", "v1")
    clear_bookmark(state_dir, "pipe_a")
    assert load_bookmark(state_dir, "pipe_a") is None


def test_clear_bookmark_noop_when_absent(state_dir):
    # Should not raise
    clear_bookmark(state_dir, "nonexistent")


# ---------------------------------------------------------------------------
# all_bookmarks
# ---------------------------------------------------------------------------

def test_all_bookmarks_empty_when_none(state_dir):
    assert all_bookmarks(state_dir) == []


def test_all_bookmarks_returns_all(state_dir):
    set_bookmark(state_dir, "pipe_b", "100")
    set_bookmark(state_dir, "pipe_a", "200")
    bms = all_bookmarks(state_dir)
    assert len(bms) == 2
    # sorted by pipeline name
    assert bms[0].pipeline == "pipe_a"
    assert bms[1].pipeline == "pipe_b"
