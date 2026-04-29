"""Tests for pipewatch.tagfilter."""
from __future__ import annotations

import pytest

from pipewatch.tagfilter import (
    TagFilterResult,
    _tags_match,
    filter_by_tags,
    format_filter_row,
    matching_pipelines,
)
from pipewatch.tagger import set_tag


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


# ---------------------------------------------------------------------------
# _tags_match
# ---------------------------------------------------------------------------

def test_tags_match_empty_criteria_always_true():
    assert _tags_match({"env": "prod"}, {}) is True


def test_tags_match_single_criterion_hit():
    assert _tags_match({"env": "prod", "team": "data"}, {"env": "prod"}) is True


def test_tags_match_single_criterion_miss():
    assert _tags_match({"env": "staging"}, {"env": "prod"}) is False


def test_tags_match_multiple_criteria_all_must_match():
    tags = {"env": "prod", "team": "data"}
    assert _tags_match(tags, {"env": "prod", "team": "data"}) is True
    assert _tags_match(tags, {"env": "prod", "team": "infra"}) is False


# ---------------------------------------------------------------------------
# filter_by_tags
# ---------------------------------------------------------------------------

def test_filter_by_tags_returns_result_for_each_pipeline(state_dir):
    pipelines = ["alpha", "beta"]
    results = filter_by_tags(pipelines, state_dir, {})
    assert len(results) == 2
    assert {r.pipeline for r in results} == {"alpha", "beta"}


def test_filter_by_tags_matched_true_when_tag_present(state_dir):
    set_tag(state_dir, "alpha", "env", "prod")
    results = filter_by_tags(["alpha"], state_dir, {"env": "prod"})
    assert results[0].matched is True


def test_filter_by_tags_matched_false_when_tag_missing(state_dir):
    results = filter_by_tags(["beta"], state_dir, {"env": "prod"})
    assert results[0].matched is False


def test_filter_by_tags_includes_all_tags_in_result(state_dir):
    set_tag(state_dir, "gamma", "team", "data")
    set_tag(state_dir, "gamma", "env", "staging")
    results = filter_by_tags(["gamma"], state_dir, {})
    assert results[0].tags == {"team": "data", "env": "staging"}


# ---------------------------------------------------------------------------
# matching_pipelines
# ---------------------------------------------------------------------------

def test_matching_pipelines_returns_only_matched_names(state_dir):
    set_tag(state_dir, "pipe_a", "env", "prod")
    names = matching_pipelines(["pipe_a", "pipe_b"], state_dir, {"env": "prod"})
    assert names == ["pipe_a"]


def test_matching_pipelines_empty_when_none_match(state_dir):
    names = matching_pipelines(["pipe_x"], state_dir, {"env": "prod"})
    assert names == []


# ---------------------------------------------------------------------------
# format_filter_row
# ---------------------------------------------------------------------------

def test_format_filter_row_shows_checkmark_when_matched():
    r = TagFilterResult(pipeline="my_pipe", tags={"env": "prod"}, matched=True)
    assert "✓" in format_filter_row(r)


def test_format_filter_row_shows_space_when_not_matched():
    r = TagFilterResult(pipeline="my_pipe", tags={}, matched=False)
    row = format_filter_row(r)
    assert "✓" not in row


def test_format_filter_row_shows_none_when_no_tags():
    r = TagFilterResult(pipeline="my_pipe", tags={}, matched=False)
    assert "(none)" in format_filter_row(r)
