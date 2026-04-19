"""Tests for pipewatch.quota."""
import pytest
from unittest.mock import patch
from pipewatch.quota import (
    load_quota,
    increment_quota,
    is_over_quota,
    reset_quota,
    quota_status,
)

TODAY = "2024-06-01"


@pytest.fixture
def state_dir(tmp_path):
    return str(tmp_path)


def _patch_today(val=TODAY):
    return patch("pipewatch.quota._today_utc", return_value=val)


def test_load_quota_defaults_for_new_pipeline(state_dir):
    with _patch_today():
        data = load_quota(state_dir, "pipe_a")
    assert data["count"] == 0
    assert data["date"] == TODAY


def test_load_quota_resets_on_new_day(state_dir):
    with _patch_today("2024-05-31"):
        increment_quota(state_dir, "pipe_a")
    with _patch_today(TODAY):
        data = load_quota(state_dir, "pipe_a")
    assert data["count"] == 0


def test_increment_quota_returns_new_count(state_dir):
    with _patch_today():
        c1 = increment_quota(state_dir, "pipe_a")
        c2 = increment_quota(state_dir, "pipe_a")
    assert c1 == 1
    assert c2 == 2


def test_is_over_quota_false_below_limit(state_dir):
    with _patch_today():
        increment_quota(state_dir, "pipe_a")
        assert not is_over_quota(state_dir, "pipe_a", max_runs=5)


def test_is_over_quota_true_at_limit(state_dir):
    with _patch_today():
        for _ in range(3):
            increment_quota(state_dir, "pipe_a")
        assert is_over_quota(state_dir, "pipe_a", max_runs=3)


def test_reset_quota_clears_count(state_dir):
    with _patch_today():
        increment_quota(state_dir, "pipe_a")
        reset_quota(state_dir, "pipe_a")
        data = load_quota(state_dir, "pipe_a")
    assert data["count"] == 0


def test_quota_status_fields(state_dir):
    with _patch_today():
        increment_quota(state_dir, "pipe_a")
        status = quota_status(state_dir, "pipe_a", max_runs=10)
    assert status["pipeline"] == "pipe_a"
    assert status["count"] == 1
    assert status["max_runs"] == 10
    assert status["over_quota"] is False
    assert status["date"] == TODAY


def test_multiple_pipelines_independent(state_dir):
    with _patch_today():
        increment_quota(state_dir, "pipe_a")
        increment_quota(state_dir, "pipe_a")
        increment_quota(state_dir, "pipe_b")
        assert load_quota(state_dir, "pipe_a")["count"] == 2
        assert load_quota(state_dir, "pipe_b")["count"] == 1
