"""Unit tests for pipewatch.sentinel."""
from __future__ import annotations

import pytest

from pipewatch.sentinel import (
    SentinelPolicy,
    SentinelViolation,
    clear_sentinel_policy,
    evaluate_sentinel,
    load_sentinel_policy,
    save_sentinel_policy,
    sentinel_violations,
)
from pipewatch.state import PipelineState


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def _state(failures: int) -> PipelineState:
    s = PipelineState()
    s.consecutive_failures = failures
    return s


def test_load_sentinel_policy_none_for_unknown(state_dir):
    assert load_sentinel_policy(state_dir, "pipe_a") is None


def test_save_and_load_sentinel_policy(state_dir):
    policy = SentinelPolicy(enabled=True, max_failures=2, notify_on_first=False)
    save_sentinel_policy(state_dir, "pipe_a", policy)
    loaded = load_sentinel_policy(state_dir, "pipe_a")
    assert loaded is not None
    assert loaded.max_failures == 2
    assert loaded.notify_on_first is False


def test_clear_sentinel_policy_removes_file(state_dir):
    policy = SentinelPolicy(enabled=True)
    save_sentinel_policy(state_dir, "pipe_a", policy)
    clear_sentinel_policy(state_dir, "pipe_a")
    assert load_sentinel_policy(state_dir, "pipe_a") is None


def test_evaluate_sentinel_no_violation_when_disabled(state_dir):
    policy = SentinelPolicy(enabled=False, max_failures=0)
    result = evaluate_sentinel("pipe_a", _state(5), policy)
    assert result is None


def test_evaluate_sentinel_no_violation_within_threshold():
    policy = SentinelPolicy(enabled=True, max_failures=3)
    result = evaluate_sentinel("pipe_a", _state(3), policy)
    assert result is None


def test_evaluate_sentinel_violation_above_threshold():
    policy = SentinelPolicy(enabled=True, max_failures=0)
    result = evaluate_sentinel("pipe_a", _state(1), policy)
    assert isinstance(result, SentinelViolation)
    assert result.pipeline == "pipe_a"
    assert result.consecutive_failures == 1
    assert result.max_allowed == 0


def test_sentinel_violation_str():
    v = SentinelViolation(pipeline="pipe_a", consecutive_failures=3, max_allowed=1)
    assert "pipe_a" in str(v)
    assert "3" in str(v)
    assert "1" in str(v)


def test_sentinel_violations_skips_missing_policy(state_dir):
    states = {"pipe_a": _state(5)}
    result = sentinel_violations(["pipe_a"], state_dir, states)
    assert result == []


def test_sentinel_violations_returns_violating_pipelines(state_dir):
    save_sentinel_policy(state_dir, "pipe_a", SentinelPolicy(enabled=True, max_failures=0))
    save_sentinel_policy(state_dir, "pipe_b", SentinelPolicy(enabled=True, max_failures=5))
    states = {"pipe_a": _state(2), "pipe_b": _state(2)}
    result = sentinel_violations(["pipe_a", "pipe_b"], state_dir, states)
    assert len(result) == 1
    assert result[0].pipeline == "pipe_a"
