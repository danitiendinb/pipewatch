"""Integration tests for sentinel + state interaction."""
from __future__ import annotations

import pytest

from pipewatch.sentinel import (
    SentinelPolicy,
    save_sentinel_policy,
    sentinel_violations,
)
from pipewatch.state import PipelineState, load as load_state, record_failure, record_success


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


@pytest.fixture()
def store(state_dir):
    from pipewatch.state import PipelineStore
    return PipelineStore(state_dir)


def test_zero_tolerance_triggered_after_one_failure(state_dir, store):
    save_sentinel_policy(state_dir, "pipe_a", SentinelPolicy(enabled=True, max_failures=0))
    record_failure(store, "pipe_a", "something broke")
    state = load_state(store, "pipe_a")
    violations = sentinel_violations(["pipe_a"], state_dir, {"pipe_a": state})
    assert len(violations) == 1
    assert violations[0].pipeline == "pipe_a"


def test_no_violation_after_success(state_dir, store):
    save_sentinel_policy(state_dir, "pipe_a", SentinelPolicy(enabled=True, max_failures=0))
    record_failure(store, "pipe_a", "oops")
    record_success(store, "pipe_a")
    state = load_state(store, "pipe_a")
    violations = sentinel_violations(["pipe_a"], state_dir, {"pipe_a": state})
    assert violations == []


def test_threshold_of_two_allows_one_failure(state_dir, store):
    save_sentinel_policy(state_dir, "pipe_a", SentinelPolicy(enabled=True, max_failures=2))
    record_failure(store, "pipe_a", "fail 1")
    record_failure(store, "pipe_a", "fail 2")
    state = load_state(store, "pipe_a")
    violations = sentinel_violations(["pipe_a"], state_dir, {"pipe_a": state})
    assert violations == []


def test_threshold_of_two_violated_on_third_failure(state_dir, store):
    save_sentinel_policy(state_dir, "pipe_a", SentinelPolicy(enabled=True, max_failures=2))
    for _ in range(3):
        record_failure(store, "pipe_a", "fail")
    state = load_state(store, "pipe_a")
    violations = sentinel_violations(["pipe_a"], state_dir, {"pipe_a": state})
    assert len(violations) == 1
    assert violations[0].consecutive_failures == 3
