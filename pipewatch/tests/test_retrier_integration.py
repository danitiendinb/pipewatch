"""Integration tests: retrier interacts with state store."""
import pytest
from pipewatch.state import PipelineState
from pipewatch.retrier import (
    RetryPolicy, increment_retry, load_retry,
    clear_retry, should_retry,
)


@pytest.fixture
def store(tmp_path):
    return PipelineState(str(tmp_path))


@pytest.fixture
def state_dir(tmp_path):
    return str(tmp_path)


def test_retry_cycle_exhausts_and_stops(state_dir):
    policy = RetryPolicy(max_retries=2)
    ts = "2024-07-01T12:00:00"
    r1 = increment_retry(state_dir, "etl", ts)
    assert should_retry(r1, policy) is True
    r2 = increment_retry(state_dir, "etl", ts)
    assert should_retry(r2, policy) is False


def test_clear_retry_resets_cycle(state_dir):
    policy = RetryPolicy(max_retries=1)
    ts = "2024-07-01T12:00:00"
    increment_retry(state_dir, "etl", ts)
    clear_retry(state_dir, "etl")
    rec = load_retry(state_dir, "etl")
    assert should_retry(rec, policy) is True


def test_multiple_pipelines_independent(state_dir):
    ts = "2024-07-01T12:00:00"
    increment_retry(state_dir, "pipe_a", ts)
    increment_retry(state_dir, "pipe_a", ts)
    increment_retry(state_dir, "pipe_b", ts)
    ra = load_retry(state_dir, "pipe_a")
    rb = load_retry(state_dir, "pipe_b")
    assert ra.attempt == 2
    assert rb.attempt == 1
