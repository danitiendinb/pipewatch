"""Tests for pipewatch.limiter."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.limiter import (
    LimiterPolicy,
    check_and_record,
    clear_limiter,
    load_limiter_state,
    save_limiter_policy,
)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_load_limiter_state_none_for_unknown(state_dir: str) -> None:
    assert load_limiter_state(state_dir, "pipe_x") is None


def test_save_and_load_limiter_policy(state_dir: str) -> None:
    policy = LimiterPolicy(max_runs=5, window_hours=12.0)
    save_limiter_policy(state_dir, "pipe_a", policy)
    state = load_limiter_state(state_dir, "pipe_a")
    assert state is not None
    assert state.policy.max_runs == 5
    assert state.policy.window_hours == 12.0


def test_save_preserves_existing_timestamps(state_dir: str) -> None:
    policy = LimiterPolicy(max_runs=3, window_hours=6.0)
    save_limiter_policy(state_dir, "pipe_b", policy)
    # Manually inject a timestamp
    path = Path(state_dir) / "pipe_b.limiter.json"
    data = json.loads(path.read_text())
    data["run_timestamps"] = ["2024-01-01T00:00:00+00:00"]
    path.write_text(json.dumps(data))
    # Re-save policy should not wipe timestamps
    save_limiter_policy(state_dir, "pipe_b", LimiterPolicy(max_runs=3, window_hours=6.0))
    state = load_limiter_state(state_dir, "pipe_b")
    assert len(state.run_timestamps) == 1


def test_check_and_record_allowed_when_no_policy(state_dir: str) -> None:
    result = check_and_record(state_dir, "unknown_pipe")
    assert result.allowed is True


def test_check_and_record_allows_up_to_max(state_dir: str) -> None:
    save_limiter_policy(state_dir, "pipe_c", LimiterPolicy(max_runs=3, window_hours=1.0))
    for _ in range(3):
        r = check_and_record(state_dir, "pipe_c")
        assert r.allowed is True
    r = check_and_record(state_dir, "pipe_c")
    assert r.allowed is False


def test_check_and_record_increments_count(state_dir: str) -> None:
    save_limiter_policy(state_dir, "pipe_d", LimiterPolicy(max_runs=10, window_hours=1.0))
    r1 = check_and_record(state_dir, "pipe_d")
    assert r1.current_count == 1
    r2 = check_and_record(state_dir, "pipe_d")
    assert r2.current_count == 2


def test_old_timestamps_pruned(state_dir: str) -> None:
    save_limiter_policy(state_dir, "pipe_e", LimiterPolicy(max_runs=2, window_hours=1.0))
    # Inject an old timestamp outside the window
    path = Path(state_dir) / "pipe_e.limiter.json"
    data = json.loads(path.read_text())
    old_ts = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    data["run_timestamps"] = [old_ts, old_ts]
    path.write_text(json.dumps(data))
    result = check_and_record(state_dir, "pipe_e")
    assert result.allowed is True


def test_clear_limiter_removes_file(state_dir: str) -> None:
    save_limiter_policy(state_dir, "pipe_f", LimiterPolicy())
    clear_limiter(state_dir, "pipe_f")
    assert load_limiter_state(state_dir, "pipe_f") is None


def test_clear_limiter_noop_when_missing(state_dir: str) -> None:
    clear_limiter(state_dir, "nonexistent")  # should not raise
