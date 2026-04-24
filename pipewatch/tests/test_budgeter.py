"""Unit tests for pipewatch.budgeter."""
from __future__ import annotations

import pytest

from pipewatch.budgeter import (
    BudgetPolicy,
    BudgetStatus,
    clear_budget_policy,
    evaluate_budget,
    load_budget_policy,
    save_budget_policy,
)
from pipewatch.state import PipelineState
from pipewatch.tests.test_state import store  # reuse fixture


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _state_with(state_dir: str, pipeline: str, statuses: list[str]) -> PipelineState:
    """Build a PipelineState populated with finished runs of given statuses."""
    import datetime
    from pipewatch.state import PipelineRun

    runs = []
    base = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    for i, s in enumerate(statuses):
        r = PipelineRun(
            run_id=f"r{i}",
            pipeline=pipeline,
            started_at=base.isoformat(),
            finished_at=(base + datetime.timedelta(seconds=10)).isoformat(),
            status=s,
            message="",
        )
        runs.append(r)
        base += datetime.timedelta(minutes=1)

    st = PipelineState(pipeline=pipeline, runs=runs)
    return st


# ---------------------------------------------------------------------------
# save / load / clear
# ---------------------------------------------------------------------------

def test_load_budget_policy_none_for_unknown(tmp_path):
    assert load_budget_policy(str(tmp_path), "pipe_x") is None


def test_save_and_load_budget_policy(tmp_path):
    policy = BudgetPolicy(max_failure_rate=0.15, window=30)
    save_budget_policy(str(tmp_path), "my_pipe", policy)
    loaded = load_budget_policy(str(tmp_path), "my_pipe")
    assert loaded is not None
    assert loaded.max_failure_rate == 0.15
    assert loaded.window == 30


def test_clear_budget_policy_removes_file(tmp_path):
    policy = BudgetPolicy(max_failure_rate=0.1)
    save_budget_policy(str(tmp_path), "my_pipe", policy)
    clear_budget_policy(str(tmp_path), "my_pipe")
    assert load_budget_policy(str(tmp_path), "my_pipe") is None


# ---------------------------------------------------------------------------
# evaluate_budget
# ---------------------------------------------------------------------------

def test_evaluate_budget_all_ok(tmp_path):
    state = _state_with(str(tmp_path), "p", ["ok"] * 10)
    policy = BudgetPolicy(max_failure_rate=0.1, window=10)
    status = evaluate_budget(state, policy)
    assert status.burned is False
    assert status.failure_rate == 0.0
    assert status.failures == 0


def test_evaluate_budget_burned(tmp_path):
    statuses = ["error"] * 5 + ["ok"] * 5
    state = _state_with(str(tmp_path), "p", statuses)
    policy = BudgetPolicy(max_failure_rate=0.1, window=10)
    status = evaluate_budget(state, policy)
    assert status.burned is True
    assert status.failure_rate == 0.5


def test_evaluate_budget_empty_state(tmp_path):
    state = PipelineState(pipeline="p", runs=[])
    policy = BudgetPolicy(max_failure_rate=0.1, window=20)
    status = evaluate_budget(state, policy)
    assert status.burned is False
    assert status.total_runs == 0
    assert status.failure_rate == 0.0


def test_evaluate_budget_remaining_failures(tmp_path):
    # 1 failure out of 10 with 10% budget → 1 allowed, 0 remaining
    statuses = ["error"] + ["ok"] * 9
    state = _state_with(str(tmp_path), "p", statuses)
    policy = BudgetPolicy(max_failure_rate=0.1, window=10)
    status = evaluate_budget(state, policy)
    assert status.remaining_failures == 0
    assert status.burned is False  # exactly at boundary, not over


def test_evaluate_budget_window_truncates(tmp_path):
    # 20 runs total; last 10 are all ok → not burned even though first 10 failed
    statuses = ["error"] * 10 + ["ok"] * 10
    state = _state_with(str(tmp_path), "p", statuses)
    policy = BudgetPolicy(max_failure_rate=0.1, window=10)
    status = evaluate_budget(state, policy)
    assert status.total_runs == 10
    assert status.failures == 0
    assert status.burned is False
