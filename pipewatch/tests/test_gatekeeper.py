"""Unit tests for pipewatch.gatekeeper."""
from __future__ import annotations

import pytest

from pipewatch.gatekeeper import (
    GateDecision,
    GatePolicy,
    clear_gate_policy,
    evaluate_gate,
    load_gate_policy,
    save_gate_policy,
)
from pipewatch.state import PipelineState
from pipewatch.tests.test_state import store  # reuse fixture


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def _empty_state(state_dir: str, name: str) -> PipelineState:
    return PipelineState.load(state_dir, name)


# ── policy persistence ────────────────────────────────────────────────────────

def test_load_gate_policy_none_for_unknown(state_dir):
    assert load_gate_policy(state_dir, "pipe") is None


def test_save_and_load_gate_policy(state_dir):
    policy = GatePolicy(min_score=60.0, max_consecutive_failures=3, require_status="ok")
    save_gate_policy(state_dir, "pipe", policy)
    loaded = load_gate_policy(state_dir, "pipe")
    assert loaded is not None
    assert loaded.min_score == 60.0
    assert loaded.max_consecutive_failures == 3
    assert loaded.require_status == "ok"


def test_clear_gate_policy_removes_file(state_dir):
    save_gate_policy(state_dir, "pipe", GatePolicy(min_score=50.0))
    clear_gate_policy(state_dir, "pipe")
    assert load_gate_policy(state_dir, "pipe") is None


# ── evaluate_gate logic ───────────────────────────────────────────────────────

def test_evaluate_gate_allowed_when_no_policy(state_dir):
    state = _empty_state(state_dir, "pipe")
    decision = evaluate_gate(state_dir, "pipe", state)
    assert decision.allowed is True
    assert decision.reasons == []


def test_evaluate_gate_blocked_by_score(state_dir):
    # No runs → score will be low (unknown status)
    save_gate_policy(state_dir, "pipe", GatePolicy(min_score=99.0))
    state = _empty_state(state_dir, "pipe")
    decision = evaluate_gate(state_dir, "pipe", state)
    assert decision.allowed is False
    assert any("score" in r for r in decision.reasons)


def test_evaluate_gate_blocked_by_consecutive_failures(state_dir):
    save_gate_policy(state_dir, "pipe", GatePolicy(max_consecutive_failures=2))
    state = _empty_state(state_dir, "pipe")
    state.consecutive_failures = 3
    decision = evaluate_gate(state_dir, "pipe", state)
    assert decision.allowed is False
    assert any("consecutive" in r for r in decision.reasons)


def test_evaluate_gate_blocked_by_required_status(state_dir):
    save_gate_policy(state_dir, "pipe", GatePolicy(require_status="ok"))
    state = _empty_state(state_dir, "pipe")
    # No runs → status is "unknown", not "ok"
    decision = evaluate_gate(state_dir, "pipe", state)
    assert decision.allowed is False
    assert any("status" in r for r in decision.reasons)


def test_evaluate_gate_multiple_reasons_accumulated(state_dir):
    save_gate_policy(
        state_dir,
        "pipe",
        GatePolicy(min_score=99.0, max_consecutive_failures=1, require_status="ok"),
    )
    state = _empty_state(state_dir, "pipe")
    state.consecutive_failures = 5
    decision = evaluate_gate(state_dir, "pipe", state)
    assert decision.allowed is False
    assert len(decision.reasons) >= 2
