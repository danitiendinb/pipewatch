import pytest
from pipewatch.escalator import (
    EscalationPolicy,
    evaluate_escalation,
    load_escalation,
    clear_escalation,
    save_escalation,
    EscalationState,
)


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


_policy = EscalationPolicy(levels=[1, 3, 10])


def test_load_escalation_default_for_unknown(state_dir):
    esc = load_escalation(state_dir, "pipe_a")
    assert esc.pipeline == "pipe_a"
    assert esc.level == 0
    assert esc.last_escalated_at is None


def test_evaluate_no_failures_returns_minus_one(state_dir):
    level = evaluate_escalation(state_dir, "p", 0, _policy)
    assert level == -1


def test_evaluate_at_first_threshold(state_dir):
    level = evaluate_escalation(state_dir, "p", 1, _policy)
    assert level == 0


def test_evaluate_at_second_threshold(state_dir):
    level = evaluate_escalation(state_dir, "p", 3, _policy)
    assert level == 1


def test_evaluate_at_third_threshold(state_dir):
    level = evaluate_escalation(state_dir, "p", 10, _policy)
    assert level == 2


def test_evaluate_persists_level(state_dir):
    evaluate_escalation(state_dir, "p", 3, _policy)
    esc = load_escalation(state_dir, "p")
    assert esc.level == 1
    assert esc.last_escalated_at is not None


def test_clear_escalation_removes_file(state_dir):
    evaluate_escalation(state_dir, "p", 3, _policy)
    clear_escalation(state_dir, "p")
    esc = load_escalation(state_dir, "p")
    assert esc.level == 0


def test_clear_escalation_noop_when_missing(state_dir):
    clear_escalation(state_dir, "nonexistent")  # should not raise


def test_multiple_pipelines_independent(state_dir):
    evaluate_escalation(state_dir, "a", 10, _policy)
    evaluate_escalation(state_dir, "b", 1, _policy)
    assert load_escalation(state_dir, "a").level == 2
    assert load_escalation(state_dir, "b").level == 0
