"""Integration: escalation level rises as failures accumulate via state store."""
import pytest
from pipewatch.state import PipelineState
from pipewatch.escalator import EscalationPolicy, evaluate_escalation, load_escalation


@pytest.fixture()
def store(tmp_path):
    return PipelineState(str(tmp_path))


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


_policy = EscalationPolicy(levels=[1, 3, 5])


def test_escalation_rises_with_failures(store, state_dir):
    for i in range(5):
        store.record_failure("pipe", f"run-{i}", "err")
    state = store.load("pipe")
    level = evaluate_escalation(state_dir, "pipe", state.consecutive_failures, _policy)
    assert level == 2


def test_escalation_resets_after_success(store, state_dir):
    for i in range(5):
        store.record_failure("pipe", f"run-{i}", "err")
    store.record_success("pipe", "run-ok")
    state = store.load("pipe")
    level = evaluate_escalation(state_dir, "pipe", state.consecutive_failures, _policy)
    assert level == -1


def test_escalation_level_one_at_three_failures(store, state_dir):
    for i in range(3):
        store.record_failure("pipe", f"r{i}", "bad")
    state = store.load("pipe")
    level = evaluate_escalation(state_dir, "pipe", state.consecutive_failures, _policy)
    assert level == 1
