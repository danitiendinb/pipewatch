"""Tests for pipewatch.scorer."""
import pytest
from pipewatch.summarizer import PipelineSummary
from pipewatch.scorer import score_pipeline, score_all, _grade


def _summary(
    pipeline="pipe",
    status="ok",
    total_runs=10,
    success_runs=10,
    consecutive_failures=0,
    is_overdue=False,
    last_run=None,
):
    return PipelineSummary(
        pipeline=pipeline,
        status=status,
        total_runs=total_runs,
        success_runs=success_runs,
        consecutive_failures=consecutive_failures,
        is_overdue=is_overdue,
        last_run=last_run,
    )


def test_grade_boundaries():
    assert _grade(100) == "A"
    assert _grade(90) == "A"
    assert _grade(89) == "B"
    assert _grade(75) == "B"
    assert _grade(74) == "C"
    assert _grade(60) == "C"
    assert _grade(59) == "D"
    assert _grade(40) == "D"
    assert _grade(39) == "F"
    assert _grade(0) == "F"


def test_perfect_score():
    hs = score_pipeline(_summary(total_runs=20, success_runs=20))
    assert hs.score == 100
    assert hs.grade == "A"


def test_unknown_status_gives_50():
    hs = score_pipeline(_summary(status="unknown", total_runs=0, success_runs=0))
    assert hs.score == 50
    assert hs.grade == "C"
    assert "no runs" in hs.reason


def test_overdue_penalty():
    base = score_pipeline(_summary(total_runs=10, success_runs=10, is_overdue=False))
    overdue = score_pipeline(_summary(total_runs=10, success_runs=10, is_overdue=True))
    assert overdue.score == base.score - 15
    assert "overdue" in overdue.reason


def test_consecutive_failure_penalty():
    no_fail = score_pipeline(_summary(total_runs=10, success_runs=10, consecutive_failures=0))
    with_fail = score_pipeline(_summary(total_runs=10, success_runs=10, consecutive_failures=3))
    assert with_fail.score == no_fail.score - 15
    assert "consecutive" in with_fail.reason


def test_consecutive_failure_penalty_capped():
    hs = score_pipeline(_summary(total_runs=10, success_runs=10, consecutive_failures=100))
    assert hs.score >= 0


def test_score_all_returns_one_per_summary():
    summaries = [_summary(pipeline=f"p{i}") for i in range(5)]
    scores = score_all(summaries)
    assert len(scores) == 5
    assert [s.pipeline for s in scores] == [f"p{i}" for i in range(5)]


def test_partial_success():
    hs = score_pipeline(_summary(total_runs=10, success_runs=7))
    assert hs.score == 70
    assert hs.grade == "C"
