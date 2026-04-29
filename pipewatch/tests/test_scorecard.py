"""Unit tests for pipewatch.scorecard."""
from __future__ import annotations

import pytest

from pipewatch.scorecard import (
    Scorecard,
    ScorecardRow,
    build_scorecard,
    format_scorecard,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

class _FakeState:
    """Minimal PipelineState stand-in."""

    def __init__(self, runs):
        self._runs = runs

    def load(self, name):
        from pipewatch.state import PipelineState as _PS
        obj = object.__new__(_PS)
        obj.runs = self._runs.get(name, [])
        obj.consecutive_failures = sum(
            1 for r in reversed(obj.runs) if getattr(r, "status", "ok") == "failure"
        )
        return obj


def _row(grade="A", score=95.0, status="ok", cf=0, overdue=False):
    return ScorecardRow(
        name="pipe", grade=grade, score=score, status=status,
        consecutive_failures=cf, overdue=overdue
    )


# ---------------------------------------------------------------------------
# Scorecard dataclass
# ---------------------------------------------------------------------------

def test_average_score_empty():
    sc = Scorecard()
    assert sc.average_score == 0.0


def test_average_score_single_row():
    sc = Scorecard(rows=[_row(score=80.0)])
    assert sc.average_score == 80.0


def test_average_score_multiple_rows():
    sc = Scorecard(rows=[_row(score=100.0), _row(score=60.0)])
    assert sc.average_score == 80.0


def test_passing_excludes_d_and_f():
    sc = Scorecard(rows=[
        _row(grade="A"), _row(grade="B"), _row(grade="C"),
        _row(grade="D"), _row(grade="F"),
    ])
    assert len(sc.passing) == 3


def test_failing_includes_d_and_f():
    sc = Scorecard(rows=[
        _row(grade="A"), _row(grade="D"), _row(grade="F"),
    ])
    assert len(sc.failing) == 2


# ---------------------------------------------------------------------------
# format_scorecard
# ---------------------------------------------------------------------------

def test_format_scorecard_contains_header():
    sc = Scorecard(rows=[_row()])
    out = format_scorecard(sc)
    assert "Pipeline" in out
    assert "Grade" in out


def test_format_scorecard_contains_pipeline_name():
    row = ScorecardRow("my_pipeline", "B", 75.0, "ok", 0, False)
    sc = Scorecard(rows=[row])
    out = format_scorecard(sc)
    assert "my_pipeline" in out


def test_format_scorecard_shows_average():
    sc = Scorecard(rows=[_row(score=90.0)])
    out = format_scorecard(sc)
    assert "90.0" in out


def test_format_scorecard_overdue_flag():
    row = ScorecardRow("pipe", "C", 55.0, "failing", 2, True)
    sc = Scorecard(rows=[row])
    out = format_scorecard(sc)
    assert "YES" in out
