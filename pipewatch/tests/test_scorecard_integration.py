"""Integration tests for the scorecard feature."""
from __future__ import annotations

import datetime
import pytest

from pipewatch.state import PipelineState, PipelineRun
from pipewatch.scorecard import build_scorecard, format_scorecard


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _utc(**kw) -> datetime.datetime:
    return datetime.datetime(2024, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc) + datetime.timedelta(**kw)


def _run(status: str, hours_ago: int = 1, duration: float = 30.0) -> PipelineRun:
    ended = _utc(hours=-hours_ago)
    started = ended - datetime.timedelta(seconds=duration)
    return PipelineRun(
        run_id=f"r-{hours_ago}-{status}",
        status=status,
        started_at=started.isoformat(),
        finished_at=ended.isoformat(),
        message="" if status == "ok" else "boom",
    )


@pytest.fixture()
def store(tmp_path):
    return PipelineState(str(tmp_path))


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_scorecard_has_row_per_pipeline(store, tmp_path):
    store.record_success("alpha")
    store.record_success("beta")
    sc = build_scorecard(["alpha", "beta"], store)
    assert len(sc.rows) == 2


def test_scorecard_rows_sorted_by_name(store):
    store.record_success("zebra")
    store.record_success("apple")
    sc = build_scorecard(["zebra", "apple"], store)
    assert sc.rows[0].name == "apple"
    assert sc.rows[1].name == "zebra"


def test_healthy_pipeline_gets_high_grade(store):
    for _ in range(5):
        store.record_success("good_pipe")
    sc = build_scorecard(["good_pipe"], store)
    assert sc.rows[0].grade in ("A", "B")


def test_failing_pipeline_gets_low_grade(store):
    for _ in range(5):
        store.record_failure("bad_pipe", "err")
    sc = build_scorecard(["bad_pipe"], store)
    assert sc.rows[0].grade in ("D", "F")


def test_format_scorecard_integration(store):
    store.record_success("pipe_a")
    store.record_failure("pipe_b", "oops")
    sc = build_scorecard(["pipe_a", "pipe_b"], store)
    out = format_scorecard(sc)
    assert "pipe_a" in out
    assert "pipe_b" in out
    assert "Average score" in out
