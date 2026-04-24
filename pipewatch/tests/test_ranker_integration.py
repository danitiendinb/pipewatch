"""Integration tests for ranker using real PipelineState."""
from __future__ import annotations

import pytest

from pipewatch.ranker import rank_pipelines, top_offenders
from pipewatch.state import PipelineState


@pytest.fixture()
def store(tmp_path):
    s = PipelineState(str(tmp_path))
    # healthy pipeline
    for _ in range(5):
        s.record_success("healthy")
    # flaky pipeline
    s.record_success("flaky")
    s.record_failure("flaky", "timeout")
    s.record_failure("flaky", "timeout")
    # dead pipeline
    for _ in range(5):
        s.record_failure("dead", "connection refused")
    return s


def test_dead_pipeline_ranked_first(store):
    ranked = rank_pipelines(["healthy", "flaky", "dead"], store)
    assert ranked[0].name == "dead"


def test_healthy_pipeline_ranked_last(store):
    ranked = rank_pipelines(["healthy", "flaky", "dead"], store)
    assert ranked[-1].name == "healthy"


def test_scores_ascending(store):
    ranked = rank_pipelines(["healthy", "flaky", "dead"], store)
    scores = [r.score for r in ranked]
    assert scores == sorted(scores)


def test_top_offenders_count(store):
    result = top_offenders(["healthy", "flaky", "dead"], store, n=2)
    assert len(result) == 2


def test_top_offenders_worst_two(store):
    result = top_offenders(["healthy", "flaky", "dead"], store, n=2)
    names = {r.name for r in result}
    assert "dead" in names
    assert "healthy" not in names
