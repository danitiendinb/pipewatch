"""Unit tests for pipewatch.ranker."""
from __future__ import annotations

import pytest

from pipewatch.ranker import RankedPipeline, format_ranked_row, rank_pipelines, top_offenders
from pipewatch.state import PipelineState
from pipewatch.tests.test_summarizer import _make_store, _run


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _store_ok(tmp_path):
    store = PipelineState(str(tmp_path))
    for _ in range(3):
        store.record_success("alpha")
    return store


def _store_failing(tmp_path):
    store = PipelineState(str(tmp_path))
    for _ in range(3):
        store.record_failure("beta", "err")
    return store


# ---------------------------------------------------------------------------
# rank_pipelines
# ---------------------------------------------------------------------------

def test_rank_pipelines_returns_all(tmp_path):
    store = _store_ok(tmp_path)
    store.record_success("gamma")
    result = rank_pipelines(["alpha", "gamma"], store)
    assert len(result) == 2


def test_rank_pipelines_worst_first(tmp_path):
    store = PipelineState(str(tmp_path))
    store.record_success("good")
    for _ in range(5):
        store.record_failure("bad", "err")
    result = rank_pipelines(["good", "bad"], store)
    assert result[0].name == "bad"
    assert result[0].rank == 1


def test_rank_pipelines_rank_field_sequential(tmp_path):
    store = PipelineState(str(tmp_path))
    store.record_success("p1")
    store.record_success("p2")
    store.record_success("p3")
    result = rank_pipelines(["p1", "p2", "p3"], store)
    ranks = [r.rank for r in result]
    assert ranks == [1, 2, 3]


def test_rank_pipelines_empty(tmp_path):
    store = PipelineState(str(tmp_path))
    result = rank_pipelines([], store)
    assert result == []


# ---------------------------------------------------------------------------
# top_offenders
# ---------------------------------------------------------------------------

def test_top_offenders_limits_results(tmp_path):
    store = PipelineState(str(tmp_path))
    names = [f"p{i}" for i in range(10)]
    for n in names:
        store.record_success(n)
    result = top_offenders(names, store, n=3)
    assert len(result) == 3


def test_top_offenders_returns_ranked_pipeline_type(tmp_path):
    store = PipelineState(str(tmp_path))
    store.record_success("only")
    result = top_offenders(["only"], store, n=5)
    assert isinstance(result[0], RankedPipeline)


# ---------------------------------------------------------------------------
# format_ranked_row
# ---------------------------------------------------------------------------

def test_format_ranked_row_contains_name():
    rp = RankedPipeline(name="my_pipe", score=72.5, grade="B", rank=2)
    row = format_ranked_row(rp)
    assert "my_pipe" in row


def test_format_ranked_row_contains_grade():
    rp = RankedPipeline(name="my_pipe", score=45.0, grade="D", rank=7)
    row = format_ranked_row(rp)
    assert "[D]" in row


def test_format_ranked_row_contains_rank():
    rp = RankedPipeline(name="my_pipe", score=90.0, grade="A", rank=1)
    row = format_ranked_row(rp)
    assert "#1" in row
