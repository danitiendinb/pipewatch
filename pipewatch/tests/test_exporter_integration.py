"""Integration-style tests: exporter round-trips through StateStore."""
import json
from pathlib import Path

import pytest

from pipewatch.state import StateStore, start, finish
from pipewatch.exporter import export_json, export_csv


@pytest.fixture()
def populated_store(tmp_path):
    store = StateStore(str(tmp_path))
    run = start("pipe_a")
    finish(run, success=True)
    store.record("pipe_a", run)

    run2 = start("pipe_a")
    finish(run2, success=False, message="timeout")
    store.record("pipe_a", run2)

    run3 = start("pipe_b")
    finish(run3, success=True)
    store.record("pipe_b", run3)
    return store


def test_json_round_trip_pipeline_names(populated_store):
    states = {
        "pipe_a": populated_store.load("pipe_a"),
        "pipe_b": populated_store.load("pipe_b"),
    }
    data = json.loads(export_json(states))
    names = {d["pipeline"] for d in data}
    assert names == {"pipe_a", "pipe_b"}


def test_json_run_count(populated_store):
    states = {"pipe_a": populated_store.load("pipe_a")}
    data = json.loads(export_json(states))
    assert len(data[0]["runs"]) == 2


def test_csv_row_count(populated_store):
    states = {
        "pipe_a": populated_store.load("pipe_a"),
        "pipe_b": populated_store.load("pipe_b"),
    }
    lines = [l for l in export_csv(states).strip().splitlines() if l]
    assert len(lines) == 4  # header + 3 runs


def test_csv_contains_failure_message(populated_store):
    states = {"pipe_a": populated_store.load("pipe_a")}
    csv_out = export_csv(states)
    assert "timeout" in csv_out
