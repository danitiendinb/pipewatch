"""Integration tests: dashboard with real state store and config."""
from __future__ import annotations

import pytest

from pipewatch.config import PipewatchConfig, PipelineConfig
from pipewatch.state import PipelineState, start, finish
from pipewatch.dashboard import run_dashboard


@pytest.fixture()
def store(tmp_path):
    return PipelineState(str(tmp_path))


@pytest.fixture()
def config(tmp_path):
    pipelines = [
        PipelineConfig(name="pipe-ok", schedule="@hourly", failure_threshold=3),
        PipelineConfig(name="pipe-fail", schedule="@hourly", failure_threshold=2),
    ]
    return PipewatchConfig(pipelines=pipelines, state_dir=str(tmp_path), log_level="INFO")


def test_dashboard_shows_both_pipelines(store, config):
    run = start("pipe-ok", store)
    finish(run, store, success=True)
    output = run_dashboard(config, store)
    assert "pipe-ok" in output
    assert "pipe-fail" in output


def test_dashboard_reflects_failure(store, config):
    for _ in range(3):
        run = start("pipe-fail", store)
        finish(run, store, success=False, message="boom")
    output = run_dashboard(config, store)
    assert "pipe-fail" in output
    assert "failures=3" in output


def test_dashboard_ok_pipeline_no_overdue_tag(store, config):
    """A recently-succeeded pipeline should not be marked overdue."""
    run = start("pipe-ok", store)
    finish(run, store, success=True)
    output = run_dashboard(config, store)
    # strip ANSI for simpler assertion
    import re
    clean = re.sub(r"\033\[[0-9;]*m", "", output)
    lines = [l for l in clean.splitlines() if "pipe-ok" in l]
    assert lines, "pipe-ok row missing"
    assert "OVERDUE" not in lines[0]
