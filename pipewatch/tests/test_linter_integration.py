"""Integration tests for the linter against real PipewatchConfig objects."""
from __future__ import annotations

import textwrap
import tempfile
import os

import pytest

from pipewatch.config import load_config
from pipewatch.linter import lint_config


def _write_config(content: str) -> str:
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False)
    tmp.write(textwrap.dedent(content))
    tmp.flush()
    tmp.close()
    return tmp.name


@pytest.fixture(autouse=True)
def _cleanup(request, tmp_path):
    yield


def test_valid_config_produces_no_issues():
    path = _write_config("""
        state_dir: /tmp/pw_lint_test
        log_level: INFO
        pipelines:
          - name: etl_load
            schedule: "0 6 * * *"
            failure_threshold: 3
        alert:
          webhook_url: null
    """)
    try:
        config = load_config(path)
        assert config is not None
        report = lint_config(config)
        assert report.ok
        assert report.issues == []
    finally:
        os.unlink(path)


def test_duplicate_names_detected():
    path = _write_config("""
        state_dir: /tmp/pw_lint_test
        log_level: INFO
        pipelines:
          - name: same
            schedule: "0 6 * * *"
            failure_threshold: 2
          - name: same
            schedule: "0 7 * * *"
            failure_threshold: 2
        alert:
          webhook_url: null
    """)
    try:
        config = load_config(path)
        assert config is not None
        report = lint_config(config)
        assert not report.ok
        assert any("Duplicate" in i.message for i in report.errors)
    finally:
        os.unlink(path)


def test_bad_cron_detected_via_full_config():
    path = _write_config("""
        state_dir: /tmp/pw_lint_test
        log_level: INFO
        pipelines:
          - name: broken_cron
            schedule: "every hour"
            failure_threshold: 1
        alert:
          webhook_url: null
    """)
    try:
        config = load_config(path)
        assert config is not None
        report = lint_config(config)
        assert not report.ok
    finally:
        os.unlink(path)
