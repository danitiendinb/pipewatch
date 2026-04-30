"""Unit tests for pipewatch.expirer."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.expirer import (
    load_expiry,
    save_expiry,
    clear_expiry,
    is_expired,
    expired_pipelines,
)
from pipewatch.state import PipelineState


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def _utc(**kwargs: int) -> datetime:
    return datetime(2024, 6, 1, tzinfo=timezone.utc) + timedelta(**kwargs)


def test_load_expiry_none_for_unknown(state_dir: str) -> None:
    assert load_expiry(state_dir, "pipe") is None


def test_save_and_load_expiry(state_dir: str) -> None:
    rec = save_expiry(state_dir, "pipe", ttl_hours=12.0)
    loaded = load_expiry(state_dir, "pipe")
    assert loaded is not None
    assert loaded.pipeline == "pipe"
    assert loaded.ttl_hours == 12.0


def test_clear_expiry_removes_file(state_dir: str) -> None:
    save_expiry(state_dir, "pipe", ttl_hours=6.0)
    clear_expiry(state_dir, "pipe")
    assert load_expiry(state_dir, "pipe") is None


def test_clear_expiry_noop_when_missing(state_dir: str) -> None:
    clear_expiry(state_dir, "pipe")  # should not raise


def test_is_expired_false_when_no_policy(state_dir: str) -> None:
    store = PipelineState(state_dir)
    assert is_expired(state_dir, "pipe", store) is False


def test_is_expired_true_when_no_successful_runs(state_dir: str) -> None:
    now = _utc()
    # policy set 2 hours ago, TTL = 1 hour → already past expiry
    with patch("pipewatch.expirer._now", return_value=now - timedelta(hours=2)):
        save_expiry(state_dir, "pipe", ttl_hours=1.0)
    store = PipelineState(state_dir)
    with patch("pipewatch.expirer._now", return_value=now):
        result = is_expired(state_dir, "pipe", store)
    assert result is True


def test_expired_pipelines_returns_subset(state_dir: str) -> None:
    now = _utc()
    with patch("pipewatch.expirer._now", return_value=now - timedelta(hours=2)):
        save_expiry(state_dir, "bad_pipe", ttl_hours=1.0)
    store = PipelineState(state_dir)
    with patch("pipewatch.expirer._now", return_value=now):
        result = expired_pipelines(state_dir, ["bad_pipe", "ok_pipe"], store)
    assert result == ["bad_pipe"]
