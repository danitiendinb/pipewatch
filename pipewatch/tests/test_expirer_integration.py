"""Integration tests for pipewatch.expirer – uses real PipelineState on disk."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.expirer import save_expiry, is_expired, expired_pipelines
from pipewatch.state import PipelineState


def _utc(**kwargs: int) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(**kwargs)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


@pytest.fixture()
def store(state_dir: str) -> PipelineState:
    return PipelineState(state_dir)


def test_pipeline_not_expired_after_recent_success(state_dir: str, store: PipelineState) -> None:
    now = _utc()
    # record a successful run 30 minutes ago
    with patch("pipewatch.state.now_iso", return_value=_utc(minutes=-30).isoformat()):
        store.start("pipe", "run-1")
        store.finish("pipe", "run-1", status="ok", message="")
    # policy TTL = 2 hours set now
    with patch("pipewatch.expirer._now", return_value=now):
        save_expiry(state_dir, "pipe", ttl_hours=2.0)
    # check 1 hour later – still within TTL
    with patch("pipewatch.expirer._now", return_value=_utc(hours=1)):
        assert is_expired(state_dir, "pipe", store) is False


def test_pipeline_expired_after_ttl_with_no_runs(state_dir: str, store: PipelineState) -> None:
    now = _utc()
    with patch("pipewatch.expirer._now", return_value=now - timedelta(hours=3)):
        save_expiry(state_dir, "pipe", ttl_hours=1.0)
    with patch("pipewatch.expirer._now", return_value=now):
        assert is_expired(state_dir, "pipe", store) is True


def test_expired_pipelines_multiple(state_dir: str, store: PipelineState) -> None:
    now = _utc()
    with patch("pipewatch.expirer._now", return_value=now - timedelta(hours=5)):
        save_expiry(state_dir, "stale", ttl_hours=2.0)
    with patch("pipewatch.expirer._now", return_value=now):
        save_expiry(state_dir, "fresh", ttl_hours=48.0)
    with patch("pipewatch.expirer._now", return_value=now):
        result = expired_pipelines(state_dir, ["stale", "fresh"], store)
    assert "stale" in result
    assert "fresh" not in result
