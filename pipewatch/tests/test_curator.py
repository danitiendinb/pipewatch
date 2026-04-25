"""Tests for pipewatch.curator."""
import pytest
from pathlib import Path

from pipewatch.curator import (
    add_to_watchlist,
    remove_from_watchlist,
    get_entry,
    load_curated,
    pipelines_by_tier,
    tier_label,
)


@pytest.fixture()
def state_dir(tmp_path: Path) -> str:
    return str(tmp_path)


def test_load_curated_empty_for_new_dir(state_dir):
    assert load_curated(state_dir) == {}


def test_add_to_watchlist_returns_entry(state_dir):
    entry = add_to_watchlist(state_dir, "pipeline_a", tier=1, reason="mission critical")
    assert entry.pipeline == "pipeline_a"
    assert entry.tier == 1
    assert entry.reason == "mission critical"
    assert entry.added_at != ""


def test_add_to_watchlist_persists(state_dir):
    add_to_watchlist(state_dir, "pipeline_b", tier=2)
    entries = load_curated(state_dir)
    assert "pipeline_b" in entries
    assert entries["pipeline_b"].tier == 2


def test_add_to_watchlist_overwrites_existing(state_dir):
    add_to_watchlist(state_dir, "pipeline_c", tier=3)
    add_to_watchlist(state_dir, "pipeline_c", tier=1, reason="upgraded")
    entry = get_entry(state_dir, "pipeline_c")
    assert entry.tier == 1
    assert entry.reason == "upgraded"


def test_add_to_watchlist_invalid_tier_raises(state_dir):
    with pytest.raises(ValueError):
        add_to_watchlist(state_dir, "pipeline_x", tier=99)


def test_remove_from_watchlist_returns_true(state_dir):
    add_to_watchlist(state_dir, "pipeline_d", tier=2)
    result = remove_from_watchlist(state_dir, "pipeline_d")
    assert result is True
    assert get_entry(state_dir, "pipeline_d") is None


def test_remove_from_watchlist_missing_returns_false(state_dir):
    result = remove_from_watchlist(state_dir, "nonexistent")
    assert result is False


def test_get_entry_none_for_unknown(state_dir):
    assert get_entry(state_dir, "ghost") is None


def test_pipelines_by_tier_filters_correctly(state_dir):
    add_to_watchlist(state_dir, "p1", tier=1)
    add_to_watchlist(state_dir, "p2", tier=2)
    add_to_watchlist(state_dir, "p3", tier=1)
    critical = pipelines_by_tier(state_dir, 1)
    assert set(critical) == {"p1", "p3"}
    important = pipelines_by_tier(state_dir, 2)
    assert important == ["p2"]


def test_pipelines_by_tier_empty_when_none_match(state_dir):
    add_to_watchlist(state_dir, "p1", tier=2)
    assert pipelines_by_tier(state_dir, 3) == []


def test_tier_label_known_values():
    assert tier_label(1) == "critical"
    assert tier_label(2) == "important"
    assert tier_label(3) == "low"


def test_tier_label_unknown_returns_unknown():
    assert tier_label(99) == "unknown"
