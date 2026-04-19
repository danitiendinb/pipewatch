"""Tests for pipewatch.pinner."""
import pytest

from pipewatch.pinner import all_pins, clear_pin, load_pin, set_pin


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def test_load_pin_none_for_unknown(state_dir):
    assert load_pin(state_dir, "etl_main") is None


def test_set_pin_returns_record(state_dir):
    rec = set_pin(state_dir, "etl_main", "v1.2.3")
    assert rec.pipeline == "etl_main"
    assert rec.version == "v1.2.3"


def test_set_pin_persists(state_dir):
    set_pin(state_dir, "etl_main", "v1.2.3", pinned_by="alice", note="hotfix")
    rec = load_pin(state_dir, "etl_main")
    assert rec is not None
    assert rec.version == "v1.2.3"
    assert rec.pinned_by == "alice"
    assert rec.note == "hotfix"


def test_set_pin_overwrites_existing(state_dir):
    set_pin(state_dir, "etl_main", "v1.0.0")
    set_pin(state_dir, "etl_main", "v2.0.0")
    assert load_pin(state_dir, "etl_main").version == "v2.0.0"


def test_clear_pin_removes_record(state_dir):
    set_pin(state_dir, "etl_main", "v1.0.0")
    result = clear_pin(state_dir, "etl_main")
    assert result is True
    assert load_pin(state_dir, "etl_main") is None


def test_clear_pin_returns_false_when_not_set(state_dir):
    assert clear_pin(state_dir, "etl_main") is False


def test_all_pins_empty(state_dir):
    assert all_pins(state_dir) == []


def test_all_pins_returns_all(state_dir):
    set_pin(state_dir, "pipeline_a", "v1")
    set_pin(state_dir, "pipeline_b", "v2")
    pins = all_pins(state_dir)
    names = {p.pipeline for p in pins}
    assert names == {"pipeline_a", "pipeline_b"}


def test_pin_has_timestamp(state_dir):
    rec = set_pin(state_dir, "etl_main", "v3.0.0")
    assert rec.pinned_at != ""
    assert "T" in rec.pinned_at
