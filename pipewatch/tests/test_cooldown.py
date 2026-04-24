"""Tests for pipewatch.cooldown and pipewatch.cli_cooldown."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.cooldown import (
    active_cooldowns,
    clear_cooldown,
    is_cooling_down,
    load_cooldown,
    set_cooldown,
)
from pipewatch.cli_cooldown import add_cooldown_subparser, cmd_cooldown


@pytest.fixture()
def state_dir(tmp_path):
    return str(tmp_path)


def _utc(offset_minutes: int = 0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)


# ---------------------------------------------------------------------------
# cooldown module
# ---------------------------------------------------------------------------

def test_load_cooldown_none_for_unknown(state_dir):
    assert load_cooldown(state_dir, "pipe_a") is None


def test_set_cooldown_returns_future_datetime(state_dir):
    exp = set_cooldown(state_dir, "pipe_a", minutes=30)
    assert exp > _utc()


def test_set_cooldown_persists(state_dir):
    set_cooldown(state_dir, "pipe_a", minutes=30)
    loaded = load_cooldown(state_dir, "pipe_a")
    assert loaded is not None
    assert loaded > _utc()


def test_is_cooling_down_true_after_set(state_dir):
    set_cooldown(state_dir, "pipe_a", minutes=60)
    assert is_cooling_down(state_dir, "pipe_a") is True


def test_is_cooling_down_false_when_no_record(state_dir):
    assert is_cooling_down(state_dir, "pipe_b") is False


def test_clear_cooldown_removes_record(state_dir):
    set_cooldown(state_dir, "pipe_a", minutes=60)
    clear_cooldown(state_dir, "pipe_a")
    assert is_cooling_down(state_dir, "pipe_a") is False


def test_active_cooldowns_returns_active(state_dir):
    set_cooldown(state_dir, "pipe_a", minutes=60)
    set_cooldown(state_dir, "pipe_b", minutes=60)
    active = active_cooldowns(state_dir)
    assert "pipe_a" in active
    assert "pipe_b" in active


def test_active_cooldowns_excludes_expired(state_dir):
    # Manually write an already-expired cooldown
    import json
    from pathlib import Path
    p = Path(state_dir) / "pipe_old.cooldown.json"
    p.write_text(json.dumps({"expires_at": _utc(-120).isoformat()}))
    active = active_cooldowns(state_dir)
    assert "pipe_old" not in active


# ---------------------------------------------------------------------------
# CLI subcommand
# ---------------------------------------------------------------------------

@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="pipewatch.yml")
    sub = p.add_subparsers(dest="command")
    add_cooldown_subparser(sub)
    return p


def test_add_cooldown_subparser_registers_command(parser):
    args = parser.parse_args(["cooldown", "status"])
    assert args.command == "cooldown"


def test_cmd_cooldown_missing_config_returns_1(tmp_path):
    args = argparse.Namespace(config=str(tmp_path / "missing.yml"), cooldown_cmd="status")
    with patch("pipewatch.cli_cooldown.load_config", return_value=None):
        assert cmd_cooldown(args) == 1


def test_cmd_cooldown_set_prints_expiry(state_dir, capsys):
    cfg = MagicMock(state_dir=state_dir)
    args = argparse.Namespace(config="x", cooldown_cmd="set", pipeline="p1", minutes=45)
    with patch("pipewatch.cli_cooldown.load_config", return_value=cfg):
        rc = cmd_cooldown(args)
    assert rc == 0
    assert "p1" in capsys.readouterr().out


def test_cmd_cooldown_status_no_active(state_dir, capsys):
    cfg = MagicMock(state_dir=state_dir)
    args = argparse.Namespace(config="x", cooldown_cmd="status")
    with patch("pipewatch.cli_cooldown.load_config", return_value=cfg):
        rc = cmd_cooldown(args)
    assert rc == 0
    assert "No active" in capsys.readouterr().out
