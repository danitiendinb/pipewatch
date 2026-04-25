"""curator.py — manage a curated list of 'watched' pipelines with priority tiers."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class CuratedEntry:
    pipeline: str
    tier: int          # 1 = critical, 2 = important, 3 = low
    reason: str = ""
    added_at: str = ""


TIERS = {1: "critical", 2: "important", 3: "low"}


def _curate_path(state_dir: str) -> Path:
    return Path(state_dir) / "curated.json"


def load_curated(state_dir: str) -> Dict[str, CuratedEntry]:
    path = _curate_path(state_dir)
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    return {k: CuratedEntry(**v) for k, v in data.items()}


def _save_curated(state_dir: str, entries: Dict[str, CuratedEntry]) -> None:
    path = _curate_path(state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({k: asdict(v) for k, v in entries.items()}, indent=2))


def add_to_watchlist(
    state_dir: str,
    pipeline: str,
    tier: int = 2,
    reason: str = "",
) -> CuratedEntry:
    from pipewatch.acknowledger import _now_iso  # reuse ISO helper

    if tier not in TIERS:
        raise ValueError(f"tier must be one of {list(TIERS)}")
    entries = load_curated(state_dir)
    entry = CuratedEntry(pipeline=pipeline, tier=tier, reason=reason, added_at=_now_iso())
    entries[pipeline] = entry
    _save_curated(state_dir, entries)
    return entry


def remove_from_watchlist(state_dir: str, pipeline: str) -> bool:
    entries = load_curated(state_dir)
    if pipeline not in entries:
        return False
    del entries[pipeline]
    _save_curated(state_dir, entries)
    return True


def get_entry(state_dir: str, pipeline: str) -> Optional[CuratedEntry]:
    return load_curated(state_dir).get(pipeline)


def pipelines_by_tier(state_dir: str, tier: int) -> List[str]:
    return [
        name
        for name, entry in load_curated(state_dir).items()
        if entry.tier == tier
    ]


def tier_label(tier: int) -> str:
    return TIERS.get(tier, "unknown")
