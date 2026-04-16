"""Simple cron-expression scheduler for pipeline expected-run tracking."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from croniter import croniter


def next_run(cron_expr: str, after: Optional[datetime] = None) -> datetime:
    """Return the next scheduled datetime for a cron expression."""
    base = after or datetime.now(timezone.utc)
    itr = croniter(cron_expr, base)
    return itr.get_next(datetime).replace(tzinfo=timezone.utc)


def last_expected_run(cron_expr: str, before: Optional[datetime] = None) -> datetime:
    """Return the most recent expected run time before *before*."""
    base = before or datetime.now(timezone.utc)
    itr = croniter(cron_expr, base)
    return itr.get_prev(datetime).replace(tzinfo=timezone.utc)


def is_overdue(cron_expr: str, last_success_iso: Optional[str], now: Optional[datetime] = None) -> bool:
    """Return True when the pipeline missed its most recent scheduled window.

    A pipeline is overdue when the last expected run time has passed and no
    successful run has been recorded since that time.
    """
    now = now or datetime.now(timezone.utc)
    expected = last_expected_run(cron_expr, before=now)
    if last_success_iso is None:
        return True
    last_ok = datetime.fromisoformat(last_success_iso)
    if last_ok.tzinfo is None:
        last_ok = last_ok.replace(tzinfo=timezone.utc)
    return last_ok < expected


def overdue_pipelines(configs: list[dict], states: dict[str, object], now: Optional[datetime] = None) -> list[str]:
    """Return names of pipelines that are overdue based on their schedule.

    *configs* is a list of dicts with keys ``name`` and ``schedule``.
    *states* maps pipeline name to a PipelineState-like object with a
    ``last_success`` attribute (ISO string or None).
    """
    overdue = []
    for cfg in configs:
        name = cfg.get("name")
        schedule = cfg.get("schedule")
        if not schedule:
            continue
        state = states.get(name)
        last_success = getattr(state, "last_success", None) if state else None
        if is_overdue(schedule, last_success, now=now):
            overdue.append(name)
    return overdue
