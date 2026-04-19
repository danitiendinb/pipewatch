"""Alert routing: send alerts to different webhooks based on pipeline labels."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.labeler import load_label
from pipewatch.alerts import send_webhook
from pipewatch.state import PipelineState


@dataclass
class RouteRule:
    label: str
    webhook_url: str


@dataclass
class RouterConfig:
    rules: List[RouteRule] = field(default_factory=list)
    default_url: Optional[str] = None


def _url_for_pipeline(name: str, state_dir: str, config: RouterConfig) -> Optional[str]:
    label = load_label(state_dir, name)
    for rule in config.rules:
        if rule.label == label:
            return rule.webhook_url
    return config.default_url


def route_alert(name: str, state: PipelineState, state_dir: str, config: RouterConfig) -> bool:
    """Send an alert for *name* to the appropriate webhook. Returns True if sent."""
    url = _url_for_pipeline(name, state_dir, config)
    if url is None:
        return False
    payload = {
        "pipeline": name,
        "consecutive_failures": state.consecutive_failures,
        "last_run": state.runs[-1].finished_at if state.runs else None,
    }
    return send_webhook(url, payload)


def route_all(
    states: Dict[str, PipelineState],
    state_dir: str,
    config: RouterConfig,
    threshold: int = 1,
) -> List[str]:
    """Route alerts for all pipelines that meet the failure threshold.
    Returns list of pipeline names for which an alert was dispatched."""
    alerted: List[str] = []
    for name, state in states.items():
        if state.consecutive_failures >= threshold:
            if route_alert(name, state, state_dir, config):
                alerted.append(name)
    return alerted
