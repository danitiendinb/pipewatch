"""Configuration loader for pipewatch."""

import os
import yaml
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class PipelineConfig:
    name: str
    schedule_cron: Optional[str] = None
    max_duration_seconds: int = 3600
    alert_on_failure: bool = True
    alert_on_timeout: bool = True
    tags: List[str] = field(default_factory=list)


@dataclass
class AlertConfig:
    webhook_url: Optional[str] = None
    email: Optional[str] = None
    slack_channel: Optional[str] = None


@dataclass
class PipewatchConfig:
    pipelines: List[PipelineConfig] = field(default_factory=list)
    alert: AlertConfig = field(default_factory=AlertConfig)
    log_level: str = "INFO"
    state_dir: str = ".pipewatch"


def load_config(path: str = "pipewatch.yml") -> PipewatchConfig:
    """Load and parse pipewatch configuration from a YAML file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r") as f:
        raw = yaml.safe_load(f) or {}

    alert_raw = raw.get("alert", {})
    alert = AlertConfig(
        webhook_url=alert_raw.get("webhook_url"),
        email=alert_raw.get("email"),
        slack_channel=alert_raw.get("slack_channel"),
    )

    pipelines = [
        PipelineConfig(
            name=p["name"],
            schedule_cron=p.get("schedule_cron"),
            max_duration_seconds=p.get("max_duration_seconds", 3600),
            alert_on_failure=p.get("alert_on_failure", True),
            alert_on_timeout=p.get("alert_on_timeout", True),
            tags=p.get("tags", []),
        )
        for p in raw.get("pipelines", [])
    ]

    return PipewatchConfig(
        pipelines=pipelines,
        alert=alert,
        log_level=raw.get("log_level", "INFO"),
        state_dir=raw.get("state_dir", ".pipewatch"),
    )
