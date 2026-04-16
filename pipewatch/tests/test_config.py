"""Tests for pipewatch configuration loader."""

import os
import pytest
import tempfile
import yaml

from pipewatch.config import load_config, PipewatchConfig, PipelineConfig, AlertConfig


SAMPLE_CONFIG = {
    "log_level": "DEBUG",
    "state_dir": "/tmp/pipewatch",
    "alert": {
        "webhook_url": "https://hooks.example.com/abc",
        "slack_channel": "#alerts",
    },
    "pipelines": [
        {
            "name": "ingest_orders",
            "schedule_cron": "0 * * * *",
            "max_duration_seconds": 600,
            "tags": ["orders", "critical"],
        },
        {
            "name": "transform_users",
            "alert_on_failure": False,
        },
    ],
}


@pytest.fixture
def config_file():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(SAMPLE_CONFIG, f)
        path = f.name
    yield path
    os.unlink(path)


def test_load_config_returns_correct_type(config_file):
    config = load_config(config_file)
    assert isinstance(config, PipewatchConfig)


def test_load_config_log_level(config_file):
    config = load_config(config_file)
    assert config.log_level == "DEBUG"


def test_load_config_state_dir(config_file):
    config = load_config(config_file)
    assert config.state_dir == "/tmp/pipewatch"


def test_load_config_alert(config_file):
    config = load_config(config_file)
    assert config.alert.webhook_url == "https://hooks.example.com/abc"
    assert config.alert.slack_channel == "#alerts"
    assert config.alert.email is None


def test_load_config_pipelines(config_file):
    config = load_config(config_file)
    assert len(config.pipelines) == 2
    p = config.pipelines[0]
    assert p.name == "ingest_orders"
    assert p.schedule_cron == "0 * * * *"
    assert p.max_duration_seconds == 600
    assert "critical" in p.tags


def test_load_config_pipeline_defaults(config_file):
    config = load_config(config_file)
    p = config.pipelines[1]
    assert p.name == "transform_users"
    assert p.alert_on_failure is False
    assert p.alert_on_timeout is True
    assert p.max_duration_seconds == 3600


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/pipewatch.yml")
