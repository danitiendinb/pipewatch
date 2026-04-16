"""Tests for the pipewatch CLI."""
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from pipewatch.cli import build_parser, main


def test_build_parser_check_command():
    parser = build_parser()
    args = parser.parse_args(["check"])
    assert args.command == "check"
    assert args.pipeline is None


def test_build_parser_check_pipeline_flag():
    parser = build_parser()
    args = parser.parse_args(["check", "--pipeline", "my_etl"])
    assert args.pipeline == "my_etl"


def test_build_parser_status_command():
    parser = build_parser()
    args = parser.parse_args(["status"])
    assert args.command == "status"


def test_build_parser_custom_config():
    parser = build_parser()
    args = parser.parse_args(["-c", "custom.yml", "check"])
    assert args.config == "custom.yml"


def test_main_exits_when_config_missing(tmp_path):
    with pytest.raises(SystemExit) as exc_info:
        main(["-c", str(tmp_path / "missing.yml"), "check"])
    assert exc_info.value.code == 1


def test_main_check_all(tmp_path):
    config_content = """
log_level: WARNING
state_dir: {state_dir}
pipelines:
  - name: test_pipe
    command: "echo ok"
    max_failures: 2
alert:
  webhooks: []
""".format(state_dir=str(tmp_path / "state"))
    config_file = tmp_path / "pipewatch.yml"
    config_file.write_text(config_content)

    with patch("pipewatch.cli.dispatch_alerts", return_value=[]) as mock_dispatch:
        main(["-c", str(config_file), "check"])
    mock_dispatch.assert_called_once()


def test_main_check_unknown_pipeline(tmp_path):
    config_content = """
log_level: WARNING
state_dir: {state_dir}
pipelines:
  - name: test_pipe
    command: "echo ok"
    max_failures: 2
alert:
  webhooks: []
""".format(state_dir=str(tmp_path / "state"))
    config_file = tmp_path / "pipewatch.yml"
    config_file.write_text(config_content)

    with pytest.raises(SystemExit) as exc_info:
        main(["-c", str(config_file), "check", "--pipeline", "nonexistent"])
    assert exc_info.value.code == 1
