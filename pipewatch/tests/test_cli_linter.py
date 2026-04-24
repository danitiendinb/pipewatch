"""Unit tests for pipewatch.cli_linter."""
from __future__ import annotations

import argparse
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.cli_linter import add_linter_subparser, cmd_lint
from pipewatch.linter import LintReport, LintIssue


@pytest.fixture()
def parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    add_linter_subparser(sub)
    return p


def test_add_linter_subparser_registers_command(parser):
    args = parser.parse_args(["lint"])
    assert args.command == "lint"


def test_add_linter_subparser_default_config(parser):
    args = parser.parse_args(["lint"])
    assert args.config == "pipewatch.yml"


def test_add_linter_subparser_strict_flag(parser):
    args = parser.parse_args(["lint", "--strict"])
    assert args.strict is True


def test_cmd_lint_missing_config_returns_1():
    args = argparse.Namespace(config="nonexistent.yml", strict=False)
    with patch("pipewatch.cli_linter.load_config", return_value=None):
        assert cmd_lint(args) == 1


def test_cmd_lint_clean_config_returns_0():
    args = argparse.Namespace(config="pipewatch.yml", strict=False)
    clean_report = LintReport()
    mock_config = MagicMock()
    with patch("pipewatch.cli_linter.load_config", return_value=mock_config), \
         patch("pipewatch.cli_linter.lint_config", return_value=clean_report), \
         patch("builtins.print"):
        assert cmd_lint(args) == 0


def test_cmd_lint_errors_returns_2():
    args = argparse.Namespace(config="pipewatch.yml", strict=False)
    bad_report = LintReport(issues=[LintIssue(pipeline="p", severity="error", message="bad")])
    mock_config = MagicMock()
    with patch("pipewatch.cli_linter.load_config", return_value=mock_config), \
         patch("pipewatch.cli_linter.lint_config", return_value=bad_report), \
         patch("builtins.print"):
        assert cmd_lint(args) == 2


def test_cmd_lint_strict_warnings_returns_3():
    args = argparse.Namespace(config="pipewatch.yml", strict=True)
    warn_report = LintReport(issues=[LintIssue(pipeline="p", severity="warning", message="meh")])
    mock_config = MagicMock()
    with patch("pipewatch.cli_linter.load_config", return_value=mock_config), \
         patch("pipewatch.cli_linter.lint_config", return_value=warn_report), \
         patch("builtins.print"):
        assert cmd_lint(args) == 3
