"""Smoke tests for the three public CLI entry points."""

from __future__ import annotations

import subprocess
import sys

BOARD_AUTOMATION_SUBCOMMANDS = {
    "mark-done",
    "auto-promote",
    "admit-backlog",
    "propagate-blocker",
    "reconcile-handoffs",
    "schedule-ready",
    "claim-ready",
    "dispatch-agent",
    "rebalance-wip",
    "enforce-ready-dependencies",
    "audit-in-progress",
    "sync-review-state",
    "codex-review-gate",
    "automerge-review",
    "review-rescue",
    "review-rescue-all",
    "enforce-execution-policy",
    "classify-parallelism",
}

BOARD_CONSUMER_SUBCOMMANDS = {
    "run",
    "one-shot",
    "status",
    "report-slo",
    "serve-status",
    "reconcile",
    "drain",
    "resume",
}

BOARD_CONTROL_PLANE_SUBCOMMANDS = {
    "tick",
    "run",
}


def _run_help(module: str) -> str:
    result = subprocess.run(
        [sys.executable, "-m", module, "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert (
        result.returncode == 0
    ), f"{module} --help exited with {result.returncode}:\n{result.stderr}"
    return result.stdout


def test_board_automation_help_lists_all_subcommands() -> None:
    output = _run_help("startupai_controller.board_automation")
    for subcmd in BOARD_AUTOMATION_SUBCOMMANDS:
        assert subcmd in output, f"board_automation --help missing subcommand: {subcmd}"


def test_board_consumer_help_lists_all_subcommands() -> None:
    output = _run_help("startupai_controller.board_consumer")
    for subcmd in BOARD_CONSUMER_SUBCOMMANDS:
        assert subcmd in output, f"board_consumer --help missing subcommand: {subcmd}"


def test_board_control_plane_help_lists_all_subcommands() -> None:
    output = _run_help("startupai_controller.board_control_plane")
    for subcmd in BOARD_CONTROL_PLANE_SUBCOMMANDS:
        assert (
            subcmd in output
        ), f"board_control_plane --help missing subcommand: {subcmd}"
