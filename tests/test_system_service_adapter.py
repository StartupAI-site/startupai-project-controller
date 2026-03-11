"""Unit tests for the local system-service adapter."""

from __future__ import annotations

import subprocess

from startupai_controller.adapters.system_service import SystemServiceAdapter


def test_is_active_returns_true_for_active_service() -> None:
    calls: list[list[str]] = []

    def fake_runner(args, **kwargs):
        calls.append(list(args))
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout="active\n",
            stderr="",
        )

    adapter = SystemServiceAdapter(subprocess_runner=fake_runner)

    assert adapter.is_active("startupai-consumer.service") is True
    assert calls == [["systemctl", "--user", "is-active", "startupai-consumer.service"]]


def test_is_active_returns_false_for_inactive_service() -> None:
    def fake_runner(args, **kwargs):
        return subprocess.CompletedProcess(
            args=args,
            returncode=3,
            stdout="inactive\n",
            stderr="",
        )

    adapter = SystemServiceAdapter(subprocess_runner=fake_runner)

    assert adapter.is_active("startupai-consumer.service") is False
