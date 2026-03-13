"""Unit tests for direct GitHub transport behavior."""

from __future__ import annotations

import subprocess

import pytest

import startupai_controller.adapters.github_transport as github_transport


def test_run_gh_retries_transient_connection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unsupported commands still use subprocess retry behavior."""
    calls = {"count": 0}

    def fake_check_output(args, text=True, stderr=None, timeout=None):
        calls["count"] += 1
        if calls["count"] == 1:
            raise subprocess.CalledProcessError(
                returncode=1,
                cmd=args,
                output="error connecting to api.github.com",
            )
        return '{"ok": true}'

    monkeypatch.setattr(github_transport.subprocess, "check_output", fake_check_output)
    monkeypatch.setattr(github_transport.time, "sleep", lambda *_: None)

    result = github_transport._run_gh(["unsupported", "command"])

    assert result == '{"ok": true}'
    assert calls["count"] == 2


def test_run_gh_fails_fast_on_timeout_expired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unsupported subprocess fallbacks still fail fast on timeout."""
    calls = {"count": 0, "timeout": None}

    def fake_check_output(args, text=True, stderr=None, timeout=None):
        calls["count"] += 1
        calls["timeout"] = timeout
        raise subprocess.TimeoutExpired(cmd=args, timeout=timeout)

    monkeypatch.setattr(github_transport.subprocess, "check_output", fake_check_output)
    monkeypatch.setattr(github_transport.time, "sleep", lambda *_: None)

    with pytest.raises(github_transport.GhCommandError, match="timed out after"):
        github_transport._run_gh(["unsupported", "command"])

    assert calls["count"] == 1
    assert calls["timeout"] == github_transport._GH_COMMAND_TIMEOUT_SECONDS
