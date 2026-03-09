"""Shared timeout policy for GitHub CLI subprocess calls."""

from __future__ import annotations

import os

DEFAULT_GH_COMMAND_TIMEOUT_SECONDS = 8.0
GH_COMMAND_TIMEOUT_ENV_VAR = "STARTUPAI_GH_TIMEOUT_SECONDS"


def gh_command_timeout_seconds() -> float:
    """Return bounded timeout for one gh subprocess invocation."""
    raw = os.getenv(GH_COMMAND_TIMEOUT_ENV_VAR, "").strip()
    if not raw:
        return DEFAULT_GH_COMMAND_TIMEOUT_SECONDS
    try:
        return max(1.0, float(raw))
    except ValueError:
        return DEFAULT_GH_COMMAND_TIMEOUT_SECONDS
