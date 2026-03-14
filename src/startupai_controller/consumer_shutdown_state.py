"""Supervisor-owned shutdown state helpers for forced drain escalation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def read_shutdown_state(path: Path) -> dict[str, Any] | None:
    """Return the persisted shutdown-state payload, if any."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def write_shutdown_state(
    path: Path,
    *,
    payload: dict[str, Any],
) -> None:
    """Persist one supervisor-authored shutdown-state payload."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def clear_shutdown_state(path: Path) -> bool:
    """Delete the shutdown-state file when present."""
    if not path.exists():
        return False
    path.unlink()
    return True
