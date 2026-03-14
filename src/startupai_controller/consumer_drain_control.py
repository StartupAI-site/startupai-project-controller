"""Drain sentinel helpers for the consumer shell and CLI."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


def drain_requested(path: Path) -> bool:
    """Return True when a graceful drain has been requested."""
    return path.exists()


def drain_requested_at(path: Path) -> str | None:
    """Return the timestamp recorded in the drain sentinel, if any."""
    if not path.exists():
        return None
    try:
        payload = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return payload or None


def request_drain(path: Path) -> None:
    """Create the drain sentinel file used for graceful maintenance."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(datetime.now(timezone.utc).isoformat(), encoding="utf-8")


def clear_drain(path: Path) -> bool:
    """Remove the drain sentinel file if present."""
    if not path.exists():
        return False
    path.unlink()
    return True
