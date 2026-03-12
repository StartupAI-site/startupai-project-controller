"""ControlPlaneStatePort — local state needed by the control-plane tick."""

from __future__ import annotations

from typing import Protocol


class ControlPlaneStatePort(Protocol):
    """Persistence operations consumed by control-plane orchestration."""

    def set_control_value(self, key: str, value: str | None) -> None:
        """Persist one control-plane state value."""
        ...

    def active_lease_issue_refs(self) -> list[str]:
        """Return issue refs currently holding active leases."""
        ...

    def control_state_snapshot(self) -> dict[str, str]:
        """Return a snapshot of stored control-plane state values."""
        ...

    def deferred_action_count(self) -> int:
        """Return the number of queued deferred actions."""
        ...

    def oldest_deferred_action_age_seconds(self) -> float | None:
        """Return the age of the oldest deferred action, if any."""
        ...

    def close(self) -> None:
        """Release any underlying resources."""
        ...
