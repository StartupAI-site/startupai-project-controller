"""ConsumerRuntimeStatePort — local state consumed by daemon orchestration."""

from __future__ import annotations

from typing import Protocol

from startupai_controller.domain.models import SessionInfo


class ConsumerRuntimeStatePort(Protocol):
    """Persistence operations consumed by preflight and daemon runtime flows."""

    def set_control_value(self, key: str, value: str | None) -> None:
        """Persist one control-plane value."""
        ...

    def active_slot_ids(self) -> list[int]:
        """Return currently occupied slot ids."""
        ...

    def active_workers(self) -> list[SessionInfo]:
        """Return active worker sessions."""
        ...

    def active_lease_count(self) -> int:
        """Return the number of active leases."""
        ...

    def active_lease_issue_refs(self) -> list[str]:
        """Return issue refs currently holding leases."""
        ...

    def expire_stale_leases(self, heartbeat_expiry_seconds: int) -> list[str]:
        """Expire stale leases and return affected issue refs."""
        ...

    def get_control_value(self, key: str) -> str | None:
        """Return one persisted control-plane value."""
        ...
