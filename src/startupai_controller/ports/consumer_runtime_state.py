"""ConsumerRuntimeStatePort — local state consumed by daemon orchestration."""

from __future__ import annotations

from typing import Protocol

from startupai_controller.domain.models import SessionInfo
from startupai_controller.ports.control_plane_state import ControlValueStorePort


class ConsumerRuntimeStatePort(ControlValueStorePort, Protocol):
    """Persistence operations consumed by preflight and daemon runtime flows."""

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

    def update_heartbeat(self, issue_ref: str) -> None:
        """Refresh the heartbeat for one in-flight lease/session."""
        ...

    def release_lease(self, issue_ref: str) -> None:
        """Release one claimed execution lease."""
        ...
