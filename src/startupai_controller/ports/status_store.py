"""StatusStorePort — local state consumed by status reporting."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from startupai_controller.domain.models import ReviewQueueEntry, SessionInfo


class MetricEventView(Protocol):
    """Read-only metric event view needed by status reporting."""

    created_at: str

    @property
    def payload(self) -> dict[str, Any]:
        """Return the decoded event payload."""
        ...


class StatusStorePort(Protocol):
    """Persistence operations consumed by consumer status reporting."""

    def latest_review_issue_refs(self) -> list[str]:
        """Return recent issue refs currently believed to be in Review."""
        ...

    def list_review_queue_items(self) -> list[ReviewQueueEntry]:
        """Return review queue items ordered for status reporting."""
        ...

    def due_review_queue_count(self, *, now: datetime) -> int:
        """Return the number of due review queue items."""
        ...

    def count_metric_events_since(self, since: datetime) -> dict[str, int]:
        """Return counts of metric events since a timestamp."""
        ...

    def occupied_slot_seconds_since(self, since: datetime, *, now: datetime) -> float:
        """Return occupied slot-seconds within a window."""
        ...

    def metric_events_since(
        self,
        since: datetime,
        *,
        event_types: tuple[str, ...] | None = None,
    ) -> list[MetricEventView]:
        """Return metric events since a timestamp."""
        ...

    def active_lease_count(self) -> int:
        """Return the number of active leases."""
        ...

    def active_slot_ids(self) -> list[int]:
        """Return currently occupied slot ids."""
        ...

    def active_workers(self) -> list[SessionInfo]:
        """Return active worker sessions."""
        ...

    def recent_sessions(self, *, limit: int = 10) -> list[SessionInfo]:
        """Return recent sessions ordered newest first."""
        ...

    def control_state_snapshot(self) -> dict[str, str]:
        """Return a snapshot of control-plane state values."""
        ...

    def deferred_action_count(self) -> int:
        """Return the number of deferred actions."""
        ...

    def oldest_deferred_action_age_seconds(self) -> float | None:
        """Return the age of the oldest deferred action, if any."""
        ...
