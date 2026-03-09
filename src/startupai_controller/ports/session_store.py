"""SessionStorePort — session and review queue persistence.

Port protocol for session tracking and review queue management.
Returns domain types from domain/models.py.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from startupai_controller.domain.models import ReviewQueueEntry, SessionInfo


class SessionStorePort(Protocol):
    """Session and review queue persistence operations."""

    def get_review_queue_item(self, issue_ref: str) -> ReviewQueueEntry | None:
        """Return the review queue entry for an issue, or None."""
        ...

    def list_due_review_items(self, now: datetime) -> list[ReviewQueueEntry]:
        """Return review queue entries due for processing."""
        ...

    def enqueue_review_item(
        self,
        issue_ref: str,
        pr_url: str,
        pr_repo: str,
        pr_number: int,
        source_session_id: str,
        next_attempt_at: datetime,
    ) -> None:
        """Add or update a review queue entry."""
        ...

    def latest_session_for_issue(self, issue_ref: str) -> SessionInfo | None:
        """Return the most recent session for an issue, or None."""
        ...

    def get_requeue_state(self, issue_ref: str) -> tuple[int, str | None]:
        """Return (requeue_count, last_requeue_reason) for an issue."""
        ...
