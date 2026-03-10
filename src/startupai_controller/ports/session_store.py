"""SessionStorePort — session and review queue persistence.

Port protocol for session tracking and review queue management.
Returns domain types from domain/models.py.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

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
        source_session_id: str | None = None,
        next_attempt_at: str | None = None,
        now: datetime | None = None,
    ) -> None:
        """Add or update a review queue entry."""
        ...

    def latest_session_for_issue(self, issue_ref: str) -> SessionInfo | None:
        """Return the most recent session for an issue, or None."""
        ...

    def get_session(self, session_id: str) -> SessionInfo | None:
        """Return a session by id, or None."""
        ...

    def get_requeue_state(self, issue_ref: str) -> tuple[int, str | None]:
        """Return (requeue_count, last_requeue_reason) for an issue."""
        ...

    # -- Methods used by dependency-violating orchestrator functions --------

    def list_review_queue_items(self) -> list[ReviewQueueEntry]:
        """Return all review-queue entries ordered by due time."""
        ...

    def delete_review_queue_item(self, issue_ref: str) -> None:
        """Delete one review-queue entry."""
        ...

    def update_review_queue_item(
        self,
        issue_ref: str,
        *,
        next_attempt_at: str,
        last_result: str,
        last_reason: str | None = None,
        last_state_digest: str | None = None,
        blocked_streak: int = 0,
        blocked_class: str | None = None,
        now: datetime | None = None,
    ) -> None:
        """Update a review-queue entry with processing results."""
        ...

    def reschedule_review_queue_item(
        self,
        issue_ref: str,
        *,
        next_attempt_at: str,
        now: datetime | None = None,
    ) -> None:
        """Reschedule a review-queue entry without recording a processing attempt."""
        ...

    def reset_requeue_count(self, issue_ref: str) -> None:
        """Reset the requeue counter (e.g. when the PR URL changes)."""
        ...

    def increment_requeue_count(self, issue_ref: str, pr_url: str) -> int:
        """Increment and return the requeue count for the current PR cycle."""
        ...

    def active_workers(self) -> list[SessionInfo]:
        """Return currently active workers (sessions with active leases)."""
        ...

    def latest_session_for_worktree(self, worktree_path: str) -> SessionInfo | None:
        """Return the most recent session that used a worktree path."""
        ...

    def update_session(self, session_id: str, **fields: Any) -> None:
        """Update session fields by session_id."""
        ...
