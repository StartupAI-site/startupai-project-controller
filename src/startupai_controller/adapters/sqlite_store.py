"""SQLite adapter — wraps consumer_db.py behind SessionStorePort.

Thin wrapper around ConsumerDB, translating between the port protocol
and the existing SQLite access layer.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from startupai_controller.domain.models import ReviewQueueEntry, SessionInfo

if TYPE_CHECKING:
    from startupai_controller.consumer_db import ConsumerDB


class SqliteSessionStore:
    """Adapter wrapping ConsumerDB behind SessionStorePort.

    Satisfies SessionStorePort protocol via structural typing.
    """

    def __init__(self, db: ConsumerDB) -> None:
        self._db = db

    def get_review_queue_item(self, issue_ref: str) -> ReviewQueueEntry | None:
        return self._db.get_review_queue_item(issue_ref)

    def list_due_review_items(self, now: datetime) -> list[ReviewQueueEntry]:
        return self._db.list_due_review_queue_items(now)

    def enqueue_review_item(
        self,
        issue_ref: str,
        pr_url: str,
        pr_repo: str,
        pr_number: int,
        source_session_id: str,
        next_attempt_at: datetime,
    ) -> None:
        self._db.enqueue_review_item(
            issue_ref=issue_ref,
            pr_url=pr_url,
            pr_repo=pr_repo,
            pr_number=pr_number,
            source_session_id=source_session_id,
            next_attempt_at=next_attempt_at,
        )

    def latest_session_for_issue(self, issue_ref: str) -> SessionInfo | None:
        return self._db.latest_session_for_issue(issue_ref)

    def get_requeue_state(self, issue_ref: str) -> tuple[int, str | None]:
        return self._db.get_requeue_state(issue_ref)
