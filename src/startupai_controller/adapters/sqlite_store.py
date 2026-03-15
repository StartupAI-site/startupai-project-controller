"""SQLite adapter — wraps the canonical ConsumerDB implementation.

Thin wrapper around the canonical SQLite implementation, translating between
the port protocol and the persistence layer.
"""

from __future__ import annotations

from datetime import datetime

from startupai_controller.domain.models import (
    ReviewQueueEntry,
    ReviewRescueEvent,
    SessionInfo,
)
from startupai_controller.ports.session_store import SessionUpdateFields

# Adapter-internal types re-exported for canonical import paths.
from startupai_controller.adapters.consumer_db_store import (  # noqa: F401
    ConsumerDB,
    MetricEvent,
    RecoveredLease,
)


class SqliteSessionStore:
    """Adapter wrapping ConsumerDB behind SessionStorePort.

    Satisfies SessionStorePort protocol via structural typing.
    """

    def __init__(self, db: ConsumerDB) -> None:
        self._db = db

    def get_review_queue_item(self, issue_ref: str) -> ReviewQueueEntry | None:
        return self._db.get_review_queue_item(issue_ref)

    def list_due_review_items(self, now: datetime) -> list[ReviewQueueEntry]:
        return self._db.list_due_review_queue_items(now=now)

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
        self._db.enqueue_review_item(
            issue_ref=issue_ref,
            pr_url=pr_url,
            pr_repo=pr_repo,
            pr_number=pr_number,
            source_session_id=source_session_id,
            next_attempt_at=next_attempt_at,
            now=now,
        )

    def latest_session_for_issue(self, issue_ref: str) -> SessionInfo | None:
        return self._db.latest_session_for_issue(issue_ref)

    def get_session(self, session_id: str) -> SessionInfo | None:
        return self._db.get_session(session_id)

    def get_requeue_state(self, issue_ref: str) -> tuple[int, str | None]:
        return self._db.get_requeue_state(issue_ref)

    # -- Methods used by dependency-violating orchestrator functions --------

    def list_review_queue_items(self) -> list[ReviewQueueEntry]:
        return self._db.list_review_queue_items()

    def delete_review_queue_item(self, issue_ref: str) -> None:
        self._db.delete_review_queue_item(issue_ref)

    def append_review_rescue_event(
        self,
        issue_ref: str,
        *,
        pr_repo: str,
        pr_number: int,
        result_kind: str,
        reason: str | None = None,
        payload_json: str | None = None,
        now: datetime | None = None,
    ) -> None:
        self._db.append_review_rescue_event(
            issue_ref,
            pr_repo=pr_repo,
            pr_number=pr_number,
            result_kind=result_kind,
            reason=reason,
            payload_json=payload_json,
            now=now,
        )

    def list_review_rescue_events(
        self,
        issue_ref: str | None = None,
    ) -> list[ReviewRescueEvent]:
        return self._db.list_review_rescue_events(issue_ref)

    def update_review_queue_item(
        self,
        issue_ref: str,
        *,
        next_attempt_at: str,
        last_result: str,
        last_reason: str | None = None,
        last_state_digest: str | None = None,
        last_check_names: tuple[str, ...] = (),
        copilot_missing_since: str | None = None,
        blocked_streak: int = 0,
        blocked_class: str | None = None,
        now: datetime | None = None,
    ) -> None:
        self._db.update_review_queue_item(
            issue_ref,
            next_attempt_at=next_attempt_at,
            last_result=last_result,
            last_reason=last_reason,
            last_state_digest=last_state_digest,
            last_check_names=last_check_names,
            copilot_missing_since=copilot_missing_since,
            blocked_streak=blocked_streak,
            blocked_class=blocked_class,
            now=now,
        )

    def reset_requeue_count(self, issue_ref: str) -> None:
        self._db.reset_requeue_count(issue_ref)

    def increment_requeue_count(self, issue_ref: str, pr_url: str) -> int:
        return self._db.increment_requeue_count(issue_ref, pr_url)

    def reschedule_review_queue_item(
        self,
        issue_ref: str,
        *,
        next_attempt_at: str,
        now: datetime | None = None,
    ) -> None:
        self._db.reschedule_review_queue_item(
            issue_ref,
            next_attempt_at=next_attempt_at,
            now=now,
        )

    def active_workers(self) -> list[SessionInfo]:
        return self._db.active_workers()

    def latest_session_for_worktree(self, worktree_path: str) -> SessionInfo | None:
        return self._db.latest_session_for_worktree(worktree_path)

    def update_session(self, session_id: str, fields: SessionUpdateFields) -> None:
        self._db.update_session(session_id, **fields)
