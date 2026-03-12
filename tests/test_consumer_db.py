"""Unit tests for consumer_db module.

Real SQLite with tmp_path -- no mocks needed.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from startupai_controller.consumer_db import (
    ConsumerDB,
    SessionInfo,
    VALID_SESSION_STATUSES,
)

# -- Fixtures -----------------------------------------------------------------


def _make_db(tmp_path: Path) -> ConsumerDB:
    """Create a ConsumerDB backed by a tmp_path SQLite file."""
    db_path = tmp_path / "test.db"
    return ConsumerDB(db_path=db_path)


def _make_memory_db() -> ConsumerDB:
    """Create a ConsumerDB with an in-memory connection factory."""
    conn = sqlite3.connect(":memory:")

    def factory() -> sqlite3.Connection:
        return conn

    return ConsumerDB(connection_factory=factory)


def _now() -> datetime:
    return datetime(2026, 3, 6, 12, 0, 0, tzinfo=timezone.utc)


# -- Schema initialization ---------------------------------------------------


class TestSchemaInit:
    def test_schema_creates_tables(self, tmp_path: Path) -> None:
        """Init creates leases and sessions tables."""
        db = _make_db(tmp_path)
        assert db.active_lease_count() == 0
        sessions = db.recent_sessions()
        assert sessions == []
        db.close()

    def test_schema_idempotent(self, tmp_path: Path) -> None:
        """Calling init multiple times is safe."""
        db = _make_db(tmp_path)
        db.active_lease_count()  # triggers init
        db.close()
        # Re-open same file
        db2 = _make_db(tmp_path)
        assert db2.active_lease_count() == 0
        db2.close()


# -- Lease acquire/release/conflict ------------------------------------------


class TestLeaseOps:
    def test_acquire_lease_success(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        result = db.acquire_lease("crew#84", "sess-001", now=_now())
        assert result is True
        assert db.active_lease_count() == 1
        db.close()

    def test_acquire_lease_conflict(self, tmp_path: Path) -> None:
        """Second lease for same issue_ref fails."""
        db = _make_db(tmp_path)
        db.acquire_lease("crew#84", "sess-001", now=_now())
        result = db.acquire_lease("crew#84", "sess-002", now=_now())
        assert result is False
        assert db.active_lease_count() == 1
        db.close()

    def test_acquire_lease_different_issues(self, tmp_path: Path) -> None:
        """Different issue_refs can be leased concurrently."""
        db = _make_db(tmp_path)
        db.acquire_lease("crew#84", "sess-001", now=_now())
        db.acquire_lease("crew#85", "sess-002", now=_now())
        assert db.active_lease_count() == 2
        db.close()

    def test_release_lease(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        db.acquire_lease("crew#84", "sess-001", now=_now())
        assert db.active_lease_count() == 1
        db.release_lease("crew#84")
        assert db.active_lease_count() == 0
        db.close()

    def test_release_nonexistent_is_noop(self, tmp_path: Path) -> None:
        """Releasing an issue that has no lease does nothing."""
        db = _make_db(tmp_path)
        db.release_lease("crew#999")
        assert db.active_lease_count() == 0
        db.close()


# -- Global cap ---------------------------------------------------------------


class TestGlobalCap:
    def test_active_lease_count_empty(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        assert db.active_lease_count() == 0
        db.close()

    def test_active_lease_count_after_operations(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        db.acquire_lease("crew#84", "sess-001", now=_now())
        db.acquire_lease("crew#85", "sess-002", now=_now())
        assert db.active_lease_count() == 2
        db.release_lease("crew#84")
        assert db.active_lease_count() == 1
        db.close()


# -- Heartbeat + stale expiry ------------------------------------------------


class TestHeartbeat:
    def test_update_heartbeat(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        t0 = _now()
        db.acquire_lease("crew#84", "sess-001", now=t0)
        t1 = t0 + timedelta(minutes=5)
        db.update_heartbeat("crew#84", now=t1)
        # Not expired after 5 min with 1 hour max age
        expired = db.expire_stale_leases(3600, now=t1)
        assert expired == []
        assert db.active_lease_count() == 1
        db.close()

    def test_expire_stale_leases_removes_old(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        t0 = _now()
        sess_id = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", sess_id, now=t0)
        db.update_session(sess_id, status="running", started_at=t0.isoformat())

        t1 = t0 + timedelta(hours=2)
        expired = db.expire_stale_leases(3600, now=t1)
        assert "crew#84" in expired
        assert db.active_lease_count() == 0

        # Session marked timeout
        session = db.get_session(sess_id)
        assert session is not None
        assert session.status == "timeout"
        db.close()

    def test_expire_stale_leases_keeps_fresh(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        t0 = _now()
        db.acquire_lease("crew#84", "sess-001", now=t0)
        t1 = t0 + timedelta(minutes=30)
        expired = db.expire_stale_leases(3600, now=t1)
        assert expired == []
        assert db.active_lease_count() == 1
        db.close()

    def test_expire_mixed_fresh_and_stale(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        t0 = _now()
        sess1 = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", sess1, now=t0)
        db.update_session(sess1, status="running")

        # crew#85 acquired 1h50m later — only 10min old at check time
        t1 = t0 + timedelta(hours=1, minutes=50)
        sess2 = db.create_session("crew#85", "codex")
        db.acquire_lease("crew#85", sess2, now=t1)
        db.update_session(sess2, status="running")

        t2 = t0 + timedelta(hours=2)
        expired = db.expire_stale_leases(3600, now=t2)
        assert "crew#84" in expired
        assert "crew#85" not in expired
        assert db.active_lease_count() == 1
        db.close()

    def test_expire_only_marks_active_sessions(self, tmp_path: Path) -> None:
        """Already-completed sessions are not re-marked on lease expiry."""
        db = _make_db(tmp_path)
        t0 = _now()
        sess_id = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", sess_id, now=t0)
        db.update_session(sess_id, status="success", completed_at=t0.isoformat())

        t1 = t0 + timedelta(hours=2)
        expired = db.expire_stale_leases(3600, now=t1)
        assert "crew#84" in expired

        session = db.get_session(sess_id)
        assert session is not None
        assert session.status == "success"  # not overwritten to timeout
        db.close()

    def test_recover_interrupted_leases_marks_running_aborted(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        t0 = _now()
        sess_id = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", sess_id, now=t0)
        db.update_session(sess_id, status="running", started_at=t0.isoformat())

        recovered = db.recover_interrupted_leases(now=t0 + timedelta(minutes=1))
        assert recovered[0].issue_ref == "crew#84"
        assert recovered[0].session_status == "running"
        assert db.active_lease_count() == 0

        session = db.get_session(sess_id)
        assert session is not None
        assert session.status == "aborted"
        assert session.failure_reason == "interrupted_restart"
        assert session.completed_at is not None
        db.close()

    def test_recover_interrupted_leases_preserves_completed_status(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        t0 = _now()
        sess_id = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", sess_id, now=t0)
        db.update_session(sess_id, status="success", completed_at=t0.isoformat())

        recovered = db.recover_interrupted_leases(now=t0 + timedelta(minutes=1))
        assert recovered[0].issue_ref == "crew#84"
        assert recovered[0].session_status == "success"
        assert db.active_lease_count() == 0

        session = db.get_session(sess_id)
        assert session is not None
        assert session.status == "success"
        db.close()


# -- Session CRUD + retry counting -------------------------------------------


class TestSessionOps:
    def test_create_session(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        sess_id = db.create_session("crew#84", "codex")
        assert isinstance(sess_id, str)
        assert len(sess_id) == 12
        session = db.get_session(sess_id)
        assert session is not None
        assert session.issue_ref == "crew#84"
        assert session.executor == "codex"
        assert session.status == "pending"
        assert session.session_kind == "new_work"
        assert session.repair_pr_url is None
        assert session.retry_count == 0
        db.close()

    def test_create_repair_session(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        sess_id = db.create_session(
            "crew#84",
            "codex",
            session_kind="repair",
            repair_pr_url="https://github.com/O/R/pull/10",
        )
        session = db.get_session(sess_id)
        assert session is not None
        assert session.session_kind == "repair"
        assert session.repair_pr_url == "https://github.com/O/R/pull/10"
        db.close()


class TestReviewQueue:
    def test_enqueue_and_get_review_item(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        now = _now()

        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/200",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=200,
            source_session_id="sess-123",
            now=now,
        )

        entry = db.get_review_queue_item("crew#84")
        assert entry is not None
        assert entry.issue_ref == "crew#84"
        assert entry.pr_number == 200
        assert entry.attempt_count == 0
        assert entry.last_result is None
        db.close()

    def test_due_review_items_and_attempt_updates(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        now = _now()
        later = now + timedelta(minutes=10)

        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/200",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=200,
            now=now,
        )
        db.enqueue_review_item(
            "crew#85",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/201",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=201,
            next_attempt_at=later.isoformat(),
            now=now,
        )

        due = db.list_due_review_queue_items(now=now + timedelta(minutes=1))
        assert [entry.issue_ref for entry in due] == ["crew#84"]
        assert db.review_queue_count() == 2
        assert db.due_review_queue_count(now=now + timedelta(minutes=1)) == 1

        db.update_review_queue_item(
            "crew#84",
            next_attempt_at=later.isoformat(),
            last_result="blocked",
            last_reason="checks pending",
            last_state_digest="digest-1",
            now=now + timedelta(minutes=2),
        )
        updated = db.get_review_queue_item("crew#84")
        assert updated is not None
        assert updated.attempt_count == 1
        assert updated.last_result == "blocked"
        assert updated.last_reason == "checks pending"
        assert updated.last_state_digest == "digest-1"
        assert updated.next_attempt_at == later.isoformat()
        db.close()

    def test_reschedule_review_queue_item_does_not_increment_attempts(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        now = _now()
        later = now + timedelta(minutes=10)

        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/200",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=200,
            now=now,
        )
        db.update_review_queue_item(
            "crew#84",
            next_attempt_at=later.isoformat(),
            last_result="blocked",
            last_reason="checks pending",
            last_state_digest="digest-1",
            now=now + timedelta(minutes=1),
        )

        wake_at = (now + timedelta(minutes=2)).isoformat()
        db.reschedule_review_queue_item(
            "crew#84",
            next_attempt_at=wake_at,
            now=now + timedelta(minutes=2),
        )

        entry = db.get_review_queue_item("crew#84")
        assert entry is not None
        assert entry.attempt_count == 1
        assert entry.last_result == "blocked"
        assert entry.last_reason == "checks pending"
        assert entry.last_state_digest == "digest-1"
        assert entry.next_attempt_at == wake_at
        db.close()

    def test_refresh_same_pr_preserves_attempt_history(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        now = _now()
        later = now + timedelta(minutes=10)

        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/200",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=200,
            source_session_id="sess-123",
            now=now,
        )
        db.update_review_queue_item(
            "crew#84",
            next_attempt_at=later.isoformat(),
            last_result="blocked",
            last_reason="checks pending",
            last_state_digest="digest-1",
            now=now + timedelta(minutes=1),
        )

        refreshed_attempt = later + timedelta(minutes=5)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/200",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=200,
            source_session_id="sess-123",
            next_attempt_at=refreshed_attempt.isoformat(),
            now=now + timedelta(minutes=2),
        )

        entry = db.get_review_queue_item("crew#84")
        assert entry is not None
        assert entry.attempt_count == 1
        assert entry.last_result == "blocked"
        assert entry.last_reason == "checks pending"
        assert entry.last_state_digest == "digest-1"
        assert entry.next_attempt_at == refreshed_attempt.isoformat()
        assert entry.last_attempt_at == (now + timedelta(minutes=1)).isoformat()
        db.close()

    def test_new_pr_resets_attempt_history(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        now = _now()
        later = now + timedelta(minutes=10)

        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/200",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=200,
            source_session_id="sess-123",
            now=now,
        )
        db.update_review_queue_item(
            "crew#84",
            next_attempt_at=later.isoformat(),
            last_result="blocked",
            last_reason="checks pending",
            now=now + timedelta(minutes=1),
        )

        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/201",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=201,
            source_session_id="sess-456",
            now=now + timedelta(minutes=2),
        )

        entry = db.get_review_queue_item("crew#84")
        assert entry is not None
        assert entry.pr_number == 201
        assert entry.source_session_id == "sess-456"
        assert entry.attempt_count == 0
        assert entry.last_result is None
        assert entry.last_reason is None
        assert entry.last_attempt_at is None
        db.close()

    def test_delete_review_item(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/200",
            pr_repo="StartupAI-site/startupai-crew",
            pr_number=200,
            now=_now(),
        )

        db.delete_review_queue_item("crew#84")

        assert db.get_review_queue_item("crew#84") is None
        assert db.review_queue_count() == 0
        db.close()

    def test_update_session_fields(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        sess_id = db.create_session("crew#84", "codex")
        db.update_session(
            sess_id,
            status="running",
            worktree_path="/tmp/wt",
            branch_name="feat/84-test",
            started_at="2026-03-06T12:00:00+00:00",
        )
        session = db.get_session(sess_id)
        assert session is not None
        assert session.status == "running"
        assert session.worktree_path == "/tmp/wt"
        assert session.branch_name == "feat/84-test"
        db.close()

    def test_update_session_completed(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        sess_id = db.create_session("crew#84", "codex")
        db.update_session(
            sess_id,
            status="success",
            completed_at="2026-03-06T12:30:00+00:00",
            outcome_json='{"outcome":"success","summary":"done"}',
            failure_reason=None,
            pr_url="https://github.com/StartupAI-site/startupai-crew/pull/100",
        )
        session = db.get_session(sess_id)
        assert session is not None
        assert session.status == "success"
        assert session.failure_reason is None
        assert session.pr_url is not None
        assert "pull/100" in session.pr_url
        db.close()

    def test_update_session_failure_reason(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        sess_id = db.create_session("crew#84", "codex")
        db.update_session(
            sess_id,
            status="failed",
            completed_at="2026-03-06T12:30:00+00:00",
            failure_reason="api_error",
            retry_count=2,
        )
        session = db.get_session(sess_id)
        assert session is not None
        assert session.failure_reason == "api_error"
        assert session.retry_count == 2
        db.close()

    def test_update_session_repair_fields(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        sess_id = db.create_session("crew#84", "codex")
        db.update_session(
            sess_id,
            session_kind="repair",
            repair_pr_url="https://github.com/O/R/pull/11",
        )
        session = db.get_session(sess_id)
        assert session is not None
        assert session.session_kind == "repair"
        assert session.repair_pr_url == "https://github.com/O/R/pull/11"
        db.close()

    def test_update_session_resolution_fields(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        sess_id = db.create_session("crew#84", "codex")
        db.update_session(
            sess_id,
            resolution_kind="already_on_main",
            verification_class="strong",
            resolution_evidence_json='{"pr_urls":["https://github.com/O/R/pull/10"]}',
            resolution_action="closed_as_already_resolved",
            done_reason="already_resolved",
        )
        session = db.get_session(sess_id)
        assert session is not None
        assert session.resolution_kind == "already_on_main"
        assert session.verification_class == "strong"
        assert session.resolution_action == "closed_as_already_resolved"
        assert session.done_reason == "already_resolved"
        db.close()

    def test_get_session_not_found(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        assert db.get_session("nonexistent") is None
        db.close()

    def test_count_retries(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        db.create_session("crew#84", "codex")
        assert db.count_retries("crew#84") == 0

        s1 = db.create_session("crew#84", "codex")
        db.update_session(s1, status="failed")
        assert db.count_retries("crew#84") == 0

        s2 = db.create_session("crew#84", "codex")
        db.update_session(s2, status="timeout")
        assert db.count_retries("crew#84") == 1

        s3 = db.create_session("crew#84", "codex")
        db.update_session(s3, status="failed", started_at="2026-03-06T12:00:00+00:00")
        assert db.count_retries("crew#84") == 2

        # Success doesn't count
        s4 = db.create_session("crew#84", "codex")
        db.update_session(s4, status="success")
        assert db.count_retries("crew#84") == 2
        db.close()

    def test_count_retries_different_issues(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        s1 = db.create_session("crew#84", "codex")
        db.update_session(s1, status="failed", started_at="2026-03-06T12:00:00+00:00")
        s2 = db.create_session("crew#85", "codex")
        db.update_session(s2, status="failed", started_at="2026-03-06T12:05:00+00:00")
        assert db.count_retries("crew#84") == 1
        assert db.count_retries("crew#85") == 1
        db.close()

    def test_count_retries_ignores_failed_sessions_without_start(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        s1 = db.create_session("crew#84", "codex")
        db.update_session(s1, status="failed")
        s2 = db.create_session("crew#84", "codex")
        db.update_session(s2, status="failed", started_at="2026-03-06T12:00:00+00:00")
        assert db.count_retries("crew#84") == 1
        db.close()


# -- Recent sessions ----------------------------------------------------------


class TestRecentSessions:
    def test_recent_sessions_ordering(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        s1 = db.create_session("crew#84", "codex")
        s2 = db.create_session("crew#85", "codex")
        s3 = db.create_session("crew#86", "codex")
        sessions = db.recent_sessions(limit=10)
        # Most recent first
        assert [s.id for s in sessions] == [s3, s2, s1]
        db.close()

    def test_recent_sessions_respects_limit(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        for i in range(5):
            db.create_session(f"crew#{80 + i}", "codex")
        sessions = db.recent_sessions(limit=2)
        assert len(sessions) == 2
        db.close()

    def test_latest_review_issue_refs_returns_latest_review_only(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        s1 = db.create_session("crew#84", "codex")
        db.update_session(s1, status="success", phase="review")
        s2 = db.create_session("crew#85", "codex")
        db.update_session(s2, status="success", phase="completed")
        s3 = db.create_session("crew#84", "codex")
        db.update_session(s3, status="failed", phase="blocked")

        review_refs = db.latest_review_issue_refs()

        assert review_refs == []

        s4 = db.create_session("crew#86", "codex")
        db.update_session(s4, status="success", phase="review")
        assert db.latest_review_issue_refs() == ["crew#86"]
        db.close()

    def test_active_workers_returns_leased_sessions(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        session_id = db.create_session(
            "crew#84",
            "codex",
            slot_id=2,
            session_kind="repair",
            repair_pr_url="https://github.com/O/R/pull/12",
        )
        db.acquire_lease("crew#84", session_id, slot_id=2, now=_now())
        db.update_session(session_id, status="running", slot_id=2)
        workers = db.active_workers()
        assert len(workers) == 1
        assert workers[0].issue_ref == "crew#84"
        assert workers[0].slot_id == 2
        assert workers[0].session_kind == "repair"
        assert workers[0].repair_pr_url == "https://github.com/O/R/pull/12"
        db.close()


class TestDeferredActions:
    def test_queue_deferred_action_dedupes_exact_duplicates(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        first = db.queue_deferred_action(
            "crew#84",
            "post_verdict_marker",
            {"pr_url": "https://github.com/O/R/pull/1", "session_id": "sess-1"},
            now=_now(),
        )
        second = db.queue_deferred_action(
            "crew#84",
            "post_verdict_marker",
            {"pr_url": "https://github.com/O/R/pull/1", "session_id": "sess-1"},
            now=_now(),
        )
        assert first == second
        assert db.deferred_action_count() == 1
        db.close()

    def test_queue_deferred_action_supersedes_older_status_action(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        db.queue_deferred_action(
            "crew#84",
            "set_status",
            {
                "issue_ref": "crew#84",
                "to_status": "In Progress",
                "from_statuses": ["Ready"],
            },
            now=_now(),
        )
        db.queue_deferred_action(
            "crew#84",
            "set_status",
            {
                "issue_ref": "crew#84",
                "to_status": "Ready",
                "from_statuses": ["In Progress"],
            },
            now=_now() + timedelta(seconds=1),
        )
        actions = db.list_deferred_actions()
        assert len(actions) == 1
        assert actions[0].payload["to_status"] == "Ready"
        db.close()

    def test_control_state_round_trip(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        db.set_control_value("degraded", "true")
        db.set_control_value("degraded_reason", "selection-error")
        assert db.get_control_value("degraded") == "true"
        assert db.control_state_snapshot()["degraded_reason"] == "selection-error"
        db.set_control_value("degraded_reason", None)
        assert db.get_control_value("degraded_reason") is None
        db.close()


class TestIssueContextCache:
    def test_issue_context_round_trip(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        now = _now()
        db.set_issue_context(
            "crew#84",
            owner="StartupAI-site",
            repo="startupai-crew",
            number=84,
            title="Test issue",
            body="Body",
            labels=["bug", "board"],
            issue_updated_at=now.isoformat(),
            fetched_at=now.isoformat(),
            expires_at=(now + timedelta(minutes=15)).isoformat(),
        )

        entry = db.get_issue_context("crew#84")
        assert entry is not None
        assert entry.issue_ref == "crew#84"
        assert entry.labels == ["bug", "board"]
        assert entry.title == "Test issue"
        db.close()


class TestMetrics:
    def test_metric_counts_since(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        now = _now()
        db.record_metric_event("candidate_selected", issue_ref="crew#84", now=now)
        db.record_metric_event("worker_durable_start", issue_ref="crew#84", now=now)

        counts = db.count_metric_events_since(now - timedelta(minutes=1))
        assert counts["candidate_selected"] == 1
        assert counts["worker_durable_start"] == 1
        db.close()

    def test_metric_events_since_filters_and_orders(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        now = _now()
        db.record_metric_event(
            "cycle_observation", payload={"ready_for_executor": 2}, now=now
        )
        db.record_metric_event(
            "worker_durable_start",
            issue_ref="crew#84",
            now=now + timedelta(minutes=5),
        )
        db.record_metric_event(
            "cycle_observation",
            payload={"ready_for_executor": 4},
            now=now + timedelta(minutes=10),
        )

        events = db.metric_events_since(
            now + timedelta(minutes=1),
            event_types=("cycle_observation",),
        )

        assert len(events) == 1
        assert events[0].event_type == "cycle_observation"
        assert events[0].payload["ready_for_executor"] == 4
        db.close()


# -- Connection factory DI ---------------------------------------------------


class TestConnectionFactoryDI:
    def test_memory_db_via_factory(self) -> None:
        """In-memory DB works via connection_factory."""
        db = _make_memory_db()
        sess_id = db.create_session("crew#84", "codex")
        session = db.get_session(sess_id)
        assert session is not None
        assert session.issue_ref == "crew#84"

    def test_factory_overrides_path(self, tmp_path: Path) -> None:
        """connection_factory takes precedence over db_path."""
        conn = sqlite3.connect(":memory:")
        db = ConsumerDB(
            db_path=tmp_path / "should-not-exist.db",
            connection_factory=lambda: conn,
        )
        db.create_session("crew#84", "codex")
        assert not (tmp_path / "should-not-exist.db").exists()


# -- Invalid field rejection --------------------------------------------------


class TestInvalidFields:
    def test_update_session_rejects_invalid_field(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        sess_id = db.create_session("crew#84", "codex")
        with pytest.raises(ValueError, match="Invalid session fields"):
            db.update_session(sess_id, bogus_field="bad")
        db.close()

    def test_update_session_rejects_invalid_status(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        sess_id = db.create_session("crew#84", "codex")
        with pytest.raises(ValueError, match="Invalid session status"):
            db.update_session(sess_id, status="bogus")
        db.close()

    def test_create_session_rejects_invalid_kind(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        with pytest.raises(ValueError, match="Invalid session kind"):
            db.create_session("crew#84", "codex", session_kind="bogus")
        db.close()

    def test_update_session_rejects_invalid_kind(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        sess_id = db.create_session("crew#84", "codex")
        with pytest.raises(ValueError, match="Invalid session kind"):
            db.update_session(sess_id, session_kind="bogus")
        db.close()


# -- Requeue cycle tracking -----------------------------------------------


class TestRequeueCycleTracking:
    def test_get_requeue_state_returns_zero_when_absent(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        count, pr_url = db.get_requeue_state("crew#88")
        assert count == 0
        assert pr_url is None
        db.close()

    def test_increment_requeue_count_stores_pr_url(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        new_count = db.increment_requeue_count(
            "crew#88", "https://github.com/o/r/pull/42"
        )
        assert new_count == 1
        count, pr_url = db.get_requeue_state("crew#88")
        assert count == 1
        assert pr_url == "https://github.com/o/r/pull/42"
        new_count2 = db.increment_requeue_count(
            "crew#88", "https://github.com/o/r/pull/42"
        )
        assert new_count2 == 2
        db.close()

    def test_reset_requeue_count(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        db.increment_requeue_count("crew#88", "https://github.com/o/r/pull/42")
        db.increment_requeue_count("crew#88", "https://github.com/o/r/pull/42")
        count, _ = db.get_requeue_state("crew#88")
        assert count == 2
        db.reset_requeue_count("crew#88")
        count, pr_url = db.get_requeue_state("crew#88")
        assert count == 0
        assert pr_url is None
        db.close()


# -- Review queue blocked_streak + blocked_class columns -------------------


class TestReviewQueueBlockedStreakColumns:
    def test_blocked_streak_defaults_to_zero(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/o/r/pull/1",
            pr_repo="o/r",
            pr_number=1,
        )
        entry = db.get_review_queue_item("crew#84")
        assert entry is not None
        assert entry.blocked_streak == 0
        assert entry.blocked_class is None
        db.close()

    def test_update_review_queue_item_writes_streak_and_class(
        self, tmp_path: Path
    ) -> None:
        db = _make_db(tmp_path)
        now = _now()
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/o/r/pull/1",
            pr_repo="o/r",
            pr_number=1,
            now=now,
        )
        db.update_review_queue_item(
            "crew#84",
            next_attempt_at=(now + datetime.resolution).isoformat(),
            last_result="blocked",
            last_reason="required checks failed",
            blocked_streak=3,
            blocked_class="failed_checks",
            now=now,
        )
        entry = db.get_review_queue_item("crew#84")
        assert entry is not None
        assert entry.blocked_streak == 3
        assert entry.blocked_class == "failed_checks"
        db.close()

    def test_update_review_queue_item_clears_streak(self, tmp_path: Path) -> None:
        db = _make_db(tmp_path)
        now = _now()
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/o/r/pull/1",
            pr_repo="o/r",
            pr_number=1,
            now=now,
        )
        db.update_review_queue_item(
            "crew#84",
            next_attempt_at=(now + datetime.resolution).isoformat(),
            last_result="blocked",
            blocked_streak=5,
            blocked_class="transient",
            now=now,
        )
        db.update_review_queue_item(
            "crew#84",
            next_attempt_at=(now + datetime.resolution).isoformat(),
            last_result="auto_merge_enabled",
            blocked_streak=0,
            blocked_class=None,
            now=now,
        )
        entry = db.get_review_queue_item("crew#84")
        assert entry is not None
        assert entry.blocked_streak == 0
        assert entry.blocked_class is None
        db.close()

    def test_enqueue_review_item_resets_blocked_streak_on_pr_change(
        self, tmp_path: Path
    ) -> None:
        """PR replacement via enqueue_review_item must clear blocked_streak/blocked_class."""
        db = _make_db(tmp_path)
        now = _now()
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/o/r/pull/1",
            pr_repo="o/r",
            pr_number=1,
            source_session_id="sess-1",
            now=now,
        )
        # Simulate blocked state accumulation
        db.update_review_queue_item(
            "crew#84",
            next_attempt_at=(now + datetime.resolution).isoformat(),
            last_result="blocked",
            last_reason="required checks failed",
            blocked_streak=4,
            blocked_class="failed_checks",
            now=now,
        )
        entry = db.get_review_queue_item("crew#84")
        assert entry is not None
        assert entry.blocked_streak == 4

        # PR replacement triggers reset path (pr_url changed)
        db.enqueue_review_item(
            "crew#84",
            pr_url="https://github.com/o/r/pull/2",
            pr_repo="o/r",
            pr_number=2,
            source_session_id="sess-2",
            now=now,
        )
        updated = db.get_review_queue_item("crew#84")
        assert updated is not None
        assert updated.pr_url == "https://github.com/o/r/pull/2"
        assert updated.blocked_streak == 0
        assert updated.blocked_class is None
        db.close()
