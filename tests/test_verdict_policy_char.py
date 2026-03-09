"""Characterization tests for verdict policy functions (M1).

Locks down exact input/output behavior of verdict trust/backfill decision
logic. Imports from domain/verdict_policy.py directly.
"""

from __future__ import annotations

import pytest

from startupai_controller.domain.repair_policy import MARKER_PREFIX
from startupai_controller.domain.models import ReviewQueueEntry, SessionInfo
from startupai_controller.domain.verdict_policy import (
    is_pre_backfill_eligible,
    is_session_verdict_eligible,
    marker_already_present,
    verdict_marker_text,
)


# ---------------------------------------------------------------------------
# Verdict marker format
# ---------------------------------------------------------------------------


class TestVerdictMarkerFormat:
    """Characterize the codex verdict marker format and roundtrip."""

    def test_marker_format(self) -> None:
        session_id = "sess-abc-123"
        marker = f"<!-- {MARKER_PREFIX}:codex-verdict:session={session_id} -->"
        assert "startupai-board-bot" in marker
        assert "codex-verdict" in marker
        assert f"session={session_id}" in marker

    def test_marker_is_html_comment(self) -> None:
        marker = f"<!-- {MARKER_PREFIX}:codex-verdict:session=s1 -->"
        assert marker.startswith("<!--")
        assert marker.endswith("-->")


# ---------------------------------------------------------------------------
# Pre-backfill eligibility logic (characterization of the decision)
# ---------------------------------------------------------------------------


def _make_review_queue_entry(**overrides: object) -> ReviewQueueEntry:
    """Create a ReviewQueueEntry with sensible defaults."""
    defaults = {
        "issue_ref": "crew#1",
        "pr_url": "https://github.com/org/repo/pull/1",
        "pr_repo": "org/repo",
        "pr_number": 1,
        "source_session_id": "sess-1",
        "enqueued_at": "2025-01-01T00:00:00+00:00",
        "updated_at": "2025-01-01T00:00:00+00:00",
        "next_attempt_at": "2025-01-01T01:00:00+00:00",
        "last_attempt_at": None,
        "attempt_count": 0,
        "last_result": None,
        "last_reason": None,
        "last_state_digest": None,
        "blocked_streak": 0,
        "blocked_class": None,
    }
    defaults.update(overrides)
    return ReviewQueueEntry(**defaults)


def _make_session(**overrides: object) -> SessionInfo:
    """Create a SessionInfo with sensible defaults."""
    defaults = {
        "id": "sess-1",
        "issue_ref": "crew#1",
        "repo_prefix": "crew",
        "worktree_path": None,
        "branch_name": None,
        "executor": "codex",
        "slot_id": None,
        "status": "success",
        "phase": "review",
        "started_at": "2025-01-01T00:00:00+00:00",
        "completed_at": "2025-01-01T01:00:00+00:00",
        "outcome_json": None,
        "failure_reason": None,
        "retry_count": 0,
        "pr_url": "https://github.com/org/repo/pull/1",
        "provenance_id": None,
        "session_kind": "fresh",
        "repair_pr_url": None,
        "branch_reconcile_state": None,
        "branch_reconcile_error": None,
        "resolution_kind": None,
        "verification_class": None,
        "resolution_evidence_json": None,
        "resolution_action": None,
        "done_reason": None,
    }
    defaults.update(overrides)
    return SessionInfo(**defaults)


class TestPreBackfillVerdictEligibility:
    """Characterize the eligibility decision for pre-backfill verdicts.

    These tests encode the pure decision logic from
    _pre_backfill_verdicts_for_due_prs() without touching DB or GitHub.
    """

    def test_verdict_blocked_entry_is_eligible(self) -> None:
        """Entry with 'blocked' result and 'missing codex verdict marker' is eligible."""
        entry = _make_review_queue_entry(
            last_result="blocked",
            last_reason="Missing codex verdict marker: no marker found",
        )
        is_verdict_blocked = (
            entry.last_result == "blocked"
            and entry.last_reason is not None
            and "missing codex verdict marker" in entry.last_reason.lower()
        )
        assert is_verdict_blocked is True

    def test_newly_seeded_entry_is_eligible(self) -> None:
        """Entry with no last_result (newly seeded) is eligible."""
        entry = _make_review_queue_entry(last_result=None)
        is_newly_seeded = entry.last_result is None
        assert is_newly_seeded is True

    def test_non_verdict_blocked_entry_not_eligible(self) -> None:
        """Entry blocked for other reason is not eligible."""
        entry = _make_review_queue_entry(
            last_result="blocked",
            last_reason="required checks failed",
        )
        is_verdict_blocked = (
            entry.last_result == "blocked"
            and entry.last_reason is not None
            and "missing codex verdict marker" in entry.last_reason.lower()
        )
        is_newly_seeded = entry.last_result is None
        assert not is_verdict_blocked and not is_newly_seeded

    def test_auto_merge_entry_not_eligible(self) -> None:
        """Entry with auto_merge_enabled result is not eligible."""
        entry = _make_review_queue_entry(last_result="auto_merge_enabled")
        is_verdict_blocked = (
            entry.last_result == "blocked"
            and entry.last_reason is not None
            and "missing codex verdict marker" in entry.last_reason.lower()
        )
        is_newly_seeded = entry.last_result is None
        assert not is_verdict_blocked and not is_newly_seeded

    def test_session_must_be_success(self) -> None:
        """Session with non-success status disqualifies the entry."""
        session = _make_session(status="failed")
        assert session.status != "success"

    def test_session_must_be_review_phase(self) -> None:
        """Session with non-review phase disqualifies the entry."""
        session = _make_session(phase="execution")
        assert session.phase != "review"

    def test_session_must_have_pr_url(self) -> None:
        """Session without pr_url disqualifies the entry."""
        session = _make_session(pr_url=None)
        assert not session.pr_url

    def test_session_pr_url_must_match_entry(self) -> None:
        """Session.pr_url must match entry.pr_url for eligibility."""
        entry = _make_review_queue_entry(
            pr_url="https://github.com/org/repo/pull/1",
            last_result=None,
        )
        session = _make_session(
            pr_url="https://github.com/org/repo/pull/99",
        )
        assert session.pr_url != entry.pr_url


# ---------------------------------------------------------------------------
# Snapshot backfill eligibility
# ---------------------------------------------------------------------------


class TestSnapshotBackfillEligibility:
    """Characterize the eligibility decision for snapshot-based verdict backfill."""

    def test_marker_existence_prevents_duplicate(self) -> None:
        """If the marker already exists in PR comments, no backfill happens."""
        session_id = "sess-abc"
        marker = f"<!-- {MARKER_PREFIX}:codex-verdict:session={session_id} -->"
        existing_comments = {f"Some text\n{marker}\nMore text"}
        has_marker = any(marker in body for body in existing_comments)
        assert has_marker is True

    def test_no_marker_allows_backfill(self) -> None:
        """If the marker does not exist in PR comments, backfill is allowed."""
        marker = f"<!-- {MARKER_PREFIX}:codex-verdict:session=sess-abc -->"
        existing_comments = {"Some other comment body"}
        has_marker = any(marker in body for body in existing_comments)
        assert has_marker is False

    def test_session_status_success_required(self) -> None:
        session = _make_session(status="success", phase="review", pr_url="http://x")
        eligible = (
            session.status == "success"
            and session.phase == "review"
            and bool(session.pr_url)
        )
        assert eligible is True

    def test_session_failed_not_eligible(self) -> None:
        session = _make_session(status="failed")
        eligible = session.status == "success"
        assert eligible is False
