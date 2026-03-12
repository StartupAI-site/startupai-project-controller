"""Characterization tests for repair policy functions (M1).

Locks down exact input/output behavior of repair/requeue pure functions.
Imports from domain/repair_policy.py directly.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from startupai_controller.domain.models import OpenPullRequestMatch
from startupai_controller.domain.repair_policy import (
    MARKER_PREFIX,
    consumer_provenance_marker as _consumer_provenance_marker,
    deterministic_branch_pattern as _deterministic_branch_pattern,
    extract_acceptance_criteria as _extract_acceptance_criteria,
    marker_for as _marker_for,
    parse_consumer_provenance as _parse_consumer_provenance,
    parse_pr_url,
)
from startupai_controller.domain.review_queue_policy import (
    MAX_REQUEUE_CYCLES,
    session_retry_due_at as _session_retry_due_at,
)
from startupai_controller.domain.models import SessionInfo

# ---------------------------------------------------------------------------
# _extract_acceptance_criteria
# ---------------------------------------------------------------------------


class TestExtractAcceptanceCriteria:
    """Characterize _extract_acceptance_criteria."""

    def test_empty_body(self) -> None:
        assert _extract_acceptance_criteria("") == ""

    def test_standard_heading(self) -> None:
        body = "## Acceptance Criteria\n- item one\n- item two\n## Other"
        result = _extract_acceptance_criteria(body)
        assert "item one" in result
        assert "item two" in result
        assert "Other" not in result

    def test_ac_abbreviation(self) -> None:
        body = "## AC\n- criteria here\n## Other"
        result = _extract_acceptance_criteria(body)
        assert "criteria here" in result

    def test_bold_format(self) -> None:
        body = "**Acceptance Criteria**\nsome criteria\n**Other**"
        result = _extract_acceptance_criteria(body)
        assert "some criteria" in result

    def test_case_insensitive(self) -> None:
        body = "## ACCEPTANCE CRITERIA\n- item\n## Other"
        result = _extract_acceptance_criteria(body)
        assert "item" in result

    def test_no_match_returns_full_body(self) -> None:
        body = "Just a regular body\nwith no headings"
        result = _extract_acceptance_criteria(body)
        assert result == body.strip()

    def test_heading_at_end_of_body(self) -> None:
        body = "## Acceptance Criteria\n- last item"
        result = _extract_acceptance_criteria(body)
        assert "last item" in result


# ---------------------------------------------------------------------------
# _deterministic_branch_pattern
# ---------------------------------------------------------------------------


class TestDeterministicBranchPattern:
    """Characterize _deterministic_branch_pattern."""

    def test_matches_canonical_branch(self) -> None:
        pattern = _deterministic_branch_pattern(88)
        assert pattern.match("feat/88-fix-something") is not None

    def test_rejects_wrong_number(self) -> None:
        pattern = _deterministic_branch_pattern(88)
        assert pattern.match("feat/99-fix-something") is None

    def test_rejects_wrong_prefix(self) -> None:
        pattern = _deterministic_branch_pattern(88)
        assert pattern.match("fix/88-fix-something") is None

    def test_rejects_no_description(self) -> None:
        pattern = _deterministic_branch_pattern(88)
        assert pattern.match("feat/88") is None

    def test_requires_lowercase_description(self) -> None:
        pattern = _deterministic_branch_pattern(88)
        assert pattern.match("feat/88-FixSomething") is None

    def test_allows_digits_in_description(self) -> None:
        pattern = _deterministic_branch_pattern(88)
        assert pattern.match("feat/88-add-v2-support") is not None


# ---------------------------------------------------------------------------
# _consumer_provenance_marker / _parse_consumer_provenance roundtrip
# ---------------------------------------------------------------------------


class TestConsumerProvenanceRoundtrip:
    """Characterize provenance marker build/parse roundtrip."""

    def test_roundtrip(self) -> None:
        marker = _consumer_provenance_marker(
            session_id="sess-123",
            issue_ref="crew#42",
            repo_prefix="crew",
            branch_name="feat/42-do-thing",
            executor="codex",
        )
        parsed = _parse_consumer_provenance(marker)
        assert parsed is not None
        assert parsed["session_id"] == "sess-123"
        assert parsed["issue_ref"] == "crew#42"
        assert parsed["repo_prefix"] == "crew"
        assert parsed["branch_name"] == "feat/42-do-thing"
        assert parsed["executor"] == "codex"

    def test_marker_is_html_comment(self) -> None:
        marker = _consumer_provenance_marker(
            session_id="s1",
            issue_ref="crew#1",
            repo_prefix="crew",
            branch_name="feat/1-test",
            executor="codex",
        )
        assert marker.startswith("<!-- ")
        assert marker.endswith(" -->")
        assert MARKER_PREFIX in marker

    def test_parse_returns_none_for_no_match(self) -> None:
        assert _parse_consumer_provenance("no marker here") is None

    def test_parse_returns_none_for_empty_string(self) -> None:
        assert _parse_consumer_provenance("") is None

    def test_marker_embedded_in_larger_text(self) -> None:
        marker = _consumer_provenance_marker(
            session_id="s1",
            issue_ref="crew#1",
            repo_prefix="crew",
            branch_name="feat/1-test",
            executor="codex",
        )
        text = f"Some PR body text.\n\n{marker}\n\nMore text."
        parsed = _parse_consumer_provenance(text)
        assert parsed is not None
        assert parsed["session_id"] == "s1"


class TestMarkerFor:
    """Characterize marker_for."""

    def test_builds_deterministic_marker(self) -> None:
        assert (
            _marker_for("consumer-result", "app#46")
            == f"<!-- {MARKER_PREFIX}:consumer-result:app#46 -->"
        )


# ---------------------------------------------------------------------------
# _session_retry_due_at
# ---------------------------------------------------------------------------


def _make_session(**overrides: object) -> SessionInfo:
    """Create a SessionInfo with sensible defaults."""
    defaults = {
        "id": "test-session",
        "issue_ref": "crew#1",
        "repo_prefix": "crew",
        "worktree_path": None,
        "branch_name": None,
        "executor": "codex",
        "slot_id": None,
        "status": "failed",
        "phase": "execution",
        "started_at": "2025-01-01T00:00:00+00:00",
        "completed_at": "2025-01-01T01:00:00+00:00",
        "outcome_json": None,
        "failure_reason": "api_error",
        "retry_count": 1,
        "pr_url": None,
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


class TestSessionRetryDueAt:
    """Characterize _session_retry_due_at."""

    def test_failed_with_retryable_reason(self) -> None:
        session = _make_session(
            status="failed",
            failure_reason="api_error",
            retry_count=1,
        )
        result = _session_retry_due_at(session, base_seconds=30, max_seconds=300)
        assert result is not None
        completed = datetime.fromisoformat(session.completed_at)
        assert result > completed

    def test_failed_with_non_retryable_reason(self) -> None:
        session = _make_session(
            status="failed",
            failure_reason="permanent_issue",
            retry_count=1,
        )
        result = _session_retry_due_at(session, base_seconds=30, max_seconds=300)
        assert result is None

    def test_failed_with_no_reason_still_retries(self) -> None:
        session = _make_session(
            status="failed",
            failure_reason=None,
            retry_count=1,
        )
        result = _session_retry_due_at(session, base_seconds=30, max_seconds=300)
        assert result is not None

    def test_timeout_with_no_reason_retries(self) -> None:
        session = _make_session(
            status="timeout",
            failure_reason=None,
            retry_count=1,
        )
        result = _session_retry_due_at(session, base_seconds=30, max_seconds=300)
        assert result is not None

    def test_success_status_not_retried(self) -> None:
        session = _make_session(status="success")
        result = _session_retry_due_at(session, base_seconds=30, max_seconds=300)
        assert result is None

    def test_running_status_not_retried(self) -> None:
        session = _make_session(status="running")
        result = _session_retry_due_at(session, base_seconds=30, max_seconds=300)
        assert result is None

    def test_no_completed_at(self) -> None:
        session = _make_session(
            status="failed",
            failure_reason="api_error",
            completed_at=None,
        )
        result = _session_retry_due_at(session, base_seconds=30, max_seconds=300)
        assert result is None

    def test_aborted_with_no_reason_not_retried(self) -> None:
        session = _make_session(
            status="aborted",
            failure_reason=None,
            retry_count=1,
        )
        result = _session_retry_due_at(session, base_seconds=30, max_seconds=300)
        assert result is None

    def test_error_with_retryable_reason(self) -> None:
        session = _make_session(
            status="error",
            failure_reason="timeout",
            retry_count=1,
        )
        result = _session_retry_due_at(session, base_seconds=30, max_seconds=300)
        assert result is not None

    def test_delay_increases_with_retry_count(self) -> None:
        session1 = _make_session(
            status="failed",
            failure_reason="api_error",
            retry_count=1,
        )
        session2 = _make_session(
            status="failed",
            failure_reason="api_error",
            retry_count=3,
        )
        result1 = _session_retry_due_at(session1, base_seconds=30, max_seconds=3000)
        result2 = _session_retry_due_at(session2, base_seconds=30, max_seconds=3000)
        assert result1 is not None
        assert result2 is not None
        completed = datetime.fromisoformat(session1.completed_at).replace(
            tzinfo=timezone.utc
        )
        delta1 = result1 - completed
        delta2 = result2 - completed
        assert delta2 > delta1


# ---------------------------------------------------------------------------
# MAX_REQUEUE_CYCLES constant
# ---------------------------------------------------------------------------


class TestMaxRequeueCycles:
    """Document the requeue ceiling constant."""

    def test_value(self) -> None:
        assert MAX_REQUEUE_CYCLES == 3


# ---------------------------------------------------------------------------
# parse_pr_url — pure URL extraction
# ---------------------------------------------------------------------------


class TestParsePrUrl:
    """Characterize parse_pr_url domain function."""

    def test_standard_url(self) -> None:
        result = parse_pr_url(
            "https://github.com/StartupAI-site/startupai-crew/pull/42"
        )
        assert result == ("StartupAI-site", "startupai-crew", 42)

    def test_empty_string(self) -> None:
        assert parse_pr_url("") is None

    def test_whitespace_only(self) -> None:
        assert parse_pr_url("   ") is None

    def test_no_match(self) -> None:
        assert parse_pr_url("not a url") is None

    def test_url_with_surrounding_text(self) -> None:
        result = parse_pr_url("PR: https://github.com/org/repo/pull/123 done")
        assert result == ("org", "repo", 123)

    def test_case_insensitive(self) -> None:
        result = parse_pr_url("https://GITHUB.COM/Org/Repo/pull/7")
        assert result == ("Org", "Repo", 7)

    def test_large_pr_number(self) -> None:
        result = parse_pr_url("https://github.com/o/r/pull/99999")
        assert result == ("o", "r", 99999)
