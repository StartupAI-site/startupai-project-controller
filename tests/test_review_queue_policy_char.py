"""Characterization tests for review-queue policy functions (M1).

Locks down exact input/output behavior of pure policy functions.
Imports from domain/review_queue_policy.py directly.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from startupai_controller.domain.review_queue_policy import (
    DEFAULT_REVIEW_QUEUE_RETRY_SECONDS,
    ESCALATION_CEILING_AUTOMERGE,
    ESCALATION_CEILING_DEFAULT,
    ESCALATION_CEILING_FAILED,
    ESCALATION_CEILING_STABLE,
    ESCALATION_CEILING_TRANSIENT,
    MAX_REQUEUE_CYCLES,
    RETRYABLE_FAILURE_REASONS,
    REVIEW_QUEUE_AUTOMERGE_RETRY_SECONDS,
    REVIEW_QUEUE_FAILED_RETRY_SECONDS,
    REVIEW_QUEUE_PENDING_AUTOMERGE_RETRY_SECONDS,
    REVIEW_QUEUE_PENDING_RETRY_SECONDS,
    REVIEW_QUEUE_SKIPPED_RETRY_SECONDS,
    REVIEW_QUEUE_STABLE_BLOCKED_RETRY_SECONDS,
    blocked_streak_needs_escalation as _blocked_streak_needs_escalation,
    blocker_class as _blocker_class,
    effective_retry_backoff as _effective_retry_backoff,
    escalation_ceiling_for_blocker_class as _escalation_ceiling_for_blocker_class,
    requeue_or_escalate as _requeue_or_escalate,
    is_retryable_failure_reason as _is_retryable_failure_reason,
    parse_iso8601_timestamp as _parse_iso8601_timestamp,
    retry_delay_seconds as _retry_delay_seconds,
    review_queue_retry_seconds_for_blocked_reason as _review_queue_retry_seconds_for_blocked_reason,
    review_queue_retry_seconds_for_partial_failure as _review_queue_retry_seconds_for_partial_failure,
    review_queue_retry_seconds_for_result as _review_queue_retry_seconds_for_result,
    review_queue_retry_seconds_for_skipped_reason as _review_queue_retry_seconds_for_skipped_reason,
)


# ---------------------------------------------------------------------------
# _blocker_class
# ---------------------------------------------------------------------------


class TestBlockerClass:
    """Characterize _blocker_class classification."""

    def test_automerge_pending_verification(self) -> None:
        assert _blocker_class("Auto-merge pending verification") == "automerge"

    def test_automerge_case_insensitive(self) -> None:
        assert _blocker_class("  AUTO-MERGE PENDING VERIFICATION  ") == "automerge"

    def test_required_checks_pending(self) -> None:
        assert _blocker_class("required checks pending") == "transient"

    def test_review_checks_pending(self) -> None:
        assert _blocker_class("review checks pending") == "transient"

    def test_required_checks_failed(self) -> None:
        assert _blocker_class("required checks failed") == "failed_checks"

    def test_review_checks_failed(self) -> None:
        assert _blocker_class("review checks failed") == "failed_checks"

    def test_mergeable_conflicting(self) -> None:
        assert _blocker_class("mergeable=conflicting") == "failed_checks"

    def test_automerge_not_enabled(self) -> None:
        assert _blocker_class("automerge-not-enabled") == "failed_checks"

    def test_missing_codex_verdict_marker(self) -> None:
        assert _blocker_class("Missing codex verdict marker") == "stable"

    def test_missing_copilot_review(self) -> None:
        assert _blocker_class("missing-copilot-review") == "stable"

    def test_draft_pr(self) -> None:
        assert _blocker_class("draft-pr") == "stable"

    def test_state_prefix(self) -> None:
        assert _blocker_class("state=CLOSED") == "stable"

    def test_unknown_reason(self) -> None:
        assert _blocker_class("something unexpected") == "default"

    def test_empty_string(self) -> None:
        assert _blocker_class("") == "default"

    def test_whitespace_only(self) -> None:
        assert _blocker_class("   ") == "default"

    def test_partial_match_not_anchored(self) -> None:
        # "required checks pending" in longer string still matches
        assert _blocker_class("foo required checks pending bar") == "transient"


# ---------------------------------------------------------------------------
# _escalation_ceiling_for_blocker_class
# ---------------------------------------------------------------------------


class TestEscalationCeiling:
    """Characterize _escalation_ceiling_for_blocker_class."""

    def test_transient(self) -> None:
        assert _escalation_ceiling_for_blocker_class("transient") == ESCALATION_CEILING_TRANSIENT

    def test_failed_checks(self) -> None:
        assert _escalation_ceiling_for_blocker_class("failed_checks") == ESCALATION_CEILING_FAILED

    def test_stable(self) -> None:
        assert _escalation_ceiling_for_blocker_class("stable") == ESCALATION_CEILING_STABLE

    def test_automerge(self) -> None:
        assert _escalation_ceiling_for_blocker_class("automerge") == ESCALATION_CEILING_AUTOMERGE

    def test_default(self) -> None:
        assert _escalation_ceiling_for_blocker_class("default") == ESCALATION_CEILING_DEFAULT

    def test_unknown_fallback(self) -> None:
        assert _escalation_ceiling_for_blocker_class("unknown_class") == ESCALATION_CEILING_DEFAULT

    def test_empty_string_fallback(self) -> None:
        assert _escalation_ceiling_for_blocker_class("") == ESCALATION_CEILING_DEFAULT


# ---------------------------------------------------------------------------
# _review_queue_retry_seconds_for_blocked_reason
# ---------------------------------------------------------------------------


class TestRetrySecondsForBlockedReason:
    """Characterize _review_queue_retry_seconds_for_blocked_reason."""

    def test_automerge_pending_verification(self) -> None:
        assert (
            _review_queue_retry_seconds_for_blocked_reason("Auto-merge pending verification")
            == REVIEW_QUEUE_PENDING_AUTOMERGE_RETRY_SECONDS
        )

    def test_required_checks_pending(self) -> None:
        assert (
            _review_queue_retry_seconds_for_blocked_reason("required checks pending")
            == REVIEW_QUEUE_PENDING_RETRY_SECONDS
        )

    def test_review_checks_pending(self) -> None:
        assert (
            _review_queue_retry_seconds_for_blocked_reason("review checks pending")
            == REVIEW_QUEUE_PENDING_RETRY_SECONDS
        )

    def test_required_checks_failed(self) -> None:
        assert (
            _review_queue_retry_seconds_for_blocked_reason("required checks failed")
            == REVIEW_QUEUE_FAILED_RETRY_SECONDS
        )

    def test_review_checks_failed(self) -> None:
        assert (
            _review_queue_retry_seconds_for_blocked_reason("review checks failed")
            == REVIEW_QUEUE_FAILED_RETRY_SECONDS
        )

    def test_mergeable_conflicting(self) -> None:
        assert (
            _review_queue_retry_seconds_for_blocked_reason("mergeable=conflicting")
            == REVIEW_QUEUE_FAILED_RETRY_SECONDS
        )

    def test_automerge_not_enabled(self) -> None:
        assert (
            _review_queue_retry_seconds_for_blocked_reason("automerge-not-enabled")
            == REVIEW_QUEUE_FAILED_RETRY_SECONDS
        )

    def test_missing_codex_verdict_marker(self) -> None:
        assert (
            _review_queue_retry_seconds_for_blocked_reason("Missing codex verdict marker")
            == REVIEW_QUEUE_STABLE_BLOCKED_RETRY_SECONDS
        )

    def test_missing_copilot_review(self) -> None:
        assert (
            _review_queue_retry_seconds_for_blocked_reason("missing-copilot-review")
            == REVIEW_QUEUE_STABLE_BLOCKED_RETRY_SECONDS
        )

    def test_draft_pr(self) -> None:
        assert (
            _review_queue_retry_seconds_for_blocked_reason("draft-pr")
            == REVIEW_QUEUE_STABLE_BLOCKED_RETRY_SECONDS
        )

    def test_state_prefix(self) -> None:
        assert (
            _review_queue_retry_seconds_for_blocked_reason("state=CLOSED")
            == REVIEW_QUEUE_STABLE_BLOCKED_RETRY_SECONDS
        )

    def test_unknown_falls_to_default(self) -> None:
        assert (
            _review_queue_retry_seconds_for_blocked_reason("something unknown")
            == DEFAULT_REVIEW_QUEUE_RETRY_SECONDS
        )


# ---------------------------------------------------------------------------
# _review_queue_retry_seconds_for_skipped_reason
# ---------------------------------------------------------------------------


class TestRetrySecondsForSkippedReason:
    """Characterize _review_queue_retry_seconds_for_skipped_reason."""

    def test_auto_merge_already_enabled(self) -> None:
        assert (
            _review_queue_retry_seconds_for_skipped_reason("auto-merge-already-enabled")
            == REVIEW_QUEUE_AUTOMERGE_RETRY_SECONDS
        )

    def test_other_reason(self) -> None:
        assert (
            _review_queue_retry_seconds_for_skipped_reason("some-other-reason")
            == REVIEW_QUEUE_SKIPPED_RETRY_SECONDS
        )

    def test_empty_string(self) -> None:
        assert (
            _review_queue_retry_seconds_for_skipped_reason("")
            == REVIEW_QUEUE_SKIPPED_RETRY_SECONDS
        )


# ---------------------------------------------------------------------------
# _review_queue_retry_seconds_for_result
# ---------------------------------------------------------------------------


class TestRetrySecondsForResult:
    """Characterize _review_queue_retry_seconds_for_result."""

    def test_auto_merge_enabled(self) -> None:
        result = SimpleNamespace(
            auto_merge_enabled=True,
            rerun_checks=(),
            blocked_reason=None,
            skipped_reason=None,
        )
        assert _review_queue_retry_seconds_for_result(result) == REVIEW_QUEUE_AUTOMERGE_RETRY_SECONDS

    def test_rerun_checks(self) -> None:
        result = SimpleNamespace(
            auto_merge_enabled=False,
            rerun_checks=("check1",),
            blocked_reason=None,
            skipped_reason=None,
        )
        assert _review_queue_retry_seconds_for_result(result) == REVIEW_QUEUE_PENDING_RETRY_SECONDS

    def test_blocked_reason(self) -> None:
        result = SimpleNamespace(
            auto_merge_enabled=False,
            rerun_checks=(),
            blocked_reason="required checks failed",
            skipped_reason=None,
        )
        assert _review_queue_retry_seconds_for_result(result) == REVIEW_QUEUE_FAILED_RETRY_SECONDS

    def test_skipped_reason(self) -> None:
        result = SimpleNamespace(
            auto_merge_enabled=False,
            rerun_checks=(),
            blocked_reason=None,
            skipped_reason="auto-merge-already-enabled",
        )
        assert _review_queue_retry_seconds_for_result(result) == REVIEW_QUEUE_AUTOMERGE_RETRY_SECONDS

    def test_no_flags_returns_default(self) -> None:
        result = SimpleNamespace(
            auto_merge_enabled=False,
            rerun_checks=(),
            blocked_reason=None,
            skipped_reason=None,
        )
        assert _review_queue_retry_seconds_for_result(result) == DEFAULT_REVIEW_QUEUE_RETRY_SECONDS

    def test_auto_merge_takes_precedence_over_rerun(self) -> None:
        result = SimpleNamespace(
            auto_merge_enabled=True,
            rerun_checks=("check1",),
            blocked_reason="something",
            skipped_reason=None,
        )
        assert _review_queue_retry_seconds_for_result(result) == REVIEW_QUEUE_AUTOMERGE_RETRY_SECONDS


# ---------------------------------------------------------------------------
# _review_queue_retry_seconds_for_partial_failure
# ---------------------------------------------------------------------------


class TestRetrySecondsForPartialFailure:
    """Characterize _review_queue_retry_seconds_for_partial_failure.

    The domain function takes (rate_limit_cooldown_seconds, error_reason_code)
    where error_reason_code is a pre-classified code ("rate_limit" or None).
    """

    def test_rate_limit_error(self) -> None:
        result = _review_queue_retry_seconds_for_partial_failure(600, "rate_limit")
        assert result == max(600, DEFAULT_REVIEW_QUEUE_RETRY_SECONDS)

    def test_non_rate_limit_error(self) -> None:
        result = _review_queue_retry_seconds_for_partial_failure(600, "some_other")
        assert result == DEFAULT_REVIEW_QUEUE_RETRY_SECONDS

    def test_none_error(self) -> None:
        result = _review_queue_retry_seconds_for_partial_failure(600, None)
        assert result == DEFAULT_REVIEW_QUEUE_RETRY_SECONDS


# ---------------------------------------------------------------------------
# _is_retryable_failure_reason
# ---------------------------------------------------------------------------


class TestIsRetryableFailureReason:
    """Characterize _is_retryable_failure_reason."""

    @pytest.mark.parametrize("reason", sorted(RETRYABLE_FAILURE_REASONS))
    def test_retryable_reasons(self, reason: str) -> None:
        assert _is_retryable_failure_reason(reason) is True

    @pytest.mark.parametrize("reason", sorted(RETRYABLE_FAILURE_REASONS))
    def test_retryable_case_insensitive(self, reason: str) -> None:
        assert _is_retryable_failure_reason(reason.upper()) is True

    @pytest.mark.parametrize("reason", sorted(RETRYABLE_FAILURE_REASONS))
    def test_retryable_with_whitespace(self, reason: str) -> None:
        assert _is_retryable_failure_reason(f"  {reason}  ") is True

    def test_non_retryable_reason(self) -> None:
        assert _is_retryable_failure_reason("permanent_failure") is False

    def test_empty_string(self) -> None:
        assert _is_retryable_failure_reason("") is False

    def test_none(self) -> None:
        assert _is_retryable_failure_reason(None) is False


# ---------------------------------------------------------------------------
# _effective_retry_backoff
# ---------------------------------------------------------------------------


class TestEffectiveRetryBackoff:
    """Characterize _effective_retry_backoff.

    The domain function takes (base_seconds, max_seconds, config_base, config_max)
    where base_seconds/max_seconds are workflow overrides (or None).
    """

    def test_config_defaults(self) -> None:
        base, max_s = _effective_retry_backoff(None, None, config_base=30, config_max=300)
        assert base == 30
        assert max_s == 300

    def test_workflow_overrides(self) -> None:
        base, max_s = _effective_retry_backoff(10, 120, config_base=30, config_max=300)
        assert base == 10
        assert max_s == 120

    def test_workflow_none_fields_fallback_to_config(self) -> None:
        base, max_s = _effective_retry_backoff(None, None, config_base=30, config_max=300)
        assert base == 30
        assert max_s == 300

    def test_base_floor_at_1(self) -> None:
        base, max_s = _effective_retry_backoff(None, None, config_base=0, config_max=10)
        assert base == 1
        assert max_s == 10

    def test_max_at_least_base(self) -> None:
        base, max_s = _effective_retry_backoff(None, None, config_base=100, config_max=10)
        assert base == 100
        assert max_s >= base


# ---------------------------------------------------------------------------
# _retry_delay_seconds
# ---------------------------------------------------------------------------


class TestRetryDelaySeconds:
    """Characterize _retry_delay_seconds."""

    def test_zero_retry_count(self) -> None:
        assert _retry_delay_seconds(0, base_seconds=30, max_seconds=300) == 0

    def test_negative_retry_count(self) -> None:
        assert _retry_delay_seconds(-1, base_seconds=30, max_seconds=300) == 0

    def test_first_retry(self) -> None:
        assert _retry_delay_seconds(1, base_seconds=30, max_seconds=300) == 30

    def test_second_retry(self) -> None:
        assert _retry_delay_seconds(2, base_seconds=30, max_seconds=300) == 60

    def test_third_retry(self) -> None:
        assert _retry_delay_seconds(3, base_seconds=30, max_seconds=300) == 120

    def test_fourth_retry(self) -> None:
        assert _retry_delay_seconds(4, base_seconds=30, max_seconds=300) == 240

    def test_capped_at_max(self) -> None:
        assert _retry_delay_seconds(5, base_seconds=30, max_seconds=300) == 300

    def test_exponent_capped_at_10(self) -> None:
        # retry_count=12 → exponent=min(11,10)=10 → base * 1024, capped at max
        result = _retry_delay_seconds(12, base_seconds=1, max_seconds=999999)
        assert result == 1 * (1 << 10)  # 1024


# ---------------------------------------------------------------------------
# _parse_iso8601_timestamp
# ---------------------------------------------------------------------------


class TestParseIso8601Timestamp:
    """Characterize _parse_iso8601_timestamp."""

    def test_none(self) -> None:
        assert _parse_iso8601_timestamp(None) is None

    def test_empty_string(self) -> None:
        assert _parse_iso8601_timestamp("") is None

    def test_invalid_string(self) -> None:
        assert _parse_iso8601_timestamp("not-a-date") is None

    def test_utc_timestamp(self) -> None:
        result = _parse_iso8601_timestamp("2025-01-15T10:00:00+00:00")
        assert result is not None
        assert result.tzinfo is not None
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10

    def test_naive_timestamp_gets_utc(self) -> None:
        result = _parse_iso8601_timestamp("2025-01-15T10:00:00")
        assert result is not None
        assert result.tzinfo == timezone.utc

    def test_non_utc_converts_to_utc(self) -> None:
        result = _parse_iso8601_timestamp("2025-01-15T12:00:00+02:00")
        assert result is not None
        assert result.hour == 10  # 12:00 +02:00 → 10:00 UTC


# ---------------------------------------------------------------------------
# requeue_or_escalate
# ---------------------------------------------------------------------------


class TestRequeueOrEscalate:
    """Characterize requeue_or_escalate domain function."""

    def test_below_ceiling_returns_requeue(self) -> None:
        assert _requeue_or_escalate(0) == "requeue"

    def test_one_below_ceiling_returns_requeue(self) -> None:
        assert _requeue_or_escalate(MAX_REQUEUE_CYCLES - 1) == "requeue"

    def test_at_ceiling_returns_escalate(self) -> None:
        assert _requeue_or_escalate(MAX_REQUEUE_CYCLES) == "escalate"

    def test_above_ceiling_returns_escalate(self) -> None:
        assert _requeue_or_escalate(MAX_REQUEUE_CYCLES + 5) == "escalate"


# ---------------------------------------------------------------------------
# blocked_streak_needs_escalation
# ---------------------------------------------------------------------------


class TestBlockedStreakNeedsEscalation:
    """Characterize blocked_streak_needs_escalation domain function."""

    def test_first_block_no_escalation(self) -> None:
        new_class, new_streak, needs = _blocked_streak_needs_escalation(
            "required checks pending", 0, None,
        )
        assert new_class == "transient"
        assert new_streak == 1
        assert needs is False

    def test_same_class_increments_streak(self) -> None:
        _, streak, _ = _blocked_streak_needs_escalation(
            "required checks pending", 5, "transient",
        )
        assert streak == 6

    def test_different_class_resets_streak(self) -> None:
        new_class, streak, _ = _blocked_streak_needs_escalation(
            "required checks failed", 5, "transient",
        )
        assert new_class == "failed_checks"
        assert streak == 1

    def test_transient_escalation_at_ceiling(self) -> None:
        _, _, needs = _blocked_streak_needs_escalation(
            "required checks pending",
            ESCALATION_CEILING_TRANSIENT - 1,
            "transient",
        )
        assert needs is True

    def test_failed_escalation_at_ceiling(self) -> None:
        _, _, needs = _blocked_streak_needs_escalation(
            "required checks failed",
            ESCALATION_CEILING_FAILED - 1,
            "failed_checks",
        )
        assert needs is True

    def test_below_ceiling_no_escalation(self) -> None:
        _, _, needs = _blocked_streak_needs_escalation(
            "required checks failed", 1, "failed_checks",
        )
        assert needs is False
