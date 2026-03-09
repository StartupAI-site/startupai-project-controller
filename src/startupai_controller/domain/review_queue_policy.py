"""Review queue policy — blocker classification, escalation, retry backoff.

Pure policy module: NO GitHub, SQLite, subprocess, or logging imports.
All functions accept primitive or domain types only.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from startupai_controller.domain.models import SessionInfo


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_REVIEW_QUEUE_BATCH_SIZE = 5
DEFAULT_REVIEW_QUEUE_RETRY_SECONDS = 300
REVIEW_QUEUE_PENDING_RETRY_SECONDS = 120
REVIEW_QUEUE_FAILED_RETRY_SECONDS = 900
REVIEW_QUEUE_STABLE_BLOCKED_RETRY_SECONDS = 1800
REVIEW_QUEUE_AUTOMERGE_RETRY_SECONDS = 3600
REVIEW_QUEUE_PENDING_AUTOMERGE_RETRY_SECONDS = 120
REVIEW_QUEUE_SKIPPED_RETRY_SECONDS = 3600
MAX_REQUEUE_CYCLES = 3
REVIEW_QUEUE_STABLE_RESULTS = {
    "auto_merge_enabled",
    "blocked",
    "skipped",
    "partial_failure",
}
RETRYABLE_FAILURE_REASONS = {
    "api_error",
    "consumer_error",
    "timeout",
    "codex_error",
    "validation_failed",
    "pr_error",
}

# Escalation ceilings per blocker class
ESCALATION_CEILING_TRANSIENT = 12   # ~24 min at 2-min pending retry
ESCALATION_CEILING_FAILED = 6       # ~90 min at 15-min failed retry
ESCALATION_CEILING_STABLE = 4       # ~120 min at 30-min stable retry
ESCALATION_CEILING_AUTOMERGE = 3    # ~3 hr at 1-hr automerge retry
ESCALATION_CEILING_DEFAULT = 8      # ~40 min at 5-min default retry


# ---------------------------------------------------------------------------
# Blocker classification
# ---------------------------------------------------------------------------


def blocker_class(blocked_reason: str) -> str:
    """Classify a blocked_reason into a stable blocker class string."""
    normalized = blocked_reason.strip().lower()
    if "auto-merge pending verification" in normalized:
        return "automerge"
    if (
        "required checks pending" in normalized
        or "review checks pending" in normalized
    ):
        return "transient"
    if (
        "required checks failed" in normalized
        or "review checks failed" in normalized
        or "mergeable=conflicting" in normalized
        or normalized == "automerge-not-enabled"
    ):
        return "failed_checks"
    if (
        "missing codex verdict marker" in normalized
        or normalized == "missing-copilot-review"
        or normalized == "draft-pr"
        or normalized.startswith("state=")
    ):
        return "stable"
    return "default"


def escalation_ceiling_for_blocker_class(blocker_class_name: str) -> int:
    """Return the max blocked-streak before escalation for a given class."""
    return {
        "transient": ESCALATION_CEILING_TRANSIENT,
        "failed_checks": ESCALATION_CEILING_FAILED,
        "stable": ESCALATION_CEILING_STABLE,
        "automerge": ESCALATION_CEILING_AUTOMERGE,
        "default": ESCALATION_CEILING_DEFAULT,
    }.get(blocker_class_name, ESCALATION_CEILING_DEFAULT)


# ---------------------------------------------------------------------------
# Retry delay computation
# ---------------------------------------------------------------------------


def review_queue_retry_seconds_for_blocked_reason(blocked_reason: str) -> int:
    """Return the retry delay for a blocked review outcome."""
    normalized = blocked_reason.strip().lower()
    if "auto-merge pending verification" in normalized:
        return REVIEW_QUEUE_PENDING_AUTOMERGE_RETRY_SECONDS
    if (
        "required checks pending" in normalized
        or "review checks pending" in normalized
    ):
        return REVIEW_QUEUE_PENDING_RETRY_SECONDS
    if (
        "required checks failed" in normalized
        or "review checks failed" in normalized
        or "mergeable=conflicting" in normalized
        or normalized == "automerge-not-enabled"
    ):
        return REVIEW_QUEUE_FAILED_RETRY_SECONDS
    if (
        "missing codex verdict marker" in normalized
        or normalized == "missing-copilot-review"
        or normalized == "draft-pr"
        or normalized.startswith("state=")
    ):
        return REVIEW_QUEUE_STABLE_BLOCKED_RETRY_SECONDS
    return DEFAULT_REVIEW_QUEUE_RETRY_SECONDS


def review_queue_retry_seconds_for_skipped_reason(skipped_reason: str) -> int:
    """Return the retry delay for a skipped review outcome."""
    normalized = skipped_reason.strip().lower()
    if normalized == "auto-merge-already-enabled":
        return REVIEW_QUEUE_AUTOMERGE_RETRY_SECONDS
    return REVIEW_QUEUE_SKIPPED_RETRY_SECONDS


def review_queue_retry_seconds_for_result(result: Any) -> int:
    """Return the next-attempt delay for one review-queue result."""
    if result.auto_merge_enabled:
        return REVIEW_QUEUE_AUTOMERGE_RETRY_SECONDS
    if result.rerun_checks:
        return REVIEW_QUEUE_PENDING_RETRY_SECONDS
    if result.blocked_reason:
        return review_queue_retry_seconds_for_blocked_reason(result.blocked_reason)
    if result.skipped_reason:
        return review_queue_retry_seconds_for_skipped_reason(result.skipped_reason)
    return DEFAULT_REVIEW_QUEUE_RETRY_SECONDS


def review_queue_retry_seconds_for_partial_failure(
    rate_limit_cooldown_seconds: int,
    error_reason_code: str | None,
) -> int:
    """Return the retry delay after a partial review-queue failure.

    Accepts primitive parameters instead of ConsumerConfig — callers
    destructure the config before calling.
    """
    if error_reason_code == "rate_limit":
        return max(rate_limit_cooldown_seconds, DEFAULT_REVIEW_QUEUE_RETRY_SECONDS)
    return DEFAULT_REVIEW_QUEUE_RETRY_SECONDS


def is_retryable_failure_reason(reason: str | None) -> bool:
    """Return True when a failure reason should cool down and retry."""
    return (reason or "").strip().lower() in RETRYABLE_FAILURE_REASONS


def effective_retry_backoff(
    base_seconds: int | None,
    max_seconds: int | None,
    config_base: int,
    config_max: int,
) -> tuple[int, int]:
    """Return effective retry backoff (base, max) in seconds.

    Accepts primitive parameters. Callers destructure config/workflow
    before calling.
    """
    effective_base = base_seconds if base_seconds is not None else config_base
    effective_max = max_seconds if max_seconds is not None else config_max
    effective_base = max(1, effective_base)
    effective_max = max(effective_max, effective_base)
    return effective_base, effective_max


def retry_delay_seconds(
    retry_count: int,
    *,
    base_seconds: int,
    max_seconds: int,
) -> int:
    """Return bounded exponential backoff delay for a retry attempt count."""
    if retry_count <= 0:
        return 0
    exponent = min(retry_count - 1, 10)
    return min(max_seconds, base_seconds * (1 << exponent))


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------


def parse_iso8601_timestamp(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp if present."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Session retry
# ---------------------------------------------------------------------------


def session_retry_due_at(
    session: SessionInfo,
    *,
    base_seconds: int,
    max_seconds: int,
) -> datetime | None:
    """Return the next retry timestamp for a terminal session, if any."""
    if session.status not in {"failed", "timeout", "aborted", "error"}:
        return None
    completed_at = parse_iso8601_timestamp(session.completed_at)
    if completed_at is None:
        return None
    if session.failure_reason is None:
        if session.status not in {"failed", "timeout"}:
            return None
        retry_count = session.retry_count or 1
    else:
        if not is_retryable_failure_reason(session.failure_reason):
            return None
        retry_count = session.retry_count or 1
    delay_secs = retry_delay_seconds(
        retry_count,
        base_seconds=base_seconds,
        max_seconds=max_seconds,
    )
    if delay_secs <= 0:
        return None
    return completed_at + timedelta(seconds=delay_secs)
