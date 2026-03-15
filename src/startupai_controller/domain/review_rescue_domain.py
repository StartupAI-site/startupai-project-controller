"""Domain rescue policy for review queue decisions."""

from __future__ import annotations

from startupai_controller.domain.models import (
    ReviewBlockerKind,
    ReviewRescueDecision,
    ReviewRescueStatus,
    ReviewRetryClass,
    ReviewSnapshot,
    ReviewTerminalReason,
)


def normalize_check_names(
    names: frozenset[str] | set[str] | tuple[str, ...],
) -> tuple[str, ...]:
    """Return stable required-check names for persistence and comparisons."""
    return tuple(sorted({name.strip() for name in names if name and name.strip()}))


def serialize_blocker_reason(
    blocker: ReviewBlockerKind,
    *,
    detail: str | None = None,
) -> str:
    """Project a blocker kind into the stable persisted reason string."""
    return (
        {
            ReviewBlockerKind.AUTO_MERGE_PENDING_VERIFICATION: (
                "auto-merge-pending-verification"
            ),
            ReviewBlockerKind.BEHIND_BRANCH_UPDATE_REQUIRED: (
                "behind-branch-update-required"
            ),
            ReviewBlockerKind.DRAFT_PR: "draft-pr",
            ReviewBlockerKind.MERGE_CONFLICT: "merge-conflict",
            ReviewBlockerKind.MERGE_STATE_UNSTABLE: "merge-state-unstable",
            ReviewBlockerKind.MISSING_COPILOT_REVIEW: "missing-copilot-review",
            ReviewBlockerKind.MISSING_CODEX_VERDICT_MARKER: (
                "missing-codex-verdict-marker"
            ),
            ReviewBlockerKind.PR_FETCH_FAILED: "pr-fetch-failed",
            ReviewBlockerKind.REQUIRED_CHECKS_FAILED: "required-checks-failed",
            ReviewBlockerKind.REQUIRED_CHECKS_PENDING: "required-checks-pending",
            ReviewBlockerKind.REVIEW_CHECKS_FAILED: "review-checks-failed",
            ReviewBlockerKind.REVIEW_CHECKS_PENDING: "review-checks-pending",
        }[blocker]
        if detail is None
        else detail
    )


def serialize_terminal_reason(reason: ReviewTerminalReason) -> str:
    """Project a terminal reason into the stable audit string."""
    return {
        ReviewTerminalReason.AUTO_MERGE_ALREADY_ENABLED: (
            "auto-merge-already-enabled-terminal"
        ),
        ReviewTerminalReason.AUTO_MERGE_ENABLED: "auto-merge-enabled",
        ReviewTerminalReason.COPILOT_REVIEW_TIMEOUT_FALLBACK: (
            "copilot-review-timeout-fallback"
        ),
        ReviewTerminalReason.PULL_REQUEST_NO_LONGER_OPEN: "pr-no-longer-open",
    }[reason]


def review_retry_class(blocker: ReviewBlockerKind) -> ReviewRetryClass:
    """Return the retry class for an active blocker."""
    return {
        ReviewBlockerKind.AUTO_MERGE_PENDING_VERIFICATION: ReviewRetryClass.AUTOMERGE,
        ReviewBlockerKind.BEHIND_BRANCH_UPDATE_REQUIRED: ReviewRetryClass.TRANSIENT,
        ReviewBlockerKind.DRAFT_PR: ReviewRetryClass.STABLE,
        ReviewBlockerKind.MERGE_CONFLICT: ReviewRetryClass.MANUAL,
        ReviewBlockerKind.MERGE_STATE_UNSTABLE: ReviewRetryClass.TRANSIENT,
        ReviewBlockerKind.MISSING_COPILOT_REVIEW: ReviewRetryClass.STABLE,
        ReviewBlockerKind.MISSING_CODEX_VERDICT_MARKER: ReviewRetryClass.STABLE,
        ReviewBlockerKind.PR_FETCH_FAILED: ReviewRetryClass.INFRASTRUCTURE,
        ReviewBlockerKind.REQUIRED_CHECKS_FAILED: ReviewRetryClass.DEFAULT,
        ReviewBlockerKind.REQUIRED_CHECKS_PENDING: ReviewRetryClass.TRANSIENT,
        ReviewBlockerKind.REVIEW_CHECKS_FAILED: ReviewRetryClass.DEFAULT,
        ReviewBlockerKind.REVIEW_CHECKS_PENDING: ReviewRetryClass.TRANSIENT,
    }[blocker]


def review_rescue_decision(snapshot: ReviewSnapshot) -> ReviewRescueDecision:
    """Return the domain rescue decision for one review snapshot."""
    if not snapshot.review_refs:
        return ReviewRescueDecision(
            status=ReviewRescueStatus.TERMINAL,
            terminal_reason=ReviewTerminalReason.PULL_REQUEST_NO_LONGER_OPEN,
        )

    pr_state = snapshot.gate_status.state.upper()
    if pr_state != "OPEN":
        return ReviewRescueDecision(
            status=ReviewRescueStatus.TERMINAL,
            terminal_reason=ReviewTerminalReason.PULL_REQUEST_NO_LONGER_OPEN,
            detail=f"state={snapshot.gate_status.state}",
        )

    if snapshot.gate_status.is_draft:
        blocker = ReviewBlockerKind.DRAFT_PR
        return ReviewRescueDecision(
            status=ReviewRescueStatus.ACTIVE,
            blocker=blocker,
            retry_class=review_retry_class(blocker),
        )

    if snapshot.gate_status.mergeable == "CONFLICTING":
        blocker = ReviewBlockerKind.MERGE_CONFLICT
        return ReviewRescueDecision(
            status=ReviewRescueStatus.ACTIVE,
            blocker=blocker,
            retry_class=review_retry_class(blocker),
        )

    if snapshot.gate_status.failed:
        blocker = ReviewBlockerKind.REQUIRED_CHECKS_FAILED
        return ReviewRescueDecision(
            status=ReviewRescueStatus.ACTIVE,
            blocker=blocker,
            retry_class=review_retry_class(blocker),
            required_check_names=normalize_check_names(snapshot.gate_status.failed),
        )

    if snapshot.gate_status.pending:
        blocker = ReviewBlockerKind.REQUIRED_CHECKS_PENDING
        return ReviewRescueDecision(
            status=ReviewRescueStatus.ACTIVE,
            blocker=blocker,
            retry_class=review_retry_class(blocker),
            required_check_names=normalize_check_names(snapshot.gate_status.pending),
        )

    if snapshot.gate_status.merge_state_status == "BEHIND":
        blocker = ReviewBlockerKind.BEHIND_BRANCH_UPDATE_REQUIRED
        return ReviewRescueDecision(
            status=ReviewRescueStatus.ACTIVE,
            blocker=blocker,
            retry_class=review_retry_class(blocker),
        )

    if snapshot.gate_status.mergeable not in {"MERGEABLE", "UNKNOWN"}:
        blocker = ReviewBlockerKind.MERGE_STATE_UNSTABLE
        return ReviewRescueDecision(
            status=ReviewRescueStatus.ACTIVE,
            blocker=blocker,
            retry_class=review_retry_class(blocker),
            detail=f"mergeable={snapshot.gate_status.mergeable}",
        )

    if not snapshot.copilot_review_present:
        blocker = ReviewBlockerKind.MISSING_COPILOT_REVIEW
        return ReviewRescueDecision(
            status=ReviewRescueStatus.ACTIVE,
            blocker=blocker,
            retry_class=review_retry_class(blocker),
        )

    if snapshot.codex_gate_code != 0:
        message = snapshot.codex_gate_message or ""
        if "missing codex verdict marker" in message.lower():
            blocker = ReviewBlockerKind.MISSING_CODEX_VERDICT_MARKER
        else:
            blocker = ReviewBlockerKind.REVIEW_CHECKS_FAILED
        return ReviewRescueDecision(
            status=ReviewRescueStatus.ACTIVE,
            blocker=blocker,
            retry_class=review_retry_class(blocker),
            detail=message or None,
        )

    if snapshot.rescue_failed:
        blocker = ReviewBlockerKind.REVIEW_CHECKS_FAILED
        return ReviewRescueDecision(
            status=ReviewRescueStatus.ACTIVE,
            blocker=blocker,
            retry_class=review_retry_class(blocker),
            detail=(
                f"review checks failed {list(normalize_check_names(snapshot.rescue_failed))}"
            ),
        )

    if snapshot.rescue_pending or snapshot.rescue_missing:
        blocker = ReviewBlockerKind.REVIEW_CHECKS_PENDING
        waiting = normalize_check_names(
            snapshot.rescue_pending | snapshot.rescue_missing
        )
        return ReviewRescueDecision(
            status=ReviewRescueStatus.ACTIVE,
            blocker=blocker,
            retry_class=review_retry_class(blocker),
            detail=f"review checks pending {list(waiting)}",
        )

    if snapshot.gate_status.auto_merge_enabled:
        return ReviewRescueDecision(
            status=ReviewRescueStatus.TERMINAL,
            terminal_reason=ReviewTerminalReason.AUTO_MERGE_ALREADY_ENABLED,
        )

    return ReviewRescueDecision(status=ReviewRescueStatus.ACTIVE)
