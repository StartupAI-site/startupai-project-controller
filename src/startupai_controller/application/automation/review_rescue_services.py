"""Small collaborators for review rescue orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.domain.models import (
    ReviewBlockerKind,
    ReviewRescueDecision,
    ReviewRescueResult,
    ReviewVerdictRecoverySweep,
    ReviewRetryClass,
    ReviewSnapshot,
    ReviewTerminalReason,
)
from startupai_controller.domain.repair_policy import (
    marker_for,
    parse_consumer_provenance,
)
from startupai_controller.domain.review_rescue_domain import (
    review_rescue_decision,
    serialize_blocker_reason,
    serialize_terminal_reason,
)
from startupai_controller.domain.review_queue_policy import parse_iso8601_timestamp
from startupai_controller.domain.verdict_policy import (
    is_pre_backfill_eligible,
    is_session_verdict_eligible,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort


def trusted_review_provenance(
    *,
    snapshot: ReviewSnapshot,
    automation_config: BoardAutomationConfig,
) -> dict[str, str] | None:
    """Return trusted provenance for a local review PR, else None."""
    author = snapshot.pr_author.strip().lower()
    if author not in automation_config.trusted_local_authors:
        return None
    provenance = parse_consumer_provenance(snapshot.pr_body or "")
    if provenance is None:
        return None
    issue_ref = provenance.get("issue_ref", "").strip()
    executor = provenance.get("executor", "").strip().lower()
    if issue_ref not in snapshot.review_refs:
        return None
    if executor not in automation_config.execution_authority_executors:
        return None
    return provenance


def _current_pr_url(context: "RescueReviewContext") -> str:
    return f"https://github.com/{context.pr_repo}/pull/{context.pr_number}"


def _eligible_verdict_session(
    *,
    store: SessionStorePort | None,
    issue_ref: str,
    pr_url: str,
    preferred_session_id: str | None = None,
):
    """Return the best eligible session for verdict backfill, else None."""
    if store is None:
        return None
    session = store.get_session(preferred_session_id) if preferred_session_id else None
    if session is not None and is_session_verdict_eligible(
        session_status=session.status,
        session_phase=session.phase,
        session_pr_url=session.pr_url,
        entry_pr_url=pr_url,
    ):
        return session
    session = store.latest_session_for_issue(issue_ref)
    if session is None:
        return None
    if not is_session_verdict_eligible(
        session_status=session.status,
        session_phase=session.phase,
        session_pr_url=session.pr_url,
        entry_pr_url=pr_url,
    ):
        return None
    return session


def _copilot_missing_since(
    *,
    context: "RescueReviewContext",
    issue_ref: str,
) -> str | None:
    if context.session_store is None:
        return None
    entry = context.session_store.get_review_queue_item(issue_ref)
    if entry is None:
        return None
    return entry.copilot_missing_since


def _copilot_fallback_comment_marker(
    *,
    pr_repo: str,
    pr_number: int,
) -> str:
    return marker_for("review-rescue-copilot-fallback", f"{pr_repo}#{pr_number}")


def post_copilot_fallback_comment(context: "RescueReviewContext") -> None:
    """Post the audit comment for a timed-out Copilot fallback once."""
    marker = _copilot_fallback_comment_marker(
        pr_repo=context.pr_repo,
        pr_number=context.pr_number,
    )
    if context.review_state_port.comment_exists(
        context.pr_repo,
        context.pr_number,
        marker,
    ):
        return
    body = "\n".join(
        [
            marker,
            "review-rescue: proceeding without Copilot review after timeout.",
            "reason: missing-copilot-review",
            f"pr: {context.pr_repo}#{context.pr_number}",
        ]
    )
    if not context.dry_run:
        context.board_port.post_issue_comment(
            context.pr_repo,
            context.pr_number,
            body,
        )


def is_trusted_review_pr(
    *,
    snapshot: ReviewSnapshot,
    automation_config: BoardAutomationConfig,
) -> bool:
    """Return True when the PR satisfies the trusted local review predicate."""
    return (
        trusted_review_provenance(
            snapshot=snapshot,
            automation_config=automation_config,
        )
        is not None
    )


@dataclass(frozen=True)
class RescueReviewContext:
    """Runtime inputs for one review rescue execution."""

    pr_repo: str
    pr_number: int
    snapshot: ReviewSnapshot
    automation_config: BoardAutomationConfig
    dry_run: bool
    pr_port: PullRequestPort
    review_state_port: ReviewStatePort
    board_port: BoardMutationPort
    session_store: SessionStorePort | None = None


def load_review_decision(context: RescueReviewContext) -> ReviewRescueDecision:
    """Return the domain decision for the current snapshot."""
    return review_rescue_decision(context.snapshot)


def backfill_verdict_if_eligible(
    context: RescueReviewContext,
) -> ReviewRescueResult | None:
    """Backfill a missing verdict marker when the PR is trusted and eligible."""
    decision = review_rescue_decision(context.snapshot)
    if decision.blocker != ReviewBlockerKind.MISSING_CODEX_VERDICT_MARKER:
        return None
    if not is_trusted_review_pr(
        snapshot=context.snapshot,
        automation_config=context.automation_config,
    ):
        return None
    provenance = trusted_review_provenance(
        snapshot=context.snapshot,
        automation_config=context.automation_config,
    )
    if provenance is None:
        return None
    if context.dry_run:
        return None
    session = _eligible_verdict_session(
        store=context.session_store,
        issue_ref=provenance["issue_ref"],
        pr_url=_current_pr_url(context),
        preferred_session_id=provenance.get("session_id"),
    )
    if session is None:
        return None
    if not context.pr_port.post_codex_verdict_if_missing(
        _current_pr_url(context),
        session.id,
    ):
        return None
    return ReviewRescueResult(
        pr_repo=context.pr_repo,
        pr_number=context.pr_number,
        last_result="verdict_backfilled",
    )


def update_branch_if_behind(context: RescueReviewContext) -> bool:
    """Refresh an eligible behind PR to the latest base branch."""
    decision = review_rescue_decision(context.snapshot)
    if decision.blocker != ReviewBlockerKind.BEHIND_BRANCH_UPDATE_REQUIRED:
        return False
    if context.dry_run:
        return True
    context.pr_port.update_branch(context.pr_repo, context.pr_number)
    return True


def copilot_fallback_is_eligible(
    *,
    context: RescueReviewContext,
) -> bool:
    """Return True when Copilot timeout fallback may proceed."""
    if review_rescue_decision(context.snapshot).blocker != (
        ReviewBlockerKind.MISSING_COPILOT_REVIEW
    ):
        return False
    provenance = trusted_review_provenance(
        snapshot=context.snapshot,
        automation_config=context.automation_config,
    )
    if provenance is None:
        return False
    missing_since = _copilot_missing_since(
        context=context,
        issue_ref=provenance["issue_ref"],
    )
    observed_at = parse_iso8601_timestamp(missing_since)
    if observed_at is None:
        return False
    timeout_seconds = context.automation_config.copilot_review_fallback_timeout_seconds
    elapsed = (datetime.now(timezone.utc) - observed_at).total_seconds()
    if elapsed < timeout_seconds:
        return False
    if context.snapshot.gate_status.state.upper() != "OPEN":
        return False
    if context.snapshot.gate_status.is_draft:
        return False
    if context.snapshot.gate_status.failed or context.snapshot.gate_status.pending:
        return False
    if context.snapshot.codex_gate_code != 0:
        return False
    if context.snapshot.rescue_failed or context.snapshot.rescue_pending:
        return False
    if context.snapshot.rescue_missing:
        return False
    if context.snapshot.gate_status.merge_state_status == "BEHIND":
        return False
    return context.snapshot.gate_status.mergeable in {"MERGEABLE", "UNKNOWN"}


def apply_review_decision(
    *,
    context: RescueReviewContext,
    decision: ReviewRescueDecision,
    automerge_runner: Callable[..., tuple[int, str]],
) -> ReviewRescueResult:
    """Project a decision into the legacy review rescue result."""
    if decision.status.name == "TERMINAL" and decision.terminal_reason is not None:
        return ReviewRescueResult(
            pr_repo=context.pr_repo,
            pr_number=context.pr_number,
            terminal_reason=serialize_terminal_reason(decision.terminal_reason),
            last_result="terminal",
        )

    if decision.blocker is None:
        code, msg = automerge_runner(
            pr_repo=context.pr_repo,
            pr_number=context.pr_number,
            snapshot=context.snapshot,
            dry_run=context.dry_run,
            pr_port=context.pr_port,
            review_state_port=context.review_state_port,
        )
        if code == 0:
            terminal_reason = ReviewTerminalReason.AUTO_MERGE_ENABLED
            return ReviewRescueResult(
                pr_repo=context.pr_repo,
                pr_number=context.pr_number,
                auto_merge_enabled=True,
                terminal_reason=serialize_terminal_reason(terminal_reason),
                last_result="success",
            )
        return ReviewRescueResult(
            pr_repo=context.pr_repo,
            pr_number=context.pr_number,
            blocked_reason=msg,
            last_result="blocked",
        )

    reason = serialize_blocker_reason(
        decision.blocker,
        detail=decision.detail,
    )
    if decision.blocker in {
        ReviewBlockerKind.MERGE_CONFLICT,
        ReviewBlockerKind.REQUIRED_CHECKS_FAILED,
    } and is_trusted_review_pr(
        snapshot=context.snapshot,
        automation_config=context.automation_config,
    ):
        return ReviewRescueResult(
            pr_repo=context.pr_repo,
            pr_number=context.pr_number,
            requeued_refs=context.snapshot.review_refs,
            queue_reason=reason,
            last_result="requeued",
            check_names=decision.required_check_names,
        )
    human_reason = reason
    if decision.blocker == ReviewBlockerKind.REQUIRED_CHECKS_FAILED:
        human_reason = f"required checks failed {list(decision.required_check_names)}"
    elif decision.blocker == ReviewBlockerKind.REQUIRED_CHECKS_PENDING:
        human_reason = f"required checks pending {list(decision.required_check_names)}"
    elif decision.blocker == ReviewBlockerKind.MERGE_CONFLICT:
        human_reason = "mergeable=CONFLICTING"
    return ReviewRescueResult(
        pr_repo=context.pr_repo,
        pr_number=context.pr_number,
        blocked_reason=human_reason,
        queue_reason=reason,
        last_result="blocked",
        check_names=decision.required_check_names,
    )


def recover_review_verdicts(
    *,
    store: SessionStorePort,
    pr_port: PullRequestPort,
    dry_run: bool = False,
) -> ReviewVerdictRecoverySweep:
    """Sweep all active review rows for eligible verdict backfill repairs."""
    scanned: list[str] = []
    eligible: list[str] = []
    backfilled: list[str] = []
    skipped: list[str] = []
    for entry in store.list_review_queue_items():
        scanned.append(entry.issue_ref)
        if not is_pre_backfill_eligible(
            last_result=entry.last_result,
            last_reason=entry.last_reason,
        ):
            skipped.append(f"{entry.issue_ref}:not-eligible")
            continue
        session = _eligible_verdict_session(
            store=store,
            issue_ref=entry.issue_ref,
            pr_url=entry.pr_url,
            preferred_session_id=entry.source_session_id,
        )
        if session is None:
            skipped.append(f"{entry.issue_ref}:no-eligible-session")
            continue
        eligible.append(entry.issue_ref)
        if dry_run:
            continue
        if pr_port.post_codex_verdict_if_missing(entry.pr_url, session.id):
            backfilled.append(entry.issue_ref)
        else:
            skipped.append(f"{entry.issue_ref}:marker-already-present")
    return ReviewVerdictRecoverySweep(
        scanned=tuple(scanned),
        eligible=tuple(eligible),
        backfilled=tuple(backfilled),
        skipped=tuple(skipped),
    )
