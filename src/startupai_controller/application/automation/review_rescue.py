"""Review-rescue application use case."""

from __future__ import annotations

from typing import Callable

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.domain.models import ReviewRescueResult, ReviewSnapshot
from startupai_controller.domain.repair_policy import parse_consumer_provenance
from startupai_controller.domain.rescue_policy import rescue_decision
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort


def _set_issue_status_if_matches(
    issue_ref: str,
    from_statuses: set[str],
    to_status: str,
    *,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    dry_run: bool = False,
) -> tuple[bool, str | None]:
    """Set one issue status through ports when the current status matches."""
    current_status = review_state_port.get_issue_status(issue_ref)
    if current_status not in from_statuses:
        return False, current_status
    if dry_run:
        return True, current_status
    if current_status != to_status:
        board_port.set_issue_status(issue_ref, to_status)
    return True, current_status


def _requeue_local_review_failures(
    pr_repo: str,
    pr_number: int,
    review_refs: tuple[str, ...],
    automation_config: BoardAutomationConfig,
    *,
    dry_run: bool = False,
    pr_author: str | None = None,
    pr_body: str | None = None,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
) -> tuple[str, ...]:
    """Return linked review refs to Ready when a local PR needs another coding pass."""
    actor = (pr_author or "").strip().lower()
    body = pr_body or ""
    if not actor and not body:
        pr_view = pr_port.get_pull_request(pr_repo, pr_number)
        if pr_view is None:
            return ()
        actor = (pr_view.author or "").strip().lower()
        body = pr_view.body or ""
    if actor not in automation_config.trusted_local_authors:
        return ()

    provenance = parse_consumer_provenance(body)
    if provenance is None:
        return ()

    issue_ref = provenance.get("issue_ref", "").strip()
    executor = provenance.get("executor", "").strip().lower()
    if executor not in automation_config.execution_authority_executors:
        return ()
    if issue_ref not in review_refs:
        return ()

    changed, _old_status = _set_issue_status_if_matches(
        issue_ref,
        {"Review"},
        "Ready",
        review_state_port=review_state_port,
        board_port=board_port,
        dry_run=dry_run,
    )
    return (issue_ref,) if changed or dry_run else ()


def _rerun_cancelled_review_checks(
    *,
    pr_repo: str,
    pr_number: int,
    snapshot: ReviewSnapshot,
    dry_run: bool,
    pr_port: PullRequestPort,
) -> ReviewRescueResult | None:
    """Handle the cancelled-check rerun phase before domain policy."""
    if not snapshot.rescue_cancelled:
        return None
    rerun_checks: list[str] = []
    for check_name in sorted(snapshot.rescue_cancelled):
        observation = snapshot.gate_status.checks.get(check_name)
        if observation is None or observation.run_id is None:
            continue
        if dry_run or pr_port.rerun_failed_check(
            pr_repo,
            observation.name,
            observation.run_id,
        ):
            rerun_checks.append(check_name)
    if not rerun_checks:
        return None
    return ReviewRescueResult(
        pr_repo=pr_repo,
        pr_number=pr_number,
        rerun_checks=tuple(rerun_checks),
    )


def _apply_review_rescue_decision(
    *,
    pr_repo: str,
    pr_number: int,
    snapshot: ReviewSnapshot,
    action: str,
    reason: str | None,
    automation_config: BoardAutomationConfig,
    dry_run: bool,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    automerge_runner: Callable[..., tuple[int, str]],
) -> ReviewRescueResult:
    """Apply the domain rescue decision through the port boundary."""
    if action == "skipped":
        return ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            skipped_reason=reason,
        )
    if action == "blocked":
        return ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            blocked_reason=reason,
        )
    if action in ("requeue_conflicting", "requeue_failed"):
        requeued_refs = _requeue_local_review_failures(
            pr_repo=pr_repo,
            pr_number=pr_number,
            review_refs=snapshot.review_refs,
            automation_config=automation_config,
            dry_run=dry_run,
            pr_author=snapshot.pr_author,
            pr_body=snapshot.pr_body,
            pr_port=pr_port,
            review_state_port=review_state_port,
            board_port=board_port,
        )
        if requeued_refs:
            return ReviewRescueResult(
                pr_repo=pr_repo,
                pr_number=pr_number,
                requeued_refs=requeued_refs,
            )
        return ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            blocked_reason=reason,
        )
    if action == "enable_automerge":
        code, _msg = automerge_runner(
            pr_repo=pr_repo,
            pr_number=pr_number,
            snapshot=snapshot,
            dry_run=dry_run,
            pr_port=pr_port,
            review_state_port=review_state_port,
        )
        return ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            auto_merge_enabled=code == 0,
            blocked_reason=None if code == 0 else "automerge-not-enabled",
        )
    return ReviewRescueResult(
        pr_repo=pr_repo,
        pr_number=pr_number,
        blocked_reason=reason,
    )


def review_rescue(
    pr_repo: str,
    pr_number: int,
    snapshot: ReviewSnapshot,
    automation_config: BoardAutomationConfig,
    *,
    dry_run: bool = False,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    automerge_runner: Callable[..., tuple[int, str]],
) -> ReviewRescueResult:
    """Reconcile one PR in Review back toward self-healing merge flow."""
    rerun_result = _rerun_cancelled_review_checks(
        pr_repo=pr_repo,
        pr_number=pr_number,
        snapshot=snapshot,
        dry_run=dry_run,
        pr_port=pr_port,
    )
    if rerun_result is not None:
        return rerun_result

    action, reason = rescue_decision(
        review_refs=tuple(snapshot.review_refs),
        has_cancelled_checks=False,
        pr_state=snapshot.gate_status.state,
        is_draft=snapshot.gate_status.is_draft,
        mergeable=snapshot.gate_status.mergeable,
        copilot_review_present=snapshot.copilot_review_present,
        codex_gate_code=snapshot.codex_gate_code,
        codex_gate_message=snapshot.codex_gate_message,
        required_failed=snapshot.gate_status.failed,
        required_pending=snapshot.gate_status.pending,
        rescue_failed=snapshot.rescue_failed,
        rescue_pending=snapshot.rescue_pending,
        rescue_missing=snapshot.rescue_missing,
        auto_merge_enabled=snapshot.gate_status.auto_merge_enabled,
    )
    return _apply_review_rescue_decision(
        pr_repo=pr_repo,
        pr_number=pr_number,
        snapshot=snapshot,
        action=action,
        reason=reason,
        automation_config=automation_config,
        dry_run=dry_run,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        automerge_runner=automerge_runner,
    )
