"""Review-rescue application use case."""

from __future__ import annotations

from typing import Callable

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.domain.automerge_policy import automerge_gate_decision
from startupai_controller.domain.models import (
    ReviewRescueResult,
    ReviewRescueSweep,
    ReviewSnapshot,
)
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


def automerge_review(
    pr_repo: str,
    pr_number: int,
    automation_config: BoardAutomationConfig,
    *,
    dry_run: bool = False,
    update_branch: bool = True,
    delete_branch: bool = True,
    snapshot: ReviewSnapshot | None = None,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    codex_gate_evaluator: Callable[..., tuple[int, str]] | None = None,
) -> tuple[int, str]:
    """Auto-merge a review PR when codex gate and required checks pass."""
    if snapshot is not None:
        review_refs = list(snapshot.review_refs)
        copilot_review_present = snapshot.copilot_review_present
        gate_code = snapshot.codex_gate_code
        gate_msg = snapshot.codex_gate_message
        status = snapshot.gate_status
    else:
        review_refs = []
        for issue_ref in pr_port.linked_issue_refs(pr_repo, pr_number):
            if review_state_port.get_issue_status(issue_ref) == "Review":
                review_refs.append(issue_ref)
        if not review_refs:
            return 2, (
                f"{pr_repo}#{pr_number}: not in board Review scope; "
                "automerge controller no-op"
            )
        copilot_review_present = pr_port.has_copilot_review_signal(
            pr_repo,
            pr_number,
        )
        if codex_gate_evaluator is None:
            return 4, f"{pr_repo}#{pr_number}: missing codex gate evaluator"
        gate_code, gate_msg = codex_gate_evaluator(
            pr_repo=pr_repo,
            pr_number=pr_number,
            dry_run=dry_run,
        )
        status = pr_port.get_gate_status(pr_repo, pr_number)

    code, msg, action = automerge_gate_decision(
        pr_repo=pr_repo,
        pr_number=pr_number,
        review_refs=tuple(review_refs),
        copilot_review_present=copilot_review_present,
        codex_gate_code=gate_code,
        codex_gate_message=gate_msg,
        pr_state=status.state,
        is_draft=status.is_draft,
        auto_merge_enabled=status.auto_merge_enabled,
        required_failed=status.failed,
        required_pending=status.pending,
        mergeable=status.mergeable,
        merge_state_status=status.merge_state_status,
    )

    if action in ("no_op", "already_enabled"):
        return code, msg

    if action == "update_branch_then_enable" and update_branch:
        if dry_run:
            return code, msg
        pr_port.update_branch(pr_repo, pr_number)

    if status.mergeable not in {"MERGEABLE", "UNKNOWN"}:
        return 2, (
            f"{pr_repo}#{pr_number}: mergeable={status.mergeable}, " "cannot auto-merge"
        )

    if dry_run:
        return 0, (
            f"{pr_repo}#{pr_number}: would enable auto-merge " "(squash, strict gates)"
        )

    merge_status = pr_port.enable_automerge(
        pr_repo,
        pr_number,
        delete_branch=delete_branch,
    )
    if merge_status == "confirmed":
        return 0, f"{pr_repo}#{pr_number}: auto-merge enabled (verified)"
    return 2, f"{pr_repo}#{pr_number}: auto-merge pending verification"


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


def _execution_authority_repo_slugs(
    config,
    automation_config: BoardAutomationConfig,
) -> tuple[str, ...]:
    """Return full repo slugs governed by execution authority."""
    slugs = {
        repo_slug
        for prefix, repo_slug in config.issue_prefixes.items()
        if prefix in automation_config.execution_authority_repos
    }
    return tuple(sorted(slugs))


def review_rescue_all(
    config,
    automation_config: BoardAutomationConfig,
    *,
    dry_run: bool = False,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    review_rescue_runner: Callable[..., ReviewRescueResult],
) -> ReviewRescueSweep:
    """Run review rescue across all governed repos."""
    repos = _execution_authority_repo_slugs(config, automation_config)
    rerun: list[str] = []
    auto_merge_enabled: list[str] = []
    requeued: list[str] = []
    blocked: list[str] = []
    skipped: list[str] = []
    scanned_prs = 0

    for pr_repo in repos:
        for pr in pr_port.list_open_prs(pr_repo):
            scanned_prs += 1
            result = review_rescue_runner(
                pr_repo=pr_repo,
                pr_number=pr.number,
                dry_run=dry_run,
                pr_port=pr_port,
                review_state_port=review_state_port,
                board_port=board_port,
            )
            ref = f"{pr_repo}#{pr.number}"
            if result.rerun_checks:
                rerun.append(f"{ref}:{','.join(result.rerun_checks)}")
            elif result.auto_merge_enabled:
                auto_merge_enabled.append(ref)
            elif result.requeued_refs:
                requeued.extend(result.requeued_refs)
            elif result.blocked_reason:
                blocked.append(f"{ref}:{result.blocked_reason}")
            elif result.skipped_reason:
                skipped.append(f"{ref}:{result.skipped_reason}")

    return ReviewRescueSweep(
        scanned_repos=repos,
        scanned_prs=scanned_prs,
        rerun=tuple(rerun),
        auto_merge_enabled=tuple(auto_merge_enabled),
        requeued=tuple(requeued),
        blocked=tuple(blocked),
        skipped=tuple(skipped),
    )
