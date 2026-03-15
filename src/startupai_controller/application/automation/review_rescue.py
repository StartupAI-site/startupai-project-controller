"""Review-rescue application use case."""

from __future__ import annotations

from dataclasses import replace
from typing import Callable

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.domain.models import (
    ReviewRescueResult,
    ReviewRescueSweep,
    ReviewSnapshot,
    ReviewVerdictRecoverySweep,
)
from startupai_controller.domain.review_rescue_domain import (
    review_rescue_decision,
    serialize_blocker_reason,
)
from startupai_controller.domain.repair_policy import parse_consumer_provenance
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.application.automation.review_rescue_services import (
    RescueReviewContext,
    apply_review_decision,
    backfill_verdict_if_eligible,
    copilot_fallback_is_eligible,
    load_review_decision,
    post_copilot_fallback_comment,
    recover_review_verdicts as recover_review_verdicts_service,
    update_branch_if_behind,
)


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
    _ = (pr_port, pr_repo, pr_number)
    if not pr_author or not pr_body:
        return ()
    author = pr_author.strip().lower()
    if author not in automation_config.trusted_local_authors:
        return ()
    provenance = parse_consumer_provenance(pr_body)
    if provenance is None:
        return ()
    if provenance.get("executor", "").strip().lower() not in (
        automation_config.execution_authority_executors
    ):
        return ()
    target_issue_ref = provenance.get("issue_ref", "").strip()
    if target_issue_ref not in review_refs:
        return ()
    requeued: list[str] = []
    changed, _old_status = _set_issue_status_if_matches(
        target_issue_ref,
        {"Review"},
        "Ready",
        review_state_port=review_state_port,
        board_port=board_port,
        dry_run=dry_run,
    )
    if changed or dry_run:
        requeued.append(target_issue_ref)
    return tuple(requeued)


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
            terminal_reason=(
                "auto-merge-already-enabled-terminal"
                if reason == "auto-merge-already-enabled"
                else None
            ),
            last_result="terminal" if reason == "auto-merge-already-enabled" else "skipped",
        )
    if action == "blocked":
        return ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            blocked_reason=reason,
            last_result="blocked",
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
                last_result="requeued",
            )
        return ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            blocked_reason=reason,
            last_result="blocked",
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
            terminal_reason="auto-merge-enabled" if code == 0 else None,
            blocked_reason=None if code == 0 else _msg,
            last_result="success" if code == 0 else "blocked",
        )
    return ReviewRescueResult(
        pr_repo=pr_repo,
        pr_number=pr_number,
        blocked_reason=reason,
        last_result="blocked",
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
    current_snapshot = snapshot

    def _load_state() -> tuple[tuple[str, ...], bool, int, str, object]:
        if current_snapshot is not None:
            return (
                tuple(current_snapshot.review_refs),
                current_snapshot.copilot_review_present,
                current_snapshot.codex_gate_code,
                current_snapshot.codex_gate_message,
                current_snapshot.gate_status,
            )
        review_refs = tuple(
            issue_ref
            for issue_ref in pr_port.linked_issue_refs(pr_repo, pr_number)
            if review_state_port.get_issue_status(issue_ref) == "Review"
        )
        if not review_refs:
            return ((), False, 0, "", pr_port.get_gate_status(pr_repo, pr_number))
        if codex_gate_evaluator is None:
            return (review_refs, False, 4, "missing codex gate evaluator", pr_port.get_gate_status(pr_repo, pr_number))
        gate_code, gate_msg = codex_gate_evaluator(
            pr_repo=pr_repo,
            pr_number=pr_number,
            dry_run=dry_run,
        )
        return (
            review_refs,
            pr_port.has_copilot_review_signal(pr_repo, pr_number),
            gate_code,
            gate_msg,
            pr_port.get_gate_status(pr_repo, pr_number),
        )

    review_refs, copilot_review_present, gate_code, gate_msg, status = _load_state()
    label = f"{pr_repo}#{pr_number}"
    if not review_refs:
        return 2, f"{label}: not in board Review scope; automerge controller no-op"
    if gate_code == 4:
        return 4, f"{label}: {gate_msg}"
    if status.state.upper() != "OPEN":
        return 2, f"{label}: state={status.state}, not OPEN"
    if status.is_draft:
        return 2, f"{label}: draft-pr"
    if status.auto_merge_enabled:
        return 0, f"{label}: auto-merge-already-enabled (already enabled)"
    if not copilot_review_present:
        return 2, f"{label}: missing-copilot-review"
    if gate_code != 0:
        return gate_code, gate_msg
    if status.failed:
        return 2, "required-checks-failed"
    if status.pending:
        return 2, "required-checks-pending"
    if status.merge_state_status == "BEHIND" and update_branch:
        if dry_run:
            return 0, f"{label}: would update branch then enable auto-merge"
        pr_port.update_branch(pr_repo, pr_number)
        if current_snapshot is not None:
            current_snapshot = pr_port.review_snapshots(
                {(pr_repo, pr_number): tuple(current_snapshot.review_refs)},
                trusted_codex_actors=frozenset(automation_config.trusted_codex_actors),
            )[(pr_repo, pr_number)]
        review_refs, copilot_review_present, gate_code, gate_msg, status = _load_state()
        if gate_code != 0:
            return gate_code, gate_msg
        if status.failed:
            return 2, "required-checks-failed"
        if status.pending:
            return 2, "required-checks-pending"
    if status.mergeable not in {"MERGEABLE", "UNKNOWN"}:
        return 2, f"mergeable={status.mergeable}"
    if dry_run:
        return 0, f"{label}: would enable auto-merge (squash, strict gates)"
    merge_status = pr_port.enable_automerge(
        pr_repo,
        pr_number,
        delete_branch=delete_branch,
    )
    if merge_status == "confirmed":
        return 0, f"{label}: auto-merge enabled (verified)"
    return 2, "auto-merge-pending-verification"


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
    session_store: SessionStorePort | None = None,
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
    context = RescueReviewContext(
        pr_repo=pr_repo,
        pr_number=pr_number,
        snapshot=snapshot,
        automation_config=automation_config,
        dry_run=dry_run,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        session_store=session_store,
    )
    backfill_result = backfill_verdict_if_eligible(context)
    if backfill_result is not None:
        snapshot = pr_port.review_snapshots(
            {(pr_repo, pr_number): tuple(snapshot.review_refs)},
            trusted_codex_actors=frozenset(automation_config.trusted_codex_actors),
        )[(pr_repo, pr_number)]
        context = RescueReviewContext(
            pr_repo=pr_repo,
            pr_number=pr_number,
            snapshot=snapshot,
            automation_config=automation_config,
            dry_run=dry_run,
            pr_port=pr_port,
            review_state_port=review_state_port,
            board_port=board_port,
            session_store=session_store,
        )
    if update_branch_if_behind(context):
        snapshot = pr_port.review_snapshots(
            {(pr_repo, pr_number): tuple(snapshot.review_refs)},
            trusted_codex_actors=frozenset(automation_config.trusted_codex_actors),
        )[(pr_repo, pr_number)]
        context = RescueReviewContext(
            pr_repo=pr_repo,
            pr_number=pr_number,
            snapshot=snapshot,
            automation_config=automation_config,
            dry_run=dry_run,
            pr_port=pr_port,
            review_state_port=review_state_port,
            board_port=board_port,
            session_store=session_store,
        )
    decision = load_review_decision(context)
    if copilot_fallback_is_eligible(context=context):
        post_copilot_fallback_comment(context)
        fallback_snapshot = replace(snapshot, copilot_review_present=True)
        code, msg = automerge_runner(
            pr_repo=pr_repo,
            pr_number=pr_number,
            snapshot=fallback_snapshot,
            dry_run=dry_run,
            pr_port=pr_port,
            review_state_port=review_state_port,
        )
        if code == 0:
            return ReviewRescueResult(
                pr_repo=pr_repo,
                pr_number=pr_number,
                auto_merge_enabled=True,
                terminal_reason="copilot-review-timeout-fallback",
                last_result="success",
            )
        return ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            blocked_reason=msg,
            queue_reason=msg,
            last_result="blocked",
        )
    result = apply_review_decision(
        context=context,
        decision=decision,
        automerge_runner=automerge_runner,
    )
    if result.queue_reason in {"required-checks-failed", "merge-conflict"}:
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
                queue_reason=result.queue_reason,
                check_names=result.check_names,
                last_result="requeued",
            )
    return result


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
    session_store: SessionStorePort | None = None,
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
                session_store=session_store,
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


def review_recover_verdicts(
    *,
    store: SessionStorePort,
    pr_port: PullRequestPort,
    dry_run: bool = False,
) -> ReviewVerdictRecoverySweep:
    """Sweep active review rows for eligible codex verdict marker repair."""
    return recover_review_verdicts_service(
        store=store,
        pr_port=pr_port,
        dry_run=dry_run,
    )
