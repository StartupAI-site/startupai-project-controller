"""Due-group review rescue processing extracted from review-queue helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, cast

import startupai_controller.consumer_review_queue_drain_processing as _drain_processing
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import ReviewGroupProcessingOutcome
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ReviewQueueEntry,
    ReviewRescueResult,
    ReviewSnapshot,
)
from startupai_controller.domain.review_queue_policy import requeue_or_escalate
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
)

from startupai_controller.consumer_review_queue_state import (
    apply_review_queue_result,
    update_board_snapshot_statuses,
)


@dataclass(frozen=True)
class ReviewRescueExecution:
    """Result of executing rescue logic for one review PR."""

    result: ReviewRescueResult
    partial_failure: bool = False
    error: str | None = None


def run_review_rescue_for_group(
    *,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    store: SessionStorePort,
    pr_port: PullRequestPort,
    pr_repo: str,
    pr_number: int,
    snapshot: ReviewSnapshot,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    review_rescue_fn: Callable[..., ReviewRescueResult],
) -> ReviewRescueExecution:
    """Run rescue logic for one due review group."""
    try:
        return ReviewRescueExecution(
            result=review_rescue_fn(
                pr_repo=pr_repo,
                pr_number=pr_number,
                config=critical_path_config,
                automation_config=automation_config,
                project_owner=config.project_owner,
                project_number=config.project_number,
                dry_run=dry_run,
                snapshot=snapshot,
                gh_runner=gh_runner,
                pr_port=pr_port,
                session_store=store,
            )
        )
    except GhQueryError as err:
        return ReviewRescueExecution(
            result=ReviewRescueResult(pr_repo=pr_repo, pr_number=pr_number),
            partial_failure=True,
            error=str(err),
        )


def apply_review_queue_group_result(
    *,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    pr_port: PullRequestPort,
    pr_repo: str,
    pr_number: int,
    entries: tuple[ReviewQueueEntry, ...],
    result: ReviewRescueResult,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    escalate_to_claude: Callable[..., None],
) -> tuple[str, ...]:
    """Persist one review-group result and return escalated issue refs."""
    if dry_run:
        return ()

    escalated: list[str] = []
    state_digest = pr_port.review_state_digests([(pr_repo, pr_number)]).get(
        (pr_repo, pr_number)
    )
    for entry in entries:
        needs_escalation = apply_review_queue_result(
            store,
            entry,
            result,
            now=now,
            last_state_digest=state_digest,
        )
        if needs_escalation:
            escalate_to_claude(
                entry.issue_ref,
                critical_path_config,
                project_owner,
                project_number,
                reason=f"review queue blocked escalation: {result.blocked_reason}",
                gh_runner=gh_runner,
            )
            store.delete_review_queue_item(entry.issue_ref)
            escalated.append(entry.issue_ref)
    return tuple(escalated)


def summarize_review_group_outcome(
    *,
    critical_path_config: CriticalPathConfig,
    store: SessionStorePort,
    project_owner: str,
    project_number: int,
    pr_repo: str,
    pr_number: int,
    entries: tuple[ReviewQueueEntry, ...],
    result: ReviewRescueResult,
    updated_snapshot: CycleBoardSnapshot,
    escalated: tuple[str, ...],
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    escalate_to_claude: Callable[..., None],
    review_group_outcome_factory: type[ReviewGroupProcessingOutcome],
) -> ReviewGroupProcessingOutcome:
    """Build the public outcome for one processed review group."""
    escalated_refs = list(escalated)
    requeued: list[str] = []
    blocked: list[str] = []
    skipped: list[str] = []
    rerun: list[str] = []
    auto_merge_enabled: list[str] = []

    pr_ref = f"{pr_repo}#{pr_number}"
    if result.rerun_checks:
        rerun.append(f"{pr_ref}:{','.join(result.rerun_checks)}")
    elif result.auto_merge_enabled:
        auto_merge_enabled.append(pr_ref)
    elif result.requeued_refs:
        for issue_ref in result.requeued_refs:
            entry = next(
                (
                    candidate
                    for candidate in entries
                    if candidate.issue_ref == issue_ref
                ),
                None,
            )
            pr_url = entry.pr_url if entry is not None else ""
            requeue_count, _ = store.get_requeue_state(issue_ref)
            if requeue_or_escalate(requeue_count) == "escalate":
                if not dry_run:
                    escalate_to_claude(
                        issue_ref,
                        critical_path_config,
                        project_owner,
                        project_number,
                        reason=(
                            f"repair requeue ceiling ({requeue_count} cycles on same PR): "
                            f"{result.blocked_reason or 'persistent check failure / conflict'}"
                        ),
                        gh_runner=gh_runner,
                    )
                    store.delete_review_queue_item(issue_ref)
                escalated_refs.append(issue_ref)
            else:
                if not dry_run:
                    store.increment_requeue_count(issue_ref, pr_url)
                    store.delete_review_queue_item(issue_ref)
                requeued.append(issue_ref)
        requeued_this_group = [
            ref for ref in result.requeued_refs if ref not in escalated_refs
        ]
        if requeued_this_group:
            updated_snapshot = update_board_snapshot_statuses(
                updated_snapshot,
                critical_path_config,
                {ref: "Ready" for ref in requeued_this_group},
            )
    elif result.blocked_reason:
        blocked.append(f"{pr_ref}:{result.blocked_reason}")
    elif result.skipped_reason:
        skipped.append(f"{pr_ref}:{result.skipped_reason}")

    return review_group_outcome_factory(
        rerun=tuple(rerun),
        auto_merge_enabled=tuple(auto_merge_enabled),
        requeued=tuple(requeued),
        blocked=tuple(blocked),
        skipped=tuple(skipped),
        escalated=tuple(escalated_refs),
        updated_snapshot=updated_snapshot,
    )


def process_due_review_group(
    *,
    config: ConsumerConfig,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    pr_port: PullRequestPort,
    pr_repo: str,
    pr_number: int,
    entries: tuple[ReviewQueueEntry, ...],
    snapshot: ReviewSnapshot | None,
    updated_snapshot: CycleBoardSnapshot,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    review_group_outcome_factory: type[ReviewGroupProcessingOutcome],
    review_rescue_fn: Callable[..., ReviewRescueResult],
    escalate_to_claude: Callable[..., None],
) -> ReviewGroupProcessingOutcome:
    """Process one due PR group from the review queue."""

    def _run_review_rescue_for_group(
        *,
        config: ConsumerConfig,
        critical_path_config: CriticalPathConfig,
        automation_config: BoardAutomationConfig,
        pr_port: PullRequestPort,
        pr_repo: str,
        pr_number: int,
        snapshot: ReviewSnapshot,
        dry_run: bool,
        gh_runner: Callable[..., str] | None,
    ) -> ReviewRescueExecution:
        return run_review_rescue_for_group(
            config=config,
            critical_path_config=critical_path_config,
            automation_config=automation_config,
            store=store,
            pr_port=pr_port,
            pr_repo=pr_repo,
            pr_number=pr_number,
            snapshot=snapshot,
            dry_run=dry_run,
            gh_runner=gh_runner,
            review_rescue_fn=review_rescue_fn,
        )

    return _drain_processing.process_due_review_group(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=pr_port,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        snapshot=snapshot,
        updated_snapshot=updated_snapshot,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        review_group_outcome_factory=review_group_outcome_factory,
        run_review_rescue_for_group_fn=cast(
            Callable[..., _drain_processing.ReviewRescueExecutionView],
            _run_review_rescue_for_group,
        ),
        apply_review_queue_group_result_fn=(
            lambda **kwargs: apply_review_queue_group_result(
                escalate_to_claude=escalate_to_claude,
                **kwargs,
            )
        ),
        summarize_review_group_outcome_fn=(
            lambda **kwargs: summarize_review_group_outcome(
                escalate_to_claude=escalate_to_claude,
                **kwargs,
            )
        ),
    )
