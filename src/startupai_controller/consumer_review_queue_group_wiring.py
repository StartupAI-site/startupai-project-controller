"""Review-queue due-group wiring extracted from consumer_review_queue_wiring."""

from __future__ import annotations

from datetime import datetime
from typing import Callable

import startupai_controller.consumer_review_queue_processing as _review_queue_processing
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    PreparedDueReviewProcessing,
    PreparedReviewQueueBatch,
    ReviewGroupProcessingOutcome,
    ReviewQueueProcessingOutcome,
)
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ReviewQueueEntry,
    ReviewRescueResult,
    ReviewSnapshot,
)
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig


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
    return _review_queue_processing.process_due_review_group(
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
        review_rescue_fn=review_rescue_fn,
        escalate_to_claude=escalate_to_claude,
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
    return _review_queue_processing.apply_review_queue_group_result(
        store=store,
        critical_path_config=critical_path_config,
        project_owner=project_owner,
        project_number=project_number,
        pr_port=pr_port,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        result=result,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        escalate_to_claude=escalate_to_claude,
    )


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
    return _review_queue_processing.summarize_review_group_outcome(
        critical_path_config=critical_path_config,
        store=store,
        project_owner=project_owner,
        project_number=project_number,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        result=result,
        updated_snapshot=updated_snapshot,
        escalated=escalated,
        dry_run=dry_run,
        gh_runner=gh_runner,
        escalate_to_claude=escalate_to_claude,
        review_group_outcome_factory=review_group_outcome_factory,
    )


def process_review_queue_due_groups(
    *,
    config: ConsumerConfig,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    pr_port: PullRequestPort,
    prepared_batch: PreparedReviewQueueBatch,
    board_snapshot: CycleBoardSnapshot,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    post_pr_codex_verdict: Callable[..., bool],
    review_rescue_fn: Callable[..., ReviewRescueResult],
    escalate_to_claude: Callable[..., None],
    prepared_due_processing_factory: type[PreparedDueReviewProcessing],
    review_group_outcome_factory: type[ReviewGroupProcessingOutcome],
    review_queue_processing_outcome_factory: type[ReviewQueueProcessingOutcome],
    gh_reason_code: Callable[[Exception], str],
    log_pre_backfill_warning: Callable[[str, Exception], None],
    log_backfill_warning: Callable[[str, str, Exception], None],
    pre_backfill_verdicts_for_due_prs_fn: Callable[..., tuple[str, ...]] | None = None,
    partition_review_queue_entries_by_probe_change_fn: (
        Callable[..., tuple[list[ReviewQueueEntry], list[ReviewQueueEntry]]] | None
    ) = None,
    repark_unchanged_review_queue_entries_fn: Callable[..., None] | None = None,
    build_review_snapshots_for_queue_entries_fn: (
        Callable[..., dict[tuple[str, int], ReviewSnapshot]] | None
    ) = None,
    backfill_review_verdicts_from_snapshots_fn: (
        Callable[..., tuple[str, ...]] | None
    ) = None,
) -> ReviewQueueProcessingOutcome:
    """Process the due PR groups for a prepared review-queue batch."""
    return _review_queue_processing.process_review_queue_due_groups(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        board_snapshot=board_snapshot,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        post_pr_codex_verdict=post_pr_codex_verdict,
        review_rescue_fn=review_rescue_fn,
        escalate_to_claude=escalate_to_claude,
        prepared_due_processing_factory=prepared_due_processing_factory,
        review_group_outcome_factory=review_group_outcome_factory,
        review_queue_processing_outcome_factory=review_queue_processing_outcome_factory,
        gh_reason_code=gh_reason_code,
        log_pre_backfill_warning=log_pre_backfill_warning,
        log_backfill_warning=log_backfill_warning,
        pre_backfill_verdicts_for_due_prs_fn=pre_backfill_verdicts_for_due_prs_fn,
        partition_review_queue_entries_by_probe_change_fn=(
            partition_review_queue_entries_by_probe_change_fn
        ),
        repark_unchanged_review_queue_entries_fn=repark_unchanged_review_queue_entries_fn,
        build_review_snapshots_for_queue_entries_fn=(
            build_review_snapshots_for_queue_entries_fn
        ),
        backfill_review_verdicts_from_snapshots_fn=(
            backfill_review_verdicts_from_snapshots_fn
        ),
    )
