"""Review-queue processing helpers extracted from board_consumer."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

import startupai_controller.consumer_review_queue_drain_processing as _drain_processing
import startupai_controller.consumer_review_queue_group_processing as _group_processing
import startupai_controller.consumer_review_queue_preparation_processing as _preparation_processing
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
    ReviewQueueDrainSummary,
    ReviewQueueEntry,
    ReviewRescueResult,
    ReviewSnapshot,
)
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.runtime.wiring import (
    ConsumerDB,
    GitHubPortBundle,
    GitHubRuntimeMemo,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
)

from startupai_controller.consumer_review_queue_state import (
    apply_review_queue_partial_failure,
    partition_review_queue_entries_by_probe_change,
    repark_unchanged_review_queue_entries,
    wakeup_changed_review_queue_entries,
)

ReviewRescueExecution = _group_processing.ReviewRescueExecution
build_review_snapshots_for_queue_entries = (
    _preparation_processing.build_review_snapshots_for_queue_entries
)
backfill_review_verdicts_from_snapshots = (
    _preparation_processing.backfill_review_verdicts_from_snapshots
)
pre_backfill_verdicts_for_due_prs = (
    _preparation_processing.pre_backfill_verdicts_for_due_prs
)
run_review_rescue_for_group = _group_processing.run_review_rescue_for_group
apply_review_queue_group_result = _group_processing.apply_review_queue_group_result
summarize_review_group_outcome = _group_processing.summarize_review_group_outcome
process_due_review_group = _group_processing.process_due_review_group


def prepare_review_queue_batch(
    *,
    config: ConsumerConfig,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    board_snapshot: CycleBoardSnapshot,
    pr_port: PullRequestPort,
    now: datetime,
    dry_run: bool,
    prepared_batch_factory: type[PreparedReviewQueueBatch],
    summary_factory: type[ReviewQueueDrainSummary],
    log_warning: Callable[[Exception], None],
    wakeup_changed_review_queue_entries_fn: (
        Callable[..., tuple[str, ...]] | None
    ) = None,
) -> tuple[PreparedReviewQueueBatch | None, ReviewQueueDrainSummary | None]:
    """Prepare the bounded review-queue workset for one drain cycle."""
    return _preparation_processing.prepare_review_queue_batch(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        board_snapshot=board_snapshot,
        pr_port=pr_port,
        now=now,
        dry_run=dry_run,
        prepared_batch_factory=prepared_batch_factory,
        summary_factory=summary_factory,
        log_warning=log_warning,
        wakeup_changed_review_queue_entries_fn=(
            wakeup_changed_review_queue_entries_fn
            or wakeup_changed_review_queue_entries
        ),
    )


def prepare_due_review_processing(
    *,
    store: SessionStorePort,
    automation_config: BoardAutomationConfig,
    pr_port: PullRequestPort,
    prepared_batch: PreparedReviewQueueBatch,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    post_pr_codex_verdict: Callable[..., bool],
    prepared_due_processing_factory: type[PreparedDueReviewProcessing],
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
) -> PreparedDueReviewProcessing:
    """Prepare the changed due-review groups and snapshots for rescue processing."""
    return _preparation_processing.prepare_due_review_processing(
        store=store,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        post_pr_codex_verdict=post_pr_codex_verdict,
        prepared_due_processing_factory=prepared_due_processing_factory,
        log_pre_backfill_warning=log_pre_backfill_warning,
        log_backfill_warning=log_backfill_warning,
        pre_backfill_verdicts_for_due_prs_fn=(
            pre_backfill_verdicts_for_due_prs_fn or pre_backfill_verdicts_for_due_prs
        ),
        partition_review_queue_entries_by_probe_change_fn=(
            partition_review_queue_entries_by_probe_change_fn
            or partition_review_queue_entries_by_probe_change
        ),
        repark_unchanged_review_queue_entries_fn=(
            repark_unchanged_review_queue_entries_fn
            or repark_unchanged_review_queue_entries
        ),
        build_review_snapshots_for_queue_entries_fn=(
            build_review_snapshots_for_queue_entries_fn
            or build_review_snapshots_for_queue_entries
        ),
        backfill_review_verdicts_from_snapshots_fn=(
            backfill_review_verdicts_from_snapshots_fn
            or backfill_review_verdicts_from_snapshots
        ),
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
    return _drain_processing.process_review_queue_due_groups(
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
        review_queue_processing_outcome_factory=review_queue_processing_outcome_factory,
        gh_reason_code=gh_reason_code,
        prepare_due_review_processing_fn=(
            lambda **kwargs: prepare_due_review_processing(
                post_pr_codex_verdict=post_pr_codex_verdict,
                prepared_due_processing_factory=prepared_due_processing_factory,
                log_pre_backfill_warning=log_pre_backfill_warning,
                log_backfill_warning=log_backfill_warning,
                pre_backfill_verdicts_for_due_prs_fn=(
                    pre_backfill_verdicts_for_due_prs_fn
                ),
                partition_review_queue_entries_by_probe_change_fn=(
                    partition_review_queue_entries_by_probe_change_fn
                ),
                repark_unchanged_review_queue_entries_fn=(
                    repark_unchanged_review_queue_entries_fn
                ),
                build_review_snapshots_for_queue_entries_fn=(
                    build_review_snapshots_for_queue_entries_fn
                ),
                backfill_review_verdicts_from_snapshots_fn=(
                    backfill_review_verdicts_from_snapshots_fn
                ),
                **kwargs,
            )
        ),
        process_due_review_group_fn=(
            lambda **kwargs: process_due_review_group(
                review_group_outcome_factory=review_group_outcome_factory,
                review_rescue_fn=review_rescue_fn,
                escalate_to_claude=escalate_to_claude,
                **kwargs,
            )
        ),
        apply_review_queue_partial_failure_fn=apply_review_queue_partial_failure,
    )


def drain_review_queue(
    config: ConsumerConfig,
    db: ConsumerDB,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    *,
    pr_port: PullRequestPort | None = None,
    session_store: SessionStorePort | None = None,
    board_snapshot: CycleBoardSnapshot | None = None,
    dry_run: bool = False,
    github_memo: GitHubRuntimeMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
    build_github_port_bundle: Callable[..., GitHubPortBundle],
    build_session_store: Callable[[ConsumerDB], SessionStorePort],
    github_memo_factory: Callable[[], GitHubRuntimeMemo],
    prepared_batch_factory: type[PreparedReviewQueueBatch],
    summary_factory: type[ReviewQueueDrainSummary],
    prepared_due_processing_factory: type[PreparedDueReviewProcessing],
    review_group_outcome_factory: type[ReviewGroupProcessingOutcome],
    review_queue_processing_outcome_factory: type[ReviewQueueProcessingOutcome],
    post_pr_codex_verdict: Callable[..., bool],
    review_rescue_fn: Callable[..., ReviewRescueResult],
    escalate_to_claude: Callable[..., None],
    gh_reason_code: Callable[[Exception], str],
    log_probe_warning: Callable[[Exception], None],
    log_pre_backfill_warning: Callable[[str, Exception], None],
    log_backfill_warning: Callable[[str, str, Exception], None],
    wakeup_changed_review_queue_entries_fn: (
        Callable[..., tuple[str, ...]] | None
    ) = None,
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
) -> tuple[ReviewQueueDrainSummary, CycleBoardSnapshot]:
    """Process a bounded batch of queued Review items."""
    return _drain_processing.drain_review_queue(
        config,
        db,
        critical_path_config,
        automation_config,
        pr_port=pr_port,
        session_store=session_store,
        board_snapshot=board_snapshot,
        dry_run=dry_run,
        github_memo=github_memo,
        gh_runner=gh_runner,
        build_github_port_bundle=build_github_port_bundle,
        build_session_store=build_session_store,
        github_memo_factory=github_memo_factory,
        summary_factory=summary_factory,
        prepare_review_queue_batch_fn=(
            lambda **kwargs: prepare_review_queue_batch(
                prepared_batch_factory=prepared_batch_factory,
                summary_factory=summary_factory,
                log_warning=log_probe_warning,
                wakeup_changed_review_queue_entries_fn=(
                    wakeup_changed_review_queue_entries_fn
                ),
                **kwargs,
            )
        ),
        process_review_queue_due_groups_fn=(
            lambda **kwargs: process_review_queue_due_groups(
                post_pr_codex_verdict=post_pr_codex_verdict,
                review_rescue_fn=review_rescue_fn,
                escalate_to_claude=escalate_to_claude,
                prepared_due_processing_factory=prepared_due_processing_factory,
                review_group_outcome_factory=review_group_outcome_factory,
                review_queue_processing_outcome_factory=(
                    review_queue_processing_outcome_factory
                ),
                gh_reason_code=gh_reason_code,
                log_pre_backfill_warning=log_pre_backfill_warning,
                log_backfill_warning=log_backfill_warning,
                pre_backfill_verdicts_for_due_prs_fn=(
                    pre_backfill_verdicts_for_due_prs_fn
                ),
                partition_review_queue_entries_by_probe_change_fn=(
                    partition_review_queue_entries_by_probe_change_fn
                ),
                repark_unchanged_review_queue_entries_fn=(
                    repark_unchanged_review_queue_entries_fn
                ),
                build_review_snapshots_for_queue_entries_fn=(
                    build_review_snapshots_for_queue_entries_fn
                ),
                backfill_review_verdicts_from_snapshots_fn=(
                    backfill_review_verdicts_from_snapshots_fn
                ),
                **kwargs,
            )
        ),
    )
