"""Review-queue shell wiring extracted from board_consumer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Callable

import startupai_controller.consumer_automation_bridge as _automation_bridge
import startupai_controller.consumer_board_state_helpers as _board_state_helpers
import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
import startupai_controller.consumer_review_queue_group_wiring as _review_queue_group_wiring
import startupai_controller.consumer_review_queue_processing as _review_queue_processing
import startupai_controller.consumer_review_queue_state as _review_queue_state
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
    build_github_port_bundle,
    build_session_store,
    gh_reason_code,
)
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

logger = logging.getLogger("board-consumer")


@dataclass(frozen=True)
class ReviewQueueWiringDeps:
    """Injected shell-facing wiring seams for review-queue processing."""

    prepared_batch_factory: type[PreparedReviewQueueBatch]
    summary_factory: type[ReviewQueueDrainSummary]
    prepared_due_processing_factory: type[PreparedDueReviewProcessing]
    review_group_outcome_factory: type[ReviewGroupProcessingOutcome]
    review_queue_processing_outcome_factory: type[ReviewQueueProcessingOutcome]
    post_pr_codex_verdict: Callable[..., bool]
    review_rescue_fn: Callable[..., ReviewRescueResult]
    escalate_to_claude: Callable[..., None]
    gh_reason_code: Callable[[Exception], str]
    log_probe_warning: Callable[[Exception], None]
    log_pre_backfill_warning: Callable[[str, Exception], None]
    log_backfill_warning: Callable[[str, str, Exception], None]
    build_github_port_bundle: Callable[..., GitHubPortBundle]
    build_session_store: Callable[[ConsumerDB], SessionStorePort]
    github_memo_factory: Callable[[], GitHubRuntimeMemo]
    wakeup_changed_review_queue_entries: Callable[..., tuple[str, ...]]
    pre_backfill_verdicts_for_due_prs: Callable[..., tuple[str, ...]]
    partition_review_queue_entries_by_probe_change: Callable[
        ...,
        tuple[list[ReviewQueueEntry], list[ReviewQueueEntry]],
    ]
    repark_unchanged_review_queue_entries: Callable[..., None]
    build_review_snapshots_for_queue_entries: Callable[
        ...,
        dict[tuple[str, int], ReviewSnapshot],
    ]
    backfill_review_verdicts_from_snapshots: Callable[..., tuple[str, ...]]


def build_review_queue_wiring_deps() -> ReviewQueueWiringDeps:
    """Build shell-facing review-queue wiring dependencies."""
    return ReviewQueueWiringDeps(
        prepared_batch_factory=PreparedReviewQueueBatch,
        summary_factory=ReviewQueueDrainSummary,
        prepared_due_processing_factory=PreparedDueReviewProcessing,
        review_group_outcome_factory=ReviewGroupProcessingOutcome,
        review_queue_processing_outcome_factory=ReviewQueueProcessingOutcome,
        post_pr_codex_verdict=_codex_comment_wiring.post_pr_codex_verdict,
        review_rescue_fn=_automation_bridge.review_rescue,
        escalate_to_claude=_board_state_helpers.escalate_to_claude_from_shell,
        gh_reason_code=gh_reason_code,
        log_probe_warning=lambda err: logger.warning(
            "Review queue wakeup probe failed: %s",
            err,
        ),
        log_pre_backfill_warning=lambda issue_ref, err: logger.warning(
            "Pre-backfill verdict failed for %s: %s",
            issue_ref,
            err,
        ),
        log_backfill_warning=lambda issue_ref, session_id, err: logger.warning(
            "Review verdict backfill failed for %s (%s): %s",
            issue_ref,
            session_id,
            err,
        ),
        build_github_port_bundle=build_github_port_bundle,
        build_session_store=build_session_store,
        github_memo_factory=GitHubRuntimeMemo,
        wakeup_changed_review_queue_entries=_review_queue_state.wakeup_changed_review_queue_entries,
        pre_backfill_verdicts_for_due_prs=_codex_comment_wiring.pre_backfill_verdicts_for_due_prs,
        partition_review_queue_entries_by_probe_change=_review_queue_state.partition_review_queue_entries_by_probe_change,
        repark_unchanged_review_queue_entries=_review_queue_state.repark_unchanged_review_queue_entries,
        build_review_snapshots_for_queue_entries=_review_queue_processing.build_review_snapshots_for_queue_entries,
        backfill_review_verdicts_from_snapshots=_codex_comment_wiring.backfill_review_verdicts_from_snapshots,
    )


def prepare_review_queue_batch(
    *,
    config: ConsumerConfig,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    board_snapshot: CycleBoardSnapshot,
    pr_port: PullRequestPort,
    now: datetime,
    dry_run: bool,
    deps: ReviewQueueWiringDeps,
) -> tuple[PreparedReviewQueueBatch | None, ReviewQueueDrainSummary | None]:
    """Prepare the bounded review-queue workset for one drain cycle."""
    return _review_queue_processing.prepare_review_queue_batch(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        board_snapshot=board_snapshot,
        pr_port=pr_port,
        now=now,
        dry_run=dry_run,
        prepared_batch_factory=deps.prepared_batch_factory,
        summary_factory=deps.summary_factory,
        log_warning=deps.log_probe_warning,
        wakeup_changed_review_queue_entries_fn=deps.wakeup_changed_review_queue_entries,
    )


def prepare_review_queue_batch_from_shell(
    *,
    config: ConsumerConfig,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    board_snapshot: CycleBoardSnapshot,
    pr_port: PullRequestPort,
    now: datetime,
    dry_run: bool,
) -> tuple[PreparedReviewQueueBatch | None, ReviewQueueDrainSummary | None]:
    """Prepare the bounded review-queue workset using live shell deps."""
    return prepare_review_queue_batch(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        board_snapshot=board_snapshot,
        pr_port=pr_port,
        now=now,
        dry_run=dry_run,
        deps=build_review_queue_wiring_deps(),
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
    deps: ReviewQueueWiringDeps,
) -> PreparedDueReviewProcessing:
    """Prepare the changed due-review groups and snapshots for rescue processing."""
    return _review_queue_processing.prepare_due_review_processing(
        store=store,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        post_pr_codex_verdict=deps.post_pr_codex_verdict,
        prepared_due_processing_factory=deps.prepared_due_processing_factory,
        log_pre_backfill_warning=deps.log_pre_backfill_warning,
        log_backfill_warning=deps.log_backfill_warning,
        pre_backfill_verdicts_for_due_prs_fn=deps.pre_backfill_verdicts_for_due_prs,
        partition_review_queue_entries_by_probe_change_fn=deps.partition_review_queue_entries_by_probe_change,
        repark_unchanged_review_queue_entries_fn=deps.repark_unchanged_review_queue_entries,
        build_review_snapshots_for_queue_entries_fn=deps.build_review_snapshots_for_queue_entries,
        backfill_review_verdicts_from_snapshots_fn=deps.backfill_review_verdicts_from_snapshots,
    )


def prepare_due_review_processing_from_shell(
    *,
    store: SessionStorePort,
    automation_config: BoardAutomationConfig,
    pr_port: PullRequestPort,
    prepared_batch: PreparedReviewQueueBatch,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> PreparedDueReviewProcessing:
    """Prepare changed due-review groups and snapshots using live shell deps."""
    return prepare_due_review_processing(
        store=store,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        deps=build_review_queue_wiring_deps(),
    )


process_due_review_group = _review_queue_group_wiring.process_due_review_group
apply_review_queue_group_result = (
    _review_queue_group_wiring.apply_review_queue_group_result
)
summarize_review_group_outcome = (
    _review_queue_group_wiring.summarize_review_group_outcome
)
process_review_queue_due_groups = (
    _review_queue_group_wiring.process_review_queue_due_groups
)


def process_due_review_group_from_shell(
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
) -> ReviewGroupProcessingOutcome:
    """Process one due PR group using live shell deps."""
    return process_due_review_group(
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
        review_group_outcome_factory=build_review_queue_wiring_deps().review_group_outcome_factory,
        review_rescue_fn=build_review_queue_wiring_deps().review_rescue_fn,
        escalate_to_claude=build_review_queue_wiring_deps().escalate_to_claude,
    )


def apply_review_queue_group_result_from_shell(
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
) -> tuple[str, ...]:
    """Persist one review-group result using live shell deps."""
    return apply_review_queue_group_result(
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
        escalate_to_claude=build_review_queue_wiring_deps().escalate_to_claude,
    )


def summarize_review_group_outcome_from_shell(
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
) -> ReviewGroupProcessingOutcome:
    """Build one review-group outcome using live shell deps."""
    return summarize_review_group_outcome(
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
        escalate_to_claude=build_review_queue_wiring_deps().escalate_to_claude,
        review_group_outcome_factory=build_review_queue_wiring_deps().review_group_outcome_factory,
    )


def process_review_queue_due_groups_from_shell(
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
    gh_runner: Callable[..., str] | None = None,
) -> ReviewQueueProcessingOutcome:
    """Process due review groups using live shell deps."""
    return process_review_queue_due_groups(
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
        post_pr_codex_verdict=build_review_queue_wiring_deps().post_pr_codex_verdict,
        review_rescue_fn=build_review_queue_wiring_deps().review_rescue_fn,
        escalate_to_claude=build_review_queue_wiring_deps().escalate_to_claude,
        prepared_due_processing_factory=build_review_queue_wiring_deps().prepared_due_processing_factory,
        review_group_outcome_factory=build_review_queue_wiring_deps().review_group_outcome_factory,
        review_queue_processing_outcome_factory=build_review_queue_wiring_deps().review_queue_processing_outcome_factory,
        gh_reason_code=build_review_queue_wiring_deps().gh_reason_code,
        log_pre_backfill_warning=build_review_queue_wiring_deps().log_pre_backfill_warning,
        log_backfill_warning=build_review_queue_wiring_deps().log_backfill_warning,
        pre_backfill_verdicts_for_due_prs_fn=build_review_queue_wiring_deps().pre_backfill_verdicts_for_due_prs,
        partition_review_queue_entries_by_probe_change_fn=build_review_queue_wiring_deps().partition_review_queue_entries_by_probe_change,
        repark_unchanged_review_queue_entries_fn=build_review_queue_wiring_deps().repark_unchanged_review_queue_entries,
        build_review_snapshots_for_queue_entries_fn=build_review_queue_wiring_deps().build_review_snapshots_for_queue_entries,
        backfill_review_verdicts_from_snapshots_fn=build_review_queue_wiring_deps().backfill_review_verdicts_from_snapshots,
    )


def drain_review_queue(
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    pr_port: PullRequestPort | None = None,
    session_store: SessionStorePort | None = None,
    board_snapshot: CycleBoardSnapshot | None = None,
    dry_run: bool = False,
    github_memo: GitHubRuntimeMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
    deps: ReviewQueueWiringDeps,
) -> tuple[ReviewQueueDrainSummary, CycleBoardSnapshot]:
    """Process a bounded batch of queued Review items."""
    return _review_queue_processing.drain_review_queue(
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
        build_github_port_bundle=deps.build_github_port_bundle,
        build_session_store=deps.build_session_store,
        github_memo_factory=deps.github_memo_factory,
        prepared_batch_factory=deps.prepared_batch_factory,
        summary_factory=deps.summary_factory,
        prepared_due_processing_factory=deps.prepared_due_processing_factory,
        review_group_outcome_factory=deps.review_group_outcome_factory,
        review_queue_processing_outcome_factory=deps.review_queue_processing_outcome_factory,
        post_pr_codex_verdict=deps.post_pr_codex_verdict,
        review_rescue_fn=deps.review_rescue_fn,
        escalate_to_claude=deps.escalate_to_claude,
        gh_reason_code=deps.gh_reason_code,
        log_probe_warning=deps.log_probe_warning,
        log_pre_backfill_warning=deps.log_pre_backfill_warning,
        log_backfill_warning=deps.log_backfill_warning,
        wakeup_changed_review_queue_entries_fn=deps.wakeup_changed_review_queue_entries,
        pre_backfill_verdicts_for_due_prs_fn=deps.pre_backfill_verdicts_for_due_prs,
        partition_review_queue_entries_by_probe_change_fn=deps.partition_review_queue_entries_by_probe_change,
        repark_unchanged_review_queue_entries_fn=deps.repark_unchanged_review_queue_entries,
        build_review_snapshots_for_queue_entries_fn=deps.build_review_snapshots_for_queue_entries,
        backfill_review_verdicts_from_snapshots_fn=deps.backfill_review_verdicts_from_snapshots,
    )


def drain_review_queue_from_shell(
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    pr_port: PullRequestPort | None = None,
    session_store: SessionStorePort | None = None,
    board_snapshot: CycleBoardSnapshot | None = None,
    dry_run: bool = False,
    github_memo: GitHubRuntimeMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[ReviewQueueDrainSummary, CycleBoardSnapshot]:
    """Process queued Review items using live shell deps."""
    return drain_review_queue(
        config=config,
        db=db,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=pr_port,
        session_store=session_store,
        board_snapshot=board_snapshot,
        dry_run=dry_run,
        github_memo=github_memo,
        gh_runner=gh_runner,
        deps=build_review_queue_wiring_deps(),
    )


def run_review_rescue_for_group_from_shell(
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
) -> _review_queue_processing.ReviewRescueExecution:
    """Run rescue logic for one due review group using live shell deps."""
    return _review_queue_processing.run_review_rescue_for_group(
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
        review_rescue_fn=build_review_queue_wiring_deps().review_rescue_fn,
    )


def seed_new_review_entries_from_shell(
    store: SessionStorePort,
    review_refs: set[str],
    existing_refs: set[str],
    *,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[str]:
    """Seed queue entries for Review issues not yet tracked."""
    return _review_queue_state.seed_new_review_entries(
        store,
        review_refs,
        existing_refs,
        dry_run=dry_run,
        now=now,
    )
