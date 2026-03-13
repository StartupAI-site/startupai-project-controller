"""Due-review drain processing extracted from review-queue helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Protocol

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


class ReviewRescueExecutionView(Protocol):
    """Structural view of one review-rescue execution result."""

    result: ReviewRescueResult
    partial_failure: bool
    error: str | None


def prepare_due_review_processing(
    *,
    store: SessionStorePort,
    automation_config: BoardAutomationConfig,
    pr_port: PullRequestPort,
    prepared_batch: PreparedReviewQueueBatch,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    prepared_due_processing_factory: type[PreparedDueReviewProcessing],
    pre_backfill_verdicts_for_due_prs_fn: Callable[..., tuple[str, ...]],
    partition_review_queue_entries_by_probe_change_fn: Callable[
        ...,
        tuple[list[ReviewQueueEntry], list[ReviewQueueEntry]],
    ],
    repark_unchanged_review_queue_entries_fn: Callable[..., None],
    build_review_snapshots_for_queue_entries_fn: Callable[
        ...,
        dict[tuple[str, int], ReviewSnapshot],
    ],
    backfill_review_verdicts_from_snapshots_fn: Callable[..., tuple[str, ...]],
) -> PreparedDueReviewProcessing:
    """Prepare the changed due-review groups and snapshots for rescue processing."""
    due_items = list(prepared_batch.due_items)
    due_pr_groups = [
        (pr_key, list(entries)) for pr_key, entries in prepared_batch.due_pr_groups
    ]
    selected_snapshot_entries = list(prepared_batch.selected_snapshot_entries)

    pre_backfilled: tuple[str, ...] = ()
    if not dry_run and due_items:
        pre_backfilled = pre_backfill_verdicts_for_due_prs_fn(
            store,
            due_items,
            pr_port=pr_port,
            gh_runner=gh_runner,
        )

    verdict_backfilled: tuple[str, ...] = ()
    partial_failure = False
    error: str | None = None
    snapshots: dict[tuple[str, int], ReviewSnapshot] = {}

    if due_pr_groups:
        try:
            changed_due_items, unchanged_due_items = (
                partition_review_queue_entries_by_probe_change_fn(
                    due_items,
                    pr_port=pr_port,
                )
            )
            if unchanged_due_items and not dry_run:
                repark_unchanged_review_queue_entries_fn(
                    store,
                    unchanged_due_items,
                    now=now,
                )
            due_pr_keys = {
                (entry.pr_repo, entry.pr_number) for entry in changed_due_items
            }
            due_pr_groups = [
                (pr_key, entries)
                for pr_key, entries in due_pr_groups
                if pr_key in due_pr_keys
            ]
            selected_snapshot_entries = [
                entry
                for entry in selected_snapshot_entries
                if (entry.pr_repo, entry.pr_number) in due_pr_keys
            ]
            due_items = changed_due_items
            if due_pr_groups:
                snapshots = build_review_snapshots_for_queue_entries_fn(
                    queue_entries=selected_snapshot_entries,
                    review_refs=set(prepared_batch.review_refs),
                    pr_port=pr_port,
                    trusted_codex_actors=frozenset(
                        automation_config.trusted_codex_actors
                    ),
                )
        except GhQueryError as err:
            partial_failure = True
            error = str(err)

        if not dry_run and not partial_failure:
            secondary_backfilled = backfill_review_verdicts_from_snapshots_fn(
                store,
                due_items,
                snapshots,
                pr_port=pr_port,
                gh_runner=gh_runner,
            )
            verdict_backfilled = tuple(
                dict.fromkeys(pre_backfilled + secondary_backfilled)
            )

    return prepared_due_processing_factory(
        due_items=tuple(due_items),
        due_pr_groups=tuple(
            (pr_key, tuple(entries)) for pr_key, entries in due_pr_groups
        ),
        snapshots=snapshots,
        verdict_backfilled=verdict_backfilled,
        partial_failure=partial_failure,
        error=error,
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
    run_review_rescue_for_group_fn: Callable[..., ReviewRescueExecutionView],
    apply_review_queue_group_result_fn: Callable[..., tuple[str, ...]],
    summarize_review_group_outcome_fn: Callable[..., ReviewGroupProcessingOutcome],
) -> ReviewGroupProcessingOutcome:
    """Process one due PR group from the review queue."""
    if snapshot is None:
        return review_group_outcome_factory(
            rerun=(),
            auto_merge_enabled=(),
            requeued=(),
            blocked=(),
            skipped=(),
            escalated=(),
            updated_snapshot=updated_snapshot,
            partial_failure=True,
            error=f"missing-review-snapshot:{pr_repo}#{pr_number}",
        )

    rescue_result = run_review_rescue_for_group_fn(
        config=config,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=pr_port,
        pr_repo=pr_repo,
        pr_number=pr_number,
        snapshot=snapshot,
        dry_run=dry_run,
        gh_runner=gh_runner,
    )
    if rescue_result.partial_failure:
        return review_group_outcome_factory(
            rerun=(),
            auto_merge_enabled=(),
            requeued=(),
            blocked=(),
            skipped=(),
            escalated=(),
            updated_snapshot=updated_snapshot,
            partial_failure=True,
            error=rescue_result.error,
        )

    escalated = apply_review_queue_group_result_fn(
        store=store,
        critical_path_config=critical_path_config,
        project_owner=config.project_owner,
        project_number=config.project_number,
        pr_port=pr_port,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        result=rescue_result.result,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
    )
    return summarize_review_group_outcome_fn(
        critical_path_config=critical_path_config,
        store=store,
        project_owner=config.project_owner,
        project_number=config.project_number,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        result=rescue_result.result,
        updated_snapshot=updated_snapshot,
        escalated=escalated,
        dry_run=dry_run,
        gh_runner=gh_runner,
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
    review_queue_processing_outcome_factory: type[ReviewQueueProcessingOutcome],
    gh_reason_code: Callable[[Exception], str],
    prepare_due_review_processing_fn: Callable[..., PreparedDueReviewProcessing],
    process_due_review_group_fn: Callable[..., ReviewGroupProcessingOutcome],
    apply_review_queue_partial_failure_fn: Callable[..., None],
) -> ReviewQueueProcessingOutcome:
    """Process the due PR groups for a prepared review-queue batch."""
    prepared_due_processing = prepare_due_review_processing_fn(
        store=store,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
    )

    rerun: list[str] = []
    auto_merge_enabled: list[str] = []
    requeued: list[str] = []
    blocked: list[str] = []
    skipped: list[str] = []
    escalated: list[str] = []
    partial_failure = prepared_due_processing.partial_failure
    error = prepared_due_processing.error
    updated_snapshot = board_snapshot

    for (pr_repo, pr_number), entries in prepared_due_processing.due_pr_groups:
        if partial_failure:
            break
        group_outcome = process_due_review_group_fn(
            config=config,
            store=store,
            critical_path_config=critical_path_config,
            automation_config=automation_config,
            pr_port=pr_port,
            pr_repo=pr_repo,
            pr_number=pr_number,
            entries=entries,
            snapshot=prepared_due_processing.snapshots.get((pr_repo, pr_number)),
            updated_snapshot=updated_snapshot,
            now=now,
            dry_run=dry_run,
            gh_runner=gh_runner,
        )
        if group_outcome.partial_failure:
            partial_failure = True
            error = group_outcome.error
            break
        rerun.extend(group_outcome.rerun)
        auto_merge_enabled.extend(group_outcome.auto_merge_enabled)
        requeued.extend(group_outcome.requeued)
        blocked.extend(group_outcome.blocked)
        skipped.extend(group_outcome.skipped)
        escalated.extend(group_outcome.escalated)
        updated_snapshot = group_outcome.updated_snapshot

    if partial_failure and not dry_run:
        apply_review_queue_partial_failure_fn(
            store,
            list(prepared_due_processing.due_items),
            config=config,
            error=error,
            gh_reason_code=gh_reason_code,
            now=now,
        )

    return review_queue_processing_outcome_factory(
        due_count=len(prepared_due_processing.due_items),
        verdict_backfilled=prepared_due_processing.verdict_backfilled,
        rerun=tuple(rerun),
        auto_merge_enabled=tuple(auto_merge_enabled),
        requeued=tuple(requeued),
        blocked=tuple(blocked),
        skipped=tuple(skipped),
        escalated=tuple(escalated),
        partial_failure=partial_failure,
        error=error,
        updated_snapshot=updated_snapshot,
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
    summary_factory: type[ReviewQueueDrainSummary],
    prepare_review_queue_batch_fn: Callable[
        ...,
        tuple[PreparedReviewQueueBatch | None, ReviewQueueDrainSummary | None],
    ],
    process_review_queue_due_groups_fn: Callable[..., ReviewQueueProcessingOutcome],
) -> tuple[ReviewQueueDrainSummary, CycleBoardSnapshot]:
    """Process a bounded batch of queued Review items."""
    memo = github_memo or github_memo_factory()
    if automation_config is None:
        return summary_factory(), (
            board_snapshot
            if board_snapshot is not None
            else build_github_port_bundle(
                config.project_owner,
                config.project_number,
                config=critical_path_config,
                github_memo=memo,
                gh_runner=gh_runner,
            ).review_state.build_board_snapshot()
        )

    effective_board_snapshot = (
        board_snapshot
        or build_github_port_bundle(
            config.project_owner,
            config.project_number,
            config=critical_path_config,
            github_memo=memo,
            gh_runner=gh_runner,
        ).review_state.build_board_snapshot()
    )

    store = session_store or build_session_store(db)
    now = datetime.now(timezone.utc)
    effective_pr_port = (
        pr_port
        or build_github_port_bundle(
            config.project_owner,
            config.project_number,
            config=critical_path_config,
            github_memo=memo,
            gh_runner=gh_runner,
        ).pull_requests
    )

    prepared_batch, empty_summary = prepare_review_queue_batch_fn(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        board_snapshot=effective_board_snapshot,
        pr_port=effective_pr_port,
        now=now,
        dry_run=dry_run,
    )
    if empty_summary is not None:
        return empty_summary, effective_board_snapshot
    assert prepared_batch is not None

    processing_outcome = process_review_queue_due_groups_fn(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=effective_pr_port,
        prepared_batch=prepared_batch,
        board_snapshot=effective_board_snapshot,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
    )

    summary = summary_factory(
        queued_count=len(prepared_batch.queue_items),
        due_count=processing_outcome.due_count,
        seeded=prepared_batch.seeded,
        removed=prepared_batch.removed,
        verdict_backfilled=processing_outcome.verdict_backfilled,
        rerun=processing_outcome.rerun,
        auto_merge_enabled=processing_outcome.auto_merge_enabled,
        requeued=processing_outcome.requeued,
        blocked=processing_outcome.blocked,
        skipped=processing_outcome.skipped,
        escalated=processing_outcome.escalated,
        partial_failure=processing_outcome.partial_failure,
        error=processing_outcome.error,
    )
    return summary, processing_outcome.updated_snapshot
