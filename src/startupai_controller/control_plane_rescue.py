"""Shared control-plane rescue operations used by consumer and control-plane shells."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from startupai_controller import consumer_review_queue_helpers as _review_queue_helpers
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.board_automation import review_rescue
from startupai_controller.control_plane_runtime import (
    _clear_degraded,
    _record_successful_github_mutation,
)
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ReviewQueueDrainSummary,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.runtime.wiring import (
    GitHubRuntimeMemo as CycleGitHubMemo,
    build_github_port_bundle,
    build_session_store,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
)


def _consumer_module():
    """Import the consumer module lazily to avoid entrypoint import cycles."""
    from startupai_controller import board_consumer

    return board_consumer


def _drain_review_queue(
    config: Any,
    db: Any,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    *,
    pr_port: PullRequestPort | None = None,
    session_store: SessionStorePort | None = None,
    board_snapshot: CycleBoardSnapshot | None = None,
    dry_run: bool = False,
    github_memo: CycleGitHubMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[ReviewQueueDrainSummary, CycleBoardSnapshot]:
    """Process a bounded batch of queued Review items."""
    consumer = _consumer_module()
    memo = github_memo or CycleGitHubMemo()
    if automation_config is None:
        return ReviewQueueDrainSummary(), (
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

    effective_board_snapshot = board_snapshot or build_github_port_bundle(
        config.project_owner,
        config.project_number,
        config=critical_path_config,
        github_memo=memo,
        gh_runner=gh_runner,
    ).review_state.build_board_snapshot()

    store = session_store or build_session_store(db)
    return _review_queue_helpers.drain_review_queue(
        config,
        db,
        critical_path_config,
        automation_config,
        pr_port=pr_port,
        session_store=store,
        board_snapshot=effective_board_snapshot,
        dry_run=dry_run,
        github_memo=memo,
        gh_runner=gh_runner,
        build_github_port_bundle=build_github_port_bundle,
        build_session_store=build_session_store,
        github_memo_factory=CycleGitHubMemo,
        prepared_batch_factory=consumer.PreparedReviewQueueBatch,
        summary_factory=ReviewQueueDrainSummary,
        prepared_due_processing_factory=consumer.PreparedDueReviewProcessing,
        review_group_outcome_factory=consumer.ReviewGroupProcessingOutcome,
        review_queue_processing_outcome_factory=consumer.ReviewQueueProcessingOutcome,
        post_pr_codex_verdict=consumer._post_pr_codex_verdict,
        review_rescue_fn=consumer.review_rescue,
        escalate_to_claude=consumer._escalate_to_claude,
        gh_reason_code=consumer.gh_reason_code,
        log_probe_warning=lambda err: consumer.logger.warning(
            "Review queue wakeup probe failed: %s",
            err,
        ),
        log_pre_backfill_warning=lambda issue_ref, err: consumer.logger.warning(
            "Pre-backfill verdict failed for %s: %s",
            issue_ref,
            err,
        ),
        log_backfill_warning=lambda issue_ref, session_id, err: consumer.logger.warning(
            "Review verdict backfill failed for %s (%s): %s",
            issue_ref,
            session_id,
            err,
        ),
        wakeup_changed_review_queue_entries_fn=consumer._wakeup_changed_review_queue_entries,
        pre_backfill_verdicts_for_due_prs_fn=consumer._pre_backfill_verdicts_for_due_prs,
        partition_review_queue_entries_by_probe_change_fn=consumer._partition_review_queue_entries_by_probe_change,
        repark_unchanged_review_queue_entries_fn=consumer._repark_unchanged_review_queue_entries,
        build_review_snapshots_for_queue_entries_fn=consumer._build_review_snapshots_for_queue_entries,
        backfill_review_verdicts_from_snapshots_fn=consumer._backfill_review_verdicts_from_snapshots,
    )


def _replay_deferred_actions(
    db: Any,
    config: Any,
    critical_path_config: CriticalPathConfig,
    *,
    pr_port: PullRequestPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[int, ...]:
    """Replay queued control-plane actions after GitHub recovery."""
    consumer = _consumer_module()
    replayed: list[int] = []
    for action in db.list_deferred_actions():
        try:
            consumer._replay_deferred_action(
                action=action,
                config=config,
                critical_path_config=critical_path_config,
                pr_port=pr_port,
                review_state_port=review_state_port,
                board_port=board_port,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                comment_checker=comment_checker,
                comment_poster=comment_poster,
                gh_runner=gh_runner,
            )
        except GhQueryError:
            raise
        except Exception as error:
            raise GhQueryError(
                f"Deferred action {action.id} failed: {error}"
            ) from error

        db.delete_deferred_action(action.id)
        _record_successful_github_mutation(db)
        replayed.append(action.id)

    if replayed:
        _clear_degraded(db)
    return tuple(replayed)
