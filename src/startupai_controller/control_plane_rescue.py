"""Shared control-plane rescue operations used by consumer and control-plane shells."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from startupai_controller.board_automation_config import BoardAutomationConfig
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
    now = datetime.now(timezone.utc)
    effective_pr_port = pr_port or build_github_port_bundle(
        config.project_owner,
        config.project_number,
        config=critical_path_config,
        github_memo=memo,
        gh_runner=gh_runner,
    ).pull_requests

    prepared_batch, empty_summary = consumer._prepare_review_queue_batch(
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

    processing_outcome = consumer._process_review_queue_due_groups(
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

    summary = ReviewQueueDrainSummary(
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
