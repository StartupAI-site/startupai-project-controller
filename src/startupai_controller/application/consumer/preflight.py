"""Preflight orchestration for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ReviewQueueDrainSummary,
)


@dataclass(frozen=True)
class PrepareCyclePhasesDeps:
    """Injected seams for the consumer preflight phase sequence."""

    run_deferred_replay_phase: Callable[..., None]
    load_board_snapshot_phase: Callable[..., CycleBoardSnapshot]
    run_executor_routing_phase: Callable[..., None]
    run_reconciliation_phase: Callable[..., None]
    run_review_queue_phase: Callable[
        ..., tuple[ReviewQueueDrainSummary, CycleBoardSnapshot]
    ]
    run_admission_phase: Callable[..., dict[str, Any]]


@dataclass(frozen=True)
class PrepareCyclePhasesResult:
    """Result of one consumer preflight phase pass."""

    board_snapshot: CycleBoardSnapshot
    review_queue_summary: ReviewQueueDrainSummary
    admission_summary: dict[str, Any]
    timings_ms: dict[str, int]


def execute_prepare_cycle_phases(
    config: Any,
    db: Any,
    *,
    runtime: Any,
    deps: PrepareCyclePhasesDeps,
    dry_run: bool = False,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> PrepareCyclePhasesResult:
    """Run the preflight phases for one consumer cycle."""
    timings_ms: dict[str, int] = {}

    deps.run_deferred_replay_phase(
        config,
        db,
        runtime,
        timings_ms=timings_ms,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )
    board_snapshot = deps.load_board_snapshot_phase(
        config,
        runtime,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
    )
    deps.run_executor_routing_phase(
        config,
        db,
        runtime,
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )
    deps.run_reconciliation_phase(
        config,
        db,
        runtime,
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    review_queue_summary, board_snapshot = deps.run_review_queue_phase(
        config,
        db,
        runtime,
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )
    admission_summary = deps.run_admission_phase(
        config,
        db,
        runtime,
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )

    return PrepareCyclePhasesResult(
        board_snapshot=board_snapshot,
        review_queue_summary=review_queue_summary,
        admission_summary=admission_summary,
        timings_ms=timings_ms,
    )


@dataclass(frozen=True)
class ReconciliationResult:
    """Summary of consumer-led board truth reconciliation."""

    moved_ready: tuple[str, ...] = ()
    moved_in_progress: tuple[str, ...] = ()
    moved_review: tuple[str, ...] = ()
    moved_blocked: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReconciliationDeps:
    """Injected seams for board-truth reconciliation."""

    build_session_store: Callable[[Any], Any]
    build_github_port_bundle: Callable[..., Any]
    issue_ref_for_snapshot: Callable[[Any], str | None]
    reconcile_active_repair_review_items: Callable[..., list[str]]
    reconcile_stale_in_progress_items: Callable[..., tuple[list[str], list[str], list[str]]]


def reconcile_board_truth(
    consumer_config: Any,
    critical_path_config: Any,
    automation_config: Any,
    db: Any,
    *,
    deps: ReconciliationDeps,
    session_store: Any | None = None,
    pr_port: Any | None = None,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
    dry_run: bool = False,
    board_snapshot: CycleBoardSnapshot | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ReconciliationResult:
    """Make board `In Progress` truthful against local consumer state."""
    if automation_config is None:
        return ReconciliationResult()

    store = session_store or deps.build_session_store(db)
    active_workers = store.active_workers()
    active_issue_refs = {worker.issue_ref for worker in active_workers}
    active_repair_issue_refs = {
        worker.issue_ref
        for worker in active_workers
        if worker.session_kind == "repair"
    }
    moved_ready: list[str] = []
    moved_in_progress: list[str] = []
    moved_review: list[str] = []
    moved_blocked: list[str] = []

    effective_pr_port = pr_port
    if effective_pr_port is None:
        effective_pr_port = deps.build_github_port_bundle(
            consumer_config.project_owner,
            consumer_config.project_number,
            config=critical_path_config,
            gh_runner=gh_runner,
        ).pull_requests
    effective_review_state_port = review_state_port or effective_pr_port
    effective_board_port = board_port or effective_pr_port

    moved_in_progress.extend(
        deps.reconcile_active_repair_review_items(
            consumer_config,
            critical_path_config,
            active_repair_issue_refs=active_repair_issue_refs,
            review_state_port=effective_review_state_port,
            board_port=effective_board_port,
            board_snapshot=board_snapshot,
            issue_ref_for_snapshot=deps.issue_ref_for_snapshot,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
            dry_run=dry_run,
        )
    )
    ready_refs, review_refs, blocked_refs = deps.reconcile_stale_in_progress_items(
        consumer_config,
        critical_path_config,
        automation_config,
        store=store,
        pr_port=effective_pr_port,
        review_state_port=effective_review_state_port,
        board_port=effective_board_port,
        board_snapshot=board_snapshot,
        issue_ref_for_snapshot=deps.issue_ref_for_snapshot,
        active_issue_refs=active_issue_refs,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )
    moved_ready.extend(ready_refs)
    moved_review.extend(review_refs)
    moved_blocked.extend(blocked_refs)

    return ReconciliationResult(
        moved_ready=tuple(moved_ready),
        moved_in_progress=tuple(moved_in_progress),
        moved_review=tuple(moved_review),
        moved_blocked=tuple(moved_blocked),
    )
