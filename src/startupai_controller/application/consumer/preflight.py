"""Preflight orchestration for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ReviewQueueDrainSummary,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort


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
) -> PrepareCyclePhasesResult:
    """Run the preflight phases for one consumer cycle."""
    timings_ms: dict[str, int] = {}

    deps.run_deferred_replay_phase(
        config,
        db,
        runtime,
        timings_ms=timings_ms,
        dry_run=dry_run,
    )
    board_snapshot = deps.load_board_snapshot_phase(
        config,
        runtime,
        timings_ms=timings_ms,
    )
    deps.run_executor_routing_phase(
        config,
        db,
        runtime,
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        dry_run=dry_run,
    )
    deps.run_reconciliation_phase(
        config,
        db,
        runtime,
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
    )
    review_queue_summary, board_snapshot = deps.run_review_queue_phase(
        config,
        db,
        runtime,
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        dry_run=dry_run,
    )
    admission_summary = deps.run_admission_phase(
        config,
        db,
        runtime,
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
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

    issue_ref_for_snapshot: Callable[[Any], str | None]
    reconcile_active_repair_review_items: Callable[..., list[str]]
    reconcile_stale_in_progress_items: Callable[
        ..., tuple[list[str], list[str], list[str]]
    ]


def reconcile_board_truth(
    consumer_config: Any,
    critical_path_config: Any,
    automation_config: Any,
    db: Any,
    *,
    deps: ReconciliationDeps,
    session_store: SessionStorePort,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    dry_run: bool = False,
    board_snapshot: CycleBoardSnapshot | None = None,
) -> ReconciliationResult:
    """Make board `In Progress` truthful against local consumer state."""
    if automation_config is None:
        return ReconciliationResult()

    store = session_store
    active_workers = store.active_workers()
    active_issue_refs = {worker.issue_ref for worker in active_workers}
    active_repair_issue_refs = {
        worker.issue_ref for worker in active_workers if worker.session_kind == "repair"
    }
    moved_ready: list[str] = []
    moved_in_progress: list[str] = []
    moved_review: list[str] = []
    moved_blocked: list[str] = []

    moved_in_progress.extend(
        deps.reconcile_active_repair_review_items(
            consumer_config,
            critical_path_config,
            active_repair_issue_refs=active_repair_issue_refs,
            review_state_port=review_state_port,
            board_port=board_port,
            board_snapshot=board_snapshot,
            issue_ref_for_snapshot=deps.issue_ref_for_snapshot,
            dry_run=dry_run,
        )
    )
    ready_refs, review_refs, blocked_refs = deps.reconcile_stale_in_progress_items(
        consumer_config,
        critical_path_config,
        automation_config,
        store=store,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        board_snapshot=board_snapshot,
        issue_ref_for_snapshot=deps.issue_ref_for_snapshot,
        active_issue_refs=active_issue_refs,
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
