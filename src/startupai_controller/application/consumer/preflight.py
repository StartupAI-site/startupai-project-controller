"""Preflight orchestration for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.application.consumer.preflight_runtime import (
    CycleRuntimeContext,
)
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ReviewQueueDrainSummary,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig


class RunDeferredReplayPhaseFn(Protocol):
    """Replay deferred actions before loading the next board snapshot."""

    def __call__(
        self,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        runtime: CycleRuntimeContext,
        *,
        timings_ms: dict[str, int],
        dry_run: bool,
    ) -> None:
        """Run the deferred replay phase."""
        ...


class LoadBoardSnapshotPhaseFn(Protocol):
    """Load the thin cycle board snapshot."""

    def __call__(
        self,
        config: ConsumerConfig,
        runtime: CycleRuntimeContext,
        *,
        timings_ms: dict[str, int],
    ) -> CycleBoardSnapshot:
        """Return the current board snapshot."""
        ...


class RunExecutorRoutingPhaseFn(Protocol):
    """Normalize executor routing before launch selection."""

    def __call__(
        self,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        runtime: CycleRuntimeContext,
        *,
        board_snapshot: CycleBoardSnapshot,
        timings_ms: dict[str, int],
        dry_run: bool,
    ) -> None:
        """Run the executor routing phase."""
        ...


class RunReconciliationPhaseFn(Protocol):
    """Reconcile board truth against local runtime state."""

    def __call__(
        self,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        runtime: CycleRuntimeContext,
        *,
        board_snapshot: CycleBoardSnapshot,
        timings_ms: dict[str, int],
    ) -> None:
        """Run the reconciliation phase."""
        ...


class RunReviewQueuePhaseFn(Protocol):
    """Drain the review queue for the current cycle."""

    def __call__(
        self,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        runtime: CycleRuntimeContext,
        *,
        board_snapshot: CycleBoardSnapshot,
        timings_ms: dict[str, int],
        dry_run: bool,
    ) -> tuple[ReviewQueueDrainSummary, CycleBoardSnapshot]:
        """Run the review queue phase."""
        ...


class RunAdmissionPhaseFn(Protocol):
    """Admit backlog items after other preflight phases complete."""

    def __call__(
        self,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        runtime: CycleRuntimeContext,
        *,
        board_snapshot: CycleBoardSnapshot,
        timings_ms: dict[str, int],
        dry_run: bool,
    ) -> dict[str, Any]:
        """Run the backlog admission phase."""
        ...


class SnapshotIssueRefView(Protocol):
    """Minimal snapshot surface needed for board-truth normalization."""

    issue_ref: str


class ReconcileActiveRepairReviewItemsFn(Protocol):
    """Return active repair items that should move back to In Progress."""

    def __call__(
        self,
        consumer_config: ConsumerConfig,
        critical_path_config: CriticalPathConfig,
        *,
        active_repair_issue_refs: set[str],
        review_state_port: ReviewStatePort,
        board_port: BoardMutationPort,
        board_snapshot: CycleBoardSnapshot | None,
        issue_ref_for_snapshot: Callable[[SnapshotIssueRefView], str | None],
        dry_run: bool,
    ) -> list[str]:
        """Return issue refs moved back to In Progress."""
        ...


class ReconcileStaleInProgressItemsFn(Protocol):
    """Return stale In Progress issues split by truthful destination lane."""

    def __call__(
        self,
        consumer_config: ConsumerConfig,
        critical_path_config: CriticalPathConfig,
        automation_config: BoardAutomationConfig,
        *,
        store: SessionStorePort,
        pr_port: PullRequestPort,
        review_state_port: ReviewStatePort,
        board_port: BoardMutationPort,
        board_snapshot: CycleBoardSnapshot | None,
        issue_ref_for_snapshot: Callable[[SnapshotIssueRefView], str | None],
        active_issue_refs: set[str],
        dry_run: bool,
    ) -> tuple[list[str], list[str], list[str]]:
        """Return (ready, review, blocked) issue refs."""
        ...


@dataclass(frozen=True)
class PrepareCyclePhasesDeps:
    """Injected seams for the consumer preflight phase sequence."""

    run_deferred_replay_phase: RunDeferredReplayPhaseFn
    load_board_snapshot_phase: LoadBoardSnapshotPhaseFn
    run_executor_routing_phase: RunExecutorRoutingPhaseFn
    run_reconciliation_phase: RunReconciliationPhaseFn
    run_review_queue_phase: RunReviewQueuePhaseFn
    run_admission_phase: RunAdmissionPhaseFn


@dataclass(frozen=True)
class PrepareCyclePhasesResult:
    """Result of one consumer preflight phase pass."""

    board_snapshot: CycleBoardSnapshot
    review_queue_summary: ReviewQueueDrainSummary
    admission_summary: dict[str, Any]
    timings_ms: dict[str, int]


def execute_prepare_cycle_phases(
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    *,
    runtime: CycleRuntimeContext,
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

    issue_ref_for_snapshot: Callable[[SnapshotIssueRefView], str | None]
    reconcile_active_repair_review_items: ReconcileActiveRepairReviewItemsFn
    reconcile_stale_in_progress_items: ReconcileStaleInProgressItemsFn


def reconcile_board_truth(
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    db: ConsumerRuntimeStatePort,
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
