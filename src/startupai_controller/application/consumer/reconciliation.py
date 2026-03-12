"""Reconciliation wiring for the consumer application layer.

This module wires the board-truth reconciliation engine by injecting
concrete dependencies from the outer layers into the use-case functions
in preflight.py and the helper functions in consumer_board_state_helpers.

board_consumer.py retains thin compatibility wrappers that delegate here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.application.consumer.preflight import (
    ReconciliationDeps,
    ReconciliationResult,
    reconcile_board_truth as _reconcile_board_truth_use_case,
)
from startupai_controller.domain.models import CycleBoardSnapshot
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig


class SnapshotIssueRefView(Protocol):
    """Minimal snapshot surface needed for issue-ref normalization."""

    issue_ref: str


# ---------------------------------------------------------------------------
# Wiring deps
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReconciliationWiringDeps:
    """Injected concrete seams for reconciliation wiring.

    These are the outer-layer functions that the reconciliation wrappers
    in board_consumer.py previously injected inline.
    """

    # Board state helpers module (real bodies)
    board_state_reconcile_active_repair: Callable[..., list[str]]
    board_state_reconcile_single: Callable[..., str]
    board_state_reconcile_stale: Callable[..., tuple[list[str], list[str], list[str]]]

    # Board transition functions
    transition_issue_to_in_progress: Callable[..., None]
    return_issue_to_ready: Callable[..., None]
    transition_issue_to_review: Callable[..., None]
    set_blocked_with_reason: Callable[..., None]

    # Utilities
    resolve_issue_coordinates: Callable[[str, CriticalPathConfig], tuple[str, str, int]]
    classify_open_pr_candidates: Callable[..., tuple[str, object | None, str]]
    reconcile_in_progress_decision: Callable[..., str]
    snapshot_to_issue_ref: Callable[[str, dict[str, str]], str | None]


# ---------------------------------------------------------------------------
# Wired helper functions
# ---------------------------------------------------------------------------


def reconcile_active_repair_review_items(
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    *,
    deps: ReconciliationWiringDeps,
    active_repair_issue_refs: set[str],
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_snapshot: CycleBoardSnapshot | None,
    issue_ref_for_snapshot: Callable[[SnapshotIssueRefView], str | None],
    dry_run: bool,
) -> list[str]:
    """Return active repair items that should move from Review back to In Progress."""
    return deps.board_state_reconcile_active_repair(
        consumer_config,
        critical_path_config,
        active_repair_issue_refs=active_repair_issue_refs,
        review_state_port=review_state_port,
        board_port=board_port,
        board_snapshot=board_snapshot,
        issue_ref_for_snapshot=issue_ref_for_snapshot,
        dry_run=dry_run,
        transition_issue_to_in_progress=deps.transition_issue_to_in_progress,
    )


def reconcile_single_in_progress_item(
    issue_ref: str,
    *,
    deps: ReconciliationWiringDeps,
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    store: SessionStorePort,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    dry_run: bool,
) -> str:
    """Reconcile one stale In Progress item and return its target lane."""
    return deps.board_state_reconcile_single(
        issue_ref,
        consumer_config=consumer_config,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        store=store,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        dry_run=dry_run,
        resolve_issue_coordinates=deps.resolve_issue_coordinates,
        classify_open_pr_candidates=deps.classify_open_pr_candidates,
        reconcile_in_progress_decision=deps.reconcile_in_progress_decision,
        return_issue_to_ready=deps.return_issue_to_ready,
        transition_issue_to_review=deps.transition_issue_to_review,
        set_blocked_with_reason=deps.set_blocked_with_reason,
    )


def reconcile_stale_in_progress_items(
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    *,
    deps: ReconciliationWiringDeps,
    store: SessionStorePort,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_snapshot: CycleBoardSnapshot | None,
    issue_ref_for_snapshot: Callable[[SnapshotIssueRefView], str | None],
    active_issue_refs: set[str],
    dry_run: bool,
) -> tuple[list[str], list[str], list[str]]:
    """Reconcile stale In Progress items back to their truthful lanes."""

    def _reconcile_single(
        issue_ref,
        *,
        consumer_config,
        critical_path_config,
        automation_config,
        store,
        pr_port,
        review_state_port,
        board_port,
        dry_run,
    ):
        return reconcile_single_in_progress_item(
            issue_ref,
            deps=deps,
            consumer_config=consumer_config,
            critical_path_config=critical_path_config,
            automation_config=automation_config,
            store=store,
            pr_port=pr_port,
            review_state_port=review_state_port,
            board_port=board_port,
            dry_run=dry_run,
        )

    return deps.board_state_reconcile_stale(
        consumer_config,
        critical_path_config,
        automation_config,
        store=store,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        board_snapshot=board_snapshot,
        issue_ref_for_snapshot=issue_ref_for_snapshot,
        active_issue_refs=active_issue_refs,
        dry_run=dry_run,
        reconcile_single_in_progress_item=_reconcile_single,
    )


# ---------------------------------------------------------------------------
# Top-level reconciliation wiring
# ---------------------------------------------------------------------------


def wire_reconcile_board_truth(
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    db: ConsumerRuntimeStatePort,
    *,
    deps: ReconciliationWiringDeps,
    session_store: SessionStorePort | None = None,
    pr_port: PullRequestPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    dry_run: bool = False,
    board_snapshot: CycleBoardSnapshot | None = None,
) -> ReconciliationResult:
    """Wire and execute board-truth reconciliation."""

    def _issue_ref_for_snapshot(snapshot: SnapshotIssueRefView) -> str | None:
        if "/" not in snapshot.issue_ref:
            return snapshot.issue_ref
        return deps.snapshot_to_issue_ref(
            snapshot.issue_ref, critical_path_config.issue_prefixes
        )

    def _wired_reconcile_active_repair(
        consumer_config,
        critical_path_config,
        *,
        active_repair_issue_refs,
        review_state_port,
        board_port,
        board_snapshot,
        issue_ref_for_snapshot,
        dry_run,
    ):
        return reconcile_active_repair_review_items(
            consumer_config,
            critical_path_config,
            deps=deps,
            active_repair_issue_refs=active_repair_issue_refs,
            review_state_port=review_state_port,
            board_port=board_port,
            board_snapshot=board_snapshot,
            issue_ref_for_snapshot=issue_ref_for_snapshot,
            dry_run=dry_run,
        )

    def _wired_reconcile_stale(
        consumer_config,
        critical_path_config,
        automation_config,
        *,
        store,
        pr_port,
        review_state_port,
        board_port,
        board_snapshot,
        issue_ref_for_snapshot,
        active_issue_refs,
        dry_run,
    ):
        return reconcile_stale_in_progress_items(
            consumer_config,
            critical_path_config,
            automation_config,
            deps=deps,
            store=store,
            pr_port=pr_port,
            review_state_port=review_state_port,
            board_port=board_port,
            board_snapshot=board_snapshot,
            issue_ref_for_snapshot=issue_ref_for_snapshot,
            active_issue_refs=active_issue_refs,
            dry_run=dry_run,
        )

    assert session_store is not None
    assert pr_port is not None
    assert review_state_port is not None
    assert board_port is not None

    return _reconcile_board_truth_use_case(
        consumer_config,
        critical_path_config,
        automation_config,
        db,
        deps=ReconciliationDeps(
            issue_ref_for_snapshot=_issue_ref_for_snapshot,
            reconcile_active_repair_review_items=_wired_reconcile_active_repair,
            reconcile_stale_in_progress_items=_wired_reconcile_stale,
        ),
        session_store=session_store,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        board_snapshot=board_snapshot,
        dry_run=dry_run,
    )
