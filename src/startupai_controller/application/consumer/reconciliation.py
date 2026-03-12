"""Reconciliation wiring for the consumer application layer.

This module wires the board-truth reconciliation engine by injecting
concrete dependencies from the outer layers into the use-case functions
in preflight.py and the helper functions in consumer_board_state_helpers.

board_consumer.py retains thin compatibility wrappers that delegate here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from startupai_controller.application.consumer.preflight import (
    ReconciliationDeps,
    ReconciliationResult,
    reconcile_board_truth as _reconcile_board_truth_use_case,
)
from startupai_controller.domain.models import IssueSnapshot


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
    resolve_issue_coordinates: Callable[..., tuple[str, str, int]]
    classify_open_pr_candidates: Callable[..., tuple[str, Any | None, str]]
    reconcile_in_progress_decision: Callable[..., str]
    snapshot_to_issue_ref: Callable[[str, dict], str | None]

    # Runtime wiring
    build_session_store: Callable[[Any], Any]
    build_github_port_bundle: Callable[..., Any]


# ---------------------------------------------------------------------------
# Wired helper functions
# ---------------------------------------------------------------------------


def reconcile_active_repair_review_items(
    consumer_config: Any,
    critical_path_config: Any,
    *,
    deps: ReconciliationWiringDeps,
    active_repair_issue_refs: set[str],
    review_state_port: Any,
    board_port: Any,
    board_snapshot: Any | None,
    issue_ref_for_snapshot: Callable[..., str | None],
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
    consumer_config: Any,
    critical_path_config: Any,
    automation_config: Any,
    store: Any,
    pr_port: Any,
    review_state_port: Any,
    board_port: Any,
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
    consumer_config: Any,
    critical_path_config: Any,
    automation_config: Any,
    *,
    deps: ReconciliationWiringDeps,
    store: Any,
    pr_port: Any,
    review_state_port: Any,
    board_port: Any,
    board_snapshot: Any | None,
    issue_ref_for_snapshot: Callable[..., str | None],
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
    consumer_config: Any,
    critical_path_config: Any,
    automation_config: Any,
    db: Any,
    *,
    deps: ReconciliationWiringDeps,
    session_store: Any | None = None,
    pr_port: Any | None = None,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
    dry_run: bool = False,
    board_snapshot: Any | None = None,
) -> ReconciliationResult:
    """Wire and execute board-truth reconciliation."""

    def _issue_ref_for_snapshot(snapshot: Any) -> str | None:
        if isinstance(snapshot, IssueSnapshot):
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

    return _reconcile_board_truth_use_case(
        consumer_config,
        critical_path_config,
        automation_config,
        db,
        deps=ReconciliationDeps(
            build_session_store=deps.build_session_store,
            build_github_port_bundle=deps.build_github_port_bundle,
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
