"""Reconciliation and recovery wiring extracted from the consumer shell."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import cast

import startupai_controller.consumer_automation_bridge as _automation_bridge
import startupai_controller.consumer_board_state_helpers as _board_state_helpers
import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
import startupai_controller.consumer_recovery_helpers as _recovery_helpers
from startupai_controller.application.consumer.preflight import ReconciliationResult
from startupai_controller.application.consumer.reconciliation import (
    ReconciliationWiringDeps,
    SnapshotIssueRefView,
    reconcile_active_repair_review_items as _reconcile_active_repair_review_items_use_case,
    reconcile_single_in_progress_item as _reconcile_single_in_progress_item_use_case,
    reconcile_stale_in_progress_items as _reconcile_stale_in_progress_items_use_case,
    wire_reconcile_board_truth as _wire_reconcile_board_truth_use_case,
)
from startupai_controller.application.consumer.recovery import (
    RecoveredLeaseInfo,
    RecoveryStatePort,
    recover_interrupted_sessions as _recover_interrupted_sessions_use_case,
)
from startupai_controller.board_automation_config import (
    BoardAutomationConfig,
    load_automation_config,
)
from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.domain.launch_policy import reconcile_in_progress_decision
from startupai_controller.domain.models import CycleBoardSnapshot
from startupai_controller.domain.scheduling_policy import (
    snapshot_to_issue_ref as _snapshot_to_issue_ref,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.ready_flow import (
    BoardInfoResolverFn,
    BoardStatusMutatorFn,
    GitHubRunnerFn,
)
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.runtime.wiring import (
    ConsumerDB,
    build_github_port_bundle,
    build_session_store,
)
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    CriticalPathConfig,
    GhQueryError,
    load_config,
)

logger = logging.getLogger("board-consumer")


def build_reconciliation_wiring_deps() -> ReconciliationWiringDeps:
    """Build the wiring deps for board-truth reconciliation."""
    return ReconciliationWiringDeps(
        board_state_reconcile_active_repair=_board_state_helpers.reconcile_active_repair_review_items,
        board_state_reconcile_single=_board_state_helpers.reconcile_single_in_progress_item,
        board_state_reconcile_stale=_board_state_helpers.reconcile_stale_in_progress_items,
        transition_issue_to_in_progress=_board_state_helpers.transition_issue_to_in_progress_from_shell,
        return_issue_to_ready=_board_state_helpers.return_issue_to_ready_from_shell,
        transition_issue_to_review=_board_state_helpers.transition_issue_to_review_from_shell,
        set_blocked_with_reason=_automation_bridge.set_blocked_with_reason,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        classify_open_pr_candidates=_codex_comment_wiring.classify_open_pr_candidates,
        reconcile_in_progress_decision=reconcile_in_progress_decision,
        snapshot_to_issue_ref=_snapshot_to_issue_ref,
    )


def reconcile_active_repair_review_items(
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    *,
    active_repair_issue_refs: set[str],
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_snapshot: CycleBoardSnapshot | None,
    issue_ref_for_snapshot: Callable[[SnapshotIssueRefView], str | None],
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
    dry_run: bool,
) -> list[str]:
    """Return active repair items that should move from Review back to In Progress."""
    return _reconcile_active_repair_review_items_use_case(
        consumer_config,
        critical_path_config,
        deps=build_reconciliation_wiring_deps(),
        active_repair_issue_refs=active_repair_issue_refs,
        review_state_port=review_state_port,
        board_port=board_port,
        board_snapshot=board_snapshot,
        issue_ref_for_snapshot=issue_ref_for_snapshot,
        dry_run=dry_run,
    )


def reconcile_single_in_progress_item(
    issue_ref: str,
    *,
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    store: SessionStorePort,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
    dry_run: bool,
) -> str:
    """Reconcile one stale In Progress item and return its target lane."""
    return _reconcile_single_in_progress_item_use_case(
        issue_ref,
        deps=build_reconciliation_wiring_deps(),
        consumer_config=consumer_config,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        store=store,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        dry_run=dry_run,
    )


def reconcile_stale_in_progress_items(
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    *,
    store: SessionStorePort,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_snapshot: CycleBoardSnapshot | None,
    issue_ref_for_snapshot: Callable[[SnapshotIssueRefView], str | None],
    active_issue_refs: set[str],
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
    dry_run: bool,
) -> tuple[list[str], list[str], list[str]]:
    """Reconcile stale In Progress items back to their truthful lanes."""
    return _reconcile_stale_in_progress_items_use_case(
        consumer_config,
        critical_path_config,
        automation_config,
        deps=build_reconciliation_wiring_deps(),
        store=store,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        board_snapshot=board_snapshot,
        issue_ref_for_snapshot=issue_ref_for_snapshot,
        active_issue_refs=active_issue_refs,
        dry_run=dry_run,
    )


def recover_interrupted_sessions(
    config: ConsumerConfig,
    db: RecoveryStatePort,
    *,
    automation_config: BoardAutomationConfig | None = None,
    pr_port: PullRequestPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[RecoveredLeaseInfo]:
    """Recover leases left behind by a previous interrupted daemon process."""
    return _recovery_helpers.recover_interrupted_sessions(
        config,
        db,
        automation_config=automation_config,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        gh_runner=gh_runner,
        load_config=load_config,
        config_error_type=ConfigError,
        logger=logger,
        recovery_use_case=_recover_interrupted_sessions_use_case,
        gh_query_error_type=GhQueryError,
        build_github_port_bundle=build_github_port_bundle,
        load_automation_config=load_automation_config,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        classify_open_pr_candidates=_codex_comment_wiring.classify_open_pr_candidates,
        return_issue_to_ready=_board_state_helpers.return_issue_to_ready_from_shell,
        transition_issue_to_review=_board_state_helpers.transition_issue_to_review_from_shell,
        set_blocked_with_reason=_automation_bridge.set_blocked_with_reason,
    )


def reconcile_board_truth(
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    db: ConsumerRuntimeStatePort,
    *,
    session_store: SessionStorePort | None = None,
    pr_port: PullRequestPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    dry_run: bool = False,
    board_snapshot: CycleBoardSnapshot | None = None,
    board_info_resolver: BoardInfoResolverFn | None = None,
    board_mutator: BoardStatusMutatorFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> ReconciliationResult:
    """Make board In Progress truthful against local consumer state."""
    effective_session_store = session_store or build_session_store(cast(ConsumerDB, db))
    github_bundle = None
    if pr_port is None or review_state_port is None or board_port is None:
        github_bundle = build_github_port_bundle(
            consumer_config.project_owner,
            consumer_config.project_number,
            config=critical_path_config,
            gh_runner=gh_runner,
        )
    if pr_port is None:
        assert github_bundle is not None
        effective_pr_port = github_bundle.pull_requests
    else:
        effective_pr_port = pr_port
    if review_state_port is None:
        assert github_bundle is not None
        effective_review_state_port = github_bundle.review_state
    else:
        effective_review_state_port = review_state_port
    if board_port is None:
        assert github_bundle is not None
        effective_board_port = github_bundle.board_mutations
    else:
        effective_board_port = board_port
    return _wire_reconcile_board_truth_use_case(
        consumer_config,
        critical_path_config,
        automation_config,
        db,
        deps=build_reconciliation_wiring_deps(),
        session_store=effective_session_store,
        pr_port=effective_pr_port,
        review_state_port=effective_review_state_port,
        board_port=effective_board_port,
        dry_run=dry_run,
        board_snapshot=board_snapshot,
    )
