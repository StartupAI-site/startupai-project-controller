"""Shell-facing operational wiring extracted from board_consumer."""

from __future__ import annotations

import logging
from pathlib import Path
import subprocess
from typing import Any, Callable, cast

import startupai_controller.consumer_automation_bridge as _automation_bridge
import startupai_controller.consumer_board_state_helpers as _board_state_helpers
import startupai_controller.consumer_claim_wiring as _claim_wiring
import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
import startupai_controller.consumer_cycle_wiring as _cycle_wiring
import startupai_controller.consumer_execution_support_helpers as _execution_support_helpers
import startupai_controller.consumer_execution_outcome_wiring as _execution_outcome_wiring
import startupai_controller.consumer_recovery_helpers as _recovery_helpers
import startupai_controller.consumer_resolution_helpers as _resolution_helpers
import startupai_controller.consumer_review_handoff_helpers as _review_handoff_helpers
import startupai_controller.consumer_review_queue_helpers as _review_queue_helpers
import startupai_controller.consumer_session_completion_helpers as _session_completion_helpers
import startupai_controller.consumer_selection_retry_wiring as _selection_retry_wiring
import startupai_controller.consumer_support_wiring as _support_wiring
from startupai_controller.automation_ready_review_wiring import (
    claim_ready_issue as _claim_ready_issue,
)
from startupai_controller.application.consumer.cycle import PreparedCycleDeps
from startupai_controller.application.consumer.reconciliation import (
    ReconciliationWiringDeps,
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
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.board_automation_config import load_automation_config
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    PendingClaimContext,
    PrCreationOutcome,
    PreparedCycleContext,
    PreparedLaunchContext,
    SelectedLaunchCandidate,
    SessionExecutionOutcome,
    WorktreePrepareError,
)
from startupai_controller.consumer_workflow import WorkflowConfigError
from startupai_controller.control_plane_runtime import (
    _mark_degraded,
    _record_successful_github_mutation,
)
from startupai_controller.domain.launch_policy import reconcile_in_progress_decision
from startupai_controller.domain.models import (
    ClaimReadyResult,
    CycleResult,
    ResolutionEvaluation,
    ReviewQueueDrainSummary,
    ReviewQueueEntry,
)
from startupai_controller.domain.scheduling_policy import (
    snapshot_to_issue_ref as _snapshot_to_issue_ref,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.process_runner import GhRunnerPort, ProcessRunnerPort
from startupai_controller.ports.ready_flow import (
    BoardInfoResolverFn,
    BoardStatusMutatorFn,
    CommentCheckerFn,
    CommentPosterFn,
    GitHubRunnerFn,
    StatusResolverFn,
)
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.runtime.wiring import (
    ConsumerDB,
    GitHubRuntimeMemo as CycleGitHubMemo,
    build_github_port_bundle,
    build_session_store,
    gh_reason_code,
)
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    CriticalPathConfig,
    GhQueryError,
    load_config,
    parse_issue_ref,
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
    consumer_config: Any,
    critical_path_config: Any,
    *,
    active_repair_issue_refs: set[str],
    review_state_port: Any,
    board_port: Any,
    board_snapshot: Any,
    issue_ref_for_snapshot: Callable[..., Any],
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
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
    consumer_config: Any,
    critical_path_config: Any,
    automation_config: Any,
    store: Any,
    pr_port: Any,
    review_state_port: Any,
    board_port: Any,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
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
    consumer_config: Any,
    critical_path_config: Any,
    automation_config: Any,
    *,
    store: Any,
    pr_port: Any,
    review_state_port: Any,
    board_port: Any,
    board_snapshot: Any,
    issue_ref_for_snapshot: Callable[..., Any],
    active_issue_refs: set[str],
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
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
    consumer_config: Any,
    critical_path_config: Any,
    automation_config: Any | None,
    db: Any,
    *,
    session_store: Any | None = None,
    pr_port: Any | None = None,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
    dry_run: bool = False,
    board_snapshot: Any | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> Any:
    """Make board In Progress truthful against local consumer state."""
    effective_session_store = session_store or build_session_store(db)
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


def block_prelaunch_issue(
    issue_ref: str,
    blocked_reason: str,
    *,
    config: Any,
    cp_config: Any,
    db: Any,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Move a launch-unready issue to Blocked before claim."""
    _execution_outcome_wiring.block_prelaunch_issue(
        issue_ref,
        blocked_reason,
        config=config,
        cp_config=cp_config,
        db=db,
        gh_query_error_type=GhQueryError,
        set_blocked_with_reason=_automation_bridge.set_blocked_with_reason,
        record_successful_github_mutation=_record_successful_github_mutation,
        mark_degraded=_mark_degraded,
        queue_status_transition=_support_wiring.queue_status_transition,
        logger=logger,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )


def select_launch_candidate_for_cycle(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    target_issue: str | None,
    review_state_port: ReviewStatePort | None = None,
    status_resolver: StatusResolverFn | None,
    gh_runner: GitHubRunnerFn | None,
) -> tuple[SelectedLaunchCandidate | None, CycleResult | None]:
    """Select a launch candidate and validate its immediate launchability."""
    return _selection_retry_wiring.select_launch_candidate_for_cycle(
        config=config,
        db=db,
        prepared=prepared,
        target_issue=target_issue,
        review_state_port=review_state_port,
        status_resolver=status_resolver,
        gh_runner=gh_runner,
        cycle_result_factory=CycleResult,
        selected_launch_candidate_factory=SelectedLaunchCandidate,
        select_candidate_for_cycle=_selection_retry_wiring.select_candidate_for_cycle_from_shell,
        parse_issue_ref=parse_issue_ref,
        effective_retry_backoff=_selection_retry_wiring.effective_retry_backoff,
        retry_backoff_active=_selection_retry_wiring.retry_backoff_active,
        maybe_activate_claim_suppression=cast(
            Callable[..., None],
            _support_wiring.maybe_activate_claim_suppression,
        ),
        mark_degraded=_mark_degraded,
        gh_reason_code=gh_reason_code,
        gh_query_error_type=GhQueryError,
        logger=logger,
    )


def prepare_selected_launch_candidate(
    *,
    selected_candidate: SelectedLaunchCandidate,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    subprocess_runner: Callable[..., Any] | None,
    status_resolver: StatusResolverFn | None,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
    pr_port: PullRequestPort | None = None,
) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
    """Prepare the selected candidate into launch-ready local context."""
    return _claim_wiring.prepare_selected_launch_candidate(
        selected_candidate=selected_candidate,
        config=config,
        db=db,
        prepared=prepared,
        subprocess_runner=subprocess_runner,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        pr_port=pr_port,
        record_metric=_support_wiring.record_metric,
        prepare_launch_candidate=_cycle_wiring.prepare_launch_candidate,
        handle_selected_launch_query_error=handle_selected_launch_query_error,
        handle_selected_launch_workflow_config_error=handle_selected_launch_workflow_config_error,
        handle_selected_launch_worktree_error=handle_selected_launch_worktree_error,
        handle_selected_launch_runtime_error=handle_selected_launch_runtime_error,
        workflow_config_error_type=WorkflowConfigError,
        worktree_prepare_error_type=WorktreePrepareError,
        gh_query_error_type=GhQueryError,
    )


def handle_selected_launch_query_error(
    *,
    candidate: str,
    err: Exception,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
) -> tuple[None, CycleResult]:
    """Handle GitHub/query failures during selected launch preparation."""
    return _claim_wiring.handle_selected_launch_query_error(
        candidate=candidate,
        err=err,
        config=config,
        db=db,
        record_metric=_support_wiring.record_metric,
        maybe_activate_claim_suppression=cast(
            _claim_wiring.MaybeActivateClaimSuppressionFn,
            _support_wiring.maybe_activate_claim_suppression,
        ),
        mark_degraded=_mark_degraded,
        gh_reason_code=gh_reason_code,
        cycle_result_factory=CycleResult,
    )


def handle_selected_launch_workflow_config_error(
    *,
    candidate: str,
    err: Exception,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    cp_config: Any,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
) -> tuple[None, CycleResult]:
    """Handle invalid workflow configuration during launch preparation."""
    return _claim_wiring.handle_selected_launch_workflow_config_error(
        candidate=candidate,
        err=err,
        config=config,
        db=db,
        cp_config=cp_config,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        block_prelaunch_issue=block_prelaunch_issue,
        record_metric=_support_wiring.record_metric,
        cycle_result_factory=CycleResult,
    )


def handle_selected_launch_worktree_error(
    *,
    candidate: str,
    err: Exception,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
) -> tuple[None, CycleResult]:
    """Handle worktree preparation failures for a selected launch candidate."""
    return _claim_wiring.handle_selected_launch_worktree_error(
        candidate=candidate,
        err=err,
        config=config,
        db=db,
        record_metric=_support_wiring.record_metric,
        cycle_result_factory=CycleResult,
    )


def handle_selected_launch_runtime_error(
    *,
    candidate: str,
    err: RuntimeError,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    cp_config: Any,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
) -> tuple[None, CycleResult]:
    """Handle workflow-hook runtime failures during launch preparation."""
    return _claim_wiring.handle_selected_launch_runtime_error(
        candidate=candidate,
        err=err,
        config=config,
        db=db,
        cp_config=cp_config,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        block_prelaunch_issue=block_prelaunch_issue,
        record_metric=_support_wiring.record_metric,
        cycle_result_factory=CycleResult,
    )


def resolve_launch_context_for_cycle(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext | None,
    target_issue: str | None,
    dry_run: bool,
    status_resolver: StatusResolverFn | None,
    subprocess_runner: Callable[..., Any] | None,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
    review_state_port: ReviewStatePort | None = None,
    pr_port: PullRequestPort | None = None,
) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
    """Resolve or prepare launch-ready work for this cycle."""
    return _cycle_wiring.resolve_launch_context_for_cycle(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        target_issue=target_issue,
        dry_run=dry_run,
        review_state_port=review_state_port,
        pr_port=pr_port,
        status_resolver=status_resolver,
        subprocess_runner=subprocess_runner,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        select_launch_candidate_for_cycle=select_launch_candidate_for_cycle,
        prepare_selected_launch_candidate=prepare_selected_launch_candidate,
        logger=logger,
    )


def open_pending_claim_session(
    *,
    db: ConsumerRuntimeStatePort,
    launch_context: PreparedLaunchContext,
    executor: str,
    slot_id: int,
) -> tuple[PendingClaimContext | None, CycleResult | None]:
    """Create the session record and acquire the lease for a launch-ready issue."""
    return _claim_wiring.open_pending_claim_session(
        db=db,
        launch_context=launch_context,
        executor=executor,
        slot_id=slot_id,
        complete_session=cast(
            _claim_wiring.CompleteSessionFn,
            _support_wiring.complete_session,
        ),
        pending_claim_context_factory=PendingClaimContext,
        cycle_result_factory=CycleResult,
    )


def enforce_claim_retry_ceiling(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    launch_context: PreparedLaunchContext,
    pending_claim: PendingClaimContext,
    cp_config: Any,
    board_info_resolver: BoardInfoResolverFn | None,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | None,
) -> CycleResult | None:
    """Abort and escalate if the issue already exhausted its retry ceiling."""
    return _claim_wiring.enforce_claim_retry_ceiling(
        config=config,
        db=db,
        launch_context=launch_context,
        pending_claim=pending_claim,
        cp_config=cp_config,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        complete_session=cast(
            _claim_wiring.CompleteSessionFn,
            _support_wiring.complete_session,
        ),
        escalate_to_claude=_board_state_helpers.escalate_to_claude_from_shell,
        cycle_result_factory=CycleResult,
        logger=logger,
    )


def attempt_launch_context_claim(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    pending_claim: PendingClaimContext,
    slot_id: int,
    status_resolver: StatusResolverFn | None,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | None,
) -> tuple[ClaimReadyResult | None, CycleResult | None]:
    """Claim board ownership for a launch-ready issue."""
    return _claim_wiring.attempt_launch_context_claim(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        pending_claim=pending_claim,
        slot_id=slot_id,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        claim_launch_ready_issue=claim_launch_ready_issue,
        handle_launch_claim_api_failure=handle_launch_claim_api_failure,
        handle_launch_claim_unexpected_failure=handle_launch_claim_unexpected_failure,
        handle_launch_claim_rejection=handle_launch_claim_rejection,
        record_metric=_support_wiring.record_metric,
        gh_query_error_type=GhQueryError,
    )


def claim_launch_ready_issue(
    candidate: str,
    *,
    config: ConsumerConfig,
    prepared: PreparedCycleContext,
    status_resolver: StatusResolverFn | None,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | None,
) -> ClaimReadyResult:
    """Execute the actual board claim for a launch-ready issue."""
    return _claim_wiring.claim_launch_ready_issue(
        candidate,
        config=config,
        prepared=prepared,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        claim_ready_issue=_automation_bridge.claim_ready_issue,
    )


def handle_launch_claim_api_failure(
    candidate: str,
    err: Exception,
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    pending_claim: PendingClaimContext,
) -> tuple[None, CycleResult]:
    """Handle a GitHub/API failure while claiming a launch-ready issue."""
    return _claim_wiring.handle_launch_claim_api_failure(
        candidate,
        err,
        config=config,
        db=db,
        pending_claim=pending_claim,
        maybe_activate_claim_suppression=cast(
            _claim_wiring.MaybeActivateClaimSuppressionFn,
            _support_wiring.maybe_activate_claim_suppression,
        ),
        mark_degraded=_mark_degraded,
        gh_reason_code=gh_reason_code,
        complete_session=cast(
            _claim_wiring.CompleteSessionFn,
            _support_wiring.complete_session,
        ),
        record_metric=_support_wiring.record_metric,
        cycle_result_factory=CycleResult,
        logger=logger,
    )


def handle_launch_claim_unexpected_failure(
    candidate: str,
    err: Exception,
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    pending_claim: PendingClaimContext,
) -> tuple[None, CycleResult]:
    """Handle an unexpected local failure while claiming a launch-ready issue."""
    return _claim_wiring.handle_launch_claim_unexpected_failure(
        candidate,
        err,
        config=config,
        db=db,
        pending_claim=pending_claim,
        complete_session=cast(
            _claim_wiring.CompleteSessionFn,
            _support_wiring.complete_session,
        ),
        record_metric=_support_wiring.record_metric,
        cycle_result_factory=CycleResult,
        logger=logger,
    )


def handle_launch_claim_rejection(
    candidate: str,
    claim_result: ClaimReadyResult,
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    pending_claim: PendingClaimContext,
) -> tuple[None, CycleResult]:
    """Handle a non-exception claim rejection for a launch-ready issue."""
    return _claim_wiring.handle_launch_claim_rejection(
        candidate,
        claim_result,
        config=config,
        db=db,
        pending_claim=pending_claim,
        complete_session=cast(
            _claim_wiring.CompleteSessionFn,
            _support_wiring.complete_session,
        ),
        record_metric=_support_wiring.record_metric,
        cycle_result_factory=CycleResult,
    )


def mark_claimed_session_running(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    launch_context: PreparedLaunchContext,
    pending_claim: PendingClaimContext,
    slot_id: int,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    cp_config: Any,
    gh_runner: GitHubRunnerFn | None,
) -> ClaimedSessionContext:
    """Persist the durable-start state and post the claim marker."""
    return _claim_wiring.mark_claimed_session_running(
        config=config,
        db=db,
        launch_context=launch_context,
        pending_claim=pending_claim,
        slot_id=slot_id,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        cp_config=cp_config,
        gh_runner=gh_runner,
        record_successful_github_mutation=_record_successful_github_mutation,
        record_metric=_support_wiring.record_metric,
        post_consumer_claim_comment=_codex_comment_wiring.post_consumer_claim_comment,
        claimed_session_context_factory=ClaimedSessionContext,
        logger=logger,
    )


def claim_launch_context(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    slot_id: int,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    status_resolver: StatusResolverFn | None = None,
    board_info_resolver: BoardInfoResolverFn | None = None,
    board_mutator: BoardStatusMutatorFn | None = None,
    comment_checker: CommentCheckerFn | None = None,
    comment_poster: CommentPosterFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> tuple[ClaimedSessionContext | None, CycleResult | None]:
    """Claim board ownership and start a durable running session."""
    return _cycle_wiring.claim_launch_context(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        slot_id=slot_id,
        review_state_port=review_state_port,
        board_port=board_port,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        open_pending_claim_session=open_pending_claim_session,
        enforce_claim_retry_ceiling=enforce_claim_retry_ceiling,
        attempt_launch_context_claim=attempt_launch_context_claim,
        mark_claimed_session_running=mark_claimed_session_running,
    )


def create_pr_for_execution_result(
    *,
    config: ConsumerConfig,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    codex_result: dict[str, Any] | None,
    session_status: str,
    failure_reason: str | None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None,
    gh_runner: GitHubRunnerFn | None,
) -> PrCreationOutcome:
    """Reuse or create a PR from claimed-session output."""
    return _execution_outcome_wiring.create_pr_for_execution_result(
        config=config,
        launch_context=launch_context,
        claimed_context=claimed_context,
        codex_result=codex_result,
        session_status=session_status,
        failure_reason=failure_reason,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        has_commits_on_branch=_execution_support_helpers.has_commits_on_branch,
        create_or_update_pr=_codex_comment_wiring.create_or_update_pr,
        pr_creation_outcome_factory=PrCreationOutcome,
        logger=logger,
    )


def handoff_execution_to_review(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    session_id: str,
    pr_url: str,
    session_status: str,
    session_store: SessionStorePort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: BoardInfoResolverFn | None = None,
    board_mutator: BoardStatusMutatorFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> ReviewQueueDrainSummary:
    """Transition a claimed session into Review and perform immediate rescue."""
    return _execution_outcome_wiring.handoff_execution_to_review(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        session_id=session_id,
        pr_url=pr_url,
        session_status=session_status,
        session_store=session_store or build_session_store(cast(ConsumerDB, db)),
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        transition_claimed_session_to_review=transition_claimed_session_to_review,
        post_claimed_session_verdict_marker=post_claimed_session_verdict_marker,
        queue_claimed_session_for_review=queue_claimed_session_for_review,
        run_immediate_review_handoff=run_immediate_review_handoff,
        record_metric=_support_wiring.record_metric,
    )


def transition_claimed_session_to_review(
    *,
    db: ConsumerRuntimeStatePort,
    issue_ref: str,
    session_id: str,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: BoardInfoResolverFn | None = None,
    board_mutator: BoardStatusMutatorFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> None:
    """Move one claimed issue into Review or queue the transition on failure."""
    _execution_outcome_wiring.transition_claimed_session_to_review(
        db=cast(_review_handoff_helpers.ReviewHandoffStatePort, db),
        issue_ref=issue_ref,
        session_id=session_id,
        config=config,
        critical_path_config=critical_path_config,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        transition_issue_to_review=_board_state_helpers.transition_issue_to_review_from_shell,
        record_successful_github_mutation=_record_successful_github_mutation,
        mark_degraded=_mark_degraded,
        queue_status_transition=_support_wiring.queue_status_transition,
        logger=logger,
    )


def post_claimed_session_verdict_marker(
    *,
    db: ConsumerRuntimeStatePort,
    pr_url: str,
    session_id: str,
    gh_runner: GitHubRunnerFn | None,
) -> None:
    """Post the codex verdict marker for a newly handed-off review PR."""
    _execution_outcome_wiring.post_claimed_session_verdict_marker(
        db=db,
        pr_url=pr_url,
        session_id=session_id,
        gh_runner=gh_runner,
        post_pr_codex_verdict=_codex_comment_wiring.post_pr_codex_verdict,
        record_successful_github_mutation=_record_successful_github_mutation,
        mark_degraded=_mark_degraded,
        queue_verdict_marker=_support_wiring.queue_verdict_marker,
        logger=logger,
    )


def queue_claimed_session_for_review(
    *,
    store: SessionStorePort,
    issue_ref: str,
    pr_url: str,
    session_id: str,
) -> ReviewQueueEntry | None:
    """Queue one claimed session for immediate review handling."""
    return _execution_outcome_wiring.queue_claimed_session_for_review(
        store=store,
        issue_ref=issue_ref,
        pr_url=pr_url,
        session_id=session_id,
        queue_review_item=_review_queue_helpers.queue_review_item,
    )


def run_immediate_review_handoff(
    *,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    store: SessionStorePort,
    queue_entry: ReviewQueueEntry,
    gh_runner: GitHubRunnerFn | None,
    db: ConsumerRuntimeStatePort,
) -> ReviewQueueDrainSummary:
    """Run immediate rescue for the just-opened review PR."""
    return _execution_outcome_wiring.run_immediate_review_handoff(
        config=config,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        store=store,
        queue_entry=queue_entry,
        gh_runner=gh_runner,
        db=db,
        build_github_port_bundle=cast(
            _review_handoff_helpers.BuildGitHubPortBundleFn,
            build_github_port_bundle,
        ),
        github_memo_factory=CycleGitHubMemo,
        build_review_snapshots_for_queue_entries=_review_queue_helpers.build_review_snapshots_for_queue_entries,
        review_rescue=_automation_bridge.review_rescue,
        apply_review_queue_result=cast(
            _review_handoff_helpers.ApplyReviewQueueResultFn,
            _review_queue_helpers.apply_review_queue_result,
        ),
        mark_degraded=_mark_degraded,
        gh_reason_code=gh_reason_code,
        summary_factory=ReviewQueueDrainSummary,
        logger=logger,
    )


def handle_non_review_execution_outcome(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    session_id: str,
    session_status: str,
    codex_result: dict[str, Any] | None,
    has_commits: bool,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    comment_poster: CommentPosterFn | None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None,
    gh_runner: GitHubRunnerFn | GhRunnerPort | None,
    pr_port: PullRequestPort | None = None,
    board_port: BoardMutationPort | None = None,
) -> tuple[str, ResolutionEvaluation | None, str | None]:
    """Handle non-review outcomes for a claimed session."""
    return _execution_outcome_wiring.handle_non_review_execution_outcome(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        session_id=session_id,
        session_status=session_status,
        codex_result=codex_result,
        has_commits=has_commits,
        pr_port=pr_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_poster=comment_poster,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        verify_resolution_payload=_support_wiring.verify_resolution_payload,
        apply_resolution_action=_resolution_helpers.apply_resolution_action_from_shell,
        return_issue_to_ready=_board_state_helpers.return_issue_to_ready_from_shell,
        record_successful_github_mutation=_record_successful_github_mutation,
        mark_degraded=_mark_degraded,
        queue_status_transition=_support_wiring.queue_status_transition,
        record_metric=_support_wiring.record_metric,
        logger=logger,
    )


def final_phase_for_claimed_session(
    *,
    launch_context: PreparedLaunchContext,
    execution_outcome: SessionExecutionOutcome,
) -> str:
    """Determine the final persisted phase for a claimed session."""
    return _execution_outcome_wiring.final_phase_for_claimed_session(
        launch_context=launch_context,
        execution_outcome=execution_outcome,
    )


def persist_claimed_session_completion(
    *,
    db: ConsumerRuntimeStatePort,
    session_id: str,
    issue_ref: str,
    execution_outcome: SessionExecutionOutcome,
    final_phase: str,
) -> None:
    """Persist the final session record for a claimed execution outcome."""
    _execution_outcome_wiring.persist_claimed_session_completion(
        db=db,
        session_id=session_id,
        issue_ref=issue_ref,
        execution_outcome=execution_outcome,
        final_phase=final_phase,
        complete_session=cast(Callable[..., None], _support_wiring.complete_session),
    )


def post_claimed_session_result_comment(
    *,
    issue_ref: str,
    session_id: str,
    codex_result: dict[str, Any] | None,
    cp_config: CriticalPathConfig,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | None,
) -> None:
    """Post the session result comment when Codex produced structured output."""
    _execution_outcome_wiring.post_claimed_session_result_comment(
        issue_ref=issue_ref,
        session_id=session_id,
        codex_result=codex_result,
        cp_config=cp_config,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        post_result_comment=_codex_comment_wiring.post_result_comment,
        logger=logger,
    )


def maybe_escalate_claimed_session_failure(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    issue_ref: str,
    effective_max_retries: int,
    session_status: str,
    codex_result: dict[str, Any] | None,
    cp_config: CriticalPathConfig,
    board_info_resolver: BoardInfoResolverFn | None,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | None,
) -> None:
    """Escalate terminal failed/timeout sessions once retry ceiling is reached."""
    _execution_outcome_wiring.maybe_escalate_claimed_session_failure(
        config=config,
        db=db,
        issue_ref=issue_ref,
        effective_max_retries=effective_max_retries,
        session_status=session_status,
        codex_result=codex_result,
        cp_config=cp_config,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        escalate_to_claude=_board_state_helpers.escalate_to_claude_from_shell,
        logger=logger,
    )


def execute_claimed_session(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    process_runner: (
        ProcessRunnerPort | Callable[..., subprocess.CompletedProcess[str]] | None
    ) = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    file_reader: Callable[[Path], str] | None = None,
    board_info_resolver: BoardInfoResolverFn | None = None,
    board_mutator: BoardStatusMutatorFn | None = None,
    comment_checker: CommentCheckerFn | None = None,
    comment_poster: CommentPosterFn | None = None,
    gh_runner: GhRunnerPort | GitHubRunnerFn | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    pr_port: PullRequestPort | None = None,
) -> SessionExecutionOutcome:
    """Execute Codex for a claimed session and apply immediate board handoff."""
    return _execution_outcome_wiring.execute_claimed_session(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        claimed_context=claimed_context,
        process_runner=(
            process_runner if process_runner is not None else subprocess_runner
        ),
        file_reader=file_reader,
        review_state_port=review_state_port,
        board_port=board_port,
        pr_port=pr_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        assemble_codex_prompt=_codex_comment_wiring.assemble_codex_prompt,
        run_codex_session=_codex_comment_wiring.run_codex_session,
        parse_codex_result=_codex_comment_wiring.parse_codex_result,
        session_status_from_codex_result=_session_completion_helpers.session_status_from_codex_result,
        create_pr_for_execution_result=create_pr_for_execution_result,
        handoff_execution_to_review=handoff_execution_to_review,
        handle_non_review_execution_outcome=handle_non_review_execution_outcome,
    )


def finalize_claimed_session(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    execution_outcome: SessionExecutionOutcome,
    board_info_resolver: BoardInfoResolverFn | None,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | None,
    review_state_port: ReviewStatePort | None = None,
) -> CycleResult:
    """Persist final session state and return the cycle result."""
    return _execution_outcome_wiring.finalize_claimed_session(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        claimed_context=claimed_context,
        execution_outcome=execution_outcome,
        review_state_port=review_state_port,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        final_phase_for_claimed_session=final_phase_for_claimed_session,
        persist_claimed_session_completion=persist_claimed_session_completion,
        post_claimed_session_result_comment=post_claimed_session_result_comment,
        maybe_escalate_claimed_session_failure=maybe_escalate_claimed_session_failure,
    )


def prepared_cycle_deps(
    *,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
) -> PreparedCycleDeps:
    """Bind compatibility seams at the outer wiring boundary."""

    def _resolve_launch_context_for_cycle(
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext | None,
        target_issue: str | None,
        dry_run: bool,
        review_state_port: ReviewStatePort | None,
        pr_port: PullRequestPort | None,
        subprocess_runner: Any | None,
        gh_runner: Callable[..., str] | None,
    ) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
        return resolve_launch_context_for_cycle(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            target_issue=target_issue,
            dry_run=dry_run,
            review_state_port=review_state_port,
            pr_port=pr_port,
            status_resolver=status_resolver,
            subprocess_runner=subprocess_runner,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )

    def _claim_launch_context(
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext,
        slot_id: int,
        review_state_port: ReviewStatePort | None,
        board_port: BoardMutationPort | None,
        gh_runner: Callable[..., str] | None,
    ) -> tuple[ClaimedSessionContext | None, CycleResult | None]:
        return claim_launch_context(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            slot_id=slot_id,
            review_state_port=review_state_port,
            board_port=board_port,
            status_resolver=status_resolver,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )

    def _execute_claimed_session(
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext,
        claimed_context: ClaimedSessionContext,
        gh_runner: GhRunnerPort | None,
        process_runner: ProcessRunnerPort | None,
        file_reader: Callable[[Path], str] | None,
        review_state_port: ReviewStatePort | None,
        board_port: BoardMutationPort | None,
        pr_port: PullRequestPort | None,
    ) -> SessionExecutionOutcome:
        return execute_claimed_session(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            claimed_context=claimed_context,
            process_runner=process_runner,
            file_reader=file_reader,
            review_state_port=review_state_port,
            board_port=board_port,
            pr_port=pr_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )

    def _finalize_claimed_session(
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext,
        claimed_context: ClaimedSessionContext,
        execution_outcome: SessionExecutionOutcome,
        review_state_port: ReviewStatePort | None,
        gh_runner: Callable[..., str] | None,
    ) -> CycleResult:
        return finalize_claimed_session(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            claimed_context=claimed_context,
            execution_outcome=execution_outcome,
            review_state_port=review_state_port,
            board_info_resolver=board_info_resolver,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )

    return _cycle_wiring.prepared_cycle_deps(
        claim_suppression_state=_support_wiring.claim_suppression_state,
        next_available_slot=_support_wiring.next_available_slot,
        resolve_launch_context_for_cycle=_resolve_launch_context_for_cycle,
        claim_launch_context=_claim_launch_context,
        execute_claimed_session=_execute_claimed_session,
        finalize_claimed_session=_finalize_claimed_session,
    )
