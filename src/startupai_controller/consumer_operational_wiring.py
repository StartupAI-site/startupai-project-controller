"""Shell-facing operational wiring extracted from board_consumer."""

from __future__ import annotations

from typing import Any, Callable

import startupai_controller.consumer_claim_wiring as _claim_wiring
import startupai_controller.consumer_cycle_wiring as _cycle_wiring
import startupai_controller.consumer_execution_outcome_wiring as _execution_outcome_wiring
import startupai_controller.consumer_recovery_helpers as _recovery_helpers
import startupai_controller.consumer_selection_retry_wiring as _selection_retry_wiring
import startupai_controller.consumer_support_wiring as _support_wiring
from startupai_controller.application.consumer.reconciliation import (
    ReconciliationWiringDeps,
    reconcile_active_repair_review_items as _reconcile_active_repair_review_items_use_case,
    reconcile_single_in_progress_item as _reconcile_single_in_progress_item_use_case,
    reconcile_stale_in_progress_items as _reconcile_stale_in_progress_items_use_case,
    wire_reconcile_board_truth as _wire_reconcile_board_truth_use_case,
)


def _shell_module():
    """Import the consumer shell lazily to avoid import cycles."""
    from startupai_controller import board_consumer_compat

    return board_consumer_compat


def build_reconciliation_wiring_deps() -> ReconciliationWiringDeps:
    """Build the wiring deps for board-truth reconciliation."""
    shell = _shell_module()
    return ReconciliationWiringDeps(
        board_state_reconcile_active_repair=shell._board_state_helpers.reconcile_active_repair_review_items,
        board_state_reconcile_single=shell._board_state_helpers.reconcile_single_in_progress_item,
        board_state_reconcile_stale=shell._board_state_helpers.reconcile_stale_in_progress_items,
        transition_issue_to_in_progress=shell._transition_issue_to_in_progress,
        return_issue_to_ready=shell._return_issue_to_ready,
        transition_issue_to_review=shell._transition_issue_to_review,
        set_blocked_with_reason=shell._set_blocked_with_reason,
        resolve_issue_coordinates=shell._resolve_issue_coordinates,
        classify_open_pr_candidates=shell._classify_open_pr_candidates,
        reconcile_in_progress_decision=shell.reconcile_in_progress_decision,
        snapshot_to_issue_ref=shell._snapshot_to_issue_ref,
        build_session_store=shell.build_session_store,
        build_github_port_bundle=shell.build_github_port_bundle,
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
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
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
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
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
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )


def recover_interrupted_sessions(
    config: Any,
    db: Any,
    *,
    automation_config: Any | None = None,
    pr_port: Any | None = None,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[Any]:
    """Recover leases left behind by a previous interrupted daemon process."""
    shell = _shell_module()
    return _recovery_helpers.recover_interrupted_sessions(
        config,
        db,
        automation_config=automation_config,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        load_config=shell.load_config,
        config_error_type=shell.ConfigError,
        logger=shell.logger,
        recovery_use_case=shell._recover_interrupted_sessions_use_case,
        gh_query_error_type=shell.GhQueryError,
        build_github_port_bundle=shell.build_github_port_bundle,
        load_automation_config=shell.load_automation_config,
        resolve_issue_coordinates=shell._resolve_issue_coordinates,
        classify_open_pr_candidates=shell._classify_open_pr_candidates,
        return_issue_to_ready=shell._return_issue_to_ready,
        transition_issue_to_review=shell._transition_issue_to_review,
        set_blocked_with_reason=shell._set_blocked_with_reason,
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
    return _wire_reconcile_board_truth_use_case(
        consumer_config,
        critical_path_config,
        automation_config,
        db,
        deps=build_reconciliation_wiring_deps(),
        session_store=session_store,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        dry_run=dry_run,
        board_snapshot=board_snapshot,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
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
    shell = _shell_module()
    _execution_outcome_wiring.block_prelaunch_issue(
        issue_ref,
        blocked_reason,
        config=config,
        cp_config=cp_config,
        db=db,
        gh_query_error_type=shell.GhQueryError,
        set_blocked_with_reason=shell._set_blocked_with_reason,
        record_successful_github_mutation=shell._record_successful_github_mutation,
        mark_degraded=shell._mark_degraded,
        queue_status_transition=shell._queue_status_transition,
        logger=shell.logger,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )


def select_launch_candidate_for_cycle(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    target_issue: str | None,
    status_resolver: Callable[..., str] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[Any | None, Any | None]:
    """Select a launch candidate and validate its immediate launchability."""
    shell = _shell_module()
    return _selection_retry_wiring.select_launch_candidate_for_cycle(
        config=config,
        db=db,
        prepared=prepared,
        target_issue=target_issue,
        status_resolver=status_resolver,
        gh_runner=gh_runner,
        cycle_result_factory=shell.CycleResult,
        selected_launch_candidate_factory=shell.SelectedLaunchCandidate,
        select_candidate_for_cycle=shell._select_candidate_for_cycle,
        parse_issue_ref=shell.parse_issue_ref,
        effective_retry_backoff=shell._effective_retry_backoff,
        retry_backoff_active=shell._retry_backoff_active,
        maybe_activate_claim_suppression=shell._maybe_activate_claim_suppression,
        mark_degraded=shell._mark_degraded,
        gh_reason_code=shell.gh_reason_code,
        gh_query_error_type=shell.GhQueryError,
        logger=shell.logger,
    )


def prepare_selected_launch_candidate(
    *,
    selected_candidate: Any,
    config: Any,
    db: Any,
    prepared: Any,
    subprocess_runner: Callable[..., Any] | None,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[Any | None, Any | None]:
    """Prepare the selected candidate into launch-ready local context."""
    shell = _shell_module()
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
        record_metric=shell._record_metric,
        prepare_launch_candidate=shell._prepare_launch_candidate,
        handle_selected_launch_query_error=shell._handle_selected_launch_query_error,
        handle_selected_launch_workflow_config_error=shell._handle_selected_launch_workflow_config_error,
        handle_selected_launch_worktree_error=shell._handle_selected_launch_worktree_error,
        handle_selected_launch_runtime_error=shell._handle_selected_launch_runtime_error,
        workflow_config_error_type=shell.WorkflowConfigError,
        worktree_prepare_error_type=shell.WorktreePrepareError,
        gh_query_error_type=shell.GhQueryError,
    )


def handle_selected_launch_query_error(
    *,
    candidate: str,
    err: Exception,
    config: Any,
    db: Any,
) -> tuple[None, Any]:
    """Handle GitHub/query failures during selected launch preparation."""
    shell = _shell_module()
    return _claim_wiring.handle_selected_launch_query_error(
        candidate=candidate,
        err=err,
        config=config,
        db=db,
        record_metric=shell._record_metric,
        maybe_activate_claim_suppression=shell._maybe_activate_claim_suppression,
        mark_degraded=shell._mark_degraded,
        gh_reason_code=shell.gh_reason_code,
        cycle_result_factory=shell.CycleResult,
    )


def handle_selected_launch_workflow_config_error(
    *,
    candidate: str,
    err: Exception,
    config: Any,
    db: Any,
    cp_config: Any,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[None, Any]:
    """Handle invalid workflow configuration during launch preparation."""
    shell = _shell_module()
    return _claim_wiring.handle_selected_launch_workflow_config_error(
        candidate=candidate,
        err=err,
        config=config,
        db=db,
        cp_config=cp_config,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        block_prelaunch_issue=shell._block_prelaunch_issue,
        record_metric=shell._record_metric,
        cycle_result_factory=shell.CycleResult,
    )


def handle_selected_launch_worktree_error(
    *,
    candidate: str,
    err: Exception,
    config: Any,
    db: Any,
) -> tuple[None, Any]:
    """Handle worktree preparation failures for a selected launch candidate."""
    shell = _shell_module()
    return _claim_wiring.handle_selected_launch_worktree_error(
        candidate=candidate,
        err=err,
        config=config,
        db=db,
        record_metric=shell._record_metric,
        cycle_result_factory=shell.CycleResult,
    )


def handle_selected_launch_runtime_error(
    *,
    candidate: str,
    err: Exception,
    config: Any,
    db: Any,
    cp_config: Any,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[None, Any]:
    """Handle workflow-hook runtime failures during launch preparation."""
    shell = _shell_module()
    return _claim_wiring.handle_selected_launch_runtime_error(
        candidate=candidate,
        err=err,
        config=config,
        db=db,
        cp_config=cp_config,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        block_prelaunch_issue=shell._block_prelaunch_issue,
        record_metric=shell._record_metric,
        cycle_result_factory=shell.CycleResult,
    )


def resolve_launch_context_for_cycle(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any | None,
    target_issue: str | None,
    dry_run: bool,
    status_resolver: Callable[..., str] | None,
    subprocess_runner: Callable[..., Any] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[Any | None, Any | None]:
    """Resolve or prepare launch-ready work for this cycle."""
    shell = _shell_module()
    return _cycle_wiring.resolve_launch_context_for_cycle(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        target_issue=target_issue,
        dry_run=dry_run,
        status_resolver=status_resolver,
        subprocess_runner=subprocess_runner,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        select_launch_candidate_for_cycle=shell._select_launch_candidate_for_cycle,
        prepare_selected_launch_candidate=shell._prepare_selected_launch_candidate,
        logger=shell.logger,
    )


def open_pending_claim_session(
    *,
    db: Any,
    launch_context: Any,
    executor: str,
    slot_id: int,
) -> tuple[Any | None, Any | None]:
    """Create the session record and acquire the lease for a launch-ready issue."""
    shell = _shell_module()
    return _claim_wiring.open_pending_claim_session(
        db=db,
        launch_context=launch_context,
        executor=executor,
        slot_id=slot_id,
        complete_session=shell._complete_session,
        pending_claim_context_factory=shell.PendingClaimContext,
        cycle_result_factory=shell.CycleResult,
    )


def enforce_claim_retry_ceiling(
    *,
    config: Any,
    db: Any,
    launch_context: Any,
    pending_claim: Any,
    cp_config: Any,
    board_info_resolver: Callable[..., Any] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> Any | None:
    """Abort and escalate if the issue already exhausted its retry ceiling."""
    shell = _shell_module()
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
        complete_session=shell._complete_session,
        escalate_to_claude=shell._escalate_to_claude,
        cycle_result_factory=shell.CycleResult,
        logger=shell.logger,
    )


def attempt_launch_context_claim(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    pending_claim: Any,
    slot_id: int,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[Any | None, Any | None]:
    """Claim board ownership for a launch-ready issue."""
    shell = _shell_module()
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
        claim_launch_ready_issue=shell._claim_launch_ready_issue,
        handle_launch_claim_api_failure=shell._handle_launch_claim_api_failure,
        handle_launch_claim_unexpected_failure=shell._handle_launch_claim_unexpected_failure,
        handle_launch_claim_rejection=shell._handle_launch_claim_rejection,
        record_metric=shell._record_metric,
        gh_query_error_type=shell.GhQueryError,
    )


def claim_launch_ready_issue(
    candidate: str,
    *,
    config: Any,
    prepared: Any,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> Any:
    """Execute the actual board claim for a launch-ready issue."""
    shell = _shell_module()
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
        claim_ready_issue=shell.claim_ready_issue,
    )


def handle_launch_claim_api_failure(
    candidate: str,
    err: Exception,
    *,
    config: Any,
    db: Any,
    pending_claim: Any,
) -> tuple[None, Any]:
    """Handle a GitHub/API failure while claiming a launch-ready issue."""
    shell = _shell_module()
    return _claim_wiring.handle_launch_claim_api_failure(
        candidate,
        err,
        config=config,
        db=db,
        pending_claim=pending_claim,
        maybe_activate_claim_suppression=shell._maybe_activate_claim_suppression,
        mark_degraded=shell._mark_degraded,
        gh_reason_code=shell.gh_reason_code,
        complete_session=shell._complete_session,
        record_metric=shell._record_metric,
        cycle_result_factory=shell.CycleResult,
        logger=shell.logger,
    )


def handle_launch_claim_unexpected_failure(
    candidate: str,
    err: Exception,
    *,
    config: Any,
    db: Any,
    pending_claim: Any,
) -> tuple[None, Any]:
    """Handle an unexpected local failure while claiming a launch-ready issue."""
    shell = _shell_module()
    return _claim_wiring.handle_launch_claim_unexpected_failure(
        candidate,
        err,
        config=config,
        db=db,
        pending_claim=pending_claim,
        complete_session=shell._complete_session,
        record_metric=shell._record_metric,
        cycle_result_factory=shell.CycleResult,
        logger=shell.logger,
    )


def handle_launch_claim_rejection(
    candidate: str,
    claim_result: Any,
    *,
    config: Any,
    db: Any,
    pending_claim: Any,
) -> tuple[None, Any]:
    """Handle a non-exception claim rejection for a launch-ready issue."""
    shell = _shell_module()
    return _claim_wiring.handle_launch_claim_rejection(
        candidate,
        claim_result,
        config=config,
        db=db,
        pending_claim=pending_claim,
        complete_session=shell._complete_session,
        record_metric=shell._record_metric,
        cycle_result_factory=shell.CycleResult,
    )


def mark_claimed_session_running(
    *,
    config: Any,
    db: Any,
    launch_context: Any,
    pending_claim: Any,
    slot_id: int,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    cp_config: Any,
    gh_runner: Callable[..., str] | None,
) -> Any:
    """Persist the durable-start state and post the claim marker."""
    shell = _shell_module()
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
        record_successful_github_mutation=shell._record_successful_github_mutation,
        record_metric=shell._record_metric,
        post_consumer_claim_comment=shell._post_consumer_claim_comment,
        claimed_session_context_factory=shell.ClaimedSessionContext,
        logger=shell.logger,
    )


def claim_launch_context(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    slot_id: int,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[Any | None, Any | None]:
    """Claim board ownership and start a durable running session."""
    shell = _shell_module()
    return _cycle_wiring.claim_launch_context(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        slot_id=slot_id,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        open_pending_claim_session=shell._open_pending_claim_session,
        enforce_claim_retry_ceiling=shell._enforce_claim_retry_ceiling,
        attempt_launch_context_claim=shell._attempt_launch_context_claim,
        mark_claimed_session_running=shell._mark_claimed_session_running,
    )


def create_pr_for_execution_result(
    *,
    config: Any,
    launch_context: Any,
    claimed_context: Any,
    codex_result: dict[str, Any] | None,
    session_status: str,
    failure_reason: str | None,
    subprocess_runner: Callable[..., Any] | None,
    gh_runner: Callable[..., str] | None,
) -> Any:
    """Reuse or create a PR from claimed-session output."""
    shell = _shell_module()
    return _execution_outcome_wiring.create_pr_for_execution_result(
        config=config,
        launch_context=launch_context,
        claimed_context=claimed_context,
        codex_result=codex_result,
        session_status=session_status,
        failure_reason=failure_reason,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        has_commits_on_branch=shell._has_commits_on_branch,
        create_or_update_pr=shell._create_or_update_pr,
        pr_creation_outcome_factory=shell.PrCreationOutcome,
        logger=shell.logger,
    )


def handoff_execution_to_review(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    session_id: str,
    pr_url: str,
    session_status: str,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> Any:
    """Transition a claimed session into Review and perform immediate rescue."""
    shell = _shell_module()
    return _cycle_wiring.handoff_execution_to_review(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        session_id=session_id,
        pr_url=pr_url,
        session_status=session_status,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        build_session_store=shell.build_session_store,
        transition_claimed_session_to_review=shell._transition_claimed_session_to_review,
        post_claimed_session_verdict_marker=shell._post_claimed_session_verdict_marker,
        queue_claimed_session_for_review=shell._queue_claimed_session_for_review,
        run_immediate_review_handoff=shell._run_immediate_review_handoff,
        record_metric=shell._record_metric,
    )


def transition_claimed_session_to_review(
    *,
    db: Any,
    issue_ref: str,
    session_id: str,
    config: Any,
    critical_path_config: Any,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Move one claimed issue into Review or queue the transition on failure."""
    shell = _shell_module()
    _execution_outcome_wiring.transition_claimed_session_to_review(
        db=db,
        issue_ref=issue_ref,
        session_id=session_id,
        config=config,
        critical_path_config=critical_path_config,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        transition_issue_to_review=shell._transition_issue_to_review,
        record_successful_github_mutation=shell._record_successful_github_mutation,
        mark_degraded=shell._mark_degraded,
        queue_status_transition=shell._queue_status_transition,
        logger=shell.logger,
    )


def post_claimed_session_verdict_marker(
    *,
    db: Any,
    pr_url: str,
    session_id: str,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Post the codex verdict marker for a newly handed-off review PR."""
    shell = _shell_module()
    _execution_outcome_wiring.post_claimed_session_verdict_marker(
        db=db,
        pr_url=pr_url,
        session_id=session_id,
        gh_runner=gh_runner,
        post_pr_codex_verdict=shell._post_pr_codex_verdict,
        record_successful_github_mutation=shell._record_successful_github_mutation,
        mark_degraded=shell._mark_degraded,
        queue_verdict_marker=shell._queue_verdict_marker,
        logger=shell.logger,
    )


def queue_claimed_session_for_review(
    *,
    store: Any,
    issue_ref: str,
    pr_url: str,
    session_id: str,
) -> Any | None:
    """Queue one claimed session for immediate review handling."""
    shell = _shell_module()
    return _execution_outcome_wiring.queue_claimed_session_for_review(
        store=store,
        issue_ref=issue_ref,
        pr_url=pr_url,
        session_id=session_id,
        queue_review_item=shell._queue_review_item,
    )


def run_immediate_review_handoff(
    *,
    config: Any,
    critical_path_config: Any,
    automation_config: Any,
    store: Any,
    queue_entry: Any,
    gh_runner: Callable[..., str] | None,
    db: Any,
) -> Any:
    """Run immediate rescue for the just-opened review PR."""
    shell = _shell_module()
    return _execution_outcome_wiring.run_immediate_review_handoff(
        config=config,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        store=store,
        queue_entry=queue_entry,
        gh_runner=gh_runner,
        db=db,
        build_github_port_bundle=shell.build_github_port_bundle,
        github_memo_factory=shell.CycleGitHubMemo,
        build_review_snapshots_for_queue_entries=shell._build_review_snapshots_for_queue_entries,
        review_rescue=shell.review_rescue,
        apply_review_queue_result=shell._apply_review_queue_result,
        mark_degraded=shell._mark_degraded,
        gh_reason_code=shell.gh_reason_code,
        summary_factory=shell.ReviewQueueDrainSummary,
        logger=shell.logger,
    )


def handle_non_review_execution_outcome(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    session_id: str,
    session_status: str,
    codex_result: dict[str, Any] | None,
    has_commits: bool,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_poster: Callable[..., None] | None,
    subprocess_runner: Callable[..., Any] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[str, Any | None, str | None]:
    """Handle non-review outcomes for a claimed session."""
    shell = _shell_module()
    return _execution_outcome_wiring.handle_non_review_execution_outcome(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        session_id=session_id,
        session_status=session_status,
        codex_result=codex_result,
        has_commits=has_commits,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_poster=comment_poster,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        build_github_port_bundle=shell.build_github_port_bundle,
        verify_resolution_payload=shell._verify_resolution_payload,
        apply_resolution_action=shell._apply_resolution_action,
        return_issue_to_ready=shell._return_issue_to_ready,
        record_successful_github_mutation=shell._record_successful_github_mutation,
        mark_degraded=shell._mark_degraded,
        queue_status_transition=shell._queue_status_transition,
        record_metric=shell._record_metric,
        logger=shell.logger,
    )


def final_phase_for_claimed_session(
    *,
    launch_context: Any,
    execution_outcome: Any,
) -> str:
    """Determine the final persisted phase for a claimed session."""
    return _execution_outcome_wiring.final_phase_for_claimed_session(
        launch_context=launch_context,
        execution_outcome=execution_outcome,
    )


def persist_claimed_session_completion(
    *,
    db: Any,
    session_id: str,
    issue_ref: str,
    execution_outcome: Any,
    final_phase: str,
) -> None:
    """Persist the final session record for a claimed execution outcome."""
    shell = _shell_module()
    _execution_outcome_wiring.persist_claimed_session_completion(
        db=db,
        session_id=session_id,
        issue_ref=issue_ref,
        execution_outcome=execution_outcome,
        final_phase=final_phase,
        complete_session=shell._complete_session,
    )


def post_claimed_session_result_comment(
    *,
    issue_ref: str,
    session_id: str,
    codex_result: dict[str, Any] | None,
    cp_config: Any,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Post the session result comment when Codex produced structured output."""
    shell = _shell_module()
    _execution_outcome_wiring.post_claimed_session_result_comment(
        issue_ref=issue_ref,
        session_id=session_id,
        codex_result=codex_result,
        cp_config=cp_config,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        post_result_comment=shell._post_result_comment,
        logger=shell.logger,
    )


def maybe_escalate_claimed_session_failure(
    *,
    config: Any,
    db: Any,
    issue_ref: str,
    effective_max_retries: int,
    session_status: str,
    codex_result: dict[str, Any] | None,
    cp_config: Any,
    board_info_resolver: Callable[..., Any] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Escalate terminal failed/timeout sessions once retry ceiling is reached."""
    shell = _shell_module()
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
        escalate_to_claude=shell._escalate_to_claude,
        logger=shell.logger,
    )


def execute_claimed_session(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    claimed_context: Any,
    subprocess_runner: Callable[..., Any] | None,
    file_reader: Callable[..., Any] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> Any:
    """Execute Codex for a claimed session and apply immediate board handoff."""
    shell = _shell_module()
    return _execution_outcome_wiring.execute_claimed_session(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        claimed_context=claimed_context,
        subprocess_runner=subprocess_runner,
        file_reader=file_reader,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        assemble_codex_prompt=shell._assemble_codex_prompt,
        run_codex_session=shell._run_codex_session,
        parse_codex_result=shell._parse_codex_result,
        session_status_from_codex_result=shell._session_status_from_codex_result,
        create_pr_for_execution_result=shell._create_pr_for_execution_result,
        handoff_execution_to_review=shell._handoff_execution_to_review,
        handle_non_review_execution_outcome=shell._handle_non_review_execution_outcome,
    )


def finalize_claimed_session(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    claimed_context: Any,
    execution_outcome: Any,
    board_info_resolver: Callable[..., Any] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> Any:
    """Persist final session state and return the cycle result."""
    shell = _shell_module()
    return _execution_outcome_wiring.finalize_claimed_session(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        claimed_context=claimed_context,
        execution_outcome=execution_outcome,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        final_phase_for_claimed_session=shell._final_phase_for_claimed_session,
        persist_claimed_session_completion=shell._persist_claimed_session_completion,
        post_claimed_session_result_comment=shell._post_claimed_session_result_comment,
        maybe_escalate_claimed_session_failure=shell._maybe_escalate_claimed_session_failure,
    )


def prepared_cycle_deps():
    """Bind board_consumer helpers for the extracted prepared-cycle slice."""
    shell = _shell_module()
    return _cycle_wiring.prepared_cycle_deps(
        claim_suppression_state=_support_wiring.claim_suppression_state,
        next_available_slot=_support_wiring.next_available_slot,
        resolve_launch_context_for_cycle=shell._resolve_launch_context_for_cycle,
        claim_launch_context=shell._claim_launch_context,
        execute_claimed_session=shell._execute_claimed_session,
        finalize_claimed_session=shell._finalize_claimed_session,
    )
