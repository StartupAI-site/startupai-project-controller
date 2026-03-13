"""Shell-facing operational wiring extracted from board_consumer."""

from __future__ import annotations

from pathlib import Path
import subprocess
from collections.abc import Callable

import startupai_controller.consumer_prepared_cycle_wiring as _prepared_cycle_wiring
import startupai_controller.consumer_reconciliation_wiring as _reconciliation_wiring
from startupai_controller.application.consumer.cycle import PreparedCycleDeps
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    PreparedCycleContext,
    PreparedLaunchContext,
    SessionExecutionOutcome,
)
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    CycleResult,
    ReviewQueueEntry,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.process_runner import GhRunnerPort, ProcessRunnerPort
from startupai_controller.ports.pull_requests import PullRequestPort
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
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig


def build_reconciliation_wiring_deps() -> (
    _reconciliation_wiring.ReconciliationWiringDeps
):
    """Build the wiring deps for board-truth reconciliation."""
    return _reconciliation_wiring.build_reconciliation_wiring_deps()


def reconcile_active_repair_review_items(
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    *,
    active_repair_sessions_by_issue: dict[str, str],
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_snapshot: CycleBoardSnapshot | None,
    issue_ref_for_snapshot: Callable[
        [_reconciliation_wiring.SnapshotIssueRefView], str | None
    ],
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
    dry_run: bool,
) -> list[str]:
    """Return active repair items that should move from Review back to In Progress."""
    return _reconciliation_wiring.reconcile_active_repair_review_items(
        consumer_config,
        critical_path_config,
        active_repair_sessions_by_issue=active_repair_sessions_by_issue,
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
    return _reconciliation_wiring.reconcile_single_in_progress_item(
        issue_ref,
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
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    *,
    store: SessionStorePort,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_snapshot: CycleBoardSnapshot | None,
    issue_ref_for_snapshot: Callable[
        [_reconciliation_wiring.SnapshotIssueRefView], str | None
    ],
    active_issue_refs: set[str],
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
    dry_run: bool,
) -> tuple[list[str], list[str], list[str]]:
    """Reconcile stale In Progress items back to their truthful lanes."""
    return _reconciliation_wiring.reconcile_stale_in_progress_items(
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
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )


def recover_interrupted_sessions(
    config: ConsumerConfig,
    db: _reconciliation_wiring.RecoveryStatePort,
    *,
    automation_config: BoardAutomationConfig | None = None,
    pr_port: PullRequestPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[_reconciliation_wiring.RecoveredLeaseInfo]:
    """Recover leases left behind by a previous interrupted daemon process."""
    return _reconciliation_wiring.recover_interrupted_sessions(
        config,
        db,
        automation_config=automation_config,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        gh_runner=gh_runner,
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
) -> _reconciliation_wiring.ReconciliationResult:
    """Make board In Progress truthful against local consumer state."""
    return _reconciliation_wiring.reconcile_board_truth(
        consumer_config,
        critical_path_config,
        automation_config,
        db,
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


SubprocessRunnerFn = _prepared_cycle_wiring.SubprocessRunnerFn
block_prelaunch_issue = _prepared_cycle_wiring.block_prelaunch_issue
select_launch_candidate_for_cycle = (
    _prepared_cycle_wiring.select_launch_candidate_for_cycle
)
prepare_selected_launch_candidate = (
    _prepared_cycle_wiring.prepare_selected_launch_candidate
)
handle_selected_launch_query_error = (
    _prepared_cycle_wiring.handle_selected_launch_query_error
)
handle_selected_launch_workflow_config_error = (
    _prepared_cycle_wiring.handle_selected_launch_workflow_config_error
)
handle_selected_launch_worktree_error = (
    _prepared_cycle_wiring.handle_selected_launch_worktree_error
)
handle_selected_launch_runtime_error = (
    _prepared_cycle_wiring.handle_selected_launch_runtime_error
)
open_pending_claim_session = _prepared_cycle_wiring.open_pending_claim_session
enforce_claim_retry_ceiling = _prepared_cycle_wiring.enforce_claim_retry_ceiling
attempt_launch_context_claim = _prepared_cycle_wiring.attempt_launch_context_claim
claim_launch_ready_issue = _prepared_cycle_wiring.claim_launch_ready_issue
handle_launch_claim_api_failure = _prepared_cycle_wiring.handle_launch_claim_api_failure
handle_launch_claim_unexpected_failure = (
    _prepared_cycle_wiring.handle_launch_claim_unexpected_failure
)
handle_launch_claim_rejection = _prepared_cycle_wiring.handle_launch_claim_rejection
mark_claimed_session_running = _prepared_cycle_wiring.mark_claimed_session_running
create_pr_for_execution_result = _prepared_cycle_wiring.create_pr_for_execution_result
transition_claimed_session_to_review = (
    _prepared_cycle_wiring.transition_claimed_session_to_review
)
post_claimed_session_verdict_marker = (
    _prepared_cycle_wiring.post_claimed_session_verdict_marker
)
run_immediate_review_handoff = _prepared_cycle_wiring.run_immediate_review_handoff
handle_non_review_execution_outcome = (
    _prepared_cycle_wiring.handle_non_review_execution_outcome
)
post_claimed_session_result_comment = (
    _prepared_cycle_wiring.post_claimed_session_result_comment
)
maybe_escalate_claimed_session_failure = (
    _prepared_cycle_wiring.maybe_escalate_claimed_session_failure
)
handoff_execution_to_review = _prepared_cycle_wiring.handoff_execution_to_review


def queue_claimed_session_for_review(
    *,
    store: SessionStorePort,
    issue_ref: str,
    pr_url: str,
    session_id: str,
) -> ReviewQueueEntry | None:
    """Queue a claimed session for later review handoff processing."""
    return _prepared_cycle_wiring.queue_claimed_session_for_review(
        store=store,
        issue_ref=issue_ref,
        pr_url=pr_url,
        session_id=session_id,
    )


def final_phase_for_claimed_session(
    *,
    launch_context: PreparedLaunchContext,
    execution_outcome: SessionExecutionOutcome,
) -> str:
    """Compute the final durable phase for a claimed session outcome."""
    return _prepared_cycle_wiring.final_phase_for_claimed_session(
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
    """Persist the terminal session status for a claimed execution result."""
    _prepared_cycle_wiring.persist_claimed_session_completion(
        db=db,
        session_id=session_id,
        issue_ref=issue_ref,
        execution_outcome=execution_outcome,
        final_phase=final_phase,
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
    subprocess_runner: SubprocessRunnerFn | None,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
    review_state_port: ReviewStatePort | None = None,
    pr_port: PullRequestPort | None = None,
) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
    """Resolve or prepare launch-ready work for this cycle."""
    return _prepared_cycle_wiring.resolve_launch_context_for_cycle(
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
        review_state_port=review_state_port,
        pr_port=pr_port,
        select_launch_candidate_for_cycle_fn=select_launch_candidate_for_cycle,
        prepare_selected_launch_candidate_fn=prepare_selected_launch_candidate,
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
    return _prepared_cycle_wiring.claim_launch_context(
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
        open_pending_claim_session_fn=open_pending_claim_session,
        enforce_claim_retry_ceiling_fn=enforce_claim_retry_ceiling,
        attempt_launch_context_claim_fn=attempt_launch_context_claim,
        mark_claimed_session_running_fn=mark_claimed_session_running,
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
    return _prepared_cycle_wiring.execute_claimed_session(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        claimed_context=claimed_context,
        process_runner=process_runner,
        subprocess_runner=subprocess_runner,
        file_reader=file_reader,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        review_state_port=review_state_port,
        board_port=board_port,
        pr_port=pr_port,
        create_pr_for_execution_result_fn=create_pr_for_execution_result,
        handoff_execution_to_review_fn=handoff_execution_to_review,
        handle_non_review_execution_outcome_fn=handle_non_review_execution_outcome,
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
    return _prepared_cycle_wiring.finalize_claimed_session(
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
        review_state_port=review_state_port,
        final_phase_for_claimed_session_fn=final_phase_for_claimed_session,
        persist_claimed_session_completion_fn=persist_claimed_session_completion,
        post_claimed_session_result_comment_fn=post_claimed_session_result_comment,
        maybe_escalate_claimed_session_failure_fn=maybe_escalate_claimed_session_failure,
    )


def prepared_cycle_deps(
    *,
    status_resolver: StatusResolverFn | None,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
) -> PreparedCycleDeps:
    """Bind compatibility seams at the outer wiring boundary."""
    return _prepared_cycle_wiring.prepared_cycle_deps(
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        resolve_launch_context_for_cycle_fn=resolve_launch_context_for_cycle,
        claim_launch_context_fn=claim_launch_context,
        execute_claimed_session_fn=execute_claimed_session,
        finalize_claimed_session_fn=finalize_claimed_session,
    )
