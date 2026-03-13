"""Prepared-cycle lifecycle wiring extracted from consumer_operational_wiring."""

from __future__ import annotations

import logging
from pathlib import Path
import subprocess
from collections.abc import Callable
from typing import cast

import startupai_controller.consumer_automation_bridge as _automation_bridge
import startupai_controller.consumer_board_state_helpers as _board_state_helpers
import startupai_controller.consumer_claimed_session_wiring as _claimed_session_wiring
import startupai_controller.consumer_claim_wiring as _claim_wiring
import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
import startupai_controller.consumer_cycle_wiring as _cycle_wiring
import startupai_controller.consumer_execution_outcome_wiring as _execution_outcome_wiring
import startupai_controller.consumer_selection_retry_wiring as _selection_retry_wiring
import startupai_controller.consumer_support_wiring as _support_wiring
from startupai_controller.application.consumer.cycle import PreparedCycleDeps
from startupai_controller.consumer_config import ConsumerConfig
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
from startupai_controller.domain.models import (
    ClaimReadyResult,
    CycleResult,
    ResolutionEvaluation,
    ReviewQueueDrainSummary,
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
from startupai_controller.runtime.wiring import gh_reason_code
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
    parse_issue_ref,
)

logger = logging.getLogger("board-consumer")

SubprocessRunnerFn = Callable[..., subprocess.CompletedProcess[str]]
_record_metric = cast(_claim_wiring.RecordMetricFn, _support_wiring.record_metric)


def block_prelaunch_issue(
    issue_ref: str,
    blocked_reason: str,
    *,
    config: ConsumerConfig,
    cp_config: CriticalPathConfig,
    db: ConsumerRuntimeStatePort,
    board_info_resolver: BoardInfoResolverFn | None = None,
    board_mutator: BoardStatusMutatorFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
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
    subprocess_runner: SubprocessRunnerFn | None,
    status_resolver: StatusResolverFn | None,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
    pr_port: PullRequestPort | None = None,
    handle_selected_launch_query_error_fn: (
        Callable[..., tuple[None, CycleResult]] | None
    ) = None,
    handle_selected_launch_workflow_config_error_fn: (
        Callable[..., tuple[None, CycleResult]] | None
    ) = None,
    handle_selected_launch_worktree_error_fn: (
        Callable[..., tuple[None, CycleResult]] | None
    ) = None,
    handle_selected_launch_runtime_error_fn: (
        Callable[..., tuple[None, CycleResult]] | None
    ) = None,
) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
    """Prepare the selected candidate into launch-ready local context."""
    effective_query_error_handler = (
        handle_selected_launch_query_error_fn or handle_selected_launch_query_error
    )
    effective_workflow_config_error_handler = (
        handle_selected_launch_workflow_config_error_fn
        or handle_selected_launch_workflow_config_error
    )
    effective_worktree_error_handler = (
        handle_selected_launch_worktree_error_fn
        or handle_selected_launch_worktree_error
    )
    effective_runtime_error_handler = (
        handle_selected_launch_runtime_error_fn or handle_selected_launch_runtime_error
    )
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
        record_metric=_record_metric,
        prepare_launch_candidate=_cycle_wiring.prepare_launch_candidate,
        handle_selected_launch_query_error=effective_query_error_handler,
        handle_selected_launch_workflow_config_error=effective_workflow_config_error_handler,
        handle_selected_launch_worktree_error=effective_worktree_error_handler,
        handle_selected_launch_runtime_error=effective_runtime_error_handler,
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
        record_metric=_record_metric,
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
    cp_config: CriticalPathConfig,
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
        record_metric=_record_metric,
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
        record_metric=_record_metric,
        cycle_result_factory=CycleResult,
    )


def handle_selected_launch_runtime_error(
    *,
    candidate: str,
    err: RuntimeError,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    cp_config: CriticalPathConfig,
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
        record_metric=_record_metric,
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
    subprocess_runner: SubprocessRunnerFn | None,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
    review_state_port: ReviewStatePort | None = None,
    pr_port: PullRequestPort | None = None,
    select_launch_candidate_for_cycle_fn: (
        Callable[..., tuple[SelectedLaunchCandidate | None, CycleResult | None]] | None
    ) = None,
    prepare_selected_launch_candidate_fn: (
        Callable[..., tuple[PreparedLaunchContext | None, CycleResult | None]] | None
    ) = None,
) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
    """Resolve or prepare launch-ready work for this cycle."""
    effective_select_launch_candidate_for_cycle = (
        select_launch_candidate_for_cycle_fn or select_launch_candidate_for_cycle
    )
    effective_prepare_selected_launch_candidate = (
        prepare_selected_launch_candidate_fn or prepare_selected_launch_candidate
    )
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
        select_launch_candidate_for_cycle=effective_select_launch_candidate_for_cycle,
        prepare_selected_launch_candidate=effective_prepare_selected_launch_candidate,
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
    cp_config: CriticalPathConfig,
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
        record_metric=_record_metric,
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
        record_metric=_record_metric,
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
        record_metric=_record_metric,
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
        record_metric=_record_metric,
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
    cp_config: CriticalPathConfig,
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
        record_metric=_record_metric,
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
    open_pending_claim_session_fn: (
        Callable[..., tuple[PendingClaimContext | None, CycleResult | None]] | None
    ) = None,
    enforce_claim_retry_ceiling_fn: Callable[..., CycleResult | None] | None = None,
    attempt_launch_context_claim_fn: (
        Callable[..., tuple[ClaimReadyResult | None, CycleResult | None]] | None
    ) = None,
    mark_claimed_session_running_fn: Callable[..., ClaimedSessionContext] | None = None,
) -> tuple[ClaimedSessionContext | None, CycleResult | None]:
    """Claim board ownership and start a durable running session."""
    effective_open_pending_claim_session = (
        open_pending_claim_session_fn or open_pending_claim_session
    )
    effective_enforce_claim_retry_ceiling = (
        enforce_claim_retry_ceiling_fn or enforce_claim_retry_ceiling
    )
    effective_attempt_launch_context_claim = (
        attempt_launch_context_claim_fn or attempt_launch_context_claim
    )
    effective_mark_claimed_session_running = (
        mark_claimed_session_running_fn or mark_claimed_session_running
    )
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
        open_pending_claim_session=effective_open_pending_claim_session,
        enforce_claim_retry_ceiling=effective_enforce_claim_retry_ceiling,
        attempt_launch_context_claim=effective_attempt_launch_context_claim,
        mark_claimed_session_running=effective_mark_claimed_session_running,
    )


create_pr_for_execution_result = _claimed_session_wiring.create_pr_for_execution_result
transition_claimed_session_to_review = (
    _claimed_session_wiring.transition_claimed_session_to_review
)
post_claimed_session_verdict_marker = (
    _claimed_session_wiring.post_claimed_session_verdict_marker
)
queue_claimed_session_for_review = (
    _claimed_session_wiring.queue_claimed_session_for_review
)
run_immediate_review_handoff = _claimed_session_wiring.run_immediate_review_handoff
handle_non_review_execution_outcome = (
    _claimed_session_wiring.handle_non_review_execution_outcome
)
final_phase_for_claimed_session = (
    _claimed_session_wiring.final_phase_for_claimed_session
)
persist_claimed_session_completion = (
    _claimed_session_wiring.persist_claimed_session_completion
)
post_claimed_session_result_comment = (
    _claimed_session_wiring.post_claimed_session_result_comment
)
maybe_escalate_claimed_session_failure = (
    _claimed_session_wiring.maybe_escalate_claimed_session_failure
)
handoff_execution_to_review = _claimed_session_wiring.handoff_execution_to_review


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
    create_pr_for_execution_result_fn: Callable[..., PrCreationOutcome] | None = None,
    handoff_execution_to_review_fn: (
        Callable[..., ReviewQueueDrainSummary] | None
    ) = None,
    handle_non_review_execution_outcome_fn: (
        Callable[..., tuple[str, ResolutionEvaluation | None, str | None]] | None
    ) = None,
) -> SessionExecutionOutcome:
    """Execute Codex for a claimed session and apply immediate board handoff."""
    return _claimed_session_wiring.execute_claimed_session(
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
        create_pr_for_execution_result_fn=(
            create_pr_for_execution_result_fn or create_pr_for_execution_result
        ),
        handoff_execution_to_review_fn=(
            handoff_execution_to_review_fn or handoff_execution_to_review
        ),
        handle_non_review_execution_outcome_fn=(
            handle_non_review_execution_outcome_fn
            or handle_non_review_execution_outcome
        ),
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
    final_phase_for_claimed_session_fn: Callable[..., str] | None = None,
    persist_claimed_session_completion_fn: Callable[..., None] | None = None,
    post_claimed_session_result_comment_fn: Callable[..., None] | None = None,
    maybe_escalate_claimed_session_failure_fn: Callable[..., None] | None = None,
) -> CycleResult:
    """Persist final session state and return the cycle result."""
    return _claimed_session_wiring.finalize_claimed_session(
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
        final_phase_for_claimed_session_fn=(
            final_phase_for_claimed_session_fn or final_phase_for_claimed_session
        ),
        persist_claimed_session_completion_fn=(
            persist_claimed_session_completion_fn or persist_claimed_session_completion
        ),
        post_claimed_session_result_comment_fn=(
            post_claimed_session_result_comment_fn
            or post_claimed_session_result_comment
        ),
        maybe_escalate_claimed_session_failure_fn=(
            maybe_escalate_claimed_session_failure_fn
            or maybe_escalate_claimed_session_failure
        ),
    )


def prepared_cycle_deps(
    *,
    status_resolver: StatusResolverFn | None,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    resolve_launch_context_for_cycle_fn: (
        Callable[..., tuple[PreparedLaunchContext | None, CycleResult | None]] | None
    ) = None,
    claim_launch_context_fn: (
        Callable[..., tuple[ClaimedSessionContext | None, CycleResult | None]] | None
    ) = None,
    execute_claimed_session_fn: Callable[..., SessionExecutionOutcome] | None = None,
    finalize_claimed_session_fn: Callable[..., CycleResult] | None = None,
) -> PreparedCycleDeps:
    """Bind compatibility seams at the outer wiring boundary."""
    effective_resolve_launch_context_for_cycle = (
        resolve_launch_context_for_cycle_fn or resolve_launch_context_for_cycle
    )
    effective_claim_launch_context = claim_launch_context_fn or claim_launch_context
    effective_execute_claimed_session = (
        execute_claimed_session_fn or execute_claimed_session
    )
    effective_finalize_claimed_session = (
        finalize_claimed_session_fn or finalize_claimed_session
    )

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
        subprocess_runner: SubprocessRunnerFn | None,
        gh_runner: GitHubRunnerFn | None,
    ) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
        return effective_resolve_launch_context_for_cycle(
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
        gh_runner: GitHubRunnerFn | None,
    ) -> tuple[ClaimedSessionContext | None, CycleResult | None]:
        return effective_claim_launch_context(
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
        return effective_execute_claimed_session(
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
        gh_runner: GitHubRunnerFn | None,
    ) -> CycleResult:
        return effective_finalize_claimed_session(
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
