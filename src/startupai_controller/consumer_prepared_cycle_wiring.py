"""Prepared-cycle lifecycle shell composing launch/claim and claimed-session wiring."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
import subprocess

import startupai_controller.consumer_claimed_session_wiring as _claimed_session_wiring
import startupai_controller.consumer_cycle_wiring as _cycle_wiring
import startupai_controller.consumer_launch_claim_wiring as _launch_claim_wiring
from startupai_controller.application.consumer.cycle import PreparedCycleDeps
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    PreparedCycleContext,
    PreparedLaunchContext,
    PrCreationOutcome,
    SessionExecutionOutcome,
)
from startupai_controller.domain.models import (
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

SubprocessRunnerFn = _launch_claim_wiring.SubprocessRunnerFn
block_prelaunch_issue = _launch_claim_wiring.block_prelaunch_issue
select_launch_candidate_for_cycle = (
    _launch_claim_wiring.select_launch_candidate_for_cycle
)
prepare_selected_launch_candidate = (
    _launch_claim_wiring.prepare_selected_launch_candidate
)
handle_selected_launch_query_error = (
    _launch_claim_wiring.handle_selected_launch_query_error
)
handle_selected_launch_workflow_config_error = (
    _launch_claim_wiring.handle_selected_launch_workflow_config_error
)
handle_selected_launch_worktree_error = (
    _launch_claim_wiring.handle_selected_launch_worktree_error
)
handle_selected_launch_runtime_error = (
    _launch_claim_wiring.handle_selected_launch_runtime_error
)
resolve_launch_context_for_cycle = _launch_claim_wiring.resolve_launch_context_for_cycle
open_pending_claim_session = _launch_claim_wiring.open_pending_claim_session
enforce_claim_retry_ceiling = _launch_claim_wiring.enforce_claim_retry_ceiling
attempt_launch_context_claim = _launch_claim_wiring.attempt_launch_context_claim
claim_launch_ready_issue = _launch_claim_wiring.claim_launch_ready_issue
handle_launch_claim_api_failure = _launch_claim_wiring.handle_launch_claim_api_failure
handle_launch_claim_unexpected_failure = (
    _launch_claim_wiring.handle_launch_claim_unexpected_failure
)
handle_launch_claim_rejection = _launch_claim_wiring.handle_launch_claim_rejection
mark_claimed_session_running = _launch_claim_wiring.mark_claimed_session_running
claim_launch_context = _launch_claim_wiring.claim_launch_context
claim_suppression_state: Callable[..., dict[str, str] | None] = (
    _launch_claim_wiring.claim_suppression_state
)
next_available_slot: Callable[..., int | None] = (
    _launch_claim_wiring.next_available_slot
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
        claim_suppression_state=claim_suppression_state,
        next_available_slot=next_available_slot,
        resolve_launch_context_for_cycle=_resolve_launch_context_for_cycle,
        claim_launch_context=_claim_launch_context,
        execute_claimed_session=_execute_claimed_session,
        finalize_claimed_session=_finalize_claimed_session,
    )
