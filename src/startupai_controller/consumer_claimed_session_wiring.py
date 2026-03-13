"""Claimed-session execution and finalization wiring extracted from prepared-cycle wiring."""

from __future__ import annotations

import logging
from pathlib import Path
import subprocess
from collections.abc import Callable
from typing import cast

import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
import startupai_controller.consumer_execution_outcome_wiring as _execution_outcome_wiring
import startupai_controller.consumer_session_completion_helpers as _session_completion_helpers
import startupai_controller.consumer_session_execution_wiring as _session_execution_wiring
import startupai_controller.consumer_support_wiring as _support_wiring
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    PrCreationOutcome,
    PreparedCycleContext,
    PreparedLaunchContext,
    SessionExecutionOutcome,
)
from startupai_controller.domain.models import (
    CycleResult,
    ResolutionEvaluation,
    ReviewQueueDrainSummary,
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
)
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.runtime.wiring import ConsumerDB, build_session_store
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

logger = logging.getLogger("board-consumer")

SubprocessRunnerFn = Callable[..., subprocess.CompletedProcess[str]]


def create_pr_for_execution_result(
    *,
    config: ConsumerConfig,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    codex_result: _session_execution_wiring.CodexResultPayload | None,
    session_status: str,
    failure_reason: str | None,
    subprocess_runner: SubprocessRunnerFn | None,
    gh_runner: GitHubRunnerFn | None,
) -> PrCreationOutcome:
    """Reuse or create a PR from claimed-session output."""
    return _session_execution_wiring.create_pr_for_execution_result(
        config=config,
        launch_context=launch_context,
        claimed_context=claimed_context,
        codex_result=codex_result,
        session_status=session_status,
        failure_reason=failure_reason,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        logger=logger,
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
    _session_execution_wiring.transition_claimed_session_to_review(
        db=db,
        issue_ref=issue_ref,
        session_id=session_id,
        config=config,
        critical_path_config=critical_path_config,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
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
    _session_execution_wiring.post_claimed_session_verdict_marker(
        db=db,
        pr_url=pr_url,
        session_id=session_id,
        gh_runner=gh_runner,
        logger=logger,
    )


def queue_claimed_session_for_review(
    *,
    store: SessionStorePort,
    issue_ref: str,
    pr_url: str,
    session_id: str,
) -> ReviewQueueEntry | None:
    """Queue a claimed session for later review handoff processing."""
    return _session_execution_wiring.queue_claimed_session_for_review(
        store=store,
        issue_ref=issue_ref,
        pr_url=pr_url,
        session_id=session_id,
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
    return _session_execution_wiring.run_immediate_review_handoff(
        config=config,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        store=store,
        queue_entry=queue_entry,
        gh_runner=gh_runner,
        db=db,
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
    codex_result: _session_execution_wiring.CodexResultPayload | None,
    has_commits: bool,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    comment_poster: CommentPosterFn | None,
    subprocess_runner: SubprocessRunnerFn | None,
    gh_runner: GitHubRunnerFn | GhRunnerPort | None,
    pr_port: PullRequestPort | None = None,
    board_port: BoardMutationPort | None = None,
) -> tuple[str, ResolutionEvaluation | None, str | None]:
    """Handle non-review outcomes for a claimed session."""
    return _session_execution_wiring.handle_non_review_execution_outcome(
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
        pr_port=pr_port,
        board_port=board_port,
        logger=logger,
    )


def final_phase_for_claimed_session(
    *,
    launch_context: PreparedLaunchContext,
    execution_outcome: SessionExecutionOutcome,
) -> str:
    """Determine the final durable phase for a claimed session outcome."""
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
    codex_result: _session_execution_wiring.CodexResultPayload | None,
    cp_config: CriticalPathConfig,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | None,
) -> None:
    """Post the session result comment when Codex produced structured output."""
    _session_execution_wiring.post_claimed_session_result_comment(
        issue_ref=issue_ref,
        session_id=session_id,
        codex_result=codex_result,
        cp_config=cp_config,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        logger=logger,
    )


def maybe_escalate_claimed_session_failure(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    issue_ref: str,
    effective_max_retries: int,
    session_status: str,
    codex_result: _session_execution_wiring.CodexResultPayload | None,
    cp_config: CriticalPathConfig,
    board_info_resolver: BoardInfoResolverFn | None,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | None,
) -> None:
    """Escalate terminal failed/timeout sessions once retry ceiling is reached."""
    _session_execution_wiring.maybe_escalate_claimed_session_failure(
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
    effective_create_pr_for_execution_result = (
        create_pr_for_execution_result_fn or create_pr_for_execution_result
    )
    effective_handoff_execution_to_review = (
        handoff_execution_to_review_fn or handoff_execution_to_review
    )
    effective_handle_non_review_execution_outcome = (
        handle_non_review_execution_outcome_fn or handle_non_review_execution_outcome
    )
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
        create_pr_for_execution_result=effective_create_pr_for_execution_result,
        handoff_execution_to_review=effective_handoff_execution_to_review,
        handle_non_review_execution_outcome=effective_handle_non_review_execution_outcome,
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
    effective_final_phase_for_claimed_session = (
        final_phase_for_claimed_session_fn or final_phase_for_claimed_session
    )
    effective_persist_claimed_session_completion = (
        persist_claimed_session_completion_fn or persist_claimed_session_completion
    )
    effective_post_claimed_session_result_comment = (
        post_claimed_session_result_comment_fn or post_claimed_session_result_comment
    )
    effective_maybe_escalate_claimed_session_failure = (
        maybe_escalate_claimed_session_failure_fn
        or maybe_escalate_claimed_session_failure
    )
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
        final_phase_for_claimed_session=effective_final_phase_for_claimed_session,
        persist_claimed_session_completion=effective_persist_claimed_session_completion,
        post_claimed_session_result_comment=effective_post_claimed_session_result_comment,
        maybe_escalate_claimed_session_failure=effective_maybe_escalate_claimed_session_failure,
    )
