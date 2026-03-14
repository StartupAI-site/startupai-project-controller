"""Session execution and review-handoff wiring extracted from the consumer shell."""

from __future__ import annotations

import subprocess
from typing import Callable, cast

import startupai_controller.consumer_automation_bridge as _automation_bridge
import startupai_controller.consumer_board_state_helpers as _board_state_helpers
import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
import startupai_controller.consumer_execution_outcome_wiring as _execution_outcome_wiring
import startupai_controller.consumer_execution_support_helpers as _execution_support_helpers
import startupai_controller.consumer_resolution_helpers as _resolution_helpers
import startupai_controller.consumer_review_handoff_helpers as _review_handoff_helpers
import startupai_controller.consumer_review_queue_helpers as _review_queue_helpers
import startupai_controller.consumer_runtime_support_wiring as _runtime_support_wiring
import startupai_controller.consumer_session_completion_helpers as _session_completion_helpers
import startupai_controller.consumer_support_wiring as _support_wiring
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.control_plane_runtime import (
    _mark_degraded,
    _record_successful_github_mutation,
)
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    CodexSessionResult,
    PrCreationOutcome,
    PreparedCycleContext,
    PreparedLaunchContext,
    SessionExecutionOutcome,
)
from startupai_controller.domain.models import (
    ResolutionEvaluation,
    ReviewQueueDrainSummary,
    ReviewQueueEntry,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.process_runner import GhRunnerPort
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
from startupai_controller.runtime.wiring import (
    GitHubRuntimeMemo as CycleGitHubMemo,
    build_github_port_bundle,
    gh_reason_code,
)
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

CodexResultPayload = CodexSessionResult
SubprocessRunnerFn = Callable[..., subprocess.CompletedProcess[str]]


def _queue_status_transition(
    db: ConsumerRuntimeStatePort,
    issue_ref: str,
    *,
    to_status: str,
    from_statuses: set[str],
    blocked_reason: str | None = None,
) -> None:
    """Adapt the deferred status queue helper to the runtime-state protocol."""
    _support_wiring.queue_status_transition(
        cast(_runtime_support_wiring.DeferredActionStorePort, db),
        issue_ref,
        to_status=to_status,
        from_statuses=from_statuses,
        blocked_reason=blocked_reason,
    )


def _queue_verdict_marker(
    db: ConsumerRuntimeStatePort,
    pr_url: str,
    session_id: str,
) -> None:
    """Adapt the deferred verdict helper to the runtime-state protocol."""
    _support_wiring.queue_verdict_marker(
        cast(_runtime_support_wiring.DeferredActionStorePort, db),
        pr_url,
        session_id,
    )


def create_pr_for_execution_result(
    *,
    config: ConsumerConfig,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    codex_result: CodexResultPayload | None,
    session_status: str,
    failure_reason: str | None,
    subprocess_runner: SubprocessRunnerFn | None,
    gh_runner: GitHubRunnerFn | None,
    logger,
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
        validate_pr_head_eligibility=_execution_support_helpers.validate_pr_head_eligibility,
        create_or_update_pr=_codex_comment_wiring.create_or_update_pr,
        pr_creation_outcome_factory=PrCreationOutcome,
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
    logger,
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
        queue_status_transition=_queue_status_transition,
        logger=logger,
    )


def post_claimed_session_verdict_marker(
    *,
    db: ConsumerRuntimeStatePort,
    pr_url: str,
    session_id: str,
    gh_runner: GitHubRunnerFn | None,
    logger,
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
        queue_verdict_marker=_queue_verdict_marker,
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
    logger,
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
    codex_result: CodexResultPayload | None,
    has_commits: bool,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    comment_poster: CommentPosterFn | None,
    subprocess_runner: SubprocessRunnerFn | None,
    gh_runner: GitHubRunnerFn | GhRunnerPort | None,
    pr_port: PullRequestPort | None = None,
    board_port: BoardMutationPort | None = None,
    failure_reason: str | None = None,
    logger,
) -> tuple[str, ResolutionEvaluation | None, str | None]:
    """Handle non-review outcomes for a claimed session."""
    return _execution_outcome_wiring.handle_non_review_execution_outcome(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        session_id=session_id,
        session_status=session_status,
        failure_reason=failure_reason,
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
    codex_result: CodexResultPayload | None,
    cp_config: CriticalPathConfig,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | None,
    logger,
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
    codex_result: CodexResultPayload | None,
    cp_config: CriticalPathConfig,
    board_info_resolver: BoardInfoResolverFn | None,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | None,
    logger,
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
