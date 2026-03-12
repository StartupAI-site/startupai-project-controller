"""Claim and selected-launch wiring extracted from board_consumer."""

from __future__ import annotations

import logging
import subprocess
from collections.abc import Callable
from typing import Protocol

import startupai_controller.consumer_claim_helpers as _claim_helpers
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    PendingClaimContext,
    PreparedCycleContext,
    PreparedLaunchContext,
    SelectedLaunchCandidate,
    WorktreePrepareError,
)
from startupai_controller.consumer_workflow import WorkflowConfigError
from startupai_controller.domain.models import ClaimReadyResult, CycleResult
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.ready_flow import (
    BoardInfoResolverFn,
    BoardStatusMutatorFn,
    CommentCheckerFn,
    CommentPosterFn,
    GitHubRunnerFn,
    StatusResolverFn,
)
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

SubprocessRunnerFn = Callable[..., subprocess.CompletedProcess[str]]


class CompleteSessionFn(Protocol):
    """Persist a terminal session update for a claim/launch failure path."""

    def __call__(
        self,
        db: ConsumerRuntimeStatePort,
        session_id: str,
        issue_ref: str,
        *,
        status: str,
        failure_reason: str | None = None,
        completed_at: str | None = None,
        **fields: object,
    ) -> None: ...


class RecordMetricFn(Protocol):
    """Persist an SLO/event metric for one issue lifecycle event."""

    def __call__(
        self,
        db: ConsumerRuntimeStatePort,
        config: ConsumerConfig,
        event_type: str,
        *,
        issue_ref: str | None = None,
        payload: dict[str, object] | None = None,
    ) -> None: ...


class MaybeActivateClaimSuppressionFn(Protocol):
    """Activate rate-limit suppression when GitHub rejects claim attempts."""

    def __call__(
        self,
        db: ConsumerRuntimeStatePort,
        config: ConsumerConfig,
        *,
        scope: str,
        error: Exception,
    ) -> bool: ...


class PrepareLaunchCandidateFn(Protocol):
    """Prepare launch-ready local state for the selected issue."""

    def __call__(
        self,
        issue_ref: str,
        *,
        config: ConsumerConfig,
        prepared: PreparedCycleContext,
        db: ConsumerRuntimeStatePort,
        subprocess_runner: SubprocessRunnerFn | None = None,
        status_resolver: StatusResolverFn | None = None,
        board_info_resolver: BoardInfoResolverFn | None = None,
        board_mutator: BoardStatusMutatorFn | None = None,
        gh_runner: GitHubRunnerFn | None = None,
        pr_port: PullRequestPort | None = None,
    ) -> PreparedLaunchContext: ...


class BlockPrelaunchIssueFn(Protocol):
    """Block an issue before claim when preparation reveals invalid launch state."""

    def __call__(
        self,
        issue_ref: str,
        blocked_reason: str,
        *,
        config: ConsumerConfig,
        cp_config: CriticalPathConfig,
        db: ConsumerRuntimeStatePort,
        board_info_resolver: BoardInfoResolverFn | None = None,
        board_mutator: BoardStatusMutatorFn | None = None,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> None: ...


class EscalateToClaudeFn(Protocol):
    """Escalate a repeatedly failing issue to Claude for intervention."""

    def __call__(
        self,
        issue_ref: str,
        config: CriticalPathConfig,
        project_owner: str,
        project_number: int,
        *,
        reason: str,
        board_info_resolver: BoardInfoResolverFn | None = None,
        comment_checker: CommentCheckerFn | None = None,
        comment_poster: CommentPosterFn | None = None,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> None: ...


class PostConsumerClaimCommentFn(Protocol):
    """Post the consumer claim marker once the durable session is running."""

    def __call__(
        self,
        issue_ref: str,
        session_id: str,
        repo_prefix: str,
        branch_name: str,
        executor: str,
        config: CriticalPathConfig,
        *,
        comment_checker: Callable[..., bool] | None = None,
        comment_poster: Callable[..., None] | None = None,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> None: ...


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
    pr_port: PullRequestPort | None,
    record_metric: RecordMetricFn,
    prepare_launch_candidate: PrepareLaunchCandidateFn,
    handle_selected_launch_query_error: Callable[..., tuple[None, CycleResult]],
    handle_selected_launch_workflow_config_error: Callable[
        ...,
        tuple[None, CycleResult],
    ],
    handle_selected_launch_worktree_error: Callable[..., tuple[None, CycleResult]],
    handle_selected_launch_runtime_error: Callable[..., tuple[None, CycleResult]],
    workflow_config_error_type: type[WorkflowConfigError],
    worktree_prepare_error_type: type[WorktreePrepareError],
    gh_query_error_type: type[Exception],
) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
    """Prepare the selected candidate into launch-ready local context."""
    record_metric(
        db,
        config,
        "candidate_selected",
        issue_ref=selected_candidate.issue_ref,
    )
    return _claim_helpers.prepare_selected_launch_candidate(
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
        prepare_launch_candidate=prepare_launch_candidate,
        handle_selected_launch_query_error=handle_selected_launch_query_error,
        handle_selected_launch_workflow_config_error=handle_selected_launch_workflow_config_error,
        handle_selected_launch_worktree_error=handle_selected_launch_worktree_error,
        handle_selected_launch_runtime_error=handle_selected_launch_runtime_error,
        workflow_config_error_type=workflow_config_error_type,
        worktree_prepare_error_type=worktree_prepare_error_type,
        gh_query_error_type=gh_query_error_type,
    )


def handle_selected_launch_query_error(
    *,
    candidate: str,
    err: Exception,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    record_metric: RecordMetricFn,
    maybe_activate_claim_suppression: MaybeActivateClaimSuppressionFn,
    mark_degraded: Callable[..., None],
    gh_reason_code: Callable[..., str],
    cycle_result_factory: type[CycleResult],
) -> tuple[None, CycleResult]:
    """Handle GitHub/query failures during selected launch preparation."""
    return _claim_helpers.handle_selected_launch_query_error(
        candidate=candidate,
        err=err,
        config=config,
        db=db,
        record_metric=record_metric,
        maybe_activate_claim_suppression=maybe_activate_claim_suppression,
        mark_degraded=mark_degraded,
        gh_reason_code=gh_reason_code,
        cycle_result_factory=cycle_result_factory,
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
    block_prelaunch_issue: BlockPrelaunchIssueFn,
    record_metric: RecordMetricFn,
    cycle_result_factory: type[CycleResult],
) -> tuple[None, CycleResult]:
    """Handle invalid workflow configuration during launch preparation."""
    return _claim_helpers.handle_selected_launch_workflow_config_error(
        candidate=candidate,
        err=err,
        config=config,
        db=db,
        cp_config=cp_config,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        block_prelaunch_issue=block_prelaunch_issue,
        record_metric=record_metric,
        cycle_result_factory=cycle_result_factory,
    )


def handle_selected_launch_worktree_error(
    *,
    candidate: str,
    err: Exception,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    record_metric: RecordMetricFn,
    cycle_result_factory: type[CycleResult],
) -> tuple[None, CycleResult]:
    """Handle worktree preparation failures for a selected launch candidate."""
    return _claim_helpers.handle_selected_launch_worktree_error(
        candidate=candidate,
        err=err,
        config=config,
        db=db,
        record_metric=record_metric,
        cycle_result_factory=cycle_result_factory,
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
    block_prelaunch_issue: BlockPrelaunchIssueFn,
    record_metric: RecordMetricFn,
    cycle_result_factory: type[CycleResult],
) -> tuple[None, CycleResult]:
    """Handle workflow-hook runtime failures during launch preparation."""
    return _claim_helpers.handle_selected_launch_runtime_error(
        candidate=candidate,
        err=err,
        config=config,
        db=db,
        cp_config=cp_config,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        block_prelaunch_issue=block_prelaunch_issue,
        record_metric=record_metric,
        cycle_result_factory=cycle_result_factory,
    )


def open_pending_claim_session(
    *,
    db: ConsumerRuntimeStatePort,
    launch_context: PreparedLaunchContext,
    executor: str,
    slot_id: int,
    complete_session: CompleteSessionFn,
    pending_claim_context_factory: type[PendingClaimContext],
    cycle_result_factory: type[CycleResult],
) -> tuple[PendingClaimContext | None, CycleResult | None]:
    """Create the session record and acquire the lease for a launch-ready issue."""
    return _claim_helpers.open_pending_claim_session(
        db=db,
        launch_context=launch_context,
        executor=executor,
        slot_id=slot_id,
        complete_session=complete_session,
        pending_claim_context_factory=pending_claim_context_factory,
        cycle_result_factory=cycle_result_factory,
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
    complete_session: CompleteSessionFn,
    escalate_to_claude: EscalateToClaudeFn,
    cycle_result_factory: type[CycleResult],
    logger: logging.Logger,
) -> CycleResult | None:
    """Abort and escalate if the issue exhausted its retry ceiling."""
    return _claim_helpers.enforce_claim_retry_ceiling(
        config=config,
        db=db,
        launch_context=launch_context,
        pending_claim=pending_claim,
        cp_config=cp_config,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        complete_session=complete_session,
        escalate_to_claude=escalate_to_claude,
        cycle_result_factory=cycle_result_factory,
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
    claim_launch_ready_issue: Callable[..., ClaimReadyResult],
    handle_launch_claim_api_failure: Callable[..., tuple[None, CycleResult]],
    handle_launch_claim_unexpected_failure: Callable[..., tuple[None, CycleResult]],
    handle_launch_claim_rejection: Callable[..., tuple[None, CycleResult]],
    record_metric: RecordMetricFn,
    gh_query_error_type: type[Exception],
) -> tuple[ClaimReadyResult | None, CycleResult | None]:
    """Claim board ownership for a launch-ready issue."""
    return _claim_helpers.attempt_launch_context_claim(
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
        record_metric=record_metric,
        gh_query_error_type=gh_query_error_type,
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
    claim_ready_issue: Callable[..., ClaimReadyResult],
) -> ClaimReadyResult:
    """Execute the actual board claim for a launch-ready issue."""
    return claim_ready_issue(
        prepared.cp_config,
        config.project_owner,
        config.project_number,
        ready_flow_port=prepared.ready_flow_port,
        executor=config.executor,
        issue_ref=candidate,
        all_prefixes=True,
        automation_config=prepared.auto_config,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        status_resolver=status_resolver,
    )


def handle_launch_claim_api_failure(
    candidate: str,
    err: Exception,
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    pending_claim: PendingClaimContext,
    maybe_activate_claim_suppression: MaybeActivateClaimSuppressionFn,
    mark_degraded: Callable[..., None],
    gh_reason_code: Callable[..., str],
    complete_session: CompleteSessionFn,
    record_metric: RecordMetricFn,
    cycle_result_factory: type[CycleResult],
    logger: logging.Logger,
) -> tuple[None, CycleResult]:
    """Handle a GitHub/API failure while claiming a launch-ready issue."""
    return _claim_helpers.handle_launch_claim_api_failure(
        candidate,
        err,
        config=config,
        db=db,
        pending_claim=pending_claim,
        maybe_activate_claim_suppression=maybe_activate_claim_suppression,
        mark_degraded=mark_degraded,
        gh_reason_code=gh_reason_code,
        complete_session=complete_session,
        record_metric=record_metric,
        cycle_result_factory=cycle_result_factory,
        logger=logger,
    )


def handle_launch_claim_unexpected_failure(
    candidate: str,
    err: Exception,
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    pending_claim: PendingClaimContext,
    complete_session: CompleteSessionFn,
    record_metric: RecordMetricFn,
    cycle_result_factory: type[CycleResult],
    logger: logging.Logger,
) -> tuple[None, CycleResult]:
    """Handle an unexpected local failure while claiming a launch-ready issue."""
    return _claim_helpers.handle_launch_claim_unexpected_failure(
        candidate,
        err,
        config=config,
        db=db,
        pending_claim=pending_claim,
        complete_session=complete_session,
        record_metric=record_metric,
        cycle_result_factory=cycle_result_factory,
        logger=logger,
    )


def handle_launch_claim_rejection(
    candidate: str,
    claim_result: ClaimReadyResult,
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    pending_claim: PendingClaimContext,
    complete_session: CompleteSessionFn,
    record_metric: RecordMetricFn,
    cycle_result_factory: type[CycleResult],
) -> tuple[None, CycleResult]:
    """Handle a non-exception claim rejection for a launch-ready issue."""
    return _claim_helpers.handle_launch_claim_rejection(
        candidate,
        claim_result,
        config=config,
        db=db,
        pending_claim=pending_claim,
        complete_session=complete_session,
        record_metric=record_metric,
        cycle_result_factory=cycle_result_factory,
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
    record_successful_github_mutation: Callable[..., None],
    record_metric: RecordMetricFn,
    post_consumer_claim_comment: PostConsumerClaimCommentFn,
    claimed_session_context_factory: type[ClaimedSessionContext],
    logger: logging.Logger,
) -> ClaimedSessionContext:
    """Persist the durable-start state and post the claim marker."""
    return _claim_helpers.mark_claimed_session_running(
        config=config,
        db=db,
        launch_context=launch_context,
        pending_claim=pending_claim,
        slot_id=slot_id,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        cp_config=cp_config,
        gh_runner=gh_runner,
        record_successful_github_mutation=record_successful_github_mutation,
        record_metric=record_metric,
        post_consumer_claim_comment=post_consumer_claim_comment,
        claimed_session_context_factory=claimed_session_context_factory,
        logger=logger,
    )
