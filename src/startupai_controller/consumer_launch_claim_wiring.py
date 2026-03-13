"""Launch/claim lifecycle wiring extracted from consumer_prepared_cycle_wiring."""

from __future__ import annotations

from collections.abc import Callable
import logging
from typing import cast

import startupai_controller.consumer_automation_bridge as _automation_bridge
import startupai_controller.consumer_board_state_helpers as _board_state_helpers
import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
import startupai_controller.consumer_launch_preparation_wiring as _launch_preparation_wiring
import startupai_controller.consumer_support_wiring as _support_wiring
import startupai_controller.consumer_cycle_wiring as _cycle_wiring
import startupai_controller.consumer_claim_wiring as _claim_wiring
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    PendingClaimContext,
    PreparedCycleContext,
    PreparedLaunchContext,
)
from startupai_controller.control_plane_runtime import (
    _mark_degraded,
    _record_successful_github_mutation,
)
from startupai_controller.domain.models import ClaimReadyResult, CycleResult
from startupai_controller.ports.board_mutations import BoardMutationPort
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
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.runtime.wiring import gh_reason_code
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
)

logger = logging.getLogger("board-consumer")

SubprocessRunnerFn = _launch_preparation_wiring.SubprocessRunnerFn
block_prelaunch_issue = _launch_preparation_wiring.block_prelaunch_issue
select_launch_candidate_for_cycle = (
    _launch_preparation_wiring.select_launch_candidate_for_cycle
)
prepare_selected_launch_candidate = (
    _launch_preparation_wiring.prepare_selected_launch_candidate
)
handle_selected_launch_query_error = (
    _launch_preparation_wiring.handle_selected_launch_query_error
)
handle_selected_launch_workflow_config_error = (
    _launch_preparation_wiring.handle_selected_launch_workflow_config_error
)
handle_selected_launch_worktree_error = (
    _launch_preparation_wiring.handle_selected_launch_worktree_error
)
handle_selected_launch_runtime_error = (
    _launch_preparation_wiring.handle_selected_launch_runtime_error
)
resolve_launch_context_for_cycle = (
    _launch_preparation_wiring.resolve_launch_context_for_cycle
)
claim_suppression_state: Callable[..., dict[str, str] | None] = (
    _support_wiring.claim_suppression_state
)
next_available_slot: Callable[..., int | None] = _support_wiring.next_available_slot
_record_metric = cast(_claim_wiring.RecordMetricFn, _support_wiring.record_metric)


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
