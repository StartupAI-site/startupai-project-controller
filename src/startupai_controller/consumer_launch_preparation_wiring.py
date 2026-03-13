"""Launch-preparation wiring extracted from consumer_launch_claim_wiring."""

from __future__ import annotations

from collections.abc import Callable
import logging
import subprocess
from typing import cast

import startupai_controller.consumer_automation_bridge as _automation_bridge
import startupai_controller.consumer_cycle_wiring as _cycle_wiring
import startupai_controller.consumer_execution_outcome_wiring as _execution_outcome_wiring
import startupai_controller.consumer_selection_retry_wiring as _selection_retry_wiring
import startupai_controller.consumer_support_wiring as _support_wiring
from startupai_controller.consumer_claim_wiring import (
    MaybeActivateClaimSuppressionFn,
    RecordMetricFn,
)
import startupai_controller.consumer_claim_wiring as _claim_wiring
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    PreparedCycleContext,
    PreparedLaunchContext,
    SelectedLaunchCandidate,
    WorktreePrepareError,
)
from startupai_controller.consumer_workflow import WorkflowConfigError
from startupai_controller.control_plane_runtime import (
    _mark_degraded,
    _record_successful_github_mutation,
)
from startupai_controller.domain.models import CycleResult
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.ready_flow import (
    BoardInfoResolverFn,
    BoardStatusMutatorFn,
    GitHubRunnerFn,
    StatusResolverFn,
)
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.runtime.wiring import gh_reason_code
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
    parse_issue_ref,
)

logger = logging.getLogger("board-consumer")

SubprocessRunnerFn = Callable[..., subprocess.CompletedProcess[str]]
_record_metric = cast(RecordMetricFn, _support_wiring.record_metric)


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
            MaybeActivateClaimSuppressionFn,
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
