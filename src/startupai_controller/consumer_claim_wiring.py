"""Claim and selected-launch wiring extracted from board_consumer."""

from __future__ import annotations

from typing import Any, Callable

import startupai_controller.consumer_claim_helpers as _claim_helpers


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
    pr_port: Any | None,
    record_metric: Callable[..., None],
    prepare_launch_candidate: Callable[..., Any],
    handle_selected_launch_query_error: Callable[..., tuple[None, Any]],
    handle_selected_launch_workflow_config_error: Callable[..., tuple[None, Any]],
    handle_selected_launch_worktree_error: Callable[..., tuple[None, Any]],
    handle_selected_launch_runtime_error: Callable[..., tuple[None, Any]],
    workflow_config_error_type: type[Exception],
    worktree_prepare_error_type: type[Exception],
    gh_query_error_type: type[Exception],
) -> tuple[Any | None, Any | None]:
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
    config: Any,
    db: Any,
    record_metric: Callable[..., None],
    maybe_activate_claim_suppression: Callable[..., None],
    mark_degraded: Callable[..., None],
    gh_reason_code: Callable[..., str],
    cycle_result_factory: Callable[..., Any],
) -> tuple[None, Any]:
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
    config: Any,
    db: Any,
    cp_config: Any,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    block_prelaunch_issue: Callable[..., None],
    record_metric: Callable[..., None],
    cycle_result_factory: Callable[..., Any],
) -> tuple[None, Any]:
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
    config: Any,
    db: Any,
    record_metric: Callable[..., None],
    cycle_result_factory: Callable[..., Any],
) -> tuple[None, Any]:
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
    err: Exception,
    config: Any,
    db: Any,
    cp_config: Any,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    block_prelaunch_issue: Callable[..., None],
    record_metric: Callable[..., None],
    cycle_result_factory: Callable[..., Any],
) -> tuple[None, Any]:
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
    db: Any,
    launch_context: Any,
    executor: str,
    slot_id: int,
    complete_session: Callable[..., None],
    pending_claim_context_factory: Callable[..., Any],
    cycle_result_factory: Callable[..., Any],
) -> tuple[Any | None, Any | None]:
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
    config: Any,
    db: Any,
    launch_context: Any,
    pending_claim: Any,
    cp_config: Any,
    board_info_resolver: Callable[..., Any] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    complete_session: Callable[..., None],
    escalate_to_claude: Callable[..., None],
    cycle_result_factory: Callable[..., Any],
    logger: Any,
) -> Any | None:
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
    claim_launch_ready_issue: Callable[..., Any],
    handle_launch_claim_api_failure: Callable[..., tuple[None, Any]],
    handle_launch_claim_unexpected_failure: Callable[..., tuple[None, Any]],
    handle_launch_claim_rejection: Callable[..., tuple[None, Any]],
    record_metric: Callable[..., None],
    gh_query_error_type: type[Exception],
) -> tuple[Any | None, Any | None]:
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
    config: Any,
    prepared: Any,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    claim_ready_issue: Callable[..., Any],
) -> Any:
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
    config: Any,
    db: Any,
    pending_claim: Any,
    maybe_activate_claim_suppression: Callable[..., None],
    mark_degraded: Callable[..., None],
    gh_reason_code: Callable[..., str],
    complete_session: Callable[..., None],
    record_metric: Callable[..., None],
    cycle_result_factory: Callable[..., Any],
    logger: Any,
) -> tuple[None, Any]:
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
    config: Any,
    db: Any,
    pending_claim: Any,
    complete_session: Callable[..., None],
    record_metric: Callable[..., None],
    cycle_result_factory: Callable[..., Any],
    logger: Any,
) -> tuple[None, Any]:
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
    claim_result: Any,
    *,
    config: Any,
    db: Any,
    pending_claim: Any,
    complete_session: Callable[..., None],
    record_metric: Callable[..., None],
    cycle_result_factory: Callable[..., Any],
) -> tuple[None, Any]:
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
    config: Any,
    db: Any,
    launch_context: Any,
    pending_claim: Any,
    slot_id: int,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    cp_config: Any,
    gh_runner: Callable[..., str] | None,
    record_successful_github_mutation: Callable[..., None],
    record_metric: Callable[..., None],
    post_consumer_claim_comment: Callable[..., None],
    claimed_session_context_factory: Callable[..., Any],
    logger: Any,
) -> Any:
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
