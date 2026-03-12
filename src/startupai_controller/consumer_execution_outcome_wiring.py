"""Execution outcome and review handoff wiring extracted from board_consumer."""

from __future__ import annotations

from typing import Any, Callable

import startupai_controller.consumer_cycle_wiring as _cycle_wiring
from startupai_controller.consumer_review_handoff_helpers import (
    post_claimed_session_verdict_marker as _post_claimed_session_verdict_marker_helper,
    queue_claimed_session_for_review as _queue_claimed_session_for_review_helper,
    run_immediate_review_handoff as _run_immediate_review_handoff_helper,
    transition_claimed_session_to_review as _transition_claimed_session_to_review_helper,
)
import startupai_controller.consumer_session_completion_helpers as _session_completion_helpers


def block_prelaunch_issue(
    issue_ref: str,
    blocked_reason: str,
    *,
    config: Any,
    cp_config: Any,
    db: Any,
    gh_query_error_type: type[Exception],
    set_blocked_with_reason: Callable[..., None],
    record_successful_github_mutation: Callable[..., None],
    mark_degraded: Callable[..., None],
    queue_status_transition: Callable[..., None],
    logger: Any,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Move a launch-unready issue to Blocked before claim."""
    try:
        set_blocked_with_reason(
            issue_ref,
            blocked_reason,
            cp_config,
            config.project_owner,
            config.project_number,
            gh_runner=gh_runner,
        )
        record_successful_github_mutation(db)
    except (gh_query_error_type, Exception) as err:
        logger.error("Prelaunch block failed for %s: %s", issue_ref, err)
        mark_degraded(db, f"prelaunch-block:{err}")
        queue_status_transition(
            db,
            issue_ref,
            to_status="Blocked",
            from_statuses={"Ready"},
            blocked_reason=blocked_reason,
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
    has_commits_on_branch: Callable[..., bool],
    create_or_update_pr: Callable[..., str],
    pr_creation_outcome_factory: Callable[..., Any],
    logger: Any,
) -> Any:
    """Reuse or create a PR from claimed-session output."""
    return _session_completion_helpers.create_pr_for_execution_result(
        config=config,
        launch_context=launch_context,
        claimed_context=claimed_context,
        codex_result=codex_result,
        session_status=session_status,
        failure_reason=failure_reason,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        has_commits_on_branch=has_commits_on_branch,
        create_or_update_pr=create_or_update_pr,
        pr_creation_outcome_factory=pr_creation_outcome_factory,
        logger=logger,
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
    transition_issue_to_review: Callable[..., None],
    record_successful_github_mutation: Callable[..., None],
    mark_degraded: Callable[..., None],
    queue_status_transition: Callable[..., None],
    logger: Any,
) -> None:
    """Move one claimed issue into Review or queue the transition on failure."""
    _transition_claimed_session_to_review_helper(
        db=db,
        issue_ref=issue_ref,
        session_id=session_id,
        config=config,
        critical_path_config=critical_path_config,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        transition_issue_to_review=transition_issue_to_review,
        record_successful_github_mutation=record_successful_github_mutation,
        mark_degraded=mark_degraded,
        queue_status_transition=queue_status_transition,
        log_error=lambda err: logger.error("Review transition failed: %s", err),
    )


def post_claimed_session_verdict_marker(
    *,
    db: Any,
    pr_url: str,
    session_id: str,
    gh_runner: Callable[..., str] | None,
    post_pr_codex_verdict: Callable[..., bool],
    record_successful_github_mutation: Callable[..., None],
    mark_degraded: Callable[..., None],
    queue_verdict_marker: Callable[..., None],
    logger: Any,
) -> None:
    """Post the codex verdict marker for a newly handed-off review PR."""
    _post_claimed_session_verdict_marker_helper(
        db=db,
        pr_url=pr_url,
        session_id=session_id,
        gh_runner=gh_runner,
        post_pr_codex_verdict=post_pr_codex_verdict,
        record_successful_github_mutation=record_successful_github_mutation,
        mark_degraded=mark_degraded,
        queue_verdict_marker=queue_verdict_marker,
        log_error=lambda err: logger.error("PR codex verdict comment failed: %s", err),
    )


def queue_claimed_session_for_review(
    *,
    store: Any,
    issue_ref: str,
    pr_url: str,
    session_id: str,
    queue_review_item: Callable[..., Any | None],
) -> Any | None:
    """Queue one claimed session for immediate review handling."""
    return _queue_claimed_session_for_review_helper(
        store=store,
        issue_ref=issue_ref,
        pr_url=pr_url,
        session_id=session_id,
        queue_review_item=queue_review_item,
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
    build_github_port_bundle: Callable[..., Any],
    github_memo_factory: Callable[[], Any],
    build_review_snapshots_for_queue_entries: Callable[..., dict[tuple[str, int], Any]],
    review_rescue: Callable[..., Any],
    apply_review_queue_result: Callable[..., None],
    mark_degraded: Callable[..., None],
    gh_reason_code: Callable[..., str],
    summary_factory: Callable[..., Any],
    logger: Any,
) -> Any:
    """Run immediate rescue for the just-opened review PR."""
    return _run_immediate_review_handoff_helper(
        config=config,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        store=store,
        queue_entry=queue_entry,
        gh_runner=gh_runner,
        db=db,
        build_github_port_bundle=build_github_port_bundle,
        github_memo_factory=github_memo_factory,
        build_review_snapshots_for_queue_entries=build_review_snapshots_for_queue_entries,
        review_rescue=review_rescue,
        apply_review_queue_result=apply_review_queue_result,
        mark_degraded=mark_degraded,
        gh_reason_code=gh_reason_code,
        summary_factory=summary_factory,
        log_warning=lambda issue_ref, _detail, err: logger.warning(
            "Immediate review clearance failed for %s: %s",
            issue_ref,
            err,
        ),
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
    pr_port: Any | None,
    board_port: Any | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_poster: Callable[..., None] | None,
    subprocess_runner: Callable[..., Any] | None,
    gh_runner: Callable[..., str] | None,
    verify_resolution_payload: Callable[..., Any],
    apply_resolution_action: Callable[..., str | None],
    return_issue_to_ready: Callable[..., None],
    record_successful_github_mutation: Callable[..., None],
    mark_degraded: Callable[..., None],
    queue_status_transition: Callable[..., None],
    record_metric: Callable[..., None],
    logger: Any,
) -> tuple[str, Any | None, str | None]:
    """Handle non-review outcomes for a claimed session."""
    return _cycle_wiring.handle_non_review_execution_outcome(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        session_id=session_id,
        session_status=session_status,
        codex_result=codex_result,
        has_commits=has_commits,
        pr_port=pr_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_poster=comment_poster,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        verify_resolution_payload=verify_resolution_payload,
        apply_resolution_action=apply_resolution_action,
        return_issue_to_ready=return_issue_to_ready,
        record_successful_github_mutation=record_successful_github_mutation,
        mark_degraded=mark_degraded,
        queue_status_transition=queue_status_transition,
        record_metric=record_metric,
        logger=logger,
    )


def final_phase_for_claimed_session(
    *,
    launch_context: Any,
    execution_outcome: Any,
) -> str:
    """Determine the final persisted phase for a claimed session."""
    return _session_completion_helpers.final_phase_for_claimed_session(
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
    complete_session: Callable[..., None],
) -> None:
    """Persist the final session record for a claimed execution outcome."""
    _session_completion_helpers.persist_claimed_session_completion(
        db=db,
        session_id=session_id,
        issue_ref=issue_ref,
        execution_outcome=execution_outcome,
        final_phase=final_phase,
        complete_session=complete_session,
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
    post_result_comment: Callable[..., None],
    logger: Any,
) -> None:
    """Post the session result comment when Codex produced structured output."""
    _session_completion_helpers.post_claimed_session_result_comment(
        issue_ref=issue_ref,
        session_id=session_id,
        codex_result=codex_result,
        cp_config=cp_config,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        post_result_comment=post_result_comment,
        logger=logger,
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
    escalate_to_claude: Callable[..., None],
    logger: Any,
) -> None:
    """Escalate terminal failed/timeout sessions once retry ceiling is reached."""
    _session_completion_helpers.maybe_escalate_claimed_session_failure(
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
        escalate_to_claude=escalate_to_claude,
        logger=logger,
    )


def execute_claimed_session(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    claimed_context: Any,
    process_runner: Any | None,
    file_reader: Callable[..., Any] | None,
    review_state_port: Any | None,
    board_port: Any | None,
    pr_port: Any | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    assemble_codex_prompt: Callable[..., str],
    run_codex_session: Callable[..., int],
    parse_codex_result: Callable[..., dict[str, Any] | None],
    session_status_from_codex_result: Callable[..., tuple[str, str | None]],
    create_pr_for_execution_result: Callable[..., Any],
    handoff_execution_to_review: Callable[..., Any],
    handle_non_review_execution_outcome: Callable[..., tuple[str, Any | None, str | None]],
) -> Any:
    """Execute Codex for a claimed session and apply immediate board handoff."""
    return _cycle_wiring.execute_claimed_session(
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
        assemble_codex_prompt=assemble_codex_prompt,
        run_codex_session=run_codex_session,
        parse_codex_result=parse_codex_result,
        session_status_from_codex_result=session_status_from_codex_result,
        create_pr_for_execution_result=create_pr_for_execution_result,
        handoff_execution_to_review=handoff_execution_to_review,
        handle_non_review_execution_outcome=handle_non_review_execution_outcome,
    )


def finalize_claimed_session(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    claimed_context: Any,
    execution_outcome: Any,
    review_state_port: Any | None,
    board_info_resolver: Callable[..., Any] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    final_phase_for_claimed_session: Callable[..., str],
    persist_claimed_session_completion: Callable[..., None],
    post_claimed_session_result_comment: Callable[..., None],
    maybe_escalate_claimed_session_failure: Callable[..., None],
) -> Any:
    """Persist final session state and return the cycle result."""
    return _cycle_wiring.finalize_claimed_session(
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
        final_phase_for_claimed_session=final_phase_for_claimed_session,
        persist_claimed_session_completion=persist_claimed_session_completion,
        post_claimed_session_result_comment=post_claimed_session_result_comment,
        maybe_escalate_claimed_session_failure=maybe_escalate_claimed_session_failure,
    )
