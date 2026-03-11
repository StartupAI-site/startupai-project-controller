"""Launch and execution wiring extracted from board_consumer."""

from __future__ import annotations

from typing import Any, Callable

from startupai_controller.application.consumer.cycle import PreparedCycleDeps
from startupai_controller.application.consumer.execution import (
    ExecutionDeps,
    FinalizeClaimedSessionDeps,
    NonReviewOutcomeDeps,
    ReviewHandoffDeps,
    execute_claimed_session as _execute_claimed_session_use_case,
    finalize_claimed_session as _finalize_claimed_session_use_case,
    handoff_execution_to_review as _handoff_execution_to_review_use_case,
    handle_non_review_execution_outcome as _handle_non_review_execution_outcome_use_case,
)
from startupai_controller.application.consumer.launch import (
    ClaimLaunchDeps,
    PrepareLaunchDeps,
    ResolveLaunchDeps,
    claim_launch_context as _claim_launch_context_use_case,
    prepare_launch_candidate as _prepare_launch_candidate_use_case,
    resolve_launch_context_for_cycle as _resolve_launch_context_for_cycle_use_case,
)
from startupai_controller.consumer_types import (
    PreparedLaunchContext,
    SessionExecutionOutcome,
)


def _shell_module():
    """Import the consumer shell lazily to avoid import cycles."""
    from startupai_controller import board_consumer

    return board_consumer


def assemble_prepared_launch_context(
    issue_ref: str,
    *,
    candidate_prefix: str,
    owner: str,
    repo: str,
    number: int,
    title: str,
    context: dict[str, Any],
    session_kind: str,
    repair_pr_url: str | None,
    repair_branch_name: str | None,
    worktree_path: str,
    branch_name: str,
    workflow_definition: Any,
    effective_consumer_config: Any,
    dependency_summary: str | None,
    branch_reconcile_state: str | None,
    branch_reconcile_error: str | None,
) -> PreparedLaunchContext:
    """Create the final launch context for a prepared candidate."""
    return PreparedLaunchContext(
        issue_ref=issue_ref,
        repo_prefix=candidate_prefix,
        owner=owner,
        repo=repo,
        number=number,
        title=title,
        issue_context=context,
        session_kind=session_kind,
        repair_pr_url=repair_pr_url,
        repair_branch_name=repair_branch_name,
        worktree_path=worktree_path,
        branch_name=branch_name,
        workflow_definition=workflow_definition,
        effective_consumer_config=effective_consumer_config,
        dependency_summary=dependency_summary,
        branch_reconcile_state=branch_reconcile_state,
        branch_reconcile_error=branch_reconcile_error,
    )


def prepare_launch_candidate(
    issue_ref: str,
    *,
    config: Any,
    prepared: Any,
    db: Any,
    subprocess_runner: Callable[..., Any] | None = None,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    pr_port: Any | None = None,
    issue_context_port: Any | None = None,
    session_store: Any | None = None,
    worktree_port: Any | None = None,
) -> PreparedLaunchContext:
    """Prepare local launch state for an issue before board claim."""
    shell = _shell_module()
    return _prepare_launch_candidate_use_case(
        issue_ref,
        config=config,
        prepared=prepared,
        db=db,
        deps=PrepareLaunchDeps(
            build_session_store=shell.build_session_store,
            build_github_port_bundle=shell.build_github_port_bundle,
            build_worktree_port=shell.build_worktree_port,
            resolve_launch_candidate_metadata=shell._resolve_launch_candidate_metadata,
            resolve_launch_issue_context=shell._resolve_launch_issue_context,
            setup_launch_worktree=shell._setup_launch_worktree,
            resolve_launch_runtime=shell._resolve_launch_runtime,
            run_launch_workspace_hooks=shell._run_launch_workspace_hooks,
            build_dependency_summary=shell._build_dependency_summary,
            assemble_prepared_launch_context=shell._assemble_prepared_launch_context,
        ),
        subprocess_runner=subprocess_runner,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        pr_port=pr_port,
        issue_context_port=issue_context_port,
        session_store=session_store,
        worktree_port=worktree_port,
    )


def resolve_launch_context_for_cycle(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any | None,
    target_issue: str | None,
    dry_run: bool,
    status_resolver: Callable[..., str] | None,
    subprocess_runner: Callable[..., Any] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    select_launch_candidate_for_cycle: Callable[..., tuple[Any | None, Any | None]],
    prepare_selected_launch_candidate: Callable[..., tuple[Any | None, Any | None]],
    logger: Any,
) -> tuple[Any | None, Any | None]:
    """Resolve or prepare launch-ready work for this cycle."""
    return _resolve_launch_context_for_cycle_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=ResolveLaunchDeps(
            select_launch_candidate_for_cycle=select_launch_candidate_for_cycle,
            prepare_selected_launch_candidate=prepare_selected_launch_candidate,
        ),
        launch_context=launch_context,
        target_issue=target_issue,
        dry_run=dry_run,
        status_resolver=status_resolver,
        subprocess_runner=subprocess_runner,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        log_dry_run=lambda issue_ref: logger.info(
            "[dry-run] Would prepare, claim, and execute: %s",
            issue_ref,
        ),
    )


def claim_launch_context(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    slot_id: int,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    open_pending_claim_session: Callable[..., tuple[Any | None, Any | None]],
    enforce_claim_retry_ceiling: Callable[..., Any | None],
    attempt_launch_context_claim: Callable[..., tuple[Any | None, Any | None]],
    mark_claimed_session_running: Callable[..., Any],
) -> tuple[Any | None, Any | None]:
    """Claim board ownership and start a durable running session."""
    return _claim_launch_context_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=ClaimLaunchDeps(
            open_pending_claim_session=open_pending_claim_session,
            enforce_claim_retry_ceiling=enforce_claim_retry_ceiling,
            attempt_launch_context_claim=attempt_launch_context_claim,
            mark_claimed_session_running=mark_claimed_session_running,
        ),
        launch_context=launch_context,
        slot_id=slot_id,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def handoff_execution_to_review(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    session_id: str,
    pr_url: str,
    session_status: str,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    build_session_store: Callable[[Any], Any],
    transition_claimed_session_to_review: Callable[..., None],
    post_claimed_session_verdict_marker: Callable[..., None],
    queue_claimed_session_for_review: Callable[..., Any | None],
    run_immediate_review_handoff: Callable[..., Any],
    record_metric: Callable[..., None],
) -> Any:
    """Transition a claimed session into Review and perform immediate rescue."""
    return _handoff_execution_to_review_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=ReviewHandoffDeps(
            build_session_store=build_session_store,
            transition_claimed_session_to_review=transition_claimed_session_to_review,
            post_claimed_session_verdict_marker=post_claimed_session_verdict_marker,
            queue_claimed_session_for_review=queue_claimed_session_for_review,
            run_immediate_review_handoff=run_immediate_review_handoff,
            record_metric=record_metric,
        ),
        launch_context=launch_context,
        session_id=session_id,
        pr_url=pr_url,
        session_status=session_status,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
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
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_poster: Callable[..., None] | None,
    subprocess_runner: Callable[..., Any] | None,
    gh_runner: Callable[..., str] | None,
    build_github_port_bundle: Callable[..., Any],
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
    return _handle_non_review_execution_outcome_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=NonReviewOutcomeDeps(
            build_github_port_bundle=build_github_port_bundle,
            verify_resolution_payload=verify_resolution_payload,
            apply_resolution_action=apply_resolution_action,
            return_issue_to_ready=return_issue_to_ready,
            record_successful_github_mutation=record_successful_github_mutation,
            mark_degraded=mark_degraded,
            queue_status_transition=queue_status_transition,
            record_metric=record_metric,
            log_ready_reset_failure=lambda err: logger.error(
                "Ready reset failed after non-PR session: %s",
                err,
            ),
        ),
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
    )


def execute_claimed_session(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    claimed_context: Any,
    subprocess_runner: Callable[..., Any] | None,
    file_reader: Callable[..., Any] | None,
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
) -> SessionExecutionOutcome:
    """Execute Codex for a claimed session and apply immediate board handoff."""
    return _execute_claimed_session_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=ExecutionDeps(
            assemble_codex_prompt=assemble_codex_prompt,
            run_codex_session=run_codex_session,
            parse_codex_result=parse_codex_result,
            session_status_from_codex_result=session_status_from_codex_result,
            create_pr_for_execution_result=create_pr_for_execution_result,
            handoff_execution_to_review=handoff_execution_to_review,
            handle_non_review_execution_outcome=handle_non_review_execution_outcome,
            build_session_execution_outcome=SessionExecutionOutcome,
        ),
        launch_context=launch_context,
        claimed_context=claimed_context,
        subprocess_runner=subprocess_runner,
        file_reader=file_reader,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def finalize_claimed_session(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    claimed_context: Any,
    execution_outcome: Any,
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
    return _finalize_claimed_session_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=FinalizeClaimedSessionDeps(
            final_phase_for_claimed_session=final_phase_for_claimed_session,
            persist_claimed_session_completion=persist_claimed_session_completion,
            post_claimed_session_result_comment=post_claimed_session_result_comment,
            maybe_escalate_claimed_session_failure=maybe_escalate_claimed_session_failure,
        ),
        launch_context=launch_context,
        claimed_context=claimed_context,
        execution_outcome=execution_outcome,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def prepared_cycle_deps(
    *,
    claim_suppression_state: Callable[..., Any],
    next_available_slot: Callable[..., Any],
    resolve_launch_context_for_cycle: Callable[..., Any],
    claim_launch_context: Callable[..., Any],
    execute_claimed_session: Callable[..., Any],
    finalize_claimed_session: Callable[..., Any],
) -> PreparedCycleDeps:
    """Bind board_consumer helpers for the extracted prepared-cycle slice."""
    return PreparedCycleDeps(
        claim_suppression_state=claim_suppression_state,
        next_available_slot=next_available_slot,
        resolve_launch_context_for_cycle=resolve_launch_context_for_cycle,
        claim_launch_context=claim_launch_context,
        execute_claimed_session=execute_claimed_session,
        finalize_claimed_session=finalize_claimed_session,
    )
