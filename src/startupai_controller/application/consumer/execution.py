"""Claimed-session execution orchestration for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from startupai_controller.domain.models import CycleResult, ReviewQueueDrainSummary


@dataclass(frozen=True)
class ExecutionDeps:
    """Injected seams for claimed-session execution."""

    assemble_codex_prompt: Callable[..., str]
    run_codex_session: Callable[..., int]
    parse_codex_result: Callable[..., dict[str, Any] | None]
    session_status_from_codex_result: Callable[..., tuple[str, str | None]]
    create_pr_for_execution_result: Callable[..., Any]
    handoff_execution_to_review: Callable[..., ReviewQueueDrainSummary]
    handle_non_review_execution_outcome: Callable[..., tuple[str, Any | None, str | None]]
    build_session_execution_outcome: Callable[..., Any]


def execute_claimed_session(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    deps: ExecutionDeps,
    launch_context: Any,
    claimed_context: Any,
    subprocess_runner: Callable[..., Any] | None,
    file_reader: Callable[[Path], str] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> Any:
    """Execute Codex for a claimed session and apply immediate board handoff."""
    candidate = launch_context.issue_ref
    session_id = claimed_context.session_id

    prompt = deps.assemble_codex_prompt(
        launch_context.issue_context,
        candidate,
        prepared.cp_config,
        launch_context.effective_consumer_config,
        launch_context.worktree_path,
        launch_context.branch_name,
        dependency_summary=launch_context.dependency_summary,
        workflow_definition=launch_context.workflow_definition,
        session_kind=launch_context.session_kind,
        repair_pr_url=launch_context.repair_pr_url,
        branch_reconcile_state=launch_context.branch_reconcile_state,
        branch_reconcile_error=launch_context.branch_reconcile_error,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = config.output_dir / f"{session_id}.json"
    exit_code = deps.run_codex_session(
        launch_context.worktree_path,
        prompt,
        config.schema_path,
        output_path,
        launch_context.effective_consumer_config.codex_timeout_seconds,
        heartbeat_fn=lambda: db.update_heartbeat(candidate),
        subprocess_runner=subprocess_runner,
    )

    codex_result = deps.parse_codex_result(output_path, file_reader=file_reader)
    session_status, failure_reason = deps.session_status_from_codex_result(
        exit_code,
        codex_result,
    )
    pr_outcome = deps.create_pr_for_execution_result(
        config=config,
        launch_context=launch_context,
        claimed_context=claimed_context,
        codex_result=codex_result,
        session_status=session_status,
        failure_reason=failure_reason,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
    )

    should_transition_to_review = bool(pr_outcome.pr_url) and (
        launch_context.session_kind != "repair"
        or pr_outcome.session_status == "success"
    )

    immediate_review_summary = ReviewQueueDrainSummary()
    resolution_evaluation = None
    done_reason: str | None = None
    effective_session_status = pr_outcome.session_status
    if should_transition_to_review:
        immediate_review_summary = deps.handoff_execution_to_review(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            session_id=session_id,
            pr_url=pr_outcome.pr_url or "",
            session_status=pr_outcome.session_status,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
    else:
        (
            effective_session_status,
            resolution_evaluation,
            done_reason,
        ) = deps.handle_non_review_execution_outcome(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            session_id=session_id,
            session_status=pr_outcome.session_status,
            codex_result=codex_result,
            has_commits=pr_outcome.has_commits,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_poster=comment_poster,
            subprocess_runner=subprocess_runner,
            gh_runner=gh_runner,
        )

    return deps.build_session_execution_outcome(
        session_status=effective_session_status,
        failure_reason=pr_outcome.failure_reason,
        pr_url=pr_outcome.pr_url,
        has_commits=pr_outcome.has_commits,
        codex_result=codex_result,
        should_transition_to_review=should_transition_to_review,
        immediate_review_summary=immediate_review_summary,
        resolution_evaluation=resolution_evaluation,
        done_reason=done_reason,
    )


@dataclass(frozen=True)
class FinalizeClaimedSessionDeps:
    """Injected seams for claimed-session finalization."""

    final_phase_for_claimed_session: Callable[..., str]
    persist_claimed_session_completion: Callable[..., None]
    post_claimed_session_result_comment: Callable[..., None]
    maybe_escalate_claimed_session_failure: Callable[..., None]


def finalize_claimed_session(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    deps: FinalizeClaimedSessionDeps,
    launch_context: Any,
    claimed_context: Any,
    execution_outcome: Any,
    board_info_resolver: Callable[..., Any] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> CycleResult:
    """Persist final session state and return the cycle result."""
    cp_config = prepared.cp_config
    candidate = launch_context.issue_ref
    session_id = claimed_context.session_id
    effective_max_retries = claimed_context.effective_max_retries

    db.release_lease(candidate)
    final_phase = deps.final_phase_for_claimed_session(
        launch_context=launch_context,
        execution_outcome=execution_outcome,
    )
    deps.persist_claimed_session_completion(
        db=db,
        session_id=session_id,
        issue_ref=candidate,
        execution_outcome=execution_outcome,
        final_phase=final_phase,
    )
    deps.post_claimed_session_result_comment(
        issue_ref=candidate,
        session_id=session_id,
        codex_result=execution_outcome.codex_result,
        cp_config=cp_config,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )
    deps.maybe_escalate_claimed_session_failure(
        config=config,
        db=db,
        issue_ref=candidate,
        effective_max_retries=effective_max_retries,
        session_status=execution_outcome.session_status,
        codex_result=execution_outcome.codex_result,
        cp_config=cp_config,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )

    return CycleResult(
        action="claimed",
        issue_ref=candidate,
        session_id=session_id,
        reason=execution_outcome.session_status,
        pr_url=execution_outcome.pr_url,
    )
