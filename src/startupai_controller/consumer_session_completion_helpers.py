"""Claimed-session completion helper cluster extracted from board_consumer."""

from __future__ import annotations

import json
from typing import Any, Callable


def session_status_from_codex_result(
    exit_code: int,
    codex_result: dict[str, Any] | None,
) -> tuple[str, str | None]:
    """Map Codex exit/result into session status and failure reason."""
    if exit_code == 0 and codex_result and codex_result.get("outcome") == "success":
        return "success", None
    if exit_code == 124:
        return "timeout", "timeout"
    if codex_result and codex_result.get("outcome") in {"failed", "blocked"}:
        return "failed", "validation_failed"
    return "failed", "codex_error"


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
    pr_url = codex_result.get("pr_url") if codex_result else None
    has_commits = False
    updated_session_status = session_status
    updated_failure_reason = failure_reason

    try:
        has_commits = has_commits_on_branch(
            launch_context.worktree_path,
            launch_context.branch_name,
            subprocess_runner=subprocess_runner,
        )
        if has_commits:
            pr_url = create_or_update_pr(
                launch_context.worktree_path,
                launch_context.branch_name,
                launch_context.number,
                launch_context.owner,
                launch_context.repo,
                launch_context.title,
                config,
                issue_ref=launch_context.issue_ref,
                session_id=claimed_context.session_id,
                gh_runner=gh_runner,
            )
    except Exception as err:
        logger.error("PR creation failed: %s", err)
        if updated_session_status == "success":
            updated_session_status = "failed"
        updated_failure_reason = "pr_error"

    return pr_creation_outcome_factory(
        pr_url=pr_url,
        has_commits=has_commits,
        session_status=updated_session_status,
        failure_reason=updated_failure_reason,
    )


def final_phase_for_claimed_session(
    *,
    launch_context: Any,
    execution_outcome: Any,
) -> str:
    """Determine the final persisted phase for a claimed session."""
    review_requeued = (
        execution_outcome.should_transition_to_review
        and launch_context.issue_ref
        in execution_outcome.immediate_review_summary.requeued
    )
    final_phase = (
        "completed"
        if review_requeued
        else (
            "review" if execution_outcome.should_transition_to_review else "completed"
        )
    )
    if (
        execution_outcome.session_status in {"failed", "timeout"}
        and not execution_outcome.pr_url
    ):
        final_phase = "blocked"
    if (
        execution_outcome.resolution_evaluation is not None
        and execution_outcome.done_reason != "already_resolved"
    ):
        final_phase = "blocked"
    if (
        launch_context.session_kind == "repair"
        and execution_outcome.session_status in {"failed", "timeout"}
    ):
        final_phase = "completed"
    return final_phase


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
    resolution_evaluation = execution_outcome.resolution_evaluation
    codex_result = execution_outcome.codex_result
    complete_session(
        db,
        session_id,
        issue_ref,
        status=execution_outcome.session_status,
        failure_reason=execution_outcome.failure_reason,
        outcome_json=json.dumps(codex_result) if codex_result else None,
        pr_url=execution_outcome.pr_url,
        phase=final_phase,
        resolution_kind=(
            resolution_evaluation.resolution_kind
            if resolution_evaluation is not None
            else None
        ),
        verification_class=(
            resolution_evaluation.verification_class
            if resolution_evaluation is not None
            else None
        ),
        resolution_evidence_json=(
            json.dumps(resolution_evaluation.evidence, sort_keys=True)
            if resolution_evaluation is not None
            else None
        ),
        resolution_action=(
            resolution_evaluation.final_action
            if resolution_evaluation is not None
            else None
        ),
        done_reason=execution_outcome.done_reason,
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
    if not codex_result:
        return
    try:
        post_result_comment(
            issue_ref,
            codex_result,
            session_id,
            cp_config,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )
    except Exception as err:
        logger.error("Result comment failed: %s", err)


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
    if session_status not in {"failed", "timeout"}:
        return
    new_retries = db.count_retries(issue_ref)
    if new_retries < effective_max_retries:
        return
    try:
        escalation_reason = ""
        if codex_result:
            escalation_reason = codex_result.get("blocker_reason") or codex_result.get(
                "summary",
                "",
            )
        escalate_to_claude(
            issue_ref,
            cp_config,
            config.project_owner,
            config.project_number,
            reason=escalation_reason
            or f"max retries ({effective_max_retries}) exceeded",
            board_info_resolver=board_info_resolver,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )
    except Exception as err:
        logger.error("Escalation failed: %s", err)
