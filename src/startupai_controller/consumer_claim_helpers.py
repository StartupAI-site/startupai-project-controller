"""Launch selection and claim helper cluster extracted from board_consumer."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable


def select_candidate_for_cycle(
    config: Any,
    db: Any,
    prepared: Any,
    *,
    target_issue: str | None = None,
    review_state_port: Any | None = None,
    status_resolver: Callable[..., str] | None = None,
    gh_runner: Callable[..., str] | None = None,
    excluded_issue_refs: set[str] | None = None,
    parse_issue_ref: Callable[[str], Any],
    effective_retry_backoff: Callable[..., tuple[int, int]],
    retry_backoff_active: Callable[..., bool],
    select_best_candidate: Callable[..., str | None],
) -> str | None:
    """Select the next eligible issue for one slot in this cycle."""
    excluded = excluded_issue_refs or set()

    def issue_filter(issue_ref: str) -> bool:
        if issue_ref in excluded:
            return False
        repo_prefix = parse_issue_ref(issue_ref).prefix
        workflow = prepared.main_workflows.get(repo_prefix)
        if workflow is None:
            return False
        base_seconds, max_seconds = effective_retry_backoff(config, workflow)
        return not retry_backoff_active(
            db,
            issue_ref,
            base_seconds=base_seconds,
            max_seconds=max_seconds,
        )

    if target_issue:
        if target_issue in excluded:
            return None
        return target_issue

    if not prepared.dispatchable_repo_prefixes:
        return None

    return select_best_candidate(
        prepared.cp_config,
        config.project_owner,
        config.project_number,
        executor=config.executor,
        repo_prefixes=prepared.dispatchable_repo_prefixes,
        automation_config=prepared.auto_config,
        review_state_port=review_state_port,
        status_resolver=status_resolver,
        ready_items=prepared.board_snapshot.items_with_status("Ready"),
        github_memo=prepared.github_memo,
        gh_runner=gh_runner,
        issue_filter=issue_filter,
    )


def select_launch_candidate_for_cycle(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    target_issue: str | None,
    review_state_port: Any | None,
    status_resolver: Callable[..., str] | None,
    gh_runner: Callable[..., str] | None,
    cycle_result_factory: Callable[..., Any],
    selected_launch_candidate_factory: Callable[..., Any],
    select_candidate_for_cycle: Callable[..., str | None],
    parse_issue_ref: Callable[[str], Any],
    effective_retry_backoff: Callable[..., tuple[int, int]],
    retry_backoff_active: Callable[..., bool],
    maybe_activate_claim_suppression: Callable[..., bool],
    mark_degraded: Callable[..., None],
    gh_reason_code: Callable[..., str],
    gh_query_error_type: type[Exception],
    logger: Any,
) -> tuple[Any | None, Any | None]:
    """Select a launch candidate and validate its immediate launchability."""
    try:
        candidate = select_candidate_for_cycle(
            config,
            db,
            prepared,
            target_issue=target_issue,
            review_state_port=review_state_port,
            status_resolver=status_resolver,
            gh_runner=gh_runner,
        )
    except gh_query_error_type as err:
        logger.error("Ready-item selection failed: %s", err)
        if not maybe_activate_claim_suppression(
            db,
            config,
            scope="preflight",
            error=err,
        ):
            mark_degraded(db, f"selection-error:{gh_reason_code(err)}:{err}")
        return None, cycle_result_factory(action="error", reason=f"selection-error:{err}")
    except Exception as err:
        logger.exception("Unexpected selection failure")
        return None, cycle_result_factory(
            action="error",
            reason=f"selection-unexpected-error:{err}",
        )

    if not candidate:
        idle_reason = (
            "no-dispatchable-repos"
            if not prepared.dispatchable_repo_prefixes
            else "no-ready-for-executor"
        )
        return None, cycle_result_factory(action="idle", reason=idle_reason)

    candidate_prefix = parse_issue_ref(candidate).prefix
    main_workflow = prepared.main_workflows.get(candidate_prefix)
    if main_workflow is None:
        status = prepared.workflow_statuses.get(candidate_prefix)
        reason = status.disabled_reason if status is not None else "workflow-missing"
        return None, cycle_result_factory(
            action="idle",
            issue_ref=candidate,
            reason=f"repo-dispatch-disabled:{reason}",
        )

    base_seconds, max_seconds = effective_retry_backoff(config, main_workflow)
    if retry_backoff_active(
        db,
        candidate,
        base_seconds=base_seconds,
        max_seconds=max_seconds,
    ):
        return None, cycle_result_factory(
            action="idle",
            issue_ref=candidate,
            reason=f"retry-backoff:{base_seconds}:{max_seconds}",
        )

    return (
        selected_launch_candidate_factory(
            issue_ref=candidate,
            repo_prefix=candidate_prefix,
            main_workflow=main_workflow,
        ),
        None,
    )


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
    candidate = selected_candidate.issue_ref
    try:
        return (
            prepare_launch_candidate(
                candidate,
                config=config,
                prepared=prepared,
                db=db,
                subprocess_runner=subprocess_runner,
                status_resolver=status_resolver,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                gh_runner=gh_runner,
                pr_port=pr_port,
            ),
            None,
        )
    except gh_query_error_type as err:
        return handle_selected_launch_query_error(
            candidate=candidate,
            err=err,
            config=config,
            db=db,
        )
    except workflow_config_error_type as err:
        return handle_selected_launch_workflow_config_error(
            candidate=candidate,
            err=err,
            config=config,
            db=db,
            cp_config=prepared.cp_config,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
    except worktree_prepare_error_type as err:
        return handle_selected_launch_worktree_error(
            candidate=candidate,
            err=err,
            config=config,
            db=db,
        )
    except RuntimeError as err:
        return handle_selected_launch_runtime_error(
            candidate=candidate,
            err=err,
            config=config,
            db=db,
            cp_config=prepared.cp_config,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )


def handle_selected_launch_query_error(
    *,
    candidate: str,
    err: Exception,
    config: Any,
    db: Any,
    record_metric: Callable[..., None],
    maybe_activate_claim_suppression: Callable[..., bool],
    mark_degraded: Callable[..., None],
    gh_reason_code: Callable[..., str],
    cycle_result_factory: Callable[..., Any],
) -> tuple[None, Any]:
    """Handle GitHub/query failures during selected launch preparation."""
    record_metric(
        db,
        config,
        "context_hydration_failed",
        issue_ref=candidate,
        payload={"reason": gh_reason_code(err), "detail": str(err)},
    )
    if maybe_activate_claim_suppression(
        db,
        config,
        scope="hydration",
        error=err,
    ):
        return None, cycle_result_factory(
            action="idle",
            issue_ref=candidate,
            reason="claim-suppressed:hydration",
        )
    mark_degraded(db, f"launch-prep:{gh_reason_code(err)}:{err}")
    return None, cycle_result_factory(
        action="error",
        issue_ref=candidate,
        reason=f"launch-prep:{err}",
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
    block_prelaunch_issue(
        candidate,
        f"workflow-config:{err}",
        config=config,
        cp_config=cp_config,
        db=db,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    record_metric(
        db,
        config,
        "worker_start_failed",
        issue_ref=candidate,
        payload={"reason": "workflow_config_error", "detail": str(err)},
    )
    return None, cycle_result_factory(
        action="error",
        issue_ref=candidate,
        reason=f"workflow-config:{err}",
    )


def handle_selected_launch_worktree_error(
    *,
    candidate: str,
    err: Any,
    config: Any,
    db: Any,
    record_metric: Callable[..., None],
    cycle_result_factory: Callable[..., Any],
) -> tuple[None, Any]:
    """Handle worktree preparation failures for a selected launch candidate."""
    record_metric(
        db,
        config,
        "worker_start_failed",
        issue_ref=candidate,
        payload={"reason": err.reason_code, "detail": err.detail},
    )
    reason = (
        err.detail
        if err.reason_code == "repair_reconcile_error"
        else f"{err.reason_code}:{err.detail}"
    )
    return None, cycle_result_factory(
        action="error",
        issue_ref=candidate,
        reason=reason,
    )


def handle_selected_launch_runtime_error(
    *,
    candidate: str,
    err: RuntimeError,
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
    block_prelaunch_issue(
        candidate,
        f"workflow-hook:{err}",
        config=config,
        cp_config=cp_config,
        db=db,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    record_metric(
        db,
        config,
        "worker_start_failed",
        issue_ref=candidate,
        payload={"reason": "workflow_hook_error", "detail": str(err)},
    )
    return None, cycle_result_factory(
        action="error",
        issue_ref=candidate,
        reason=f"workflow-hook:{err}",
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
    candidate = launch_context.issue_ref
    session_id = db.create_session(
        candidate,
        repo_prefix=launch_context.repo_prefix,
        executor=executor,
        slot_id=slot_id,
        phase="launch_ready",
        session_kind=launch_context.session_kind,
        repair_pr_url=launch_context.repair_pr_url,
    )
    db.update_session(session_id, provenance_id=session_id, phase="launch_ready")
    now = datetime.now(timezone.utc)
    try:
        lease_acquired = db.acquire_lease(candidate, session_id, slot_id=slot_id, now=now)
    except TypeError:
        lease_acquired = db.acquire_lease(candidate, session_id, now=now)
    if not lease_acquired:
        complete_session(
            db,
            session_id,
            candidate,
            status="aborted",
            failure_reason="lease_conflict",
        )
        return None, cycle_result_factory(
            action="idle",
            issue_ref=candidate,
            session_id=session_id,
            reason="lease-conflict",
        )
    return (
        pending_claim_context_factory(
            session_id=session_id,
            effective_max_retries=launch_context.effective_consumer_config.max_retries,
        ),
        None,
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
    """Abort and escalate if the issue already exhausted its retry ceiling."""
    candidate = launch_context.issue_ref
    retries = db.count_retries(candidate)
    if retries < pending_claim.effective_max_retries:
        return None
    db.release_lease(candidate)
    complete_session(
        db,
        pending_claim.session_id,
        candidate,
        status="failed",
        failure_reason="max_retries_exceeded",
    )
    try:
        escalate_to_claude(
            candidate,
            cp_config,
            config.project_owner,
            config.project_number,
            reason=f"max retries ({pending_claim.effective_max_retries}) exceeded",
            board_info_resolver=board_info_resolver,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )
    except Exception as err:
        logger.error("Escalation failed: %s", err)
    return cycle_result_factory(
        action="error",
        issue_ref=candidate,
        session_id=pending_claim.session_id,
        reason="max-retries-exceeded",
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
    candidate = launch_context.issue_ref
    record_metric(
        db,
        config,
        "claim_attempted",
        issue_ref=candidate,
        payload={"slot_id": slot_id},
    )
    try:
        claim_result = claim_launch_ready_issue(
            candidate,
            config=config,
            prepared=prepared,
            status_resolver=status_resolver,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )
    except gh_query_error_type as err:
        return handle_launch_claim_api_failure(
            candidate,
            err,
            config=config,
            db=db,
            pending_claim=pending_claim,
        )
    except Exception as err:
        return handle_launch_claim_unexpected_failure(
            candidate,
            err,
            config=config,
            db=db,
            pending_claim=pending_claim,
        )

    if claim_result.claimed:
        return claim_result, None
    return handle_launch_claim_rejection(
        candidate,
        claim_result,
        config=config,
        db=db,
        pending_claim=pending_claim,
    )


def handle_launch_claim_api_failure(
    candidate: str,
    err: Exception,
    *,
    config: Any,
    db: Any,
    pending_claim: Any,
    maybe_activate_claim_suppression: Callable[..., bool],
    mark_degraded: Callable[..., None],
    gh_reason_code: Callable[..., str],
    complete_session: Callable[..., None],
    record_metric: Callable[..., None],
    cycle_result_factory: Callable[..., Any],
    logger: Any,
) -> tuple[None, Any]:
    """Handle a GitHub/API failure while claiming a launch-ready issue."""
    logger.error("Claim failed after launch prep for %s: %s", candidate, err)
    if not maybe_activate_claim_suppression(
        db,
        config,
        scope="claim",
        error=err,
    ):
        mark_degraded(db, f"claim-error:{gh_reason_code(err)}:{err}")
    db.release_lease(candidate)
    complete_session(
        db,
        pending_claim.session_id,
        candidate,
        status="failed",
        failure_reason="api_error",
    )
    record_metric(
        db,
        config,
        "worker_start_failed",
        issue_ref=candidate,
        payload={"reason": "claim_error", "detail": str(err)},
    )
    return None, cycle_result_factory(
        action="error",
        issue_ref=candidate,
        session_id=pending_claim.session_id,
        reason=f"claim-error:{err}",
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
    logger.exception("Unexpected claim failure after launch prep for %s", candidate)
    db.release_lease(candidate)
    complete_session(
        db,
        pending_claim.session_id,
        candidate,
        status="failed",
        failure_reason="consumer_error",
    )
    record_metric(
        db,
        config,
        "worker_start_failed",
        issue_ref=candidate,
        payload={"reason": "claim_unexpected_error", "detail": str(err)},
    )
    return None, cycle_result_factory(
        action="error",
        issue_ref=candidate,
        session_id=pending_claim.session_id,
        reason=f"claim-unexpected-error:{err}",
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
    db.release_lease(candidate)
    terminal_status = (
        "aborted"
        if claim_result.reason in {"wip-limit", "status-not-ready:In Progress"}
        else "failed"
    )
    failure_reason = {
        "wip-limit": "claim_rejected_wip_limit",
        "status-not-ready:In Progress": "claim_rejected_status_changed",
    }.get(claim_result.reason, "claim_rejected")
    complete_session(
        db,
        pending_claim.session_id,
        candidate,
        status=terminal_status,
        failure_reason=failure_reason,
    )
    record_metric(
        db,
        config,
        "worker_start_failed",
        issue_ref=candidate,
        payload={"reason": failure_reason},
    )
    return None, cycle_result_factory(
        action="idle",
        issue_ref=candidate,
        session_id=pending_claim.session_id,
        reason=f"claim-rejected:{claim_result.reason}",
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
    candidate = launch_context.issue_ref
    record_successful_github_mutation(db)
    record_metric(db, config, "claim_succeeded", issue_ref=candidate)
    db.update_session(
        pending_claim.session_id,
        status="running",
        slot_id=slot_id,
        worktree_path=launch_context.worktree_path,
        branch_name=launch_context.branch_name,
        phase="running",
        started_at=datetime.now(timezone.utc).isoformat(),
        session_kind=launch_context.session_kind,
        repair_pr_url=launch_context.repair_pr_url,
        branch_reconcile_state=launch_context.branch_reconcile_state,
        branch_reconcile_error=launch_context.branch_reconcile_error,
    )
    record_metric(
        db,
        config,
        "worker_durable_start",
        issue_ref=candidate,
        payload={"slot_id": slot_id, "worktree_path": launch_context.worktree_path},
    )
    try:
        post_consumer_claim_comment(
            candidate,
            pending_claim.session_id,
            launch_context.repo_prefix,
            launch_context.branch_name,
            config.executor,
            cp_config,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )
    except Exception as err:
        logger.error("Consumer claim marker failed: %s", err)
    return claimed_session_context_factory(
        session_id=pending_claim.session_id,
        effective_max_retries=pending_claim.effective_max_retries,
        slot_id=slot_id,
    )
