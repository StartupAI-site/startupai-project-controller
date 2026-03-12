"""Interrupted-session recovery wiring extracted from board_consumer."""

from __future__ import annotations

from typing import Any, Callable

from startupai_controller.application.consumer.recovery import RecoveryDeps


def recover_interrupted_sessions(
    config: Any,
    db: Any,
    *,
    automation_config: Any | None = None,
    pr_port: Any | None = None,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    load_config: Callable[[Any], Any],
    config_error_type: type[Exception],
    logger: Any,
    recovery_use_case: Callable[..., list[Any]],
    gh_query_error_type: type[Exception],
    build_github_port_bundle: Callable[..., Any],
    load_automation_config: Callable[[Any], Any],
    resolve_issue_coordinates: Callable[..., tuple[str, str, int]],
    classify_open_pr_candidates: Callable[..., tuple[str, Any | None, str]],
    return_issue_to_ready: Callable[..., None],
    transition_issue_to_review: Callable[..., None],
    set_blocked_with_reason: Callable[..., None],
) -> list[Any]:
    """Recover leases left behind by a previous interrupted daemon process."""
    recovered = db.recover_interrupted_leases()
    if not recovered:
        return []

    try:
        cp_config = load_config(config.critical_paths_path)
    except config_error_type as err:
        logger.error("Interrupted-session recovery skipped: %s", err)
        return recovered

    bundle = None
    if pr_port is None or review_state_port is None or board_port is None:
        bundle = build_github_port_bundle(
            config.project_owner,
            config.project_number,
            config=cp_config,
            gh_runner=gh_runner,
        )
    effective_pr_port = pr_port or bundle.pull_requests
    effective_review_state_port = review_state_port or bundle.review_state
    effective_board_port = board_port or bundle.board_mutations

    return recovery_use_case(
        config,
        db,
        recovered=recovered,
        cp_config=cp_config,
        deps=RecoveryDeps(
            gh_query_error_type=gh_query_error_type,
            load_automation_config=load_automation_config,
            resolve_issue_coordinates=resolve_issue_coordinates,
            classify_open_pr_candidates=classify_open_pr_candidates,
            return_issue_to_ready=return_issue_to_ready,
            transition_issue_to_review=transition_issue_to_review,
            set_blocked_with_reason=set_blocked_with_reason,
        ),
        automation_config=automation_config,
        pr_port=effective_pr_port,
        review_state_port=effective_review_state_port,
        board_port=effective_board_port,
        log_error=lambda issue_ref, err: logger.error(
            "Interrupted-session board recovery failed for %s: %s",
            issue_ref,
            err,
        ),
    )
