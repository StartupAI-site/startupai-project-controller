"""Deferred-action replay helpers extracted from board_consumer."""

from __future__ import annotations

from typing import Any, Callable

from startupai_controller.validate_critical_path_promotion import GhQueryError


def replay_deferred_status_action(
    *,
    payload: dict[str, Any],
    config: Any,
    critical_path_config: Any,
    review_state_port: Any | None,
    board_port: Any | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    set_blocked_with_reason: Callable[..., None],
    transition_issue_to_review: Callable[..., None],
    transition_issue_to_in_progress: Callable[..., None],
    return_issue_to_ready: Callable[..., None],
) -> None:
    """Replay a deferred issue status transition."""
    issue_ref = str(payload["issue_ref"])
    to_status = str(payload["to_status"])
    from_statuses = {str(value) for value in payload.get("from_statuses", [])}
    blocked_reason = payload.get("blocked_reason")
    if to_status == "Blocked":
        set_blocked_with_reason(
            issue_ref,
            str(blocked_reason or "deferred-control-plane"),
            critical_path_config,
            config.project_owner,
            config.project_number,
            gh_runner=gh_runner,
        )
        return
    if to_status == "Review":
        transition_issue_to_review(
            issue_ref,
            critical_path_config,
            config.project_owner,
            config.project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        return
    if to_status == "In Progress":
        transition_issue_to_in_progress(
            issue_ref,
            critical_path_config,
            config.project_owner,
            config.project_number,
            from_statuses=from_statuses,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        return
    if to_status == "Ready":
        return_issue_to_ready(
            issue_ref,
            critical_path_config,
            config.project_owner,
            config.project_number,
            from_statuses=from_statuses,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        return
    raise GhQueryError(f"Unsupported deferred status target: {to_status}")


def replay_deferred_verdict_marker(
    *,
    payload: dict[str, Any],
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    post_pr_codex_verdict: Callable[..., bool],
) -> None:
    """Replay a deferred PR verdict marker post."""
    post_pr_codex_verdict(
        str(payload["pr_url"]),
        str(payload["session_id"]),
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def replay_deferred_issue_comment(
    *,
    payload: dict[str, Any],
    critical_path_config: Any,
    board_port: Any | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    resolve_issue_coordinates: Callable[..., tuple[str, str, int]],
    runtime_comment_poster: Callable[..., None],
) -> None:
    """Replay a deferred issue comment post."""
    issue_ref = str(payload["issue_ref"])
    owner, repo, number = resolve_issue_coordinates(issue_ref, critical_path_config)
    body = str(payload["body"])
    if board_port is not None:
        board_port.post_issue_comment(f"{owner}/{repo}", number, body)
        return
    poster = comment_poster or runtime_comment_poster
    poster(owner, repo, number, body, gh_runner=gh_runner)


def replay_deferred_issue_close(
    *,
    payload: dict[str, Any],
    critical_path_config: Any,
    board_port: Any | None,
    gh_runner: Callable[..., str] | None,
    resolve_issue_coordinates: Callable[..., tuple[str, str, int]],
    runtime_issue_closer: Callable[..., None],
) -> None:
    """Replay a deferred issue close."""
    issue_ref = str(payload["issue_ref"])
    owner, repo, number = resolve_issue_coordinates(issue_ref, critical_path_config)
    if board_port is not None:
        board_port.close_issue(f"{owner}/{repo}", number)
        return
    runtime_issue_closer(owner, repo, number, gh_runner=gh_runner)


def replay_deferred_check_rerun(
    *,
    payload: dict[str, Any],
    pr_port: Any | None,
    gh_runner: Callable[..., str] | None,
    runtime_failed_check_rerun: Callable[..., None],
) -> None:
    """Replay a deferred failed-check rerun request."""
    pr_repo = str(payload["pr_repo"])
    check_name = str(payload.get("check_name") or "")
    run_id = int(payload["run_id"])
    if pr_port is not None:
        if not pr_port.rerun_failed_check(pr_repo, check_name, run_id):
            raise GhQueryError(
                f"Failed rerunning check for {pr_repo} run {run_id}"
            )
        return
    runtime_failed_check_rerun(pr_repo, run_id, gh_runner=gh_runner)


def replay_deferred_automerge_enable(
    *,
    payload: dict[str, Any],
    pr_port: Any | None,
    gh_runner: Callable[..., str] | None,
    runtime_automerge_enabler: Callable[..., None],
) -> None:
    """Replay a deferred auto-merge enablement."""
    pr_repo = str(payload["pr_repo"])
    pr_number = int(payload["pr_number"])
    if pr_port is not None:
        pr_port.enable_automerge(pr_repo, pr_number)
        return
    runtime_automerge_enabler(pr_repo, pr_number, gh_runner=gh_runner)


def replay_deferred_action(
    *,
    action: Any,
    config: Any,
    critical_path_config: Any,
    pr_port: Any | None,
    review_state_port: Any | None,
    board_port: Any | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    set_blocked_with_reason: Callable[..., None],
    transition_issue_to_review: Callable[..., None],
    transition_issue_to_in_progress: Callable[..., None],
    return_issue_to_ready: Callable[..., None],
    post_pr_codex_verdict: Callable[..., bool],
    resolve_issue_coordinates: Callable[..., tuple[str, str, int]],
    runtime_comment_poster: Callable[..., None],
    runtime_issue_closer: Callable[..., None],
    runtime_failed_check_rerun: Callable[..., None],
    runtime_automerge_enabler: Callable[..., None],
) -> None:
    """Execute one deferred control-plane action."""
    payload = action.payload
    if action.action_type == "set_status":
        replay_deferred_status_action(
            payload=payload,
            config=config,
            critical_path_config=critical_path_config,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
            set_blocked_with_reason=set_blocked_with_reason,
            transition_issue_to_review=transition_issue_to_review,
            transition_issue_to_in_progress=transition_issue_to_in_progress,
            return_issue_to_ready=return_issue_to_ready,
        )
        return
    if action.action_type == "post_verdict_marker":
        replay_deferred_verdict_marker(
            payload=payload,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
            post_pr_codex_verdict=post_pr_codex_verdict,
        )
        return
    if action.action_type == "post_issue_comment":
        replay_deferred_issue_comment(
            payload=payload,
            critical_path_config=critical_path_config,
            board_port=board_port,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
            resolve_issue_coordinates=resolve_issue_coordinates,
            runtime_comment_poster=runtime_comment_poster,
        )
        return
    if action.action_type == "close_issue":
        replay_deferred_issue_close(
            payload=payload,
            critical_path_config=critical_path_config,
            board_port=board_port,
            gh_runner=gh_runner,
            resolve_issue_coordinates=resolve_issue_coordinates,
            runtime_issue_closer=runtime_issue_closer,
        )
        return
    if action.action_type == "rerun_check":
        replay_deferred_check_rerun(
            payload=payload,
            pr_port=pr_port,
            gh_runner=gh_runner,
            runtime_failed_check_rerun=runtime_failed_check_rerun,
        )
        return
    if action.action_type == "enable_automerge":
        replay_deferred_automerge_enable(
            payload=payload,
            pr_port=pr_port,
            gh_runner=gh_runner,
            runtime_automerge_enabler=runtime_automerge_enabler,
        )
        return
    raise GhQueryError(f"Unsupported deferred action type: {action.action_type}")


def replay_deferred_actions(
    db: Any,
    config: Any,
    critical_path_config: Any,
    *,
    pr_port: Any | None = None,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    replay_deferred_action: Callable[..., None],
    record_successful_github_mutation: Callable[..., None],
    clear_degraded: Callable[[Any], None],
) -> tuple[int, ...]:
    """Replay queued control-plane actions after GitHub recovery."""
    replayed: list[int] = []
    for action in db.list_deferred_actions():
        try:
            replay_deferred_action(
                action=action,
                config=config,
                critical_path_config=critical_path_config,
                pr_port=pr_port,
                review_state_port=review_state_port,
                board_port=board_port,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                comment_checker=comment_checker,
                comment_poster=comment_poster,
                gh_runner=gh_runner,
            )
        except GhQueryError:
            raise
        except Exception as error:
            raise GhQueryError(
                f"Deferred action {action.id} failed: {error}"
            ) from error

        db.delete_deferred_action(action.id)
        record_successful_github_mutation(db)
        replayed.append(action.id)

    if replayed:
        clear_degraded(db)
    return tuple(replayed)
