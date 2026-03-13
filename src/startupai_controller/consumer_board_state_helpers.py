"""Board-state transition and reconciliation helpers extracted from board_consumer."""

from __future__ import annotations

import logging
from typing import Any, Callable

from startupai_controller import consumer_automation_bridge as _automation_bridge
import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
from startupai_controller import (
    consumer_execution_support_helpers as _execution_support_helpers,
)
import startupai_controller.consumer_resolution_helpers as _resolution_helpers
from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.domain.repair_policy import marker_for as _marker_for
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import (
    SessionStorePort,
    SessionUpdateFields,
)
from startupai_controller.runtime.wiring import build_github_port_bundle
from startupai_controller.validate_critical_path_promotion import (
    GhQueryError,
    parse_issue_ref,
)

logger = logging.getLogger("board-consumer")


def transition_issue_to_review(
    issue_ref: str,
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Move a successfully submitted issue from In Progress to Review."""
    code, message = _automation_bridge.sync_review_state(
        event_kind="pr_ready_for_review",
        issue_ref=issue_ref,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    if code in (0, 2):
        return
    raise GhQueryError(f"Failed moving {issue_ref} to Review: {message}")


def transition_issue_to_in_progress(
    issue_ref: str,
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    build_github_port_bundle: Callable[..., Any],
    from_statuses: set[str] | None = None,
    active_session_id: str | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Move an actively running local repair back into In Progress."""
    port_review = review_state_port
    port_board = board_port
    if port_review is None or port_board is None:
        bundle = build_github_port_bundle(
            project_owner,
            project_number,
            config=config,
            gh_runner=gh_runner,
        )
        port_review = port_review or bundle.review_state
        port_board = port_board or bundle.board_mutations
    old_status = port_review.get_issue_status(issue_ref)
    allowed_statuses = from_statuses or {"Review"}
    same_session_ready_race = old_status == "Ready" and active_session_id is not None
    if old_status in allowed_statuses or same_session_ready_race:
        port_board.set_issue_status(issue_ref, "In Progress")
        changed = True
    else:
        changed = False
    if changed or old_status == "In Progress":
        return
    raise GhQueryError(
        f"Failed moving {issue_ref} to In Progress: current status={old_status}"
    )


def return_issue_to_ready(
    issue_ref: str,
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    build_github_port_bundle: Callable[..., Any],
    from_statuses: set[str] | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Move a non-running claimed issue back to Ready so the lane stays truthful."""
    port_review = review_state_port
    port_board = board_port
    if port_review is None or port_board is None:
        bundle = build_github_port_bundle(
            project_owner,
            project_number,
            config=config,
            gh_runner=gh_runner,
        )
        port_review = port_review or bundle.review_state
        port_board = port_board or bundle.board_mutations
    old_status = port_review.get_issue_status(issue_ref)
    if old_status in (from_statuses or {"In Progress"}):
        port_board.set_issue_status(issue_ref, "Ready")
        changed = True
    else:
        changed = False
    if changed or old_status == "Ready":
        return
    raise GhQueryError(
        f"Failed moving {issue_ref} back to Ready: current status={old_status}"
    )


def reconcile_active_repair_review_items(
    consumer_config: Any,
    critical_path_config: Any,
    *,
    active_repair_sessions_by_issue: dict[str, str],
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_snapshot: Any | None,
    issue_ref_for_snapshot: Callable[..., str | None],
    dry_run: bool,
    transition_issue_to_in_progress: Callable[..., None],
) -> list[str]:
    """Return active repair items that should move from Review back to In Progress."""
    moved_in_progress: list[str] = []
    review_items = (
        board_snapshot.items_with_status("Review")
        if board_snapshot is not None
        else review_state_port.list_issues_by_status("Review")
    )
    for snapshot in review_items:
        issue_ref = issue_ref_for_snapshot(snapshot)
        if issue_ref is None:
            continue
        parsed = parse_issue_ref(issue_ref)
        if parsed.prefix not in consumer_config.repo_prefixes:
            continue
        if snapshot.executor.strip().lower() != consumer_config.executor:
            continue
        active_session_id = active_repair_sessions_by_issue.get(issue_ref)
        if active_session_id is None:
            continue

        if not dry_run:
            transition_issue_to_in_progress(
                issue_ref,
                critical_path_config,
                consumer_config.project_owner,
                consumer_config.project_number,
                active_session_id=active_session_id,
                review_state_port=review_state_port,
                board_port=board_port,
            )
        moved_in_progress.append(issue_ref)
    return moved_in_progress


def reconcile_single_in_progress_item(
    issue_ref: str,
    *,
    consumer_config: Any,
    critical_path_config: Any,
    automation_config: Any,
    store: SessionStorePort,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    dry_run: bool,
    resolve_issue_coordinates: Callable[..., tuple[str, str, int]],
    classify_open_pr_candidates: Callable[..., tuple[str, Any | None, str]],
    reconcile_in_progress_decision: Callable[..., str],
    return_issue_to_ready: Callable[..., None],
    transition_issue_to_review: Callable[..., None],
    set_blocked_with_reason: Callable[..., None],
) -> str:
    """Reconcile one stale In Progress item and return its target lane."""
    owner, repo, number = resolve_issue_coordinates(issue_ref, critical_path_config)
    latest_session = store.latest_session_for_issue(issue_ref)
    expected_branch = latest_session.branch_name if latest_session else None
    classification, pr_match, reason = classify_open_pr_candidates(
        issue_ref,
        owner,
        repo,
        number,
        automation_config,
        expected_branch=expected_branch,
        pr_port=pr_port,
    )
    target = reconcile_in_progress_decision(
        classification,
        has_latest_session=latest_session is not None,
        session_kind=latest_session.session_kind if latest_session else None,
        session_status=latest_session.status if latest_session else None,
    )

    if target == "ready":
        if classification == "adoptable" and pr_match is not None:
            if latest_session is not None and not dry_run:
                store.update_session(
                    latest_session.id,
                    SessionUpdateFields(pr_url=pr_match.url),
                )
        if not dry_run:
            return_issue_to_ready(
                issue_ref,
                critical_path_config,
                consumer_config.project_owner,
                consumer_config.project_number,
                from_statuses={"In Progress"},
                review_state_port=review_state_port,
                board_port=board_port,
            )
        return "ready"

    if target == "review":
        if latest_session is not None and pr_match is not None and not dry_run:
            store.update_session(
                latest_session.id,
                SessionUpdateFields(
                    pr_url=pr_match.url,
                    phase="review",
                ),
            )
        if not dry_run:
            transition_issue_to_review(
                issue_ref,
                critical_path_config,
                consumer_config.project_owner,
                consumer_config.project_number,
                review_state_port=review_state_port,
                board_port=board_port,
            )
        return "review"

    if not dry_run:
        set_blocked_with_reason(
            issue_ref,
            f"execution-authority:{reason}",
            critical_path_config,
            consumer_config.project_owner,
            consumer_config.project_number,
            review_state_port=review_state_port,
            board_port=board_port,
        )
    return "blocked"


def reconcile_stale_in_progress_items(
    consumer_config: Any,
    critical_path_config: Any,
    automation_config: Any,
    *,
    store: SessionStorePort,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_snapshot: Any | None,
    issue_ref_for_snapshot: Callable[..., str | None],
    active_issue_refs: set[str],
    dry_run: bool,
    reconcile_single_in_progress_item: Callable[..., str],
) -> tuple[list[str], list[str], list[str]]:
    """Reconcile stale In Progress items back to their truthful lanes."""
    moved_ready: list[str] = []
    moved_review: list[str] = []
    moved_blocked: list[str] = []
    in_progress = (
        board_snapshot.items_with_status("In Progress")
        if board_snapshot is not None
        else review_state_port.list_issues_by_status("In Progress")
    )
    for snapshot in in_progress:
        issue_ref = issue_ref_for_snapshot(snapshot)
        if issue_ref is None:
            continue
        parsed = parse_issue_ref(issue_ref)
        if parsed.prefix not in consumer_config.repo_prefixes:
            continue
        if snapshot.executor.strip().lower() != consumer_config.executor:
            continue
        if issue_ref in active_issue_refs:
            continue

        target = reconcile_single_in_progress_item(
            issue_ref,
            consumer_config=consumer_config,
            critical_path_config=critical_path_config,
            automation_config=automation_config,
            store=store,
            pr_port=pr_port,
            review_state_port=review_state_port,
            board_port=board_port,
            dry_run=dry_run,
        )
        if target == "ready":
            moved_ready.append(issue_ref)
        elif target == "review":
            moved_review.append(issue_ref)
        else:
            moved_blocked.append(issue_ref)
    return moved_ready, moved_review, moved_blocked


def transition_issue_to_review_from_shell(
    issue_ref: str,
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Move a successfully submitted issue from In Progress to Review."""
    return transition_issue_to_review(
        issue_ref,
        config,
        project_owner,
        project_number,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )


def transition_issue_to_in_progress_from_shell(
    issue_ref: str,
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    from_statuses: set[str] | None = None,
    active_session_id: str | None = None,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Move an actively running local repair back into In Progress."""
    del board_info_resolver, board_mutator
    return transition_issue_to_in_progress(
        issue_ref,
        config,
        project_owner,
        project_number,
        build_github_port_bundle=build_github_port_bundle,
        from_statuses=from_statuses,
        active_session_id=active_session_id,
        review_state_port=review_state_port,
        board_port=board_port,
        gh_runner=gh_runner,
    )


def return_issue_to_ready_from_shell(
    issue_ref: str,
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    from_statuses: set[str] | None = None,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Move a non-running claimed issue back to Ready so the lane stays truthful."""
    del board_info_resolver, board_mutator
    return return_issue_to_ready(
        issue_ref,
        config,
        project_owner,
        project_number,
        build_github_port_bundle=build_github_port_bundle,
        from_statuses=from_statuses,
        review_state_port=review_state_port,
        board_port=board_port,
        gh_runner=gh_runner,
    )


def escalate_to_claude_from_shell(
    issue_ref: str,
    config: Any,
    project_owner: str,
    project_number: int,
    reason: str = "",
    *,
    board_info_resolver: Callable[..., Any] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Block the issue for Claude handoff and post one escalation comment."""
    return _execution_support_helpers.escalate_to_claude(
        issue_ref,
        config,
        project_owner,
        project_number,
        reason,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        marker_for=_marker_for,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        set_blocked_with_reason=_automation_bridge.set_blocked_with_reason,
        set_issue_handoff_target=lambda *args, **kwargs: _resolution_helpers.set_issue_handoff_target(
            *args,
            build_github_port_bundle=build_github_port_bundle,
            **kwargs,
        ),
        default_review_comment_checker=_codex_comment_wiring.default_review_comment_checker,
        runtime_comment_poster=_codex_comment_wiring.runtime_comment_poster,
        logger=logger,
    )
