"""Neutral automation bridge for consumer-side board/review actions."""

from __future__ import annotations

from typing import Any, Callable

from startupai_controller.application.automation.ready_claim import (
    _set_blocked_with_reason as _app_set_blocked_with_reason,
    _transition_issue_status as _app_transition_issue_status,
)
from startupai_controller.application.automation.review_wiring import (
    automerge_review as _wiring_automerge_review,
    review_rescue as _wiring_review_rescue,
    sync_review_state as _wiring_sync_review_state,
)
from startupai_controller.automation_board_state_helpers import (
    legacy_board_status_mutator as _legacy_board_status_mutator_helper,
    mark_issues_done as _mark_issues_done_helper,
    set_blocked_with_reason as _set_blocked_with_reason_helper,
    set_board_status as _set_board_status_helper,
    transition_issue_status as _transition_issue_status_helper,
)
from startupai_controller.automation_port_helpers import (
    _default_board_mutation_port,
    _default_pr_port,
    _default_review_state_port,
    _query_issue_board_info,
    _set_single_select_field,
    query_closing_issues,
)


def _set_board_status(
    project_id: str,
    item_id: str,
    status: str,
    *,
    board_port: Any | None = None,
    project_owner: str,
    project_number: int,
    config: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility helper that writes the Status field via BoardMutationPort."""
    _set_board_status_helper(
        project_id,
        item_id,
        status,
        board_port=board_port,
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
        set_single_select_field_fn=_set_single_select_field,
    )


def _legacy_board_status_mutator(
    project_owner: str,
    project_number: int,
    config: Any,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> Callable[..., None]:
    """Adapt legacy project-item status helpers to the application boundary."""
    return _legacy_board_status_mutator_helper(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
        set_board_status_fn=_set_board_status,
    )


def _transition_issue_status(
    issue_ref: str,
    from_statuses: set[str],
    to_status: str,
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[bool, str]:
    """Transition issue status through ports, with legacy fallback for tests."""
    return _transition_issue_status_helper(
        issue_ref,
        from_statuses,
        to_status,
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        legacy_board_status_mutator_fn=_legacy_board_status_mutator,
        app_transition_issue_status_fn=_app_transition_issue_status,
    )


def set_blocked_with_reason(
    issue_ref: str,
    reason: str,
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set Status=Blocked and Blocked Reason on a board item."""
    _set_blocked_with_reason_helper(
        issue_ref,
        reason,
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        legacy_board_status_mutator_fn=_legacy_board_status_mutator,
        app_set_blocked_with_reason_fn=_app_set_blocked_with_reason,
    )


def mark_issues_done(
    issues: list[Any],
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Mark linked issues as Done on the board."""
    return _mark_issues_done_helper(
        issues,
        config,
        project_owner,
        project_number,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        transition_issue_status_fn=_transition_issue_status,
    )


def sync_review_state(
    event_kind: str,
    issue_ref: str,
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    automation_config: Any | None = None,
    pr_state: str | None = None,
    review_state: str | None = None,
    checks_state: str | None = None,
    failed_checks: list[str] | None = None,
    dry_run: bool = False,
    github_bundle: Any | None = None,
    pr_port: Any | None = None,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[int, str]:
    """Sync board state based on PR/review/check events."""
    return _wiring_sync_review_state(
        event_kind,
        issue_ref,
        config,
        project_owner,
        project_number,
        automation_config=automation_config,
        pr_state=pr_state,
        review_state=review_state,
        checks_state=checks_state,
        failed_checks=failed_checks,
        dry_run=dry_run,
        github_bundle=github_bundle,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        default_pr_port_fn=_default_pr_port,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        legacy_board_status_mutator_fn=_legacy_board_status_mutator,
    )


def _automerge_review(
    pr_repo: str,
    pr_number: int,
    config: Any,
    automation_config: Any,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    update_branch: bool = True,
    delete_branch: bool = True,
    snapshot: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
    pr_port: Any | None = None,
    review_state_port: Any | None = None,
) -> tuple[int, str]:
    """Auto-merge a review PR when snapshot state already satisfies strict gates."""
    return _wiring_automerge_review(
        pr_repo,
        pr_number,
        config,
        automation_config,
        project_owner,
        project_number,
        dry_run=dry_run,
        update_branch=update_branch,
        delete_branch=delete_branch,
        snapshot=snapshot,
        gh_runner=gh_runner,
        pr_port=pr_port,
        review_state_port=review_state_port,
        default_pr_port_fn=_default_pr_port,
        default_review_state_port_fn=_default_review_state_port,
    )


def review_rescue(
    pr_repo: str,
    pr_number: int,
    config: Any,
    automation_config: Any,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    snapshot: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
    pr_port: Any | None = None,
    review_state_port: Any | None = None,
    board_port: Any | None = None,
) -> Any:
    """Reconcile one PR in Review back toward self-healing merge flow."""
    return _wiring_review_rescue(
        pr_repo,
        pr_number,
        config,
        automation_config,
        project_owner,
        project_number,
        dry_run=dry_run,
        snapshot=snapshot,
        gh_runner=gh_runner,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        default_pr_port_fn=_default_pr_port,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        query_closing_issues_fn=query_closing_issues,
        query_issue_board_info_fn=_query_issue_board_info,
        automerge_review_fn=_automerge_review,
    )
