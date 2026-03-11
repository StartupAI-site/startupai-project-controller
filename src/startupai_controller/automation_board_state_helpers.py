"""Shared board-transition helpers extracted from board_automation.py.

These are outer-layer compatibility helpers that bridge the legacy
gh_runner / board_info_resolver paths and the hexagonal port layer.
They are NOT domain or application logic — they are wiring glue.

Callers in board_automation.py retain thin wrappers that inject the
concrete port-factory and query helpers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from startupai_controller.board_automation_config import (
    DEFAULT_PROJECT_NUMBER,
    DEFAULT_PROJECT_OWNER,
)
from startupai_controller.domain.models import LinkedIssue
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

if TYPE_CHECKING:
    from startupai_controller.board_automation import BoardInfo

    from startupai_controller.ports.board_mutations import (
        BoardMutationPort as _BoardMutationPort,
    )
    from startupai_controller.ports.review_state import (
        ReviewStatePort as _ReviewStatePort,
    )


# ---------------------------------------------------------------------------
# Low-level status mutation
# ---------------------------------------------------------------------------


def set_board_status(
    project_id: str,
    item_id: str,
    status: str,
    *,
    board_port: _BoardMutationPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helpers
    set_single_select_field_fn: Callable[..., None] | None = None,
) -> None:
    """Compatibility helper that writes the Status field via BoardMutationPort."""
    if board_port is None and gh_runner is not None:
        from startupai_controller.promote_ready import (
            _query_status_field_option as _legacy_query_status_field_option,
            _set_board_status as _legacy_set_board_status,
        )

        field_id, option_id = _legacy_query_status_field_option(
            project_id,
            status,
            gh_runner=gh_runner,
        )
        _legacy_set_board_status(
            project_id,
            item_id,
            field_id,
            option_id,
            gh_runner=gh_runner,
        )
        return

    set_single_select_field_fn(
        project_id,
        item_id,
        "Status",
        status,
        board_port=board_port,
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
    )


# ---------------------------------------------------------------------------
# Status-if-changed
# ---------------------------------------------------------------------------


def set_status_if_changed(
    issue_ref: str,
    from_statuses: set[str],
    to_status: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helpers
    query_issue_board_info_fn: Callable[..., BoardInfo] | None = None,
    set_board_status_fn: Callable[..., None] | None = None,
) -> tuple[bool, str]:
    """Legacy-compatible status transition helper for test seams."""
    resolver = board_info_resolver or query_issue_board_info_fn
    info = resolver(issue_ref, config, project_owner, project_number)
    current_status = info.status
    if current_status not in from_statuses:
        return False, current_status
    if not dry_run:
        if board_mutator is not None:
            board_mutator(info.project_id, info.item_id, to_status)
        elif board_port is not None:
            board_port.set_issue_status(issue_ref, to_status)
        else:
            set_board_status_fn(
                info.project_id,
                info.item_id,
                to_status,
                project_owner=project_owner,
                project_number=project_number,
                config=config,
                gh_runner=gh_runner,
            )
    return True, current_status


# ---------------------------------------------------------------------------
# Legacy board-status mutator factory
# ---------------------------------------------------------------------------


def legacy_board_status_mutator(
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    *,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helper
    set_board_status_fn: Callable[..., None] | None = None,
) -> Callable[..., None]:
    """Adapt legacy project-item status helpers to the application boundary."""

    def mutate(project_id: str, item_id: str, status: str = "Blocked") -> None:
        set_board_status_fn(
            project_id,
            item_id,
            status,
            project_owner=project_owner,
            project_number=project_number,
            config=config,
            gh_runner=gh_runner,
        )

    return mutate


# ---------------------------------------------------------------------------
# Blocked-with-reason
# ---------------------------------------------------------------------------


def set_blocked_with_reason(
    issue_ref: str,
    reason: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helpers
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    legacy_board_status_mutator_fn: Callable[..., Callable] | None = None,
    app_set_blocked_with_reason_fn: Callable[..., None] | None = None,
) -> None:
    """Set Status=Blocked and Blocked Reason on a board item."""
    review_state_port = review_state_port or default_review_state_port_fn(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    board_port = board_port or default_board_mutation_port_fn(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    status_mutator = board_mutator
    if status_mutator is None and board_info_resolver is not None:
        status_mutator = legacy_board_status_mutator_fn(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    app_set_blocked_with_reason_fn(
        issue_ref,
        reason,
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=status_mutator,
        gh_runner=gh_runner,
    )


# ---------------------------------------------------------------------------
# Transition issue status
# ---------------------------------------------------------------------------


def transition_issue_status(
    issue_ref: str,
    from_statuses: set[str],
    to_status: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helpers
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    legacy_board_status_mutator_fn: Callable[..., Callable] | None = None,
    app_transition_issue_status_fn: Callable[..., tuple[bool, str]] | None = None,
) -> tuple[bool, str]:
    """Transition issue status through ports, with legacy fallback for tests."""
    review_state_port = review_state_port or default_review_state_port_fn(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    board_port = board_port or default_board_mutation_port_fn(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    status_mutator = board_mutator
    if status_mutator is None and board_info_resolver is not None:
        status_mutator = legacy_board_status_mutator_fn(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    return app_transition_issue_status_fn(
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
        board_mutator=status_mutator,
        gh_runner=gh_runner,
    )


# ---------------------------------------------------------------------------
# Mark issues done
# ---------------------------------------------------------------------------


def mark_issues_done(
    issues: list[LinkedIssue],
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helper
    transition_issue_status_fn: Callable[..., tuple[bool, str]] | None = None,
) -> list[str]:
    """Mark linked issues as Done on the board. Returns list of refs marked Done."""
    marked: list[str] = []

    for issue in issues:
        changed, old_status = transition_issue_status_fn(
            issue.ref,
            {"Review", "In Progress", "Blocked", "Ready", "Backlog"},
            "Done",
            config,
            project_owner,
            project_number,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        if changed:
            marked.append(issue.ref)

    return marked
