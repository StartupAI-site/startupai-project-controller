"""Dispatch use case for claimed in-progress issues."""

from __future__ import annotations

from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.domain.models import DispatchResult
from startupai_controller.domain.repair_policy import marker_for as _marker_for
from startupai_controller.domain.scheduling_policy import VALID_EXECUTORS
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
)


def dispatch_agent(
    *,
    issue_refs: list[str],
    config: CriticalPathConfig,
    dispatch_target: str,
    dry_run: bool = False,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
) -> DispatchResult:
    """Dispatch eligible In Progress issues according to dispatch target."""
    result = DispatchResult()

    for issue_ref in issue_refs:
        owner, repo, number = _resolve_issue_coordinates(issue_ref, config)
        status = review_state_port.get_issue_status(issue_ref)
        if status != "In Progress":
            result.skipped.append((issue_ref, f"status={status or 'unknown'}"))
            continue

        executor = review_state_port.get_issue_fields(issue_ref).executor.strip().lower()
        if executor not in VALID_EXECUTORS:
            result.skipped.append((issue_ref, f"executor={executor or 'unset'}"))
            continue

        marker = _marker_for("dispatch-agent", issue_ref)
        if review_state_port.comment_exists(f"{owner}/{repo}", number, marker):
            result.skipped.append((issue_ref, "already-dispatched"))
            continue

        if dry_run:
            result.dispatched.append(issue_ref)
            continue

        if dispatch_target == "executor":
            body = (
                f"{marker}\n"
                f"Dispatch acknowledged for `Executor={executor}` lane.\n"
                "Execution is handled by the assigned local agent lane."
            )
            try:
                board_port.post_issue_comment(f"{owner}/{repo}", number, body)
                result.dispatched.append(issue_ref)
            except GhQueryError as error:
                reason_code = "comment-api-error"
                result.failed.append((issue_ref, f"{reason_code}:{error}"))
            continue

        result.failed.append(
            (issue_ref, f"unsupported-dispatch-target:{dispatch_target}")
        )

    return result


# ---------------------------------------------------------------------------
# Wiring entry-point (port materialisation + closure assembly)
# ---------------------------------------------------------------------------


def wire_dispatch_agent(
    issue_refs: list[str],
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver=None,
    board_mutator=None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helpers
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    query_issue_board_info_fn=None,
    query_project_item_field_fn=None,
    comment_exists_fn=None,
    post_comment_fn=None,
) -> DispatchResult:
    """Wire port materialization and closures, then delegate to core dispatch."""
    del board_info_resolver, board_mutator, query_issue_board_info_fn
    del query_project_item_field_fn, comment_exists_fn, post_comment_fn
    target = automation_config.dispatch_target
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

    return dispatch_agent(
        issue_refs=issue_refs,
        config=config,
        dispatch_target=target,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
    )
