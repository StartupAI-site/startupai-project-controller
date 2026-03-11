"""Dispatch use case for claimed in-progress issues."""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.domain.models import DispatchResult
from startupai_controller.domain.repair_policy import marker_for as _marker_for
from startupai_controller.domain.scheduling_policy import VALID_EXECUTORS
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
)

if TYPE_CHECKING:
    from startupai_controller.ports.board_mutations import BoardMutationPort as _BoardMutationPort
    from startupai_controller.ports.review_state import ReviewStatePort as _ReviewStatePort


def dispatch_agent(
    *,
    issue_refs: list[str],
    config: CriticalPathConfig,
    dispatch_target: str,
    dry_run: bool = False,
    gh_runner=None,
    resolve_issue_status: Callable[[str], str | None],
    resolve_executor: Callable[[str], str],
    comment_exists: Callable[..., bool],
    post_comment: Callable[..., None],
) -> DispatchResult:
    """Dispatch eligible In Progress issues according to dispatch target."""
    result = DispatchResult()

    for issue_ref in issue_refs:
        owner, repo, number = _resolve_issue_coordinates(issue_ref, config)
        status = resolve_issue_status(issue_ref)
        if status != "In Progress":
            result.skipped.append((issue_ref, f"status={status or 'unknown'}"))
            continue

        executor = resolve_executor(issue_ref).strip().lower()
        if executor not in VALID_EXECUTORS:
            result.skipped.append((issue_ref, f"executor={executor or 'unset'}"))
            continue

        marker = _marker_for("dispatch-agent", issue_ref)
        if comment_exists(owner, repo, number, marker, gh_runner=gh_runner):
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
                post_comment(owner, repo, number, body, gh_runner=gh_runner)
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
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., object] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helpers
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    query_issue_board_info_fn: Callable[..., object] | None = None,
    query_project_item_field_fn: Callable[..., str] | None = None,
    comment_exists_fn: Callable[..., bool] | None = None,
    post_comment_fn: Callable[..., None] | None = None,
) -> DispatchResult:
    """Wire port materialization and closures, then delegate to core dispatch."""
    target = automation_config.dispatch_target
    use_ports = (board_info_resolver is None) or review_state_port is not None or board_port is not None
    if use_ports:
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
    resolve_info = board_info_resolver or query_issue_board_info_fn

    def _resolve_status(issue_ref: str) -> str | None:
        if use_ports:
            return review_state_port.get_issue_status(issue_ref)
        return resolve_info(
            issue_ref,
            config,
            project_owner,
            project_number,
        ).status

    def _resolve_executor(issue_ref: str) -> str:
        if use_ports:
            return review_state_port.get_issue_fields(issue_ref).executor
        return query_project_item_field_fn(
            issue_ref,
            "Executor",
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        )

    def _post_dispatch_comment(
        owner: str,
        repo: str,
        number: int,
        body: str,
        *,
        gh_runner: Callable[..., str] | None = None,
    ) -> None:
        if use_ports:
            board_port.post_issue_comment(f"{owner}/{repo}", number, body)
            return
        post_comment_fn(
            owner,
            repo,
            number,
            body,
            project_owner=project_owner,
            project_number=project_number,
            config=config,
            gh_runner=gh_runner,
        )

    return dispatch_agent(
        issue_refs=issue_refs,
        config=config,
        dispatch_target=target,
        dry_run=dry_run,
        gh_runner=gh_runner,
        resolve_issue_status=_resolve_status,
        resolve_executor=_resolve_executor,
        comment_exists=comment_exists_fn,
        post_comment=_post_dispatch_comment,
    )
