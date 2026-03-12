"""Dispatch use case for claimed in-progress issues."""

from startupai_controller.board_graph import _resolve_issue_coordinates
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
