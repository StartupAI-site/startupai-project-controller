"""Ready-queue dependency guard use case."""

from __future__ import annotations

from typing import Callable

from startupai_controller.domain.models import IssueSnapshot
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.application.automation.ready_claim import (
    _set_blocked_with_reason,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
)


def enforce_ready_dependency_guard(
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    ready_items: list[IssueSnapshot],
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    dry_run: bool = False,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    find_unmet_dependencies: Callable[..., list[tuple[str, str]]],
) -> list[str]:
    """Block Ready issues with unmet predecessors and return corrected refs."""
    unmet = find_unmet_dependencies(
        config=config,
        ready_items=ready_items,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        status_resolver=review_state_port.get_issue_status,
        project_owner=project_owner,
        project_number=project_number,
    )
    for ref, reason in unmet:
        if not dry_run:
            try:
                _set_blocked_with_reason(
                    ref,
                    reason,
                    config,
                    project_owner,
                    project_number,
                    dry_run=dry_run,
                    review_state_port=review_state_port,
                    board_port=board_port,
                )
            except GhQueryError:
                pass
    return [ref for ref, _ in unmet]
