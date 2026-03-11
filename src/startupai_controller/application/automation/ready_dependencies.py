"""Ready-queue dependency guard use case."""

from __future__ import annotations

from typing import Callable

from startupai_controller.domain.models import IssueSnapshot
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
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver=None,
    board_mutator=None,
    gh_runner=None,
    find_unmet_dependencies: Callable[..., list[tuple[str, str]]],
    set_blocked_with_reason: Callable[..., None],
) -> list[str]:
    """Block Ready issues with unmet predecessors and return corrected refs."""
    unmet = find_unmet_dependencies(
        config=config,
        ready_items=ready_items,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        status_resolver=status_resolver,
        project_owner=project_owner,
        project_number=project_number,
    )
    for ref, reason in unmet:
        if not dry_run:
            try:
                set_blocked_with_reason(
                    ref,
                    reason,
                    config,
                    project_owner,
                    project_number,
                    board_info_resolver=board_info_resolver,
                    board_mutator=board_mutator,
                    gh_runner=gh_runner,
                )
            except GhQueryError:
                pass
    return [ref for ref, _ in unmet]
