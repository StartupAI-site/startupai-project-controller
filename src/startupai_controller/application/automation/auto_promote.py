"""Automatic successor-promotion use case."""

from __future__ import annotations

from typing import Callable

from startupai_controller.domain.models import PromotionResult
from startupai_controller.domain.repair_policy import (
    MARKER_PREFIX,
    marker_for as _marker_for,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    direct_successors,
    parse_issue_ref,
)


def auto_promote_successors(
    *,
    issue_ref: str,
    config: CriticalPathConfig,
    this_repo_prefix: str,
    project_owner: str,
    project_number: int,
    automation_config,
    dry_run: bool = False,
    status_resolver=None,
    board_info_resolver=None,
    board_mutator=None,
    gh_runner=None,
    promote_to_ready: Callable[..., tuple[int, str]],
    controller_owned_resolver: Callable[[str], bool],
    comment_exists: Callable[..., bool],
    post_cross_repo_comment: Callable[..., None],
    resolve_issue_parts: Callable[[str, CriticalPathConfig], tuple[str, str, int]],
    new_handoff_job_id: Callable[[str, str], str],
) -> PromotionResult:
    """Promote eligible same-repo successors and bridge cross-repo ones."""
    result = PromotionResult()
    successors = direct_successors(config, issue_ref)

    if not successors:
        return result

    for successor_ref in sorted(successors):
        parsed = parse_issue_ref(successor_ref)

        if parsed.prefix == this_repo_prefix:
            code, output = promote_to_ready(
                issue_ref=successor_ref,
                config=config,
                project_owner=project_owner,
                project_number=project_number,
                dry_run=dry_run,
                status_resolver=status_resolver,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                controller_owned_resolver=controller_owned_resolver,
            )
            if code == 0:
                result.promoted.append(successor_ref)
            else:
                result.skipped.append((successor_ref, output))
            continue

        job_id = new_handoff_job_id(issue_ref, successor_ref)
        marker = _marker_for("promote-bridge", successor_ref)
        succ_owner, succ_repo, succ_number = resolve_issue_parts(
            successor_ref,
            config,
        )

        if comment_exists(succ_owner, succ_repo, succ_number, marker):
            result.skipped.append((successor_ref, "Bridge comment already exists"))
            continue

        if not dry_run:
            handoff_marker = f"<!-- {MARKER_PREFIX}:handoff:job={job_id} -->"
            body = (
                f"{marker}\n"
                f"{handoff_marker}\n"
                f"**Auto-promote candidate**: `{successor_ref}` may be "
                f"eligible for Ready now that `{issue_ref}` is Done.\n\n"
                "Run from the appropriate repo:\n"
                f"```\nmake promote-ready ISSUE={successor_ref}\n```"
            )
            post_cross_repo_comment(
                succ_owner,
                succ_repo,
                succ_number,
                body,
                gh_runner=gh_runner,
            )

        result.cross_repo_pending.append(successor_ref)
        result.handoff_jobs.append(job_id)

    return result
