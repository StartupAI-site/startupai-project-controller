"""Blocker propagation use case — post advisory comments on successors."""

from __future__ import annotations

import sys
from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.review_state import ReviewStatePort

from startupai_controller.domain.repair_policy import marker_for as _marker_for
from startupai_controller.validate_critical_path_promotion import (
    direct_successors,
    parse_issue_ref,
)


def propagate_blocker(
    issue_ref: str | None,
    config,
    this_repo_prefix: str | None,
    project_owner: str,
    project_number: int,
    *,
    sweep_blocked: bool = False,
    all_prefixes: bool = False,
    dry_run: bool = False,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
) -> list[str]:
    """Propagate blocker info to successors. Returns list of commented refs."""
    commented: list[str] = []

    if sweep_blocked:
        blocked_items = review_state_port.list_issues_by_status("Blocked")
        for snapshot in blocked_items:
            ref = snapshot.issue_ref

            if not all_prefixes and this_repo_prefix:
                parsed = parse_issue_ref(ref)
                if parsed.prefix != this_repo_prefix:
                    continue

            blocked_reason = review_state_port.get_issue_fields(ref).blocked_reason
            if not blocked_reason:
                continue

            new_comments = _propagate_single_blocker(
                ref,
                blocked_reason,
                config,
                review_state_port=review_state_port,
                board_port=board_port,
                dry_run=dry_run,
            )
            commented.extend(new_comments)
    else:
        # Single-issue mode
        if issue_ref is None:
            return commented

        blocked_reason = review_state_port.get_issue_fields(issue_ref).blocked_reason
        if not blocked_reason:
            return commented

        commented = _propagate_single_blocker(
            issue_ref,
            blocked_reason,
            config,
            review_state_port=review_state_port,
            board_port=board_port,
            dry_run=dry_run,
        )

    return commented


def _propagate_single_blocker(
    issue_ref: str,
    blocked_reason: str,
    config,
    *,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    dry_run: bool = False,
) -> list[str]:
    """Post advisory comments on successors of a single blocked issue."""
    commented: list[str] = []
    successors = direct_successors(config, issue_ref)

    for successor_ref in sorted(successors):
        marker = _marker_for("blocker", issue_ref)
        succ_owner, succ_repo, succ_number = _resolve_issue_coordinates(
            successor_ref,
            config,
        )

        if review_state_port.comment_exists(
            f"{succ_owner}/{succ_repo}",
            succ_number,
            marker,
        ):
            continue

        if not dry_run:
            body = (
                f"{marker}\n"
                f"**Upstream blocker**: `{issue_ref}` is Blocked "
                f"(reason: {blocked_reason}).\n"
                f"This may affect `{successor_ref}`."
            )
            try:
                board_port.post_issue_comment(
                    f"{succ_owner}/{succ_repo}",
                    succ_number,
                    body,
                )
            except Exception:
                # Cross-repo comment failure is non-fatal — log and continue
                print(
                    f"WARNING: Failed posting blocker comment on " f"{successor_ref}",
                    file=sys.stderr,
                )
                continue

        commented.append(successor_ref)

    return commented
