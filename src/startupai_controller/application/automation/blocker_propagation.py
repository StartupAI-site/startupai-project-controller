"""Blocker propagation use case — post advisory comments on successors."""

from __future__ import annotations

import sys
from typing import Callable

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
    review_state_port=None,
    board_port=None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    board_info_resolver=None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helpers
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    list_project_items_by_status_fn: Callable[..., list] | None = None,
    snapshot_to_issue_ref_fn: Callable[..., str | None] | None = None,
    query_project_item_field_fn: Callable[..., str] | None = None,
    comment_exists_fn: Callable[..., bool] | None = None,
    post_comment_fn: Callable[..., None] | None = None,
    issue_ref_to_repo_parts_fn: Callable[..., tuple[str, str, int]] | None = None,
) -> list[str]:
    """Propagate blocker info to successors. Returns list of commented refs."""
    check_comment = comment_checker or comment_exists_fn
    post_comment = comment_poster or post_comment_fn
    commented: list[str] = []

    use_ports = (board_info_resolver is None) or review_state_port is not None or board_port is not None
    if use_ports:
        if review_state_port is None and default_review_state_port_fn is not None:
            review_state_port = default_review_state_port_fn(
                project_owner,
                project_number,
                config,
                gh_runner=gh_runner,
            )
        if comment_poster is None and board_port is None and default_board_mutation_port_fn is not None:
            board_port = default_board_mutation_port_fn(
                project_owner,
                project_number,
                config,
                gh_runner=gh_runner,
            )

    if sweep_blocked:
        # Sweep mode: scan all Blocked items
        if use_ports and review_state_port is not None:
            blocked_items = review_state_port.list_issues_by_status("Blocked")
        elif list_project_items_by_status_fn is not None:
            blocked_items = list_project_items_by_status_fn(
                "Blocked", project_owner, project_number, gh_runner=gh_runner
            )
        else:
            blocked_items = []
        for snapshot in blocked_items:
            if use_ports:
                ref = snapshot.issue_ref
            else:
                if snapshot_to_issue_ref_fn is not None:
                    ref = snapshot_to_issue_ref_fn(snapshot.issue_ref, config.issue_prefixes)
                else:
                    continue
                if ref is None:
                    continue

            if not all_prefixes and this_repo_prefix:
                parsed = parse_issue_ref(ref)
                if parsed.prefix != this_repo_prefix:
                    continue

            if use_ports and review_state_port is not None:
                blocked_reason = review_state_port.get_issue_fields(
                    ref
                ).blocked_reason
            elif query_project_item_field_fn is not None:
                blocked_reason = query_project_item_field_fn(
                    ref,
                    "Blocked Reason",
                    config,
                    project_owner,
                    project_number,
                    gh_runner=gh_runner,
                )
            else:
                continue
            if not blocked_reason:
                continue

            new_comments = _propagate_single_blocker(
                ref,
                blocked_reason,
                config,
                check_comment=check_comment,
                post_comment=post_comment,
                board_port=board_port,
                project_owner=project_owner,
                project_number=project_number,
                gh_runner=gh_runner,
                dry_run=dry_run,
                issue_ref_to_repo_parts_fn=issue_ref_to_repo_parts_fn,
            )
            commented.extend(new_comments)
    else:
        # Single-issue mode
        if issue_ref is None:
            return commented

        if use_ports and review_state_port is not None:
            blocked_reason = review_state_port.get_issue_fields(
                issue_ref
            ).blocked_reason
        elif query_project_item_field_fn is not None:
            blocked_reason = query_project_item_field_fn(
                issue_ref,
                "Blocked Reason",
                config,
                project_owner,
                project_number,
                gh_runner=gh_runner,
            )
        else:
            blocked_reason = ""
        if not blocked_reason:
            return commented

        commented = _propagate_single_blocker(
            issue_ref,
            blocked_reason,
            config,
            check_comment=check_comment,
            post_comment=post_comment,
            board_port=board_port,
            project_owner=project_owner,
            project_number=project_number,
            gh_runner=gh_runner,
            dry_run=dry_run,
            issue_ref_to_repo_parts_fn=issue_ref_to_repo_parts_fn,
        )

    return commented


def _propagate_single_blocker(
    issue_ref: str,
    blocked_reason: str,
    config,
    *,
    check_comment: Callable[..., bool] | None,
    post_comment: Callable[..., None] | None,
    board_port=None,
    project_owner: str = "",
    project_number: int = 0,
    gh_runner: Callable[..., str] | None = None,
    dry_run: bool = False,
    issue_ref_to_repo_parts_fn: Callable[..., tuple[str, str, int]] | None = None,
) -> list[str]:
    """Post advisory comments on successors of a single blocked issue."""
    commented: list[str] = []
    successors = direct_successors(config, issue_ref)

    for successor_ref in sorted(successors):
        marker = _marker_for("blocker", issue_ref)
        if issue_ref_to_repo_parts_fn is not None:
            succ_owner, succ_repo, succ_number = issue_ref_to_repo_parts_fn(
                successor_ref, config
            )
        else:
            continue

        if check_comment is not None and check_comment(succ_owner, succ_repo, succ_number, marker):
            continue

        if not dry_run:
            body = (
                f"{marker}\n"
                f"**Upstream blocker**: `{issue_ref}` is Blocked "
                f"(reason: {blocked_reason}).\n"
                f"This may affect `{successor_ref}`."
            )
            try:
                if board_port is not None:
                    board_port.post_issue_comment(
                        f"{succ_owner}/{succ_repo}",
                        succ_number,
                        body,
                    )
                elif post_comment is not None:
                    post_comment(succ_owner, succ_repo, succ_number, body)
            except Exception:
                # Cross-repo comment failure is non-fatal — log and continue
                print(
                    f"WARNING: Failed posting blocker comment on "
                    f"{successor_ref}",
                    file=sys.stderr,
                )
                continue

        commented.append(successor_ref)

    return commented
