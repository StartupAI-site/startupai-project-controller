"""Stale in-progress audit use case."""

from __future__ import annotations

from datetime import datetime, timezone

from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.domain.models import IssueSnapshot
from startupai_controller.domain.repair_policy import marker_for as _marker_for
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
    parse_issue_ref,
)


def audit_in_progress(
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    pr_port: PullRequestPort,
    max_age_hours: int = 24,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    dry_run: bool = False,
) -> list[str]:
    """Escalate stale In Progress issues with no linked PR."""
    now = datetime.now(timezone.utc)
    stale_refs: list[str] = []
    in_progress_items = review_state_port.list_issues_by_status("In Progress")

    for snapshot in in_progress_items:
        ref = snapshot.issue_ref
        if not all_prefixes and this_repo_prefix:
            parsed = parse_issue_ref(ref)
            if parsed.prefix != this_repo_prefix:
                continue

        if pr_port.list_open_prs_for_issue(config.issue_prefixes[parse_issue_ref(ref).prefix], _resolve_issue_coordinates(ref, config)[2]):
            continue

        owner, repo, number = _resolve_issue_coordinates(ref, config)
        updated_at = review_state_port.issue_updated_at(f"{owner}/{repo}", number)
        if updated_at is None:
            continue
        age_hours = (now - updated_at).total_seconds() / 3600
        if age_hours < max_age_hours:
            continue

        stale_refs.append(ref)
        if dry_run:
            continue

        marker = _marker_for("stale-in-progress", ref)
        current_status = review_state_port.get_issue_status(ref)
        if current_status in {None, "NOT_ON_BOARD"}:
            continue

        try:
            board_port.set_issue_field(ref, "Handoff To", "claude")
        except GhQueryError:
            pass

        if not review_state_port.comment_exists(f"{owner}/{repo}", number, marker):
            body = (
                f"{marker}\n"
                f"Stale `In Progress` for ~{int(age_hours)}h with no linked PR. "
                "Escalating handoff to `claude` (board field `Handoff To` updated)."
            )
            try:
                board_port.post_issue_comment(f"{owner}/{repo}", number, body)
            except GhQueryError:
                pass

    return stale_refs


# ---------------------------------------------------------------------------
# Wiring entry-point (port materialisation + closure assembly)
# ---------------------------------------------------------------------------


def wire_audit_in_progress(
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    max_age_hours: int = 24,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    dry_run: bool = False,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    pr_port: PullRequestPort | None = None,
    gh_runner=None,
    board_info_resolver=None,
    comment_checker=None,
    comment_poster=None,
    # Injected board-automation helpers
    default_review_state_port_fn,
    default_board_mutation_port_fn,
    default_pr_port_fn,
    list_project_items_by_status_fn=None,
    query_issue_board_info_fn=None,
    set_single_select_field_fn=None,
    comment_exists_fn=None,
    post_comment_fn=None,
    query_project_item_field_fn=None,
    query_issue_updated_at_fn=None,
    snapshot_to_issue_ref_fn=None,
) -> list[str]:
    """Wire port materialization and closures, then delegate to core audit."""
    del board_info_resolver, comment_checker, comment_poster, list_project_items_by_status_fn
    del query_issue_board_info_fn, set_single_select_field_fn, comment_exists_fn
    del post_comment_fn, query_project_item_field_fn, query_issue_updated_at_fn
    del snapshot_to_issue_ref_fn
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
    pr_port = pr_port or default_pr_port_fn(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )

    return audit_in_progress(
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        review_state_port=review_state_port,
        board_port=board_port,
        pr_port=pr_port,
        max_age_hours=max_age_hours,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        dry_run=dry_run,
    )
