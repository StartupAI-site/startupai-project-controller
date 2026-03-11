"""Stale in-progress audit use case."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.domain.models import IssueSnapshot
from startupai_controller.domain.repair_policy import marker_for as _marker_for
from startupai_controller.ports.board_mutations import BoardMutationPort
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
    in_progress_items: list[IssueSnapshot],
    use_ports: bool,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    max_age_hours: int = 24,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    dry_run: bool = False,
    gh_runner=None,
    resolve_issue_status: Callable[[str], str],
    query_project_item_field: Callable[..., str],
    query_issue_updated_at: Callable[..., datetime],
    comment_exists: Callable[..., bool],
    post_comment: Callable[..., None],
    set_handoff_target: Callable[[str], None],
    snapshot_to_issue_ref: Callable[[str, dict[str, str]], str | None],
) -> list[str]:
    """Escalate stale In Progress issues with no linked PR."""
    now = datetime.now(timezone.utc)
    stale_refs: list[str] = []

    for snapshot in in_progress_items:
        if use_ports:
            ref = snapshot.issue_ref
        else:
            ref = snapshot_to_issue_ref(snapshot.issue_ref, config.issue_prefixes)
            if ref is None:
                continue
        if not all_prefixes and this_repo_prefix:
            parsed = parse_issue_ref(ref)
            if parsed.prefix != this_repo_prefix:
                continue

        pr_field = query_project_item_field(
            ref,
            "PR",
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        ).strip()
        if pr_field and pr_field.lower() not in {"n/a", "none"}:
            continue

        owner, repo, number = _resolve_issue_coordinates(ref, config)
        updated_at = query_issue_updated_at(
            owner,
            repo,
            number,
            gh_runner=gh_runner,
        )
        age_hours = (now - updated_at).total_seconds() / 3600
        if age_hours < max_age_hours:
            continue

        stale_refs.append(ref)
        if dry_run:
            continue

        marker = _marker_for("stale-in-progress", ref)
        current_status = resolve_issue_status(ref)
        if current_status in {None, "NOT_ON_BOARD"}:
            continue

        try:
            if use_ports:
                board_port.set_issue_field(ref, "Handoff To", "claude")
            else:
                set_handoff_target(ref)
        except GhQueryError:
            pass

        if not comment_exists(owner, repo, number, marker, gh_runner=gh_runner):
            body = (
                f"{marker}\n"
                f"Stale `In Progress` for ~{int(age_hours)}h with no linked PR. "
                "Escalating handoff to `claude` (board field `Handoff To` updated)."
            )
            try:
                post_comment(owner, repo, number, body, gh_runner=gh_runner)
            except GhQueryError:
                pass

    return stale_refs
