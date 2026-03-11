"""Stale in-progress audit use case."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable

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

if TYPE_CHECKING:
    from startupai_controller.ports.board_mutations import BoardMutationPort as _BoardMutationPort
    from startupai_controller.ports.review_state import ReviewStatePort as _ReviewStatePort


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
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., object] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helpers
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    list_project_items_by_status_fn: Callable[..., list] | None = None,
    query_issue_board_info_fn: Callable[..., object] | None = None,
    set_single_select_field_fn: Callable[..., None] | None = None,
    comment_exists_fn: Callable[..., bool] | None = None,
    post_comment_fn: Callable[..., None] | None = None,
    query_project_item_field_fn: Callable[..., str] | None = None,
    query_issue_updated_at_fn: Callable[..., datetime] | None = None,
    snapshot_to_issue_ref_fn: Callable[..., str | None] | None = None,
) -> list[str]:
    """Wire port materialization and closures, then delegate to core audit."""
    use_ports = (
        board_info_resolver is None
    ) or review_state_port is not None or board_port is not None
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
        in_progress_items = review_state_port.list_issues_by_status("In Progress")
    else:
        in_progress_items = list_project_items_by_status_fn(
            "In Progress", project_owner, project_number, gh_runner=gh_runner
        )
    checker = comment_checker or comment_exists_fn
    resolve_info = board_info_resolver or query_issue_board_info_fn
    info_cache: dict[str, object] = {}

    def _resolve_status(ref: str) -> str:
        if use_ports:
            return review_state_port.get_issue_status(ref)
        info = info_cache.get(ref)
        if info is None:
            info = resolve_info(ref, config, project_owner, project_number)
            info_cache[ref] = info
        return info.status

    def _set_handoff(ref: str) -> None:
        info = info_cache.get(ref)
        if info is None:
            info = resolve_info(ref, config, project_owner, project_number)
            info_cache[ref] = info
        set_single_select_field_fn(
            info.project_id,
            info.item_id,
            "Handoff To",
            "claude",
            project_owner=project_owner,
            project_number=project_number,
            config=config,
            gh_runner=gh_runner,
        )

    def _post_audit_comment(
        owner: str,
        repo: str,
        number: int,
        body: str,
        *,
        gh_runner: Callable[..., str] | None = None,
    ) -> None:
        if comment_poster is not None:
            comment_poster(owner, repo, number, body, gh_runner=gh_runner)
            return
        post_comment_fn(
            owner,
            repo,
            number,
            body,
            board_port=board_port,
            project_owner=project_owner,
            project_number=project_number,
            config=config,
            gh_runner=gh_runner,
        )

    return audit_in_progress(
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        in_progress_items=in_progress_items,
        use_ports=use_ports,
        review_state_port=review_state_port,
        board_port=board_port,
        max_age_hours=max_age_hours,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        dry_run=dry_run,
        gh_runner=gh_runner,
        resolve_issue_status=_resolve_status,
        query_project_item_field=query_project_item_field_fn,
        query_issue_updated_at=query_issue_updated_at_fn,
        comment_exists=checker,
        post_comment=_post_audit_comment,
        set_handoff_target=_set_handoff,
        snapshot_to_issue_ref=snapshot_to_issue_ref_fn,
    )
