"""Codex review fail routing — move failed issues back to In Progress with handoff."""

from __future__ import annotations

from typing import Callable

from startupai_controller.domain.repair_policy import marker_for as _marker_for
from startupai_controller.domain.scheduling_policy import VALID_EXECUTORS


def apply_codex_fail_routing(
    issue_ref: str,
    route: str,
    checklist: list[str],
    config,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port=None,
    board_port=None,
    board_info_resolver: Callable[..., object] | None = None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helpers
    transition_issue_status_fn: Callable[..., tuple[bool, str]] | None = None,
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    query_project_item_field_fn: Callable[..., str] | None = None,
    issue_ref_to_repo_parts_fn: Callable[..., tuple[str, str, int]] | None = None,
    comment_exists_fn: Callable[..., bool] | None = None,
) -> None:
    """Route failed codex review back to In Progress with explicit handoff."""
    if transition_issue_status_fn is None:
        return
    _changed, old_status = transition_issue_status_fn(
        issue_ref,
        {"Review"},
        "In Progress",
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        gh_runner=gh_runner,
    )

    if old_status not in {"Review", "In Progress"}:
        return

    if route == "executor":
        if review_state_port is not None or board_info_resolver is None:
            if default_review_state_port_fn is not None:
                review_state_port = review_state_port or default_review_state_port_fn(
                    project_owner,
                    project_number,
                    config,
                    gh_runner=gh_runner,
                )
            executor = review_state_port.get_issue_fields(issue_ref).executor.lower()
        else:
            if query_project_item_field_fn is None:
                executor = "human"
            else:
                executor = query_project_item_field_fn(
                    issue_ref,
                    "Executor",
                    config,
                    project_owner,
                    project_number,
                    gh_runner=gh_runner,
                ).lower()
        handoff_target = executor if executor in VALID_EXECUTORS else "human"
    elif route in VALID_EXECUTORS:
        handoff_target = route
    else:
        handoff_target = "human"

    if not dry_run:
        if default_board_mutation_port_fn is not None:
            board_port = board_port or default_board_mutation_port_fn(
                project_owner,
                project_number,
                config,
                gh_runner=gh_runner,
            )
        if board_port is not None:
            board_port.set_issue_field(issue_ref, "Handoff To", handoff_target)

    if issue_ref_to_repo_parts_fn is None:
        return
    owner, repo, number = issue_ref_to_repo_parts_fn(issue_ref, config)
    marker = _marker_for("codex-review-fail", issue_ref)
    if comment_exists_fn is not None and comment_exists_fn(
        owner, repo, number, marker, gh_runner=gh_runner
    ):
        return

    checklist_text = ""
    if checklist:
        checklist_text = "\n".join(f"- [ ] {item}" for item in checklist)
    else:
        checklist_text = "- [ ] Address Codex review findings"

    body = (
        f"{marker}\n"
        f"Codex review verdict: `fail`\n"
        f"Route: `{route}` (handoff: `{handoff_target}`)\n\n"
        "Required fixes:\n"
        f"{checklist_text}"
    )
    if not dry_run:
        if default_board_mutation_port_fn is not None:
            board_port = board_port or default_board_mutation_port_fn(
                project_owner,
                project_number,
                config,
                gh_runner=gh_runner,
            )
        if board_port is not None:
            board_port.post_issue_comment(f"{owner}/{repo}", number, body)
