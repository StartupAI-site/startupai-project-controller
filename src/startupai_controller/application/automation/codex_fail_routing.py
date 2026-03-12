"""Codex review fail routing — move failed issues back to In Progress with handoff."""

from __future__ import annotations

from typing import Callable

from startupai_controller.domain.repair_policy import marker_for as _marker_for
from startupai_controller.domain.scheduling_policy import VALID_EXECUTORS
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.review_state import ReviewStatePort


def apply_codex_fail_routing(
    issue_ref: str,
    route: str,
    checklist: list[str],
    config,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    issue_ref_to_repo_parts_fn: Callable[[str, object], tuple[str, str, int]],
) -> None:
    """Route failed codex review back to In Progress with explicit handoff."""
    del project_owner, project_number
    old_status = review_state_port.get_issue_status(issue_ref) or "NOT_ON_BOARD"
    if old_status == "Review" and not dry_run:
        board_port.set_issue_status(issue_ref, "In Progress")

    if old_status not in {"Review", "In Progress"}:
        return

    if route == "executor":
        executor = review_state_port.get_issue_fields(issue_ref).executor.lower()
        handoff_target = executor if executor in VALID_EXECUTORS else "human"
    elif route in VALID_EXECUTORS:
        handoff_target = route
    else:
        handoff_target = "human"

    if not dry_run:
        board_port.set_issue_field(issue_ref, "Handoff To", handoff_target)

    owner, repo, number = issue_ref_to_repo_parts_fn(issue_ref, config)
    marker = _marker_for("codex-review-fail", issue_ref)
    if review_state_port.comment_exists(f"{owner}/{repo}", number, marker):
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
        board_port.post_issue_comment(f"{owner}/{repo}", number, body)
