"""Board field mutation helpers shared across automation use cases."""

from __future__ import annotations

from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.review_state import ReviewStatePort


def set_handoff_target(
    issue_ref: str,
    target: str,
    config,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
) -> None:
    """Set the board Handoff To field for an issue."""
    del config, project_owner, project_number
    current_status = review_state_port.get_issue_status(issue_ref)
    if current_status in {None, "NOT_ON_BOARD"}:
        return
    if dry_run:
        return
    board_port.set_issue_field(issue_ref, "Handoff To", target)
