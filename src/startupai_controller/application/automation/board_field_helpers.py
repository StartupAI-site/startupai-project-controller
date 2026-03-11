"""Board field mutation helpers shared across automation use cases."""

from __future__ import annotations

from typing import Callable


def set_handoff_target(
    issue_ref: str,
    target: str,
    config,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port=None,
    board_port=None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helpers
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
) -> None:
    """Set the board Handoff To field for an issue."""
    if default_review_state_port_fn is not None:
        review_state_port = review_state_port or default_review_state_port_fn(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    if review_state_port is None:
        return
    current_status = review_state_port.get_issue_status(issue_ref)
    if current_status in {None, "NOT_ON_BOARD"}:
        return
    if dry_run:
        return
    if default_board_mutation_port_fn is not None:
        board_port = board_port or default_board_mutation_port_fn(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    if board_port is not None:
        board_port.set_issue_field(issue_ref, "Handoff To", target)
