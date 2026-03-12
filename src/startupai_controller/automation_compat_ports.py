"""Explicit runtime compatibility adapters for automation wiring.

These helpers preserve legacy board/comment/status fallback behavior while
keeping the major wiring modules focused on composition rather than adapter
wrapping mechanics.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Callable


class _DelegatingPort:
    """Delegate unknown attributes to an underlying port implementation."""

    def __init__(self, base) -> None:
        self._base = base

    def __getattr__(self, name: str):
        return getattr(self._base, name)


def _split_repo_slug(repo: str) -> tuple[str, str]:
    owner, repo_name = repo.split("/", 1)
    return owner, repo_name


def wrap_review_state_port(
    review_state_port,
    *,
    config,
    project_owner: str,
    project_number: int,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., object] | None = None,
    comment_exists_fn: Callable[..., bool] | None = None,
    gh_runner: Callable[..., str] | None = None,
):
    """Overlay legacy status/comment seams onto a typed review-state port."""
    if review_state_port is None or (
        status_resolver is None
        and board_info_resolver is None
        and comment_exists_fn is None
    ):
        return review_state_port

    class _CompatReviewStatePort(_DelegatingPort):
        def get_issue_status(self, issue_ref: str):
            if board_info_resolver is not None:
                return board_info_resolver(
                    issue_ref,
                    config,
                    project_owner,
                    project_number,
                ).status
            if status_resolver is not None:
                return status_resolver(
                    issue_ref,
                    config,
                    project_owner,
                    project_number,
                )
            return self._base.get_issue_status(issue_ref)

        def comment_exists(self, repo: str, issue_number: int, marker: str) -> bool:
            if comment_exists_fn is not None:
                owner, repo_name = _split_repo_slug(repo)
                return comment_exists_fn(
                    owner,
                    repo_name,
                    issue_number,
                    marker,
                    gh_runner=gh_runner,
                )
            return self._base.comment_exists(repo, issue_number, marker)

    return _CompatReviewStatePort(review_state_port)


def wrap_board_port(
    board_port,
    *,
    config,
    project_owner: str,
    project_number: int,
    board_info_resolver: Callable[..., object] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
):
    """Overlay legacy board mutation/comment seams onto a typed board port."""
    if board_port is None or (board_mutator is None and comment_poster is None):
        return board_port

    class _CompatBoardMutationPort(_DelegatingPort):
        def set_issue_status(self, issue_ref: str, status: str) -> None:
            if board_mutator is not None and board_info_resolver is not None:
                info = board_info_resolver(
                    issue_ref,
                    config,
                    project_owner,
                    project_number,
                )
                board_mutator(info.project_id, info.item_id, status)
                return
            self._base.set_issue_status(issue_ref, status)

        def post_issue_comment(self, repo: str, issue_number: int, body: str) -> None:
            if comment_poster is not None:
                owner, repo_name = _split_repo_slug(repo)
                comment_poster(
                    owner,
                    repo_name,
                    issue_number,
                    body,
                    gh_runner=gh_runner,
                )
                return
            self._base.post_issue_comment(repo, issue_number, body)

    return _CompatBoardMutationPort(board_port)


def wrap_status_transition_board_port(
    board_port,
    *,
    config,
    project_owner: str,
    project_number: int,
    board_info_resolver: Callable[..., object] | None = None,
    board_mutator: Callable[..., None] | None = None,
):
    """Overlay legacy board-mutator behavior on a typed board port."""
    if board_mutator is None or board_info_resolver is None:
        return board_port

    if board_port is None:
        board_port = SimpleNamespace()

    class _CompatBoardMutationPort(_DelegatingPort):
        def set_issue_status(self, issue_ref: str, status: str) -> None:
            info = board_info_resolver(
                issue_ref,
                config,
                project_owner,
                project_number,
            )
            board_mutator(info.project_id, info.item_id, status)

    return _CompatBoardMutationPort(board_port)


def wrap_status_transition_review_state_port(
    review_state_port,
    *,
    config,
    project_owner: str,
    project_number: int,
    board_info_resolver: Callable[..., object] | None = None,
):
    """Overlay legacy board-info status reads on a typed review-state port."""
    if board_info_resolver is None:
        return review_state_port

    if review_state_port is None:
        review_state_port = SimpleNamespace()

    class _CompatReviewStatePort(_DelegatingPort):
        def get_issue_status(self, issue_ref: str):
            info = board_info_resolver(
                issue_ref,
                config,
                project_owner,
                project_number,
            )
            return info.status

    return _CompatReviewStatePort(review_state_port)
