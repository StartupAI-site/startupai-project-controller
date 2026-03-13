"""Explicit runtime compatibility adapters for automation wiring.

These helpers preserve legacy board/comment/status fallback behavior while
keeping the major wiring modules focused on composition rather than adapter
wrapping mechanics.
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace
from typing import Any, Callable

from startupai_controller.ports.ready_flow import BoardInfoView


class _DelegatingPort:
    """Delegate unknown attributes to an underlying port implementation."""

    def __init__(self, base: Any) -> None:
        self._base = base

    def __getattr__(self, name: str) -> Any:
        return getattr(self._base, name)


def _split_repo_slug(repo: str) -> tuple[str, str]:
    owner, repo_name = repo.split("/", 1)
    return owner, repo_name


def _supports_keyword_argument(fn: Callable[..., Any], name: str) -> bool:
    """Return True when one compatibility callable accepts a keyword argument."""
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    for parameter in signature.parameters.values():
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            return True
        if parameter.name == name:
            return True
    return False


def _positional_parameter_count(fn: Callable[..., Any]) -> int | None:
    """Return the number of declared positional parameters when introspectable."""
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return None
    return sum(
        parameter.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        for parameter in signature.parameters.values()
    )


def wrap_review_state_port(
    review_state_port,
    *,
    config,
    project_owner: str,
    project_number: int,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., BoardInfoView] | None = None,
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

    resolve_status = status_resolver
    resolve_board_info = board_info_resolver
    comment_exists = comment_exists_fn

    class _CompatReviewStatePort(_DelegatingPort):
        def get_issue_status(self, issue_ref: str):
            if resolve_board_info is not None:
                return resolve_board_info(
                    issue_ref,
                    config,
                    project_owner,
                    project_number,
                ).status
            if resolve_status is not None:
                return resolve_status(
                    issue_ref,
                    config,
                    project_owner,
                    project_number,
                )
            return self._base.get_issue_status(issue_ref)

        def comment_exists(self, repo: str, issue_number: int, marker: str) -> bool:
            if comment_exists is not None:
                owner, repo_name = _split_repo_slug(repo)
                kwargs = (
                    {"gh_runner": gh_runner}
                    if _supports_keyword_argument(comment_exists, "gh_runner")
                    else {}
                )
                positional_count = _positional_parameter_count(comment_exists)
                if positional_count is not None and positional_count <= 3:
                    return comment_exists(
                        repo,
                        issue_number,
                        marker,
                        **kwargs,
                    )
                return comment_exists(
                    owner,
                    repo_name,
                    issue_number,
                    marker,
                    **kwargs,
                )
            return self._base.comment_exists(repo, issue_number, marker)

    return _CompatReviewStatePort(review_state_port)


def wrap_board_port(
    board_port,
    *,
    config,
    project_owner: str,
    project_number: int,
    board_info_resolver: Callable[..., BoardInfoView] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
):
    """Overlay legacy board mutation/comment seams onto a typed board port."""
    if board_port is None or (board_mutator is None and comment_poster is None):
        return board_port

    resolve_board_info = board_info_resolver
    mutate_board = board_mutator
    post_comment = comment_poster

    class _CompatBoardMutationPort(_DelegatingPort):
        def set_issue_status(self, issue_ref: str, status: str) -> None:
            if mutate_board is not None and resolve_board_info is not None:
                info = resolve_board_info(
                    issue_ref,
                    config,
                    project_owner,
                    project_number,
                )
                mutate_board(info.project_id, info.item_id, status)
                return
            self._base.set_issue_status(issue_ref, status)

        def post_issue_comment(self, repo: str, issue_number: int, body: str) -> None:
            if post_comment is not None:
                owner, repo_name = _split_repo_slug(repo)
                kwargs = (
                    {"gh_runner": gh_runner}
                    if _supports_keyword_argument(post_comment, "gh_runner")
                    else {}
                )
                positional_count = _positional_parameter_count(post_comment)
                if positional_count is not None and positional_count <= 3:
                    post_comment(
                        repo,
                        issue_number,
                        body,
                        **kwargs,
                    )
                else:
                    post_comment(
                        owner,
                        repo_name,
                        issue_number,
                        body,
                        **kwargs,
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
    board_info_resolver: Callable[..., BoardInfoView] | None = None,
    board_mutator: Callable[..., None] | None = None,
):
    """Overlay legacy board-mutator behavior on a typed board port."""
    if board_mutator is None or board_info_resolver is None:
        return board_port

    resolve_board_info = board_info_resolver
    mutate_board = board_mutator

    if board_port is None:
        board_port = SimpleNamespace()

    class _CompatBoardMutationPort(_DelegatingPort):
        def set_issue_status(self, issue_ref: str, status: str) -> None:
            info = resolve_board_info(
                issue_ref,
                config,
                project_owner,
                project_number,
            )
            mutate_board(info.project_id, info.item_id, status)

    return _CompatBoardMutationPort(board_port)


def wrap_status_transition_review_state_port(
    review_state_port,
    *,
    config,
    project_owner: str,
    project_number: int,
    board_info_resolver: Callable[..., BoardInfoView] | None = None,
):
    """Overlay legacy board-info status reads on a typed review-state port."""
    if board_info_resolver is None:
        return review_state_port

    resolve_board_info = board_info_resolver

    if review_state_port is None:
        review_state_port = SimpleNamespace()

    class _CompatReviewStatePort(_DelegatingPort):
        def get_issue_status(self, issue_ref: str):
            info = resolve_board_info(
                issue_ref,
                config,
                project_owner,
                project_number,
            )
            return info.status

    return _CompatReviewStatePort(review_state_port)
