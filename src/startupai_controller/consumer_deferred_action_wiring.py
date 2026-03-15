"""Shell-facing deferred-action wiring extracted from control-plane rescue."""

from __future__ import annotations

from typing import Callable

import startupai_controller.consumer_automation_bridge as _automation_bridge
import startupai_controller.consumer_board_state_helpers as _board_state_helpers
import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller import (
    consumer_deferred_action_helpers as _deferred_action_helpers,
)
from startupai_controller.control_plane_runtime import (
    _clear_degraded,
    _record_successful_github_mutation,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

BoardInfoResolver = _deferred_action_helpers.BoardInfoResolver
BoardMutator = _deferred_action_helpers.BoardMutator
CommentChecker = _deferred_action_helpers.CommentChecker
CommentPoster = _deferred_action_helpers.CommentPoster
DeferredActionStorePort = _deferred_action_helpers.DeferredActionStorePort


def _runtime_automerge_enabler(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Enable auto-merge without exposing the adapter return payload."""
    _codex_comment_wiring.runtime_automerge_enabler(
        pr_repo,
        pr_number,
        gh_runner=gh_runner,
    )


def replay_deferred_status_action_from_shell(
    *,
    payload: _deferred_action_helpers.DeferredActionPayload,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    board_info_resolver: _deferred_action_helpers.BoardInfoResolver | None,
    board_mutator: _deferred_action_helpers.BoardMutator | None,
    gh_runner: _deferred_action_helpers.GhRunner | None,
) -> None:
    """Replay one deferred status transition using live shell seams."""
    _deferred_action_helpers.replay_deferred_status_action(
        payload=payload,
        config=config,
        critical_path_config=critical_path_config,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        set_blocked_with_reason=_automation_bridge.set_blocked_with_reason,
        transition_issue_to_review=_board_state_helpers.transition_issue_to_review_from_shell,
        transition_issue_to_in_progress=_board_state_helpers.transition_issue_to_in_progress_from_shell,
        return_issue_to_ready=_board_state_helpers.return_issue_to_ready_from_shell,
    )


def replay_deferred_action_from_shell(
    *,
    action: _deferred_action_helpers.DeferredActionView,
    db: _deferred_action_helpers.DeferredActionStorePort,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    pr_port: PullRequestPort | None,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    board_info_resolver: _deferred_action_helpers.BoardInfoResolver | None,
    board_mutator: _deferred_action_helpers.BoardMutator | None,
    comment_checker: _deferred_action_helpers.CommentChecker | None,
    comment_poster: _deferred_action_helpers.CommentPoster | None,
    gh_runner: _deferred_action_helpers.GhRunner | None,
) -> None:
    """Execute one deferred action using live shell seams."""
    _deferred_action_helpers.replay_deferred_action(
        action=action,
        db=db,
        config=config,
        critical_path_config=critical_path_config,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        set_blocked_with_reason=_automation_bridge.set_blocked_with_reason,
        transition_issue_to_review=_board_state_helpers.transition_issue_to_review_from_shell,
        transition_issue_to_in_progress=_board_state_helpers.transition_issue_to_in_progress_from_shell,
        return_issue_to_ready=_board_state_helpers.return_issue_to_ready_from_shell,
        post_pr_codex_verdict=_codex_comment_wiring.post_pr_codex_verdict,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        runtime_comment_poster=_codex_comment_wiring.runtime_comment_poster,
        runtime_issue_closer=_codex_comment_wiring.runtime_issue_closer,
        runtime_failed_check_rerun=_codex_comment_wiring.runtime_failed_check_rerun,
        runtime_automerge_enabler=_runtime_automerge_enabler,
    )


def replay_deferred_actions_from_shell(
    db: _deferred_action_helpers.DeferredActionStorePort,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    *,
    pr_port: PullRequestPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: _deferred_action_helpers.BoardInfoResolver | None = None,
    board_mutator: _deferred_action_helpers.BoardMutator | None = None,
    comment_checker: _deferred_action_helpers.CommentChecker | None = None,
    comment_poster: _deferred_action_helpers.CommentPoster | None = None,
    gh_runner: _deferred_action_helpers.GhRunner | None = None,
) -> tuple[int, ...]:
    """Replay queued control-plane actions with live shell wiring."""
    return _deferred_action_helpers.replay_deferred_actions(
        db,
        config,
        critical_path_config,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        replay_deferred_action=replay_deferred_action_from_shell,
        record_successful_github_mutation=_record_successful_github_mutation,
        clear_degraded=_clear_degraded,
    )
