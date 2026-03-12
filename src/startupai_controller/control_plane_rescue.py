"""Shared control-plane rescue operations used by consumer and control-plane shells."""

from __future__ import annotations

import logging
from typing import Any, Callable

import startupai_controller.consumer_automation_bridge as _automation_bridge
from startupai_controller import consumer_board_state_helpers as _board_state_helpers
import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
from startupai_controller import (
    consumer_deferred_action_helpers as _deferred_action_helpers,
)
import startupai_controller.consumer_review_queue_wiring as _review_queue_wiring
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.control_plane_runtime import (
    _clear_degraded,
    _record_successful_github_mutation,
)
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ReviewQueueDrainSummary,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.runtime.wiring import (
    build_github_port_bundle,
    ConsumerDB,
    GitHubRuntimeMemo,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
)

logger = logging.getLogger("board-consumer")


def _runtime_automerge_enabler(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Enable auto-merge for deferred actions without exposing the status value."""
    _codex_comment_wiring.runtime_automerge_enabler(
        pr_repo,
        pr_number,
        gh_runner=gh_runner,
    )


def _drain_review_queue(
    config: Any,
    db: ConsumerDB,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    *,
    pr_port: PullRequestPort | None = None,
    session_store: SessionStorePort | None = None,
    board_snapshot: CycleBoardSnapshot | None = None,
    dry_run: bool = False,
    github_memo: GitHubRuntimeMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[ReviewQueueDrainSummary, CycleBoardSnapshot]:
    """Process a bounded batch of queued Review items."""
    return _review_queue_wiring.drain_review_queue_from_shell(
        config=config,
        db=db,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=pr_port,
        session_store=session_store,
        board_snapshot=board_snapshot,
        dry_run=dry_run,
        github_memo=github_memo,
        gh_runner=gh_runner,
    )


def _replay_deferred_actions(
    db: Any,
    config: Any,
    critical_path_config: CriticalPathConfig,
    *,
    pr_port: PullRequestPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[int, ...]:
    """Replay queued control-plane actions after GitHub recovery."""
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
        replay_deferred_action=lambda **kwargs: _deferred_action_helpers.replay_deferred_action(
            **kwargs,
            set_blocked_with_reason=_automation_bridge.set_blocked_with_reason,
            transition_issue_to_review=_board_state_helpers.transition_issue_to_review,
            transition_issue_to_in_progress=lambda issue_ref, config, project_owner, project_number, **inner_kwargs: _board_state_helpers.transition_issue_to_in_progress(
                issue_ref,
                config,
                project_owner,
                project_number,
                build_github_port_bundle=build_github_port_bundle,
                from_statuses=inner_kwargs.get("from_statuses"),
                review_state_port=inner_kwargs.get("review_state_port"),
                board_port=inner_kwargs.get("board_port"),
                gh_runner=inner_kwargs.get("gh_runner"),
            ),
            return_issue_to_ready=lambda issue_ref, config, project_owner, project_number, **inner_kwargs: _board_state_helpers.return_issue_to_ready(
                issue_ref,
                config,
                project_owner,
                project_number,
                build_github_port_bundle=build_github_port_bundle,
                from_statuses=inner_kwargs.get("from_statuses"),
                review_state_port=inner_kwargs.get("review_state_port"),
                board_port=inner_kwargs.get("board_port"),
                gh_runner=inner_kwargs.get("gh_runner"),
            ),
            post_pr_codex_verdict=_codex_comment_wiring.post_pr_codex_verdict,
            resolve_issue_coordinates=_resolve_issue_coordinates,
            runtime_comment_poster=_codex_comment_wiring.runtime_comment_poster,
            runtime_issue_closer=_codex_comment_wiring.runtime_issue_closer,
            runtime_failed_check_rerun=_codex_comment_wiring.runtime_failed_check_rerun,
            runtime_automerge_enabler=_runtime_automerge_enabler,
        ),
        record_successful_github_mutation=_record_successful_github_mutation,
        clear_degraded=_clear_degraded,
    )
