"""Shared control-plane rescue operations used by consumer and control-plane shells."""

from __future__ import annotations

from typing import Callable

import startupai_controller.consumer_deferred_action_wiring as _deferred_action_wiring
import startupai_controller.consumer_review_queue_wiring as _review_queue_wiring
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ReviewQueueDrainSummary,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.runtime.wiring import (
    ConsumerDB,
    GitHubRuntimeMemo,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
)


def _drain_review_queue(
    config: ConsumerConfig,
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
    db: _deferred_action_wiring.DeferredActionStorePort,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    *,
    pr_port: PullRequestPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: _deferred_action_wiring.BoardInfoResolver | None = None,
    board_mutator: _deferred_action_wiring.BoardMutator | None = None,
    comment_checker: _deferred_action_wiring.CommentChecker | None = None,
    comment_poster: _deferred_action_wiring.CommentPoster | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[int, ...]:
    """Replay queued control-plane actions after GitHub recovery."""
    return _deferred_action_wiring.replay_deferred_actions_from_shell(
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
    )
