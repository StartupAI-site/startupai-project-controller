"""Interrupted-session recovery wiring extracted from board_consumer."""

from __future__ import annotations

from logging import Logger
from pathlib import Path
from typing import Callable, Protocol

from startupai_controller.application.consumer.recovery import (
    ClassifyOpenPrCandidatesFn,
    LoadAutomationConfigFn,
    RecoveryDeps,
    RecoveredLeaseInfo,
    RecoveryStatePort,
    ResolveIssueCoordinatesFn,
    ReturnIssueToReadyFn,
    SetBlockedWithReasonFn,
    TransitionIssueToReviewFn,
)
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.runtime.wiring import GitHubPortBundle
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig


class LoadCriticalPathConfigFn(Protocol):
    """Load the critical-path config used to resolve issue refs."""

    def __call__(self, path: Path) -> CriticalPathConfig:
        """Return the parsed critical-path config."""
        ...


class RecoveryUseCaseFn(Protocol):
    """Typed recovery use-case seam consumed by the shell helper."""

    def __call__(
        self,
        config: ConsumerConfig,
        db: RecoveryStatePort,
        *,
        recovered: list[RecoveredLeaseInfo] | None = None,
        cp_config: CriticalPathConfig,
        deps: RecoveryDeps,
        automation_config: BoardAutomationConfig | None = None,
        pr_port: PullRequestPort,
        review_state_port: ReviewStatePort,
        board_port: BoardMutationPort,
        log_error: Callable[[str, Exception], None] | None = None,
    ) -> list[RecoveredLeaseInfo]:
        """Recover interrupted leases and update the board truthfully."""
        ...


class BuildGitHubPortBundleFn(Protocol):
    """Build the canonical GitHub-backed port bundle when ports are omitted."""

    def __call__(
        self,
        project_owner: str,
        project_number: int,
        *,
        config: CriticalPathConfig | None = None,
        gh_runner: Callable[..., str] | None = None,
    ) -> GitHubPortBundle:
        """Return the GitHub port bundle for one command/cycle."""
        ...


def recover_interrupted_sessions(
    config: ConsumerConfig,
    db: RecoveryStatePort,
    *,
    automation_config: BoardAutomationConfig | None = None,
    pr_port: PullRequestPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    gh_runner: Callable[..., str] | None = None,
    load_config: LoadCriticalPathConfigFn,
    config_error_type: type[Exception],
    logger: Logger,
    recovery_use_case: RecoveryUseCaseFn,
    gh_query_error_type: type[Exception],
    build_github_port_bundle: BuildGitHubPortBundleFn,
    load_automation_config: LoadAutomationConfigFn,
    resolve_issue_coordinates: ResolveIssueCoordinatesFn,
    classify_open_pr_candidates: ClassifyOpenPrCandidatesFn,
    return_issue_to_ready: ReturnIssueToReadyFn,
    transition_issue_to_review: TransitionIssueToReviewFn,
    set_blocked_with_reason: SetBlockedWithReasonFn,
) -> list[RecoveredLeaseInfo]:
    """Recover leases left behind by a previous interrupted daemon process."""
    recovered = db.recover_interrupted_leases()
    if not recovered:
        return []

    try:
        cp_config = load_config(config.critical_paths_path)
    except config_error_type as err:
        logger.error("Interrupted-session recovery skipped: %s", err)
        return recovered

    if pr_port is None or review_state_port is None or board_port is None:
        bundle = build_github_port_bundle(
            config.project_owner,
            config.project_number,
            config=cp_config,
            gh_runner=gh_runner,
        )
        effective_pr_port = pr_port or bundle.pull_requests
        effective_review_state_port = review_state_port or bundle.review_state
        effective_board_port = board_port or bundle.board_mutations
    else:
        effective_pr_port = pr_port
        effective_review_state_port = review_state_port
        effective_board_port = board_port

    return recovery_use_case(
        config,
        db,
        recovered=recovered,
        cp_config=cp_config,
        deps=RecoveryDeps(
            gh_query_error_type=gh_query_error_type,
            load_automation_config=load_automation_config,
            resolve_issue_coordinates=resolve_issue_coordinates,
            classify_open_pr_candidates=classify_open_pr_candidates,
            return_issue_to_ready=return_issue_to_ready,
            transition_issue_to_review=transition_issue_to_review,
            set_blocked_with_reason=set_blocked_with_reason,
        ),
        automation_config=automation_config,
        pr_port=effective_pr_port,
        review_state_port=effective_review_state_port,
        board_port=effective_board_port,
        log_error=lambda issue_ref, err: logger.error(
            "Interrupted-session board recovery failed for %s: %s",
            issue_ref,
            err,
        ),
    )
