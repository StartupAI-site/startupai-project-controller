"""ReadyFlowPort — control-plane ready-queue routing and admission operations."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.domain.models import (
    AdmissionDecision,
    ClaimReadyResult,
    CycleBoardSnapshot,
    ExecutorRoutingDecision,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

GitHubRunnerFn = Callable[..., str]


class BoardInfoView(Protocol):
    """Minimal board identity/status used by legacy compatibility seams."""

    status: str
    item_id: str
    project_id: str


class GitHubBundleView(Protocol):
    """Minimal GitHub bundle surface consumed by ready-flow admission."""

    review_state: ReviewStatePort
    pull_requests: PullRequestPort
    board_mutations: BoardMutationPort
    github_memo: object


StatusResolverFn = Callable[
    [str, CriticalPathConfig, str, int],
    str,
]
BoardInfoResolverFn = Callable[
    [str, CriticalPathConfig, str, int],
    BoardInfoView,
]
BoardStatusMutatorFn = Callable[[str, str, str], None]
CommentCheckerFn = Callable[[str, str, int, str], bool]
CommentPosterFn = Callable[[str, str, int, str], None]


class ReadyFlowPort(Protocol):
    """Ready queue routing and backlog admission operations."""

    def route_protected_queue_executors(
        self,
        config: CriticalPathConfig,
        automation_config: BoardAutomationConfig | None,
        project_owner: str,
        project_number: int,
        *,
        statuses: tuple[str, ...] = (),
        dry_run: bool = False,
        board_snapshot: CycleBoardSnapshot | None = None,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> ExecutorRoutingDecision:
        """Normalize protected queue executor ownership."""
        ...

    def admit_backlog_items(
        self,
        config: CriticalPathConfig,
        automation_config: BoardAutomationConfig | None,
        project_owner: str,
        project_number: int,
        *,
        dispatchable_repo_prefixes: tuple[str, ...] | None = None,
        active_lease_issue_refs: tuple[str, ...] = (),
        dry_run: bool = False,
        board_snapshot: CycleBoardSnapshot | None = None,
        github_bundle: GitHubBundleView | None = None,
        github_memo: object | None = None,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> AdmissionDecision:
        """Admit governed Backlog items into Ready."""
        ...

    def admission_summary_payload(
        self,
        decision: AdmissionDecision,
        *,
        enabled: bool,
    ) -> dict[str, object]:
        """Convert admission results into a JSON-friendly payload."""
        ...

    def claim_ready_issue(
        self,
        config: CriticalPathConfig,
        project_owner: str,
        project_number: int,
        *,
        executor: str,
        issue_ref: str | None = None,
        next_issue: bool = False,
        this_repo_prefix: str | None = None,
        all_prefixes: bool = False,
        per_executor_wip_limit: int = 3,
        automation_config: BoardAutomationConfig | None = None,
        dry_run: bool = False,
        review_state_port: ReviewStatePort | None = None,
        board_port: BoardMutationPort | None = None,
        status_resolver: StatusResolverFn | None = None,
        board_info_resolver: BoardInfoResolverFn | None = None,
        board_mutator: BoardStatusMutatorFn | None = None,
        comment_checker: CommentCheckerFn | None = None,
        comment_poster: CommentPosterFn | None = None,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> ClaimReadyResult:
        """Claim one Ready issue for a specific executor."""
        ...
