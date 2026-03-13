"""Composition root helpers for controller runtime wiring.

This module is the only place where orchestration chooses concrete adapter
implementations for runtime execution. Bundles are constructed per command
or per daemon-cycle context; there are no process-global singletons here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable
import subprocess

from startupai_controller.adapters.board_mutation import GitHubBoardMutationAdapter
from startupai_controller.adapters.pull_requests import (
    CycleGitHubMemo,
    GitHubPullRequestAdapter,
)
from startupai_controller.adapters.github_http_adapter import (
    begin_request_stats,
    end_request_stats,
)
from startupai_controller.adapters.github_transport import _run_gh, gh_reason_code
from startupai_controller.adapters.review_state import (
    GitHubReviewStateAdapter,
    clear_cycle_board_snapshot_cache,
)
from startupai_controller.adapters.local_process import LocalProcessAdapter
from startupai_controller.adapters.ready_flow import BoardAutomationReadyFlowAdapter
from startupai_controller.adapters.system_service import SystemServiceAdapter
from startupai_controller.adapters.sqlite_store import (
    ConsumerDB,
    MetricEvent,
    RecoveredLease,
    SqliteSessionStore,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.issue_context import IssueContextPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.process_runner import GhRunnerPort, ProcessRunnerPort
from startupai_controller.ports.ready_flow import GitHubRunnerFn, ReadyFlowPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.service_control import ServiceControlPort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.ports.worktrees import WorktreePort
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig


@dataclass(frozen=True)
class GitHubPortBundle:
    """Per-command/per-cycle GitHub-backed port bundle."""

    pull_requests: PullRequestPort
    review_state: ReviewStatePort
    board_mutations: BoardMutationPort
    issue_context: IssueContextPort
    github_memo: CycleGitHubMemo


GitHubRuntimeMemo = CycleGitHubMemo
GitHubRunnerInput = GitHubRunnerFn | GhRunnerPort | None


def gh_runner_callable(
    *,
    gh_runner: GitHubRunnerInput = None,
) -> GitHubRunnerFn | None:
    """Normalize one runtime gh runner input into the callable adapter seam."""
    if gh_runner is None:
        return None
    if hasattr(gh_runner, "run_gh"):
        return gh_runner.run_gh
    return gh_runner


def build_github_port_bundle(
    project_owner: str,
    project_number: int,
    *,
    config: CriticalPathConfig | None = None,
    github_memo: CycleGitHubMemo | None = None,
    gh_runner: GitHubRunnerInput = None,
) -> GitHubPortBundle:
    """Build the per-command/per-cycle GitHub adapter bundle."""
    memo = github_memo or CycleGitHubMemo()
    gh_runner_fn = gh_runner_callable(gh_runner=gh_runner)
    pr_adapter = GitHubPullRequestAdapter(
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        github_memo=memo,
        gh_runner=gh_runner_fn,
    )
    review_state_adapter = GitHubReviewStateAdapter(
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        github_memo=memo,
        gh_runner=gh_runner_fn,
    )
    board_mutation_adapter = GitHubBoardMutationAdapter(
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        github_memo=memo,
        gh_runner=gh_runner_fn,
    )
    return GitHubPortBundle(
        pull_requests=pr_adapter,
        review_state=review_state_adapter,
        board_mutations=board_mutation_adapter,
        issue_context=pr_adapter,
        github_memo=memo,
    )


def build_session_store(db: ConsumerDB) -> SessionStorePort:
    """Build the SQLite-backed session store port."""
    return SqliteSessionStore(db)


def open_consumer_db(db_path) -> ConsumerDB:
    """Open the concrete SQLite store at the runtime wiring boundary."""
    return ConsumerDB(db_path=db_path)


def build_worktree_port(
    *,
    gh_runner: GitHubRunnerInput = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> WorktreePort:
    """Build the local worktree/process adapter."""
    return LocalProcessAdapter(
        gh_runner=gh_runner_callable(gh_runner=gh_runner),
        subprocess_runner=subprocess_runner,
    )


def build_process_runner_port(
    *,
    gh_runner: GitHubRunnerInput = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> ProcessRunnerPort:
    """Build the local process runner port."""
    return LocalProcessAdapter(
        gh_runner=gh_runner_callable(gh_runner=gh_runner),
        subprocess_runner=subprocess_runner,
    )


def build_gh_runner_port(
    *,
    gh_runner: GitHubRunnerInput = None,
) -> GhRunnerPort:
    """Build the local gh runner port."""
    if gh_runner is not None and hasattr(gh_runner, "run_gh"):
        return gh_runner
    return LocalProcessAdapter(gh_runner=gh_runner_callable(gh_runner=gh_runner))


def build_service_control_port(
    *,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> ServiceControlPort:
    """Build the local system-service adapter."""
    return SystemServiceAdapter(subprocess_runner=subprocess_runner)


def build_ready_flow_port() -> ReadyFlowPort:
    """Build the ready-flow adapter."""
    return BoardAutomationReadyFlowAdapter()


def clear_github_runtime_caches() -> None:
    """Clear short-lived GitHub adapter caches owned by the runtime boundary."""
    clear_cycle_board_snapshot_cache()


def new_github_runtime_memo() -> GitHubRuntimeMemo:
    """Build a fresh per-command/per-cycle GitHub memo."""
    return GitHubRuntimeMemo()


def begin_runtime_request_stats():
    """Begin GitHub request statistics collection at the runtime boundary."""
    return begin_request_stats()


def end_runtime_request_stats(token):
    """End GitHub request statistics collection at the runtime boundary."""
    return end_request_stats(token)


def run_runtime_gh(
    args: list[str],
    *,
    gh_runner: GitHubRunnerInput = None,
    operation_type: str = "",
) -> str:
    """Execute one GitHub CLI command through the runtime boundary."""
    return _run_gh(
        args,
        gh_runner=gh_runner_callable(gh_runner=gh_runner),
        operation_type=operation_type,
    )


def runtime_gh_reason_code(error: Exception) -> str:
    """Classify one GitHub transport/query error at the runtime boundary."""
    return gh_reason_code(error)
