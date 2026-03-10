"""Composition root helpers for controller runtime wiring.

This module is the only place where orchestration chooses concrete adapter
implementations for runtime execution. Bundles are constructed per command
or per daemon-cycle context; there are no process-global singletons here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable
import subprocess

from startupai_controller.adapters.github_cli import CycleGitHubMemo, GitHubCliAdapter
from startupai_controller.adapters.local_process import LocalProcessAdapter
from startupai_controller.adapters.sqlite_store import ConsumerDB, SqliteSessionStore
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.issue_context import IssueContextPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
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


def build_github_port_bundle(
    project_owner: str,
    project_number: int,
    *,
    config: CriticalPathConfig | None = None,
    github_memo: CycleGitHubMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> GitHubPortBundle:
    """Build the per-command/per-cycle GitHub adapter bundle."""
    memo = github_memo or CycleGitHubMemo()
    adapter = GitHubCliAdapter(
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        github_memo=memo,
        gh_runner=gh_runner,
    )
    return GitHubPortBundle(
        pull_requests=adapter,
        review_state=adapter,
        board_mutations=adapter,
        issue_context=adapter,
        github_memo=memo,
    )


def build_session_store(db: ConsumerDB) -> SessionStorePort:
    """Build the SQLite-backed session store port."""
    return SqliteSessionStore(db)


def build_worktree_port(
    *,
    gh_runner: Callable[..., str] | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> WorktreePort:
    """Build the local worktree/process adapter."""
    return LocalProcessAdapter(
        gh_runner=gh_runner,
        subprocess_runner=subprocess_runner,
    )
