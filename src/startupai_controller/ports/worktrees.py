"""WorktreePort — local git worktree and workspace operations."""

from __future__ import annotations

from typing import Protocol

from startupai_controller.domain.models import (
    RepairBranchReconcileOutcome,
    WorktreeEntry,
)


class WorktreePort(Protocol):
    """Git worktree lifecycle operations."""

    def list_worktrees(self, repo_root: str) -> list[WorktreeEntry]:
        """Return all worktrees for a repository root."""
        ...

    def is_clean(self, worktree_path: str) -> bool:
        """Return True when the worktree has no local changes."""
        ...

    def fast_forward_existing(self, worktree_path: str, branch: str) -> None:
        """Fast-forward a clean reused worktree to the remote branch when possible."""
        ...

    def create_issue_worktree(
        self,
        issue_ref: str,
        title: str,
        *,
        branch_name_override: str | None = None,
    ) -> WorktreeEntry:
        """Create or reuse the issue worktree via the local worktree toolchain."""
        ...

    def reconcile_repair_branch(
        self,
        worktree_path: str,
        branch: str,
    ) -> RepairBranchReconcileOutcome:
        """Reconcile a repair branch against origin/main."""
        ...

    def run_workspace_hooks(
        self,
        commands: tuple[str, ...],
        *,
        worktree_path: str,
        issue_ref: str,
        branch_name: str,
    ) -> None:
        """Run repo-owned workspace hook commands in the worktree."""
        ...

    def cleanup(self, worktree_path: str) -> None:
        """Remove a worktree and its directory."""
        ...
