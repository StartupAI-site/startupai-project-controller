"""WorktreePort — git worktree lifecycle management.

Port protocol for preparing and cleaning up worktrees.
"""

from __future__ import annotations

from typing import Protocol


class WorktreePort(Protocol):
    """Git worktree lifecycle operations."""

    def prepare(self, branch: str, repo_path: str) -> str:
        """Prepare a worktree for the given branch. Returns worktree path."""
        ...

    def cleanup(self, worktree_path: str) -> None:
        """Remove a worktree and its directory."""
        ...
