"""Local process adapter — wraps worktree and subprocess operations.

Implements WorktreePort and GhRunnerPort by delegating to existing
subprocess-based implementations.
"""

from __future__ import annotations

from collections.abc import Callable


class LocalProcessAdapter:
    """Adapter for local git worktree and gh CLI operations.

    Satisfies WorktreePort and GhRunnerPort protocols via structural typing.
    """

    def __init__(
        self,
        *,
        gh_runner: Callable[..., str] | None = None,
    ) -> None:
        self._gh_runner = gh_runner

    # -- WorktreePort methods --

    def prepare(self, branch: str, repo_path: str) -> str:
        """Prepare a worktree for the given branch. Returns worktree path."""
        import subprocess

        result = subprocess.run(
            ["git", "worktree", "add", "-b", branch, f"../{branch}", "HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        return str(result.stdout.strip()) or f"../{branch}"

    def cleanup(self, worktree_path: str) -> None:
        """Remove a worktree and its directory."""
        import shutil
        import subprocess

        try:
            subprocess.run(
                ["git", "worktree", "remove", worktree_path, "--force"],
                capture_output=True,
                text=True,
                check=False,
            )
        except Exception:
            pass
        try:
            shutil.rmtree(worktree_path, ignore_errors=True)
        except Exception:
            pass

    # -- GhRunnerPort methods --

    def run_gh(self, args: list[str], *, check: bool = True) -> str:
        """Execute a gh CLI command and return stdout."""
        from startupai_controller.board_io import _run_gh

        runner = self._gh_runner or _run_gh
        if self._gh_runner is not None:
            return runner(args, check=check)
        return runner(args)
