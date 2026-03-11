"""Local process adapter — local worktree, git, shell, and gh operations."""

from __future__ import annotations

from collections.abc import Callable
import os
from pathlib import Path
import re
import subprocess

from startupai_controller.domain.models import (
    RepairBranchReconcileOutcome,
    WorktreeEntry,
)


def _git_command_detail(result: subprocess.CompletedProcess[str]) -> str:
    """Return the most useful human-readable detail from a git subprocess result."""
    return result.stderr.strip() or result.stdout.strip() or "unknown-error"


class LocalProcessAdapter:
    """Adapter for local git worktree and shell operations."""

    def __init__(
        self,
        *,
        gh_runner: Callable[..., str] | None = None,
        subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self._gh_runner = gh_runner
        self._subprocess_runner = subprocess_runner or (
            lambda args, **kw: subprocess.run(args, **kw)
        )

    def run(self, args: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
        """Execute one local process command."""
        return self._subprocess_runner(args, **kwargs)

    # -- WorktreePort methods --

    def list_worktrees(self, repo_root: str) -> list[WorktreeEntry]:
        """Return all worktrees for a repository root."""
        result = self._subprocess_runner(
            ["git", "-C", repo_root, "worktree", "list", "--porcelain"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(_git_command_detail(result))

        records: list[WorktreeEntry] = []
        path = ""
        branch = ""
        for line in result.stdout.splitlines():
            if not line.strip():
                if path and branch:
                    records.append(WorktreeEntry(path=path, branch_name=branch))
                path = ""
                branch = ""
                continue
            if line.startswith("worktree "):
                path = line.split(" ", 1)[1].strip()
            elif line.startswith("branch "):
                ref = line.split(" ", 1)[1].strip()
                branch = ref.removeprefix("refs/heads/")
        if path and branch:
            records.append(WorktreeEntry(path=path, branch_name=branch))
        return records

    def is_clean(self, worktree_path: str) -> bool:
        """Return True when the worktree has no local changes."""
        result = self._subprocess_runner(
            ["git", "-C", worktree_path, "status", "--porcelain"],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0 and not result.stdout.strip()

    def fast_forward_existing(self, worktree_path: str, branch: str) -> None:
        """Fast-forward a clean reused worktree to the remote branch when possible."""
        status_result = self._subprocess_runner(
            ["git", "-C", worktree_path, "status", "--porcelain"],
            capture_output=True,
            text=True,
        )
        if status_result.returncode != 0 or status_result.stdout.strip():
            return

        fetch_result = self._subprocess_runner(
            ["git", "-C", worktree_path, "fetch", "origin", branch],
            capture_output=True,
            text=True,
        )
        if fetch_result.returncode != 0:
            return

        counts_result = self._subprocess_runner(
            [
                "git",
                "-C",
                worktree_path,
                "rev-list",
                "--left-right",
                "--count",
                f"HEAD...origin/{branch}",
            ],
            capture_output=True,
            text=True,
        )
        if counts_result.returncode != 0:
            return

        parts = counts_result.stdout.strip().split()
        if len(parts) != 2:
            return
        ahead, behind = (int(parts[0]), int(parts[1]))
        if behind == 0 or ahead != 0:
            return

        ff_result = self._subprocess_runner(
            ["git", "-C", worktree_path, "merge", "--ff-only", f"origin/{branch}"],
            capture_output=True,
            text=True,
        )
        if ff_result.returncode != 0:
            raise RuntimeError(
                f"existing worktree fast-forward failed for {branch}: "
                f"{_git_command_detail(ff_result)}"
            )

    def create_issue_worktree(
        self,
        issue_ref: str,
        title: str,
        *,
        branch_name_override: str | None = None,
    ) -> WorktreeEntry:
        """Create or reuse the issue worktree via the local worktree toolchain."""
        issue_prefix, issue_number = issue_ref.split("#", maxsplit=1)
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:40]
        branch = branch_name_override or f"feat/{issue_number}-{slug}"
        worktree_branch_arg = branch_name_override or f"feat/{slug}"
        expected_worktree_path = (
            os.path.expanduser(f"~/projects/worktrees/{issue_prefix}/{branch}")
            if branch_name_override
            else os.path.expanduser(
                f"~/projects/worktrees/{issue_prefix}/feat/{issue_number}-{slug}"
            )
        )

        wt_script = os.path.expanduser("~/.claude/skills/worktree/scripts/wt-create.sh")
        result = self._subprocess_runner(
            [
                wt_script,
                issue_prefix,
                worktree_branch_arg,
                "--agent",
                "board-consumer",
                "--issue",
                issue_number,
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            if (
                "Worktree already exists at" in result.stderr
                and os.path.isdir(expected_worktree_path)
            ):
                branch_result = self._subprocess_runner(
                    ["git", "-C", expected_worktree_path, "branch", "--show-current"],
                    capture_output=True,
                    text=True,
                )
                if branch_result.returncode == 0:
                    current_branch = branch_result.stdout.strip()
                    if current_branch == branch:
                        self.fast_forward_existing(expected_worktree_path, branch)
                        return WorktreeEntry(
                            path=expected_worktree_path,
                            branch_name=branch,
                        )
            raise RuntimeError(
                f"wt-create.sh failed (exit {result.returncode}): {result.stderr}"
            )

        worktree_path = ""
        for line in result.stdout.splitlines():
            if line.startswith("Worktree ready:"):
                worktree_path = line.split(":", 1)[1].strip()
                break
        if not worktree_path:
            worktree_path = expected_worktree_path
        return WorktreeEntry(path=worktree_path, branch_name=branch)

    def reconcile_repair_branch(
        self,
        worktree_path: str,
        branch: str,
    ) -> RepairBranchReconcileOutcome:
        """Reconcile a repair branch against its remote and origin/main."""

        def run_git(*args: str) -> subprocess.CompletedProcess[str]:
            return self._subprocess_runner(
                ["git", "-C", worktree_path, *args],
                capture_output=True,
                text=True,
            )

        checkout_result = run_git("checkout", branch)
        if checkout_result.returncode != 0:
            return RepairBranchReconcileOutcome(
                "reconcile_setup_failed",
                f"checkout-branch:{_git_command_detail(checkout_result)}",
            )

        status_result = run_git("status", "--porcelain")
        if status_result.returncode != 0:
            return RepairBranchReconcileOutcome(
                "reconcile_setup_failed",
                f"status:{_git_command_detail(status_result)}",
            )
        if status_result.stdout.strip():
            return RepairBranchReconcileOutcome(
                "reconcile_setup_failed",
                "worktree-dirty-before-repair-reconcile",
            )

        fetch_result = run_git("fetch", "origin", "main", branch)
        if fetch_result.returncode != 0:
            return RepairBranchReconcileOutcome(
                "reconcile_setup_failed",
                f"fetch:{_git_command_detail(fetch_result)}",
            )

        counts_result = run_git(
            "rev-list",
            "--left-right",
            "--count",
            f"HEAD...origin/{branch}",
        )
        if counts_result.returncode != 0:
            return RepairBranchReconcileOutcome(
                "reconcile_setup_failed",
                f"branch-sync:{_git_command_detail(counts_result)}",
            )

        parts = counts_result.stdout.strip().split()
        if len(parts) != 2:
            return RepairBranchReconcileOutcome(
                "reconcile_setup_failed",
                f"branch-sync:unexpected-rev-list-output:{counts_result.stdout.strip()}",
            )

        ahead, behind = (int(parts[0]), int(parts[1]))
        synced_remote_branch = False
        if ahead > 0 and behind > 0:
            return RepairBranchReconcileOutcome(
                "reconcile_setup_failed",
                "repair-branch-diverged-from-origin",
            )
        if ahead == 0 and behind > 0:
            ff_result = run_git("merge", "--ff-only", f"origin/{branch}")
            if ff_result.returncode != 0:
                return RepairBranchReconcileOutcome(
                    "reconcile_setup_failed",
                    f"fast-forward-branch:{_git_command_detail(ff_result)}",
                )
            synced_remote_branch = True

        merge_main_result = run_git("merge", "--no-edit", "origin/main")
        if merge_main_result.returncode == 0:
            detail = _git_command_detail(merge_main_result)
            if "Already up to date." in detail:
                return RepairBranchReconcileOutcome(
                    "fast_forwarded_repair_branch" if synced_remote_branch else "up_to_date"
                )
            return RepairBranchReconcileOutcome("merged_main")

        merge_head_result = run_git("rev-parse", "-q", "--verify", "MERGE_HEAD")
        if merge_head_result.returncode == 0:
            return RepairBranchReconcileOutcome("conflicted_main_merge")

        return RepairBranchReconcileOutcome(
            "reconcile_setup_failed",
            f"merge-main:{_git_command_detail(merge_main_result)}",
        )

    def run_workspace_hooks(
        self,
        commands: tuple[str, ...],
        *,
        worktree_path: str,
        issue_ref: str,
        branch_name: str,
    ) -> None:
        """Run repo-owned workspace hook commands in the claimed worktree."""
        if not commands:
            return
        env = os.environ.copy()
        env.update(
            {
                "STARTUPAI_ISSUE_REF": issue_ref,
                "STARTUPAI_WORKTREE_PATH": worktree_path,
                "STARTUPAI_BRANCH_NAME": branch_name,
            }
        )
        for command in commands:
            result = self._subprocess_runner(
                ["bash", "-lc", command],
                cwd=worktree_path,
                env=env,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                detail = (result.stderr or result.stdout or "").strip()
                raise RuntimeError(
                    f"workspace hook failed for '{command}' "
                    f"(exit {result.returncode}): {detail}"
                )

    def cleanup(self, worktree_path: str) -> None:
        """Remove a worktree and its directory."""
        try:
            self._subprocess_runner(
                ["git", "worktree", "remove", worktree_path, "--force"],
                capture_output=True,
                text=True,
                check=False,
            )
        except Exception:
            pass
        try:
            import shutil

            shutil.rmtree(worktree_path, ignore_errors=True)
        except Exception:
            pass

    # -- GhRunnerPort methods --

    def run_gh(self, args: list[str], *, check: bool = True) -> str:
        """Execute a gh CLI command and return stdout."""
        from startupai_controller.adapters.github_transport import _run_gh

        runner = self._gh_runner or _run_gh
        if self._gh_runner is not None:
            return runner(args, check=check)
        return runner(args)
