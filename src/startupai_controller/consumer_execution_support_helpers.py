"""Execution-support helpers for the consumer shell."""

from __future__ import annotations

from datetime import datetime, timezone
import subprocess
from typing import Any, Callable

from startupai_controller.runtime.wiring import (
    build_worktree_port as _build_worktree_port,
)
from startupai_controller.validate_critical_path_promotion import (
    direct_predecessors,
    in_any_critical_path as _in_any_critical_path,
)


class BranchPublicationError(RuntimeError):
    """Typed PR-head publication failure raised before PR creation."""

    def __init__(self, *, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        self.detail = detail
        super().__init__(detail)


def run_workspace_hooks(
    commands: tuple[str, ...],
    *,
    worktree_path: str,
    issue_ref: str,
    branch_name: str,
    worktree_port: Any | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    build_worktree_port: Callable[..., Any] = _build_worktree_port,
) -> None:
    """Run repo-owned workspace hook commands in the claimed worktree."""
    port = worktree_port or build_worktree_port(subprocess_runner=subprocess_runner)
    port.run_workspace_hooks(
        commands,
        worktree_path=worktree_path,
        issue_ref=issue_ref,
        branch_name=branch_name,
    )


def escalate_to_claude(
    issue_ref: str,
    config: Any,
    project_owner: str,
    project_number: int,
    reason: str = "",
    *,
    board_info_resolver: Callable[..., Any] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    marker_for: Callable[[str, str], str],
    resolve_issue_coordinates: Callable[[str, Any], tuple[str, str, int]],
    set_blocked_with_reason: Callable[..., None],
    set_issue_handoff_target: Callable[..., None],
    default_review_comment_checker: Callable[..., Callable[..., bool]],
    runtime_comment_poster: Callable[..., None],
    logger: Any,
) -> None:
    """Block the issue for Claude handoff and post one escalation comment."""
    marker = marker_for("consumer-escalation", issue_ref)
    owner, repo, number = resolve_issue_coordinates(issue_ref, config)
    blocked_reason = reason or "max retries exceeded"

    set_blocked_with_reason(
        issue_ref,
        blocked_reason,
        config,
        project_owner,
        project_number,
        board_info_resolver=board_info_resolver,
        gh_runner=gh_runner,
    )

    try:
        set_issue_handoff_target(
            issue_ref,
            "claude",
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        )
    except Exception:
        logger.warning("Failed to set Handoff To field for %s", issue_ref)

    checker = comment_checker or default_review_comment_checker(gh_runner=gh_runner)
    if checker(owner, repo, number, marker, gh_runner=gh_runner):
        return

    body = (
        f"{marker}\n"
        "**Escalation**: codex execution exhausted retries.\n\n"
        f"Reason: {blocked_reason}\n\n"
        "Handoff To: `claude`"
    )
    poster = comment_poster or runtime_comment_poster
    poster(owner, repo, number, body, gh_runner=gh_runner)


def build_dependency_summary(
    issue_ref: str,
    config: Any,
    project_owner: str,
    project_number: int,
    *,
    in_any_critical_path: Callable[[Any, str], bool] = _in_any_critical_path,
) -> str:
    """Build a human-readable dependency summary for the codex prompt."""
    if not in_any_critical_path(config, issue_ref):
        return "(Not in critical path.)"

    preds = direct_predecessors(config, issue_ref)
    if not preds:
        return "(No predecessors.)"

    lines = [f"- {pred} (Done)" for pred in sorted(preds)]
    return "\n".join(lines)


def has_commits_on_branch(
    worktree_path: str,
    branch: str,
    *,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> bool:
    """Check if the branch has commits beyond main."""
    runner = subprocess_runner or (
        lambda args, **kwargs: subprocess.run(args, **kwargs)
    )
    result = runner(
        ["git", "-C", worktree_path, "log", "main..HEAD", "--oneline"],
        capture_output=True,
        text=True,
    )
    return bool(result.stdout.strip())


def validate_branch_publication(
    worktree_path: str,
    branch: str,
    *,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> None:
    """Require that the PR head branch exists locally and is published on origin."""
    runner = subprocess_runner or (
        lambda args, **kwargs: subprocess.run(args, **kwargs)
    )

    def _git(*git_args: str) -> subprocess.CompletedProcess[str]:
        return runner(
            ["git", "-C", worktree_path, *git_args],
            capture_output=True,
            text=True,
        )

    current_branch = _git("branch", "--show-current")
    current_branch_name = current_branch.stdout.strip()
    if current_branch.returncode != 0 or not current_branch_name:
        detail = (
            current_branch.stderr.strip()
            or current_branch.stdout.strip()
            or "unable to resolve current branch"
        )
        raise BranchPublicationError(
            reason_code="branch_not_published",
            detail=f"cannot resolve PR head branch in {worktree_path}: {detail}",
        )
    if current_branch_name != branch:
        raise BranchPublicationError(
            reason_code="branch_mismatch",
            detail=(
                f"refusing PR creation for {branch}: worktree is on "
                f"{current_branch_name}"
            ),
        )

    local_branch = _git("show-ref", "--verify", f"refs/heads/{branch}")
    if local_branch.returncode != 0:
        raise BranchPublicationError(
            reason_code="branch_not_published",
            detail=f"head branch {branch} does not exist locally",
        )

    remote_branch = _git("ls-remote", "--exit-code", "--heads", "origin", branch)
    if remote_branch.returncode != 0 or not remote_branch.stdout.strip():
        raise BranchPublicationError(
            reason_code="branch_not_published",
            detail=f"head branch {branch} is not published on origin",
        )
