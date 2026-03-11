"""Launch preparation and worktree wiring extracted from board_consumer."""

from __future__ import annotations

import subprocess
from typing import Any, Callable

from startupai_controller.consumer_launch_helpers import (
    resolve_launch_candidate_metadata as _resolve_launch_candidate_metadata_helper,
    resolve_launch_issue_context as _resolve_launch_issue_context_helper,
    resolve_launch_runtime as _resolve_launch_runtime_helper,
    run_launch_workspace_hooks as _run_launch_workspace_hooks_helper,
    setup_launch_worktree as _setup_launch_worktree_helper,
)
from startupai_controller.consumer_worktree_helpers import (
    create_worktree as _create_worktree_helper,
    fast_forward_existing_worktree as _fast_forward_existing_worktree_helper,
    prepare_worktree as _prepare_worktree_helper,
    reconcile_repair_branch as _reconcile_repair_branch_helper,
)


def _shell_module():
    """Import the consumer shell lazily to avoid import cycles."""
    from startupai_controller import board_consumer_compat

    return board_consumer_compat


def git_command_detail(result: subprocess.CompletedProcess[str]) -> str:
    """Return the most useful human-readable detail from a git subprocess result."""
    return result.stderr.strip() or result.stdout.strip() or "unknown-error"


def prepare_worktree(
    issue_ref: str,
    title: str,
    config: Any,
    db: Any,
    *,
    branch_name_override: str | None = None,
    session_store: Any | None = None,
    worktree_port: Any | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> tuple[str, str]:
    """Create or safely adopt a worktree for an issue."""
    shell = _shell_module()
    return _prepare_worktree_helper(
        issue_ref,
        title,
        config,
        db,
        parse_issue_ref=shell.parse_issue_ref,
        build_session_store=shell.build_session_store,
        build_worktree_port=shell.build_worktree_port,
        list_repo_worktrees=shell._list_repo_worktrees,
        worktree_is_clean=shell._worktree_is_clean,
        worktree_ownership_is_safe=shell._worktree_ownership_is_safe,
        create_worktree=shell._create_worktree,
        record_metric=shell._record_metric,
        error_cls=shell.WorktreePrepareError,
        log_warning=lambda ref, err: shell.logger.warning(
            "Worktree reuse lookup failed for %s (%s); falling back to create",
            ref,
            err,
        ),
        branch_name_override=branch_name_override,
        session_store=session_store,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )


def create_worktree(
    issue_ref: str,
    title: str,
    config: Any,
    *,
    branch_name_override: str | None = None,
    worktree_port: Any | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> tuple[str, str]:
    """Create a worktree for the issue."""
    shell = _shell_module()
    return _create_worktree_helper(
        issue_ref,
        title,
        config,
        build_worktree_port=shell.build_worktree_port,
        branch_name_override=branch_name_override,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )


def fast_forward_existing_worktree(
    worktree_path: str,
    branch: str,
    *,
    worktree_port: Any | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> None:
    """Fast-forward a clean reused worktree when possible."""
    shell = _shell_module()
    _fast_forward_existing_worktree_helper(
        worktree_path,
        branch,
        build_worktree_port=shell.build_worktree_port,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )


def reconcile_repair_branch(
    worktree_path: str,
    branch: str,
    *,
    worktree_port: Any | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> Any:
    """Reconcile a repair branch against origin/main and its remote head."""
    shell = _shell_module()
    return _reconcile_repair_branch_helper(
        worktree_path,
        branch,
        build_worktree_port=shell.build_worktree_port,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )


def setup_launch_worktree(
    issue_ref: str,
    title: str,
    session_kind: str,
    repair_branch_name: str | None,
    *,
    config: Any,
    cp_config: Any,
    db: Any,
    session_store: Any | None = None,
    worktree_port: Any | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, str, str | None, str | None]:
    """Set up a launch worktree and reconcile repair branches when needed."""
    shell = _shell_module()
    return _setup_launch_worktree_helper(
        issue_ref,
        title,
        session_kind,
        repair_branch_name,
        config=config,
        cp_config=cp_config,
        db=db,
        prepare_worktree=shell._prepare_worktree,
        record_metric=shell._record_metric,
        block_prelaunch_issue=shell._block_prelaunch_issue,
        reconcile_repair_branch=shell._reconcile_repair_branch,
        worktree_error_cls=shell.WorktreePrepareError,
        session_store=session_store,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )


def resolve_launch_runtime(
    candidate_prefix: str,
    worktree_path: str,
    *,
    config: Any,
    prepared: Any,
) -> tuple[Any, Any]:
    """Load worktree workflow and effective consumer config."""
    shell = _shell_module()
    return _resolve_launch_runtime_helper(
        candidate_prefix,
        worktree_path,
        config=config,
        prepared=prepared,
        load_worktree_workflow=shell.load_worktree_workflow,
    )


def resolve_launch_candidate_metadata(
    issue_ref: str,
    *,
    cp_config: Any,
    auto_config: Any,
    board_snapshot: Any,
    pr_port: Any,
    gh_runner: Callable[..., str] | None,
) -> tuple[Any, ...]:
    """Resolve launch candidate identity and repair-session metadata."""
    shell = _shell_module()
    return _resolve_launch_candidate_metadata_helper(
        issue_ref,
        cp_config=cp_config,
        auto_config=auto_config,
        board_snapshot=board_snapshot,
        pr_port=pr_port,
        gh_runner=gh_runner,
        parse_issue_ref=shell.parse_issue_ref,
        resolve_issue_coordinates=shell._resolve_issue_coordinates,
        snapshot_for_issue=shell._snapshot_for_issue,
        classify_open_pr_candidates=shell._classify_open_pr_candidates,
        launch_session_kind=shell._launch_session_kind,
    )


def resolve_launch_issue_context(
    issue_ref: str,
    *,
    owner: str,
    repo: str,
    number: int,
    snapshot: Any | None,
    config: Any,
    db: Any,
    issue_context_port: Any,
    gh_runner: Callable[..., str] | None,
) -> tuple[dict[str, Any], str]:
    """Hydrate launch issue context and compute the launch title."""
    shell = _shell_module()
    return _resolve_launch_issue_context_helper(
        issue_ref,
        owner=owner,
        repo=repo,
        number=number,
        snapshot=snapshot,
        config=config,
        db=db,
        issue_context_port=issue_context_port,
        gh_runner=gh_runner,
        hydrate_issue_context=shell._hydrate_issue_context,
    )


def run_launch_workspace_hooks(
    workflow_definition: Any,
    *,
    worktree_path: str,
    issue_ref: str,
    branch_name: str,
    worktree_port: Any,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None,
) -> None:
    """Run workflow-defined launch hooks for a prepared worktree."""
    shell = _shell_module()
    _run_launch_workspace_hooks_helper(
        workflow_definition,
        worktree_path=worktree_path,
        issue_ref=issue_ref,
        branch_name=branch_name,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
        run_workspace_hooks=shell._run_workspace_hooks,
    )
