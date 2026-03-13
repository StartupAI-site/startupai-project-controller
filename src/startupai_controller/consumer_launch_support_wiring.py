"""Launch preparation and worktree wiring extracted from board_consumer."""

from __future__ import annotations

import logging
import subprocess
from collections.abc import Callable
from typing import cast

import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
import startupai_controller.consumer_execution_support_helpers as _execution_support_helpers
import startupai_controller.consumer_operational_wiring as _operational_wiring
import startupai_controller.consumer_support_wiring as _support_wiring
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.board_graph import _resolve_issue_coordinates
from startupai_controller.consumer_config import ConsumerConfig
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
from startupai_controller.consumer_types import WorktreePrepareError
from startupai_controller.consumer_types import (
    IssueContextPayload,
    PreparedCycleContext,
)
from startupai_controller.consumer_workflow import (
    WorkflowDefinition,
    load_worktree_workflow,
)
from startupai_controller.domain.launch_policy import (
    launch_session_kind as _launch_session_kind,
)
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    IssueSnapshot,
    RepairBranchReconcileOutcome,
)
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.issue_context import IssueContextPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.ready_flow import (
    BoardInfoResolverFn,
    BoardStatusMutatorFn,
    GitHubRunnerFn,
)
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.ports.worktrees import WorktreePort
from startupai_controller.runtime.wiring import build_session_store, build_worktree_port
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    parse_issue_ref,
)

logger = logging.getLogger("board-consumer")
SubprocessRunnerFn = Callable[..., subprocess.CompletedProcess[str]]


def git_command_detail(result: subprocess.CompletedProcess[str]) -> str:
    """Return the most useful human-readable detail from a git subprocess result."""
    return result.stderr.strip() or result.stdout.strip() or "unknown-error"


def prepare_worktree(
    issue_ref: str,
    title: str,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    *,
    branch_name_override: str | None = None,
    session_store: SessionStorePort | None = None,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
) -> tuple[str, str]:
    """Create or safely adopt a worktree for an issue."""
    return _prepare_worktree_helper(
        issue_ref,
        title,
        config,
        db,
        parse_issue_ref=parse_issue_ref,
        build_session_store=build_session_store,
        build_worktree_port=build_worktree_port,
        list_repo_worktrees=_support_wiring.list_repo_worktrees,
        worktree_is_clean=_support_wiring.worktree_is_clean,
        worktree_ownership_is_safe=_support_wiring.worktree_ownership_is_safe,
        create_worktree=create_worktree,
        record_metric=_support_wiring.record_metric,
        error_cls=WorktreePrepareError,
        log_warning=lambda ref, err: logger.warning(
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
    config: ConsumerConfig,
    *,
    branch_name_override: str | None = None,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
) -> tuple[str, str]:
    """Create a worktree for the issue."""
    return _create_worktree_helper(
        issue_ref,
        title,
        config,
        build_worktree_port=build_worktree_port,
        branch_name_override=branch_name_override,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )


def fast_forward_existing_worktree(
    worktree_path: str,
    branch: str,
    *,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
) -> None:
    """Fast-forward a clean reused worktree when possible."""
    _fast_forward_existing_worktree_helper(
        worktree_path,
        branch,
        build_worktree_port=build_worktree_port,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )


def reconcile_repair_branch(
    worktree_path: str,
    branch: str,
    *,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
) -> RepairBranchReconcileOutcome:
    """Reconcile a repair branch against origin/main and its remote head."""
    return _reconcile_repair_branch_helper(
        worktree_path,
        branch,
        build_worktree_port=build_worktree_port,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )


def setup_launch_worktree(
    issue_ref: str,
    title: str,
    session_kind: str,
    repair_branch_name: str | None,
    *,
    config: ConsumerConfig,
    cp_config: CriticalPathConfig,
    db: ConsumerRuntimeStatePort,
    session_store: SessionStorePort | None = None,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
    board_info_resolver: BoardInfoResolverFn | None = None,
    board_mutator: BoardStatusMutatorFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> tuple[str, str, str | None, str | None]:
    """Set up a launch worktree and reconcile repair branches when needed."""
    return _setup_launch_worktree_helper(
        issue_ref,
        title,
        session_kind,
        repair_branch_name,
        config=config,
        cp_config=cp_config,
        db=db,
        prepare_worktree=prepare_worktree,
        record_metric=_support_wiring.record_metric,
        block_prelaunch_issue=_operational_wiring.block_prelaunch_issue,
        reconcile_repair_branch=reconcile_repair_branch,
        worktree_error_cls=WorktreePrepareError,
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
    config: ConsumerConfig,
    prepared: PreparedCycleContext,
) -> tuple[WorkflowDefinition, ConsumerConfig]:
    """Load worktree workflow and effective consumer config."""
    return _resolve_launch_runtime_helper(
        candidate_prefix,
        worktree_path,
        config=config,
        prepared=prepared,
        load_worktree_workflow=load_worktree_workflow,
    )


def resolve_launch_candidate_metadata(
    issue_ref: str,
    *,
    cp_config: CriticalPathConfig,
    auto_config: BoardAutomationConfig | None,
    board_snapshot: CycleBoardSnapshot,
    pr_port: PullRequestPort,
    gh_runner: GitHubRunnerFn | None,
) -> tuple[str, str, str, int, IssueSnapshot | None, str, str | None, str | None]:
    """Resolve launch candidate identity and repair-session metadata."""
    return _resolve_launch_candidate_metadata_helper(
        issue_ref,
        cp_config=cp_config,
        auto_config=auto_config,
        board_snapshot=board_snapshot,
        pr_port=pr_port,
        gh_runner=gh_runner,
        parse_issue_ref=parse_issue_ref,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        snapshot_for_issue=cast(
            Callable[
                [CycleBoardSnapshot, str, CriticalPathConfig], IssueSnapshot | None
            ],
            _support_wiring.snapshot_for_issue,
        ),
        classify_open_pr_candidates=_codex_comment_wiring.classify_open_pr_candidates,
        launch_session_kind=_launch_session_kind,
    )


def resolve_launch_issue_context(
    issue_ref: str,
    *,
    owner: str,
    repo: str,
    number: int,
    snapshot: IssueSnapshot | None,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    issue_context_port: IssueContextPort,
    gh_runner: GitHubRunnerFn | None,
) -> tuple[IssueContextPayload, str]:
    """Hydrate launch issue context and compute the launch title."""
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
        hydrate_issue_context=_support_wiring.hydrate_issue_context,
    )


def run_launch_workspace_hooks(
    workflow_definition: WorkflowDefinition,
    *,
    worktree_path: str,
    issue_ref: str,
    branch_name: str,
    worktree_port: WorktreePort,
    subprocess_runner: SubprocessRunnerFn | None,
) -> None:
    """Run workflow-defined launch hooks for a prepared worktree."""
    _run_launch_workspace_hooks_helper(
        workflow_definition,
        worktree_path=worktree_path,
        issue_ref=issue_ref,
        branch_name=branch_name,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
        run_workspace_hooks=_execution_support_helpers.run_workspace_hooks,
    )
