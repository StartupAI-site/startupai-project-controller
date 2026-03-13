"""Launch preparation helper cluster extracted from board_consumer."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import subprocess
from collections.abc import Callable
from typing import cast

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import IssueContextPayload
from startupai_controller.consumer_types import (
    PreparedCycleContext,
    WorktreePrepareError,
)
from startupai_controller.consumer_workflow import WorkflowDefinition
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    IssueSnapshot,
    OpenPullRequestMatch,
    RepairBranchReconcileOutcome,
)
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.issue_context import IssueContextPort
from startupai_controller.ports.ready_flow import (
    BoardInfoResolverFn,
    BoardStatusMutatorFn,
)
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.ports.worktrees import WorktreePort
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    IssueRef,
)

SubprocessRunnerFn = Callable[..., subprocess.CompletedProcess[str]]


def setup_launch_worktree(
    issue_ref: str,
    title: str,
    session_kind: str,
    repair_branch_name: str | None,
    *,
    config: ConsumerConfig,
    cp_config: CriticalPathConfig,
    db: ConsumerRuntimeStatePort,
    prepare_worktree: Callable[..., tuple[str, str]],
    record_metric: Callable[..., None],
    block_prelaunch_issue: Callable[..., None],
    reconcile_repair_branch: Callable[..., RepairBranchReconcileOutcome],
    worktree_error_cls: type[WorktreePrepareError],
    session_store: SessionStorePort | None = None,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
    board_info_resolver: BoardInfoResolverFn | None = None,
    board_mutator: BoardStatusMutatorFn | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, str, str | None, str | None]:
    """Set up worktree and reconcile repair branch if needed."""
    try:
        worktree_path, branch_name = prepare_worktree(
            issue_ref,
            title,
            config,
            db,
            branch_name_override=repair_branch_name,
            session_store=session_store,
            worktree_port=worktree_port,
            subprocess_runner=subprocess_runner,
        )
    except worktree_error_cls as err:
        typed_err = cast(WorktreePrepareError, err)
        record_metric(
            db,
            config,
            "worktree_blocked",
            issue_ref=issue_ref,
            payload={"reason": typed_err.reason_code, "detail": typed_err.detail},
        )
        blocked_reason = (
            typed_err.reason_code
            if typed_err.reason_code == "worktree_in_use"
            else f"workspace_prepare:{typed_err.detail}"
        )
        block_prelaunch_issue(
            issue_ref,
            blocked_reason,
            config=config,
            cp_config=cp_config,
            db=db,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        raise
    except RuntimeError as err:
        record_metric(
            db,
            config,
            "worktree_blocked",
            issue_ref=issue_ref,
            payload={"reason": "workspace_error", "detail": str(err)},
        )
        blocked_reason = f"workspace_prepare:{err}"
        block_prelaunch_issue(
            issue_ref,
            blocked_reason,
            config=config,
            cp_config=cp_config,
            db=db,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        raise worktree_error_cls("workspace_error", str(err)) from err

    branch_reconcile_state: str | None = None
    branch_reconcile_error: str | None = None
    if session_kind == "repair":
        reconcile_outcome = reconcile_repair_branch(
            worktree_path,
            branch_name,
            worktree_port=worktree_port,
            subprocess_runner=subprocess_runner,
        )
        branch_reconcile_state = reconcile_outcome.state
        branch_reconcile_error = reconcile_outcome.error
        if branch_reconcile_state == "reconcile_setup_failed":
            blocked_reason = (
                "repair-branch-reconcile:"
                f"{branch_reconcile_error or 'unknown-error'}"
            )
            block_prelaunch_issue(
                issue_ref,
                blocked_reason,
                config=config,
                cp_config=cp_config,
                db=db,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                gh_runner=gh_runner,
            )
            record_metric(
                db,
                config,
                "worker_start_failed",
                issue_ref=issue_ref,
                payload={"reason": "repair_reconcile_error", "detail": blocked_reason},
            )
            raise worktree_error_cls("repair_reconcile_error", blocked_reason)

    return worktree_path, branch_name, branch_reconcile_state, branch_reconcile_error


def resolve_launch_runtime(
    candidate_prefix: str,
    worktree_path: str,
    *,
    config: ConsumerConfig,
    prepared: PreparedCycleContext,
    load_worktree_workflow: Callable[..., WorkflowDefinition],
) -> tuple[WorkflowDefinition, ConsumerConfig]:
    """Load worktree workflow and compute effective runtime config."""
    workflow_definition = load_worktree_workflow(
        candidate_prefix,
        Path(worktree_path),
        filename=config.workflow_filename,
    )

    main_workflow = prepared.main_workflows.get(candidate_prefix)
    effective_max_retries = (
        main_workflow.runtime.max_retries
        if main_workflow is not None and main_workflow.runtime.max_retries is not None
        else config.max_retries
    )
    effective_validation_cmd = (
        workflow_definition.runtime.validation_cmd
        if workflow_definition.runtime.validation_cmd is not None
        else config.validation_cmd
    )
    effective_timeout_seconds = (
        workflow_definition.runtime.codex_timeout_seconds
        if workflow_definition.runtime.codex_timeout_seconds is not None
        else config.codex_timeout_seconds
    )
    effective_consumer_config = replace(
        config,
        validation_cmd=effective_validation_cmd,
        codex_timeout_seconds=effective_timeout_seconds,
        max_retries=effective_max_retries,
    )
    return workflow_definition, effective_consumer_config


def resolve_launch_candidate_metadata(
    issue_ref: str,
    *,
    cp_config: CriticalPathConfig,
    auto_config: BoardAutomationConfig | None,
    board_snapshot: CycleBoardSnapshot,
    pr_port: PullRequestPort,
    gh_runner: Callable[..., str] | None,
    parse_issue_ref: Callable[[str], IssueRef],
    resolve_issue_coordinates: Callable[
        [str, CriticalPathConfig], tuple[str, str, int]
    ],
    snapshot_for_issue: Callable[
        [CycleBoardSnapshot, str, CriticalPathConfig], IssueSnapshot | None
    ],
    classify_open_pr_candidates: Callable[
        ..., tuple[str, OpenPullRequestMatch | None, str]
    ],
    launch_session_kind: Callable[[str, OpenPullRequestMatch | None], str],
) -> tuple[str, str, str, int, IssueSnapshot | None, str, str | None, str | None]:
    """Resolve launch candidate identity and repair-session metadata."""
    candidate_prefix = parse_issue_ref(issue_ref).prefix
    owner, repo, number = resolve_issue_coordinates(issue_ref, cp_config)
    snapshot = snapshot_for_issue(board_snapshot, issue_ref, cp_config)
    session_kind = "new_work"
    repair_pr_url: str | None = None
    repair_branch_name: str | None = None
    if auto_config is not None:
        classification, pr_match, _reason = classify_open_pr_candidates(
            issue_ref,
            owner,
            repo,
            number,
            auto_config,
            pr_port=pr_port,
            gh_runner=gh_runner,
        )
        session_kind = launch_session_kind(classification, pr_match)
        if session_kind == "repair" and pr_match is not None:
            repair_pr_url = pr_match.url
            repair_branch_name = pr_match.branch_name
    return (
        candidate_prefix,
        owner,
        repo,
        number,
        snapshot,
        session_kind,
        repair_pr_url,
        repair_branch_name,
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
    gh_runner: Callable[..., str] | None,
    hydrate_issue_context: Callable[..., IssueContextPayload],
) -> tuple[IssueContextPayload, str]:
    """Hydrate launch issue context and compute the launch title."""
    context = hydrate_issue_context(
        issue_ref,
        owner=owner,
        repo=repo,
        number=number,
        snapshot=snapshot,
        config=config,
        db=db,
        issue_context_port=issue_context_port,
        gh_runner=gh_runner,
    )
    title = str(
        context.get("title")
        or (snapshot.title if snapshot is not None else f"issue-{number}")
    )
    return context, title


def run_launch_workspace_hooks(
    workflow_definition: WorkflowDefinition,
    *,
    worktree_path: str,
    issue_ref: str,
    branch_name: str,
    worktree_port: WorktreePort,
    subprocess_runner: SubprocessRunnerFn | None,
    run_workspace_hooks: Callable[..., None],
) -> None:
    """Run launch workspace hooks around the prepared worktree."""
    run_workspace_hooks(
        workflow_definition.runtime.workspace_hooks.get("after_create", ()),
        worktree_path=worktree_path,
        issue_ref=issue_ref,
        branch_name=branch_name,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )
    run_workspace_hooks(
        workflow_definition.runtime.workspace_hooks.get("before_run", ()),
        worktree_path=worktree_path,
        issue_ref=issue_ref,
        branch_name=branch_name,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )
