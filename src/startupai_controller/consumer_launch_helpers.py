"""Launch preparation helper cluster extracted from board_consumer."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, Callable


def setup_launch_worktree(
    issue_ref: str,
    title: str,
    session_kind: str,
    repair_branch_name: str | None,
    *,
    config: Any,
    cp_config: Any,
    db: Any,
    prepare_worktree: Callable[..., tuple[str, str]],
    record_metric: Callable[..., None],
    block_prelaunch_issue: Callable[..., None],
    reconcile_repair_branch: Callable[..., Any],
    worktree_error_cls: type[Exception],
    session_store: Any | None = None,
    worktree_port: Any | None = None,
    subprocess_runner: Callable[..., Any] | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
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
        record_metric(
            db,
            config,
            "worktree_blocked",
            issue_ref=issue_ref,
            payload={"reason": err.reason_code, "detail": err.detail},
        )
        blocked_reason = (
            err.reason_code
            if err.reason_code == "worktree_in_use"
            else f"workspace_prepare:{err.detail}"
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
    config: Any,
    prepared: Any,
    load_worktree_workflow: Callable[..., Any],
) -> tuple[Any, Any]:
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
    cp_config: Any,
    auto_config: Any,
    board_snapshot: Any,
    pr_port: Any,
    gh_runner: Callable[..., str] | None,
    parse_issue_ref: Callable[[str], Any],
    resolve_issue_coordinates: Callable[..., tuple[str, str, int]],
    snapshot_for_issue: Callable[..., Any],
    classify_open_pr_candidates: Callable[..., tuple[str, Any | None, str]],
    launch_session_kind: Callable[..., str],
) -> tuple[str, str, str, int, Any, str, str | None, str | None]:
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
    snapshot: Any,
    config: Any,
    db: Any,
    issue_context_port: Any,
    gh_runner: Callable[..., str] | None,
    hydrate_issue_context: Callable[..., dict[str, Any]],
) -> tuple[dict[str, Any], str]:
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
    workflow_definition: Any,
    *,
    worktree_path: str,
    issue_ref: str,
    branch_name: str,
    worktree_port: Any,
    subprocess_runner: Callable[..., Any] | None,
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
