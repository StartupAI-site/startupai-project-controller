"""Launch and claim orchestration for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from startupai_controller.domain.models import CycleResult
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.issue_context import IssueContextPort
from startupai_controller.ports.process_runner import GhRunnerPort, ProcessRunnerPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.ports.worktrees import WorktreePort


@dataclass(frozen=True)
class ResolveLaunchDeps:
    """Injected seams for launch-context resolution."""

    select_launch_candidate_for_cycle: Callable[
        ..., tuple[Any | None, CycleResult | None]
    ]
    prepare_selected_launch_candidate: Callable[
        ..., tuple[Any | None, CycleResult | None]
    ]


@dataclass(frozen=True)
class PrepareLaunchDeps:
    """Injected seams for launch preparation."""

    resolve_launch_candidate_metadata: Callable[..., tuple[Any, ...]]
    resolve_launch_issue_context: Callable[..., tuple[Any, str]]
    setup_launch_worktree: Callable[..., tuple[str, str, str | None, str | None]]
    resolve_launch_runtime: Callable[..., tuple[Any, Any]]
    run_launch_workspace_hooks: Callable[..., None]
    build_dependency_summary: Callable[..., str]
    assemble_prepared_launch_context: Callable[..., Any]


def resolve_launch_context_for_cycle(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    deps: ResolveLaunchDeps,
    launch_context: Any | None,
    target_issue: str | None,
    dry_run: bool,
    log_dry_run: Callable[[str], None] | None = None,
) -> tuple[Any | None, CycleResult | None]:
    """Resolve or prepare launch-ready work for this cycle."""
    if launch_context is not None:
        return launch_context, None

    selected_candidate, cycle_result = deps.select_launch_candidate_for_cycle(
        config=config,
        db=db,
        prepared=prepared,
        target_issue=target_issue,
    )
    if cycle_result is not None:
        return None, cycle_result

    assert selected_candidate is not None
    if dry_run:
        if log_dry_run is not None:
            log_dry_run(selected_candidate.issue_ref)
        return None, CycleResult(
            action="claimed",
            issue_ref=selected_candidate.issue_ref,
            reason="dry-run",
        )

    return deps.prepare_selected_launch_candidate(
        selected_candidate=selected_candidate,
        config=config,
        db=db,
        prepared=prepared,
    )


def prepare_launch_candidate(
    issue_ref: str,
    *,
    config: Any,
    prepared: Any,
    db: Any,
    deps: PrepareLaunchDeps,
    subprocess_runner: ProcessRunnerPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    gh_runner: GhRunnerPort | None = None,
    pr_port: PullRequestPort,
    issue_context_port: IssueContextPort | None = None,
    session_store: SessionStorePort,
    worktree_port: WorktreePort,
) -> Any:
    """Prepare local launch state for an issue before board claim."""
    cp_config = prepared.cp_config
    auto_config = prepared.auto_config
    store = session_store
    effective_pr_port = pr_port
    effective_issue_context_port = issue_context_port or effective_pr_port
    effective_worktree_port = worktree_port

    (
        candidate_prefix,
        owner,
        repo,
        number,
        snapshot,
        session_kind,
        repair_pr_url,
        repair_branch_name,
    ) = deps.resolve_launch_candidate_metadata(
        issue_ref,
        cp_config=cp_config,
        auto_config=auto_config,
        board_snapshot=prepared.board_snapshot,
        pr_port=effective_pr_port,
    )
    context, title = deps.resolve_launch_issue_context(
        issue_ref,
        owner=owner,
        repo=repo,
        number=number,
        snapshot=snapshot,
        config=config,
        db=db,
        issue_context_port=effective_issue_context_port,
    )
    worktree_path, branch_name, branch_reconcile_state, branch_reconcile_error = (
        deps.setup_launch_worktree(
            issue_ref,
            title,
            session_kind,
            repair_branch_name,
            config=config,
            cp_config=cp_config,
            db=db,
            session_store=store,
            worktree_port=effective_worktree_port,
            subprocess_runner=(
                subprocess_runner.run if subprocess_runner is not None else None
            ),
        )
    )

    workflow_definition, effective_consumer_config = deps.resolve_launch_runtime(
        candidate_prefix,
        worktree_path,
        config=config,
        prepared=prepared,
    )
    deps.run_launch_workspace_hooks(
        workflow_definition,
        worktree_path=worktree_path,
        issue_ref=issue_ref,
        branch_name=branch_name,
        worktree_port=effective_worktree_port,
        subprocess_runner=(
            subprocess_runner.run if subprocess_runner is not None else None
        ),
    )

    dependency_summary = deps.build_dependency_summary(
        issue_ref,
        cp_config,
        config.project_owner,
        config.project_number,
    )
    return deps.assemble_prepared_launch_context(
        issue_ref,
        candidate_prefix=candidate_prefix,
        owner=owner,
        repo=repo,
        number=number,
        title=title,
        context=context,
        session_kind=session_kind,
        repair_pr_url=repair_pr_url,
        repair_branch_name=repair_branch_name,
        worktree_path=worktree_path,
        branch_name=branch_name,
        workflow_definition=workflow_definition,
        effective_consumer_config=effective_consumer_config,
        dependency_summary=dependency_summary,
        branch_reconcile_state=branch_reconcile_state,
        branch_reconcile_error=branch_reconcile_error,
    )


@dataclass(frozen=True)
class ClaimLaunchDeps:
    """Injected seams for launch-claim orchestration."""

    open_pending_claim_session: Callable[..., tuple[Any | None, CycleResult | None]]
    enforce_claim_retry_ceiling: Callable[..., CycleResult | None]
    attempt_launch_context_claim: Callable[..., tuple[Any | None, CycleResult | None]]
    mark_claimed_session_running: Callable[..., Any]


def claim_launch_context(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    deps: ClaimLaunchDeps,
    launch_context: Any,
    slot_id: int,
) -> tuple[Any | None, CycleResult | None]:
    """Claim board ownership and start a durable running session."""
    pending_claim, cycle_result = deps.open_pending_claim_session(
        db=db,
        launch_context=launch_context,
        executor=config.executor,
        slot_id=slot_id,
    )
    if cycle_result is not None:
        return None, cycle_result

    assert pending_claim is not None
    retry_ceiling_result = deps.enforce_claim_retry_ceiling(
        config=config,
        db=db,
        launch_context=launch_context,
        pending_claim=pending_claim,
        cp_config=prepared.cp_config,
    )
    if retry_ceiling_result is not None:
        return None, retry_ceiling_result

    _claim_result, cycle_result = deps.attempt_launch_context_claim(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        pending_claim=pending_claim,
        slot_id=slot_id,
    )
    if cycle_result is not None:
        return None, cycle_result

    return (
        deps.mark_claimed_session_running(
            config=config,
            db=db,
            launch_context=launch_context,
            pending_claim=pending_claim,
            slot_id=slot_id,
            cp_config=prepared.cp_config,
        ),
        None,
    )
