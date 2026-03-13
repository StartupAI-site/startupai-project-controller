"""Launch and execution wiring extracted from board_consumer."""

from __future__ import annotations

import logging
import subprocess
from collections.abc import Callable
from typing import Protocol, cast

import startupai_controller.consumer_comment_pr_helpers as _comment_pr_helpers
import startupai_controller.consumer_execution_support_helpers as _execution_support_helpers
import startupai_controller.consumer_launch_support_wiring as _launch_support_wiring
from startupai_controller.application.consumer.cycle import PreparedCycleDeps
from startupai_controller.application.consumer.launch import (
    ClaimLaunchDeps,
    PrepareLaunchDeps,
    ResolveLaunchDeps,
    claim_launch_context as _claim_launch_context_use_case,
    prepare_launch_candidate as _prepare_launch_candidate_use_case,
    resolve_launch_context_for_cycle as _resolve_launch_context_for_cycle_use_case,
)
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    IssueContextPayload,
    PendingClaimContext,
    PreparedCycleContext,
    PreparedLaunchContext,
    SelectedLaunchCandidate,
    SessionExecutionOutcome,
)
from startupai_controller.consumer_workflow import WorkflowDefinition
from startupai_controller.domain.models import (
    ClaimReadyResult,
    CycleBoardSnapshot,
    CycleResult,
    IssueSnapshot,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.issue_context import IssueContextPort
from startupai_controller.ports.process_runner import GhRunnerPort, ProcessRunnerPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.ready_flow import (
    BoardInfoResolverFn,
    BoardStatusMutatorFn,
    CommentCheckerFn,
    CommentPosterFn,
    GitHubRunnerFn,
    StatusResolverFn,
)
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.ports.worktrees import WorktreePort
from startupai_controller.runtime.wiring import (
    build_gh_runner_port,
    ConsumerDB,
    build_github_port_bundle,
    build_process_runner_port,
    build_session_store,
    build_worktree_port,
)
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

logger = logging.getLogger("board-consumer")
SubprocessRunnerFn = Callable[..., subprocess.CompletedProcess[str]]


class LoggerLike(Protocol):
    """Minimal logger surface used by the cycle wiring wrappers."""

    def info(self, msg: str, *args: object) -> None:
        """Record one informational log line."""
        ...


def assemble_prepared_launch_context(
    issue_ref: str,
    *,
    candidate_prefix: str,
    owner: str,
    repo: str,
    number: int,
    title: str,
    context: IssueContextPayload,
    session_kind: str,
    repair_pr_url: str | None,
    repair_branch_name: str | None,
    worktree_path: str,
    branch_name: str,
    workflow_definition: WorkflowDefinition,
    effective_consumer_config: ConsumerConfig,
    dependency_summary: str,
    branch_reconcile_state: str | None,
    branch_reconcile_error: str | None,
) -> PreparedLaunchContext:
    """Create the final launch context for a prepared candidate."""
    return PreparedLaunchContext(
        issue_ref=issue_ref,
        repo_prefix=candidate_prefix,
        owner=owner,
        repo=repo,
        number=number,
        title=title,
        issue_context=context,
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


def prepare_launch_candidate(
    issue_ref: str,
    *,
    config: ConsumerConfig,
    prepared: PreparedCycleContext,
    db: ConsumerRuntimeStatePort,
    subprocess_runner: ProcessRunnerPort | SubprocessRunnerFn | None = None,
    review_state_port: ReviewStatePort | None = None,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: BoardInfoResolverFn | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: GitHubRunnerFn | GhRunnerPort | None = None,
    pr_port: PullRequestPort | None = None,
    issue_context_port: IssueContextPort | None = None,
    session_store: SessionStorePort | None = None,
    worktree_port: WorktreePort | None = None,
) -> PreparedLaunchContext:
    """Prepare local launch state for an issue before board claim."""
    process_runner_port = (
        cast(ProcessRunnerPort, subprocess_runner)
        if hasattr(subprocess_runner, "run")
        else build_process_runner_port(
            gh_runner=cast(GitHubRunnerFn | None, gh_runner),
            subprocess_runner=cast(SubprocessRunnerFn | None, subprocess_runner),
        )
    )
    gh_port = (
        cast(GhRunnerPort, gh_runner)
        if hasattr(gh_runner, "run_gh")
        else build_gh_runner_port(gh_runner=cast(GitHubRunnerFn | None, gh_runner))
    )
    gh_runner_fn = gh_port.run_gh if gh_port is not None else None
    github_bundle = (
        None
        if pr_port is not None and issue_context_port is not None
        else build_github_port_bundle(
            config.project_owner,
            config.project_number,
            config=prepared.cp_config,
            github_memo=prepared.github_memo,
            gh_runner=gh_runner_fn,
        )
    )
    effective_pr_port = pr_port or (
        github_bundle.pull_requests if github_bundle is not None else None
    )
    effective_issue_context_port = issue_context_port or (
        github_bundle.issue_context if github_bundle is not None else None
    )
    assert effective_pr_port is not None
    assert effective_issue_context_port is not None

    def _resolve_launch_candidate_metadata(
        issue_ref: str,
        *,
        cp_config: CriticalPathConfig,
        auto_config: BoardAutomationConfig | None,
        board_snapshot: CycleBoardSnapshot,
        pr_port: PullRequestPort,
    ) -> tuple[str, str, str, int, IssueSnapshot | None, str, str | None, str | None]:
        return _launch_support_wiring.resolve_launch_candidate_metadata(
            issue_ref,
            cp_config=cp_config,
            auto_config=auto_config,
            board_snapshot=board_snapshot,
            pr_port=pr_port,
            gh_runner=gh_runner_fn,
        )

    def _resolve_launch_issue_context(
        issue_ref: str,
        *,
        owner: str,
        repo: str,
        number: int,
        snapshot: IssueSnapshot | None,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        issue_context_port: IssueContextPort,
    ) -> tuple[IssueContextPayload, str]:
        return _launch_support_wiring.resolve_launch_issue_context(
            issue_ref,
            owner=owner,
            repo=repo,
            number=number,
            snapshot=snapshot,
            config=config,
            db=db,
            issue_context_port=issue_context_port,
            gh_runner=gh_runner_fn,
        )

    def _setup_launch_worktree(
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
    ) -> tuple[str, str, str | None, str | None]:
        return _launch_support_wiring.setup_launch_worktree(
            issue_ref,
            title,
            session_kind,
            repair_branch_name,
            config=config,
            cp_config=cp_config,
            db=db,
            session_store=session_store,
            worktree_port=worktree_port,
            subprocess_runner=subprocess_runner,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner_fn,
        )

    return _prepare_launch_candidate_use_case(
        issue_ref,
        config=config,
        prepared=prepared,
        db=db,
        deps=PrepareLaunchDeps(
            resolve_launch_candidate_metadata=_resolve_launch_candidate_metadata,
            resolve_launch_issue_context=_resolve_launch_issue_context,
            setup_launch_worktree=_setup_launch_worktree,
            resolve_launch_runtime=_launch_support_wiring.resolve_launch_runtime,
            run_launch_workspace_hooks=_launch_support_wiring.run_launch_workspace_hooks,
            build_dependency_summary=_execution_support_helpers.build_dependency_summary,
            assemble_prepared_launch_context=assemble_prepared_launch_context,
        ),
        subprocess_runner=process_runner_port,
        review_state_port=review_state_port,
        gh_runner=gh_port,
        pr_port=effective_pr_port,
        issue_context_port=effective_issue_context_port,
        session_store=session_store or build_session_store(cast(ConsumerDB, db)),
        worktree_port=worktree_port
        or build_worktree_port(
            subprocess_runner=(
                process_runner_port.run if process_runner_port is not None else None
            ),
            gh_runner=gh_runner_fn,
        ),
    )


def resolve_launch_context_for_cycle(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext | None,
    target_issue: str | None,
    dry_run: bool,
    review_state_port: ReviewStatePort | None,
    pr_port: PullRequestPort | None,
    status_resolver: Callable[..., str] | None,
    subprocess_runner: ProcessRunnerPort | SubprocessRunnerFn | None,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: GitHubRunnerFn | GhRunnerPort | None,
    select_launch_candidate_for_cycle: Callable[
        ...,
        tuple[SelectedLaunchCandidate | None, CycleResult | None],
    ],
    prepare_selected_launch_candidate: Callable[
        ...,
        tuple[PreparedLaunchContext | None, CycleResult | None],
    ],
    logger: LoggerLike,
) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
    """Resolve or prepare launch-ready work for this cycle."""

    def _select_launch_candidate(
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        target_issue: str | None,
    ) -> tuple[SelectedLaunchCandidate | None, CycleResult | None]:
        return select_launch_candidate_for_cycle(
            config=config,
            db=db,
            prepared=prepared,
            target_issue=target_issue,
            status_resolver=status_resolver,
            gh_runner=gh_runner,
        )

    def _prepare_selected_launch_candidate(
        *,
        selected_candidate: SelectedLaunchCandidate,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
    ) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
        return prepare_selected_launch_candidate(
            selected_candidate=selected_candidate,
            config=config,
            db=db,
            prepared=prepared,
            subprocess_runner=subprocess_runner,
            status_resolver=status_resolver,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
            pr_port=pr_port,
        )

    return _resolve_launch_context_for_cycle_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=ResolveLaunchDeps(
            select_launch_candidate_for_cycle=_select_launch_candidate,
            prepare_selected_launch_candidate=_prepare_selected_launch_candidate,
        ),
        launch_context=launch_context,
        target_issue=target_issue,
        dry_run=dry_run,
        log_dry_run=lambda issue_ref: logger.info(
            "[dry-run] Would prepare, claim, and execute: %s",
            issue_ref,
        ),
    )


def claim_launch_context(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    slot_id: int,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    status_resolver: StatusResolverFn | None,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | GhRunnerPort | None,
    open_pending_claim_session: Callable[
        ...,
        tuple[PendingClaimContext | None, CycleResult | None],
    ],
    enforce_claim_retry_ceiling: Callable[..., CycleResult | None],
    attempt_launch_context_claim: Callable[
        ...,
        tuple[ClaimReadyResult | None, CycleResult | None],
    ],
    mark_claimed_session_running: Callable[..., ClaimedSessionContext],
) -> tuple[ClaimedSessionContext | None, CycleResult | None]:
    """Claim board ownership and start a durable running session."""
    effective_comment_checker = comment_checker or (
        _comment_pr_helpers.comment_checker_from_review_state_port(review_state_port)
        if review_state_port is not None
        else None
    )
    effective_status_resolver = status_resolver or (
        review_state_port.get_issue_status if review_state_port is not None else None
    )

    def _enforce_claim_retry_ceiling(
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        launch_context: PreparedLaunchContext,
        pending_claim: PendingClaimContext,
        cp_config: CriticalPathConfig,
    ) -> CycleResult | None:
        return enforce_claim_retry_ceiling(
            config=config,
            db=db,
            launch_context=launch_context,
            pending_claim=pending_claim,
            cp_config=cp_config,
            board_info_resolver=board_info_resolver,
            comment_checker=effective_comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )

    def _attempt_launch_context_claim(
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext,
        pending_claim: PendingClaimContext,
        slot_id: int,
    ) -> tuple[ClaimReadyResult | None, CycleResult | None]:
        return attempt_launch_context_claim(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            pending_claim=pending_claim,
            slot_id=slot_id,
            status_resolver=effective_status_resolver,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_checker=effective_comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )

    def _mark_claimed_session_running(
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        launch_context: PreparedLaunchContext,
        pending_claim: PendingClaimContext,
        slot_id: int,
        cp_config: CriticalPathConfig,
    ) -> ClaimedSessionContext:
        return mark_claimed_session_running(
            config=config,
            db=db,
            launch_context=launch_context,
            pending_claim=pending_claim,
            slot_id=slot_id,
            comment_checker=effective_comment_checker,
            comment_poster=comment_poster,
            cp_config=cp_config,
            gh_runner=gh_runner,
        )

    return _claim_launch_context_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=ClaimLaunchDeps(
            open_pending_claim_session=open_pending_claim_session,
            enforce_claim_retry_ceiling=_enforce_claim_retry_ceiling,
            attempt_launch_context_claim=_attempt_launch_context_claim,
            mark_claimed_session_running=_mark_claimed_session_running,
        ),
        launch_context=launch_context,
        slot_id=slot_id,
    )


def prepared_cycle_deps(
    *,
    claim_suppression_state: Callable[..., dict[str, str] | None],
    next_available_slot: Callable[..., int | None],
    resolve_launch_context_for_cycle: Callable[
        ...,
        tuple[PreparedLaunchContext | None, CycleResult | None],
    ],
    claim_launch_context: Callable[
        ...,
        tuple[ClaimedSessionContext | None, CycleResult | None],
    ],
    execute_claimed_session: Callable[..., SessionExecutionOutcome],
    finalize_claimed_session: Callable[..., CycleResult],
) -> PreparedCycleDeps:
    """Bind board_consumer helpers for the extracted prepared-cycle slice."""
    return PreparedCycleDeps(
        claim_suppression_state=claim_suppression_state,
        next_available_slot=next_available_slot,
        resolve_launch_context_for_cycle=resolve_launch_context_for_cycle,
        claim_launch_context=claim_launch_context,
        execute_claimed_session=execute_claimed_session,
        finalize_claimed_session=finalize_claimed_session,
    )
