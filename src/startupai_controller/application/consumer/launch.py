"""Launch and claim orchestration for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    IssueContextPayload,
    PendingClaimContext,
    PreparedCycleContext,
    PreparedLaunchContext,
    SelectedLaunchCandidate,
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
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.ports.worktrees import WorktreePort
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

if TYPE_CHECKING:
    import subprocess

SubprocessRunnerFn = Callable[..., "subprocess.CompletedProcess[str]"]


class SelectLaunchCandidateForCycleFn(Protocol):
    """Select a candidate to launch for the current cycle."""

    def __call__(
        self,
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        target_issue: str | None,
    ) -> tuple[SelectedLaunchCandidate | None, CycleResult | None]: ...


class PrepareSelectedLaunchCandidateFn(Protocol):
    """Turn a selected launch candidate into prepared local state."""

    def __call__(
        self,
        *,
        selected_candidate: SelectedLaunchCandidate,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
    ) -> tuple[PreparedLaunchContext | None, CycleResult | None]: ...


class ResolveLaunchCandidateMetadataFn(Protocol):
    """Resolve metadata needed to prepare a launch candidate."""

    def __call__(
        self,
        issue_ref: str,
        *,
        cp_config: CriticalPathConfig,
        auto_config: BoardAutomationConfig | None,
        board_snapshot: CycleBoardSnapshot,
        pr_port: PullRequestPort,
    ) -> tuple[
        str, str, str, int, IssueSnapshot | None, str, str | None, str | None
    ]: ...


class ResolveLaunchIssueContextFn(Protocol):
    """Resolve issue context and title for a launch candidate."""

    def __call__(
        self,
        issue_ref: str,
        *,
        owner: str,
        repo: str,
        number: int,
        snapshot: IssueSnapshot | None,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        issue_context_port: IssueContextPort,
    ) -> tuple[IssueContextPayload, str]: ...


class SetupLaunchWorktreeFn(Protocol):
    """Prepare or reuse the launch worktree for a candidate."""

    def __call__(
        self,
        issue_ref: str,
        title: str,
        session_kind: str,
        repair_branch_name: str | None,
        *,
        config: ConsumerConfig,
        cp_config: CriticalPathConfig,
        db: ConsumerRuntimeStatePort,
        session_store: SessionStorePort,
        worktree_port: WorktreePort,
        subprocess_runner: SubprocessRunnerFn | None = None,
    ) -> tuple[str, str, str | None, str | None]: ...


class ResolveLaunchRuntimeFn(Protocol):
    """Resolve runtime config and workflow definition for a launch."""

    def __call__(
        self,
        candidate_prefix: str,
        worktree_path: str,
        *,
        config: ConsumerConfig,
        prepared: PreparedCycleContext,
    ) -> tuple[WorkflowDefinition, ConsumerConfig]: ...


class RunLaunchWorkspaceHooksFn(Protocol):
    """Run pre-launch hooks for a prepared worktree."""

    def __call__(
        self,
        workflow_definition: WorkflowDefinition,
        *,
        worktree_path: str,
        issue_ref: str,
        branch_name: str,
        worktree_port: WorktreePort,
        subprocess_runner: SubprocessRunnerFn | None,
    ) -> None: ...


class AssemblePreparedLaunchContextFn(Protocol):
    """Build the final prepared launch context for downstream steps."""

    def __call__(
        self,
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
    ) -> PreparedLaunchContext: ...


class OpenPendingClaimSessionFn(Protocol):
    """Create the pending claim session record for a prepared launch."""

    def __call__(
        self,
        *,
        db: ConsumerRuntimeStatePort,
        launch_context: PreparedLaunchContext,
        executor: str,
        slot_id: int,
    ) -> tuple[PendingClaimContext | None, CycleResult | None]: ...


class EnforceClaimRetryCeilingFn(Protocol):
    """Stop work when the candidate exceeded its retry ceiling."""

    def __call__(
        self,
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        launch_context: PreparedLaunchContext,
        pending_claim: PendingClaimContext,
        cp_config: CriticalPathConfig,
    ) -> CycleResult | None: ...


class AttemptLaunchContextClaimFn(Protocol):
    """Attempt to claim board ownership for a prepared launch."""

    def __call__(
        self,
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext,
        pending_claim: PendingClaimContext,
        slot_id: int,
    ) -> tuple[ClaimReadyResult | None, CycleResult | None]: ...


class MarkClaimedSessionRunningFn(Protocol):
    """Persist the durable running-session state after claim."""

    def __call__(
        self,
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        launch_context: PreparedLaunchContext,
        pending_claim: PendingClaimContext,
        slot_id: int,
        cp_config: CriticalPathConfig,
    ) -> ClaimedSessionContext: ...


@dataclass(frozen=True)
class ResolveLaunchDeps:
    """Injected seams for launch-context resolution."""

    select_launch_candidate_for_cycle: SelectLaunchCandidateForCycleFn
    prepare_selected_launch_candidate: PrepareSelectedLaunchCandidateFn


@dataclass(frozen=True)
class PrepareLaunchDeps:
    """Injected seams for launch preparation."""

    resolve_launch_candidate_metadata: ResolveLaunchCandidateMetadataFn
    resolve_launch_issue_context: ResolveLaunchIssueContextFn
    setup_launch_worktree: SetupLaunchWorktreeFn
    resolve_launch_runtime: ResolveLaunchRuntimeFn
    run_launch_workspace_hooks: RunLaunchWorkspaceHooksFn
    build_dependency_summary: Callable[..., str]
    assemble_prepared_launch_context: AssemblePreparedLaunchContextFn


def resolve_launch_context_for_cycle(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    deps: ResolveLaunchDeps,
    launch_context: PreparedLaunchContext | None,
    target_issue: str | None,
    dry_run: bool,
    log_dry_run: Callable[[str], None] | None = None,
) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
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
    config: ConsumerConfig,
    prepared: PreparedCycleContext,
    db: ConsumerRuntimeStatePort,
    deps: PrepareLaunchDeps,
    subprocess_runner: ProcessRunnerPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    gh_runner: GhRunnerPort | None = None,
    pr_port: PullRequestPort,
    issue_context_port: IssueContextPort,
    session_store: SessionStorePort,
    worktree_port: WorktreePort,
) -> PreparedLaunchContext:
    """Prepare local launch state for an issue before board claim."""
    cp_config = prepared.cp_config
    auto_config = prepared.auto_config
    store = session_store
    effective_pr_port = pr_port
    effective_issue_context_port = issue_context_port
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

    open_pending_claim_session: OpenPendingClaimSessionFn
    enforce_claim_retry_ceiling: EnforceClaimRetryCeilingFn
    attempt_launch_context_claim: AttemptLaunchContextClaimFn
    mark_claimed_session_running: MarkClaimedSessionRunningFn


def claim_launch_context(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    deps: ClaimLaunchDeps,
    launch_context: PreparedLaunchContext,
    slot_id: int,
) -> tuple[ClaimedSessionContext | None, CycleResult | None]:
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
