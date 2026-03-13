"""Claimed-session execution orchestration for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Protocol, cast

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    CodexSessionResult,
    PrCreationOutcome,
    PreparedCycleContext,
    PreparedLaunchContext,
    SessionExecutionOutcome,
)
from startupai_controller.consumer_workflow import WorkflowDefinition
from startupai_controller.domain.models import CycleResult, ReviewQueueDrainSummary
from startupai_controller.domain.models import ResolutionEvaluation, ReviewQueueEntry
from startupai_controller.domain.resolution_policy import ResolutionPayload
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.process_runner import GhRunnerPort, ProcessRunnerPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

if TYPE_CHECKING:
    import subprocess

CodexResultPayload = CodexSessionResult
SubprocessRunnerFn = Callable[..., "subprocess.CompletedProcess[str]"]


class CreatePrForExecutionResultFn(Protocol):
    """Create or salvage a PR for one claimed execution result."""

    def __call__(
        self,
        *,
        config: ConsumerConfig,
        launch_context: PreparedLaunchContext,
        claimed_context: ClaimedSessionContext,
        codex_result: CodexResultPayload | None,
        session_status: str,
        failure_reason: str | None,
        subprocess_runner: SubprocessRunnerFn | None,
        gh_runner: Callable[..., str] | None,
    ) -> PrCreationOutcome: ...


class HandoffExecutionToReviewFn(Protocol):
    """Transition a claimed execution into review and rescue processing."""

    def __call__(
        self,
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext,
        session_id: str,
        pr_url: str,
        session_status: str,
        session_store: SessionStorePort,
        review_state_port: ReviewStatePort | None,
        board_port: BoardMutationPort | None,
    ) -> ReviewQueueDrainSummary: ...


class HandleNonReviewExecutionOutcomeFn(Protocol):
    """Apply non-review post-execution state transitions."""

    def __call__(
        self,
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext,
        session_id: str,
        session_status: str,
        codex_result: CodexResultPayload | None,
        has_commits: bool,
        gh_runner: GhRunnerPort | Callable[..., str] | None,
        pr_port: PullRequestPort,
        board_port: BoardMutationPort,
    ) -> tuple[str, ResolutionEvaluation | None, str | None]: ...


class BuildSessionExecutionOutcomeFn(Protocol):
    """Build the final session-execution outcome value object."""

    def __call__(
        self,
        *,
        session_status: str,
        failure_reason: str | None,
        pr_url: str | None,
        has_commits: bool,
        codex_result: CodexResultPayload | None,
        should_transition_to_review: bool,
        immediate_review_summary: ReviewQueueDrainSummary,
        resolution_evaluation: ResolutionEvaluation | None,
        done_reason: str | None,
    ) -> SessionExecutionOutcome: ...


class VerifyResolutionPayloadFn(Protocol):
    """Verify the structured resolution payload emitted by Codex."""

    def __call__(
        self,
        issue_ref: str,
        resolution: ResolutionPayload | None,
        *,
        config: ConsumerConfig,
        workflows: dict[str, WorkflowDefinition],
        pr_port: PullRequestPort,
    ) -> ResolutionEvaluation: ...


class ApplyResolutionActionFn(Protocol):
    """Apply the verified resolution action to board/runtime state."""

    def __call__(
        self,
        issue_ref: str,
        resolution_evaluation: ResolutionEvaluation,
        *,
        session_id: str,
        db: ConsumerRuntimeStatePort,
        config: ConsumerConfig,
        critical_path_config: CriticalPathConfig,
        board_port: BoardMutationPort,
    ) -> str | None: ...


class TransitionClaimedSessionToReviewFn(Protocol):
    """Transition a claimed issue into Review."""

    def __call__(
        self,
        *,
        db: ConsumerRuntimeStatePort,
        issue_ref: str,
        session_id: str,
        config: ConsumerConfig,
        critical_path_config: CriticalPathConfig,
        review_state_port: ReviewStatePort | None,
        board_port: BoardMutationPort | None,
    ) -> None: ...


class PostClaimedSessionVerdictMarkerFn(Protocol):
    """Post the structured review verdict marker for a claimed session."""

    def __call__(
        self,
        *,
        db: ConsumerRuntimeStatePort,
        pr_url: str,
        session_id: str,
    ) -> None: ...


class RunImmediateReviewHandoffFn(Protocol):
    """Run immediate review rescue for a freshly opened review PR."""

    def __call__(
        self,
        *,
        config: ConsumerConfig,
        critical_path_config: CriticalPathConfig,
        automation_config: BoardAutomationConfig,
        store: SessionStorePort,
        queue_entry: ReviewQueueEntry,
        db: ConsumerRuntimeStatePort,
    ) -> ReviewQueueDrainSummary: ...


class RecordMetricFn(Protocol):
    """Record one consumer metric event."""

    def __call__(
        self,
        db: ConsumerRuntimeStatePort,
        config: ConsumerConfig,
        metric_name: str,
        *,
        issue_ref: str | None = None,
    ) -> None: ...


class ReturnIssueToReadyFn(Protocol):
    """Return one issue to Ready after a non-review outcome."""

    def __call__(
        self,
        issue_ref: str,
        config: CriticalPathConfig,
        project_owner: str,
        project_number: int,
        *,
        board_port: BoardMutationPort | None = None,
    ) -> None: ...


class QueueStatusTransitionFn(Protocol):
    """Queue one deferred board-status transition."""

    def __call__(
        self,
        db: ConsumerRuntimeStatePort,
        issue_ref: str,
        *,
        to_status: str,
        from_statuses: set[str],
        blocked_reason: str | None = None,
    ) -> None: ...


class FinalPhaseForClaimedSessionFn(Protocol):
    """Resolve the final persisted phase for a claimed session."""

    def __call__(
        self,
        *,
        launch_context: PreparedLaunchContext,
        execution_outcome: SessionExecutionOutcome,
    ) -> str: ...


class PersistClaimedSessionCompletionFn(Protocol):
    """Persist the final session state for one claimed execution."""

    def __call__(
        self,
        *,
        db: ConsumerRuntimeStatePort,
        session_id: str,
        issue_ref: str,
        execution_outcome: SessionExecutionOutcome,
        final_phase: str,
    ) -> None: ...


class PostClaimedSessionResultCommentFn(Protocol):
    """Post the final result comment for one claimed session."""

    def __call__(
        self,
        *,
        issue_ref: str,
        session_id: str,
        codex_result: CodexResultPayload | None,
        cp_config: CriticalPathConfig,
    ) -> None: ...


class MaybeEscalateClaimedSessionFailureFn(Protocol):
    """Escalate a terminal claimed-session failure after retries exhaust."""

    def __call__(
        self,
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        issue_ref: str,
        effective_max_retries: int,
        session_status: str,
        codex_result: CodexResultPayload | None,
        cp_config: CriticalPathConfig,
    ) -> None: ...


class QueueClaimedSessionForReviewFn(Protocol):
    """Queue a claimed session for review processing."""

    def __call__(
        self,
        *,
        store: SessionStorePort,
        issue_ref: str,
        pr_url: str,
        session_id: str,
    ) -> ReviewQueueEntry | None: ...


@dataclass(frozen=True)
class ExecutionDeps:
    """Injected seams for claimed-session execution."""

    assemble_codex_prompt: Callable[..., str]
    run_codex_session: Callable[..., int]
    parse_codex_result: Callable[..., CodexResultPayload | None]
    session_status_from_codex_result: Callable[..., tuple[str, str | None]]
    create_pr_for_execution_result: CreatePrForExecutionResultFn
    handoff_execution_to_review: HandoffExecutionToReviewFn
    handle_non_review_execution_outcome: HandleNonReviewExecutionOutcomeFn
    build_session_execution_outcome: BuildSessionExecutionOutcomeFn


@dataclass(frozen=True)
class ReviewHandoffDeps:
    """Injected seams for claimed-session review handoff."""

    transition_claimed_session_to_review: TransitionClaimedSessionToReviewFn
    post_claimed_session_verdict_marker: PostClaimedSessionVerdictMarkerFn
    queue_claimed_session_for_review: QueueClaimedSessionForReviewFn
    run_immediate_review_handoff: RunImmediateReviewHandoffFn
    record_metric: RecordMetricFn


@dataclass(frozen=True)
class NonReviewOutcomeDeps:
    """Injected seams for non-review execution outcomes."""

    verify_resolution_payload: VerifyResolutionPayloadFn
    apply_resolution_action: ApplyResolutionActionFn
    return_issue_to_ready: ReturnIssueToReadyFn
    record_successful_github_mutation: Callable[[ConsumerRuntimeStatePort], None]
    mark_degraded: Callable[[ConsumerRuntimeStatePort, str], None]
    queue_status_transition: QueueStatusTransitionFn
    record_metric: RecordMetricFn
    log_ready_reset_failure: Callable[[Exception], None]


def execute_claimed_session(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    deps: ExecutionDeps,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    session_store: SessionStorePort,
    gh_runner: GhRunnerPort | None,
    process_runner: ProcessRunnerPort | None,
    file_reader: Callable[[Path], str] | None,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    pr_port: PullRequestPort | None,
) -> SessionExecutionOutcome:
    """Execute Codex for a claimed session and apply immediate board handoff."""
    candidate = launch_context.issue_ref
    session_id = claimed_context.session_id

    prompt = deps.assemble_codex_prompt(
        launch_context.issue_context,
        candidate,
        prepared.cp_config,
        launch_context.effective_consumer_config,
        launch_context.worktree_path,
        launch_context.branch_name,
        dependency_summary=launch_context.dependency_summary,
        workflow_definition=launch_context.workflow_definition,
        session_kind=launch_context.session_kind,
        repair_pr_url=launch_context.repair_pr_url,
        branch_reconcile_state=launch_context.branch_reconcile_state,
        branch_reconcile_error=launch_context.branch_reconcile_error,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = config.output_dir / f"{session_id}.json"
    exit_code = deps.run_codex_session(
        launch_context.worktree_path,
        prompt,
        config.schema_path,
        output_path,
        launch_context.effective_consumer_config.codex_timeout_seconds,
        heartbeat_fn=lambda: db.update_heartbeat(candidate),
        subprocess_runner=process_runner.run if process_runner is not None else None,
    )

    codex_result = deps.parse_codex_result(output_path, file_reader=file_reader)
    session_status, failure_reason = deps.session_status_from_codex_result(
        exit_code,
        codex_result,
    )
    pr_outcome = deps.create_pr_for_execution_result(
        config=config,
        launch_context=launch_context,
        claimed_context=claimed_context,
        codex_result=codex_result,
        session_status=session_status,
        failure_reason=failure_reason,
        subprocess_runner=process_runner.run if process_runner is not None else None,
        gh_runner=gh_runner.run_gh if gh_runner is not None else None,
    )

    should_transition_to_review = bool(pr_outcome.pr_url) and (
        launch_context.session_kind != "repair"
        or pr_outcome.session_status == "success"
    )

    immediate_review_summary = ReviewQueueDrainSummary()
    resolution_evaluation = None
    done_reason: str | None = None
    effective_session_status = pr_outcome.session_status
    if should_transition_to_review:
        immediate_review_summary = deps.handoff_execution_to_review(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            session_id=session_id,
            pr_url=pr_outcome.pr_url or "",
            session_status=pr_outcome.session_status,
            session_store=session_store,
            review_state_port=review_state_port,
            board_port=board_port,
        )
    else:
        if pr_port is None or board_port is None:
            raise ValueError(
                "pr_port and board_port are required for non-review execution outcomes"
            )
        (
            effective_session_status,
            resolution_evaluation,
            done_reason,
        ) = deps.handle_non_review_execution_outcome(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            session_id=session_id,
            session_status=pr_outcome.session_status,
            codex_result=codex_result,
            has_commits=pr_outcome.has_commits,
            gh_runner=gh_runner,
            pr_port=pr_port,
            board_port=board_port,
        )

    return deps.build_session_execution_outcome(
        session_status=effective_session_status,
        failure_reason=pr_outcome.failure_reason,
        pr_url=pr_outcome.pr_url,
        has_commits=pr_outcome.has_commits,
        codex_result=codex_result,
        should_transition_to_review=should_transition_to_review,
        immediate_review_summary=immediate_review_summary,
        resolution_evaluation=resolution_evaluation,
        done_reason=done_reason,
    )


def handoff_execution_to_review(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    deps: ReviewHandoffDeps,
    launch_context: PreparedLaunchContext,
    session_id: str,
    pr_url: str,
    session_status: str,
    session_store: SessionStorePort,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
) -> ReviewQueueDrainSummary:
    """Transition a claimed session into Review and perform immediate rescue."""
    cp_config = prepared.cp_config
    auto_config = prepared.auto_config
    candidate = launch_context.issue_ref

    deps.transition_claimed_session_to_review(
        db=db,
        issue_ref=candidate,
        session_id=session_id,
        config=config,
        critical_path_config=cp_config,
        review_state_port=review_state_port,
        board_port=board_port,
    )
    deps.record_metric(db, config, "session_transition_review", issue_ref=candidate)

    immediate_review_summary = ReviewQueueDrainSummary()
    if session_status != "success":
        return immediate_review_summary

    handoff_store = session_store
    deps.post_claimed_session_verdict_marker(
        db=db,
        pr_url=pr_url,
        session_id=session_id,
    )
    queue_entry = deps.queue_claimed_session_for_review(
        store=handoff_store,
        issue_ref=candidate,
        pr_url=pr_url,
        session_id=session_id,
    )
    if queue_entry is None or auto_config is None:
        return immediate_review_summary

    return deps.run_immediate_review_handoff(
        config=config,
        critical_path_config=cp_config,
        automation_config=auto_config,
        store=handoff_store,
        queue_entry=queue_entry,
        db=db,
    )


def handle_non_review_execution_outcome(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    deps: NonReviewOutcomeDeps,
    launch_context: PreparedLaunchContext,
    session_id: str,
    session_status: str,
    codex_result: CodexResultPayload | None,
    has_commits: bool,
    gh_runner: GhRunnerPort | Callable[..., str] | None,
    pr_port: PullRequestPort,
    board_port: BoardMutationPort,
) -> tuple[str, ResolutionEvaluation | None, str | None]:
    """Handle non-review outcomes for a claimed session."""
    cp_config = prepared.cp_config
    candidate = launch_context.issue_ref
    resolution_evaluation = None
    done_reason: str | None = None
    updated_session_status = session_status
    if gh_runner is None:
        effective_gh_runner = None
    elif hasattr(gh_runner, "run_gh"):
        effective_gh_runner = cast(GhRunnerPort, gh_runner).run_gh
    else:
        effective_gh_runner = cast(Callable[..., str], gh_runner)

    if session_status == "success" and not has_commits:
        resolution_evaluation = deps.verify_resolution_payload(
            candidate,
            codex_result["resolution"] if codex_result else None,
            config=launch_context.effective_consumer_config,
            workflows=prepared.main_workflows,
            pr_port=pr_port,
        )
        done_reason = deps.apply_resolution_action(
            candidate,
            resolution_evaluation,
            session_id=session_id,
            db=db,
            config=config,
            critical_path_config=cp_config,
            board_port=board_port,
        )
        if done_reason == "already_resolved":
            deps.record_metric(
                db, config, "session_transition_done", issue_ref=candidate
            )
        return updated_session_status, resolution_evaluation, done_reason

    try:
        deps.return_issue_to_ready(
            candidate,
            cp_config,
            config.project_owner,
            config.project_number,
            board_port=board_port,
        )
        deps.record_successful_github_mutation(db)
    except Exception as err:
        deps.log_ready_reset_failure(err)
        deps.mark_degraded(db, f"ready-reset:{err}")
        deps.queue_status_transition(
            db,
            candidate,
            to_status="Ready",
            from_statuses={"In Progress", "Review"},
        )
    if session_status == "failed" and not has_commits and codex_result is None:
        updated_session_status = "aborted"
    return updated_session_status, resolution_evaluation, done_reason


@dataclass(frozen=True)
class FinalizeClaimedSessionDeps:
    """Injected seams for claimed-session finalization."""

    final_phase_for_claimed_session: FinalPhaseForClaimedSessionFn
    persist_claimed_session_completion: PersistClaimedSessionCompletionFn
    post_claimed_session_result_comment: PostClaimedSessionResultCommentFn
    maybe_escalate_claimed_session_failure: MaybeEscalateClaimedSessionFailureFn


def finalize_claimed_session(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    deps: FinalizeClaimedSessionDeps,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    execution_outcome: SessionExecutionOutcome,
    review_state_port: ReviewStatePort | None,
) -> CycleResult:
    """Persist final session state and return the cycle result."""
    cp_config = prepared.cp_config
    candidate = launch_context.issue_ref
    session_id = claimed_context.session_id
    effective_max_retries = claimed_context.effective_max_retries

    db.release_lease(candidate)
    final_phase = deps.final_phase_for_claimed_session(
        launch_context=launch_context,
        execution_outcome=execution_outcome,
    )
    deps.persist_claimed_session_completion(
        db=db,
        session_id=session_id,
        issue_ref=candidate,
        execution_outcome=execution_outcome,
        final_phase=final_phase,
    )
    deps.post_claimed_session_result_comment(
        issue_ref=candidate,
        session_id=session_id,
        codex_result=execution_outcome.codex_result,
        cp_config=cp_config,
    )
    deps.maybe_escalate_claimed_session_failure(
        config=config,
        db=db,
        issue_ref=candidate,
        effective_max_retries=effective_max_retries,
        session_status=execution_outcome.session_status,
        codex_result=execution_outcome.codex_result,
        cp_config=cp_config,
    )

    return CycleResult(
        action="claimed",
        issue_ref=candidate,
        session_id=session_id,
        reason=execution_outcome.session_status,
        pr_url=execution_outcome.pr_url,
    )
