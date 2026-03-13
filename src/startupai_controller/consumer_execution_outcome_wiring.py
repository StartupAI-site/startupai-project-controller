"""Execution outcome and review handoff wiring extracted from board_consumer."""

from __future__ import annotations

from pathlib import Path
import subprocess
from typing import Callable, Protocol, cast

import startupai_controller.consumer_comment_pr_helpers as _comment_pr_helpers
import startupai_controller.consumer_review_handoff_helpers as _review_handoff_helpers
from startupai_controller.application.consumer.execution import (
    ExecutionDeps,
    FinalizeClaimedSessionDeps,
    NonReviewOutcomeDeps,
    ReviewHandoffDeps,
    execute_claimed_session as _execute_claimed_session_use_case,
    finalize_claimed_session as _finalize_claimed_session_use_case,
    handoff_execution_to_review as _handoff_execution_to_review_use_case,
    handle_non_review_execution_outcome as _handle_non_review_execution_outcome_use_case,
)
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
import startupai_controller.consumer_session_completion_helpers as _session_completion_helpers
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    CodexSessionResult,
    PrCreationOutcome,
    PreparedCycleContext,
    PreparedLaunchContext,
    SessionExecutionOutcome,
)
from startupai_controller.consumer_workflow import WorkflowDefinition
from startupai_controller.domain.models import (
    CycleResult,
    ResolutionEvaluation,
    ReviewQueueDrainSummary,
    ReviewQueueEntry,
)
from startupai_controller.domain.resolution_policy import ResolutionPayload
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.process_runner import GhRunnerPort, ProcessRunnerPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.ready_flow import (
    BoardInfoResolverFn,
    BoardStatusMutatorFn,
    CommentCheckerFn,
    CommentPosterFn,
    GitHubRunnerFn,
)
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.runtime.wiring import (
    ConsumerDB,
    build_gh_runner_port,
    build_github_port_bundle,
    build_process_runner_port,
    build_session_store,
)
from startupai_controller.consumer_drain_control import (
    drain_requested as _drain_requested,
)
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

CodexResultPayload = CodexSessionResult
SubprocessRunnerFn = Callable[..., subprocess.CompletedProcess[str]]


class LoggerLike(Protocol):
    """Minimal logger surface required by the execution wiring."""

    def error(self, msg: str, *args: object) -> None:
        """Record one error log line."""
        ...

    def warning(self, msg: str, *args: object) -> None:
        """Record one warning log line."""
        ...


def _gh_runner_callable(
    gh_runner: GitHubRunnerFn | GhRunnerPort | None,
) -> GitHubRunnerFn | None:
    if gh_runner is None:
        return None
    if hasattr(gh_runner, "run_gh"):
        return cast(GhRunnerPort, gh_runner).run_gh
    return cast(GitHubRunnerFn, gh_runner)


def _gh_runner_port(
    gh_runner: GitHubRunnerFn | GhRunnerPort | None,
) -> GhRunnerPort | None:
    if gh_runner is None:
        return None
    if hasattr(gh_runner, "run_gh"):
        return cast(GhRunnerPort, gh_runner)
    return build_gh_runner_port(gh_runner=cast(GitHubRunnerFn, gh_runner))


def _process_runner_port(
    process_runner: ProcessRunnerPort | SubprocessRunnerFn | None,
    *,
    gh_runner: GitHubRunnerFn | None,
) -> ProcessRunnerPort | None:
    if process_runner is None:
        return None
    if hasattr(process_runner, "run"):
        return cast(ProcessRunnerPort, process_runner)
    return build_process_runner_port(
        gh_runner=gh_runner,
        subprocess_runner=cast(SubprocessRunnerFn, process_runner),
    )


def block_prelaunch_issue(
    issue_ref: str,
    blocked_reason: str,
    *,
    config: ConsumerConfig,
    cp_config: CriticalPathConfig,
    db: ConsumerRuntimeStatePort,
    gh_query_error_type: type[Exception],
    set_blocked_with_reason: Callable[..., None],
    record_successful_github_mutation: Callable[[ConsumerRuntimeStatePort], None],
    mark_degraded: Callable[[ConsumerRuntimeStatePort, str], None],
    queue_status_transition: Callable[..., None],
    logger: LoggerLike,
    board_info_resolver: BoardInfoResolverFn | None = None,
    board_mutator: BoardStatusMutatorFn | None = None,
    gh_runner: GitHubRunnerFn | None = None,
) -> None:
    """Move a launch-unready issue to Blocked before claim."""
    del board_info_resolver, board_mutator
    try:
        set_blocked_with_reason(
            issue_ref,
            blocked_reason,
            cp_config,
            config.project_owner,
            config.project_number,
            gh_runner=gh_runner,
        )
        record_successful_github_mutation(db)
    except (gh_query_error_type, Exception) as err:
        logger.error("Prelaunch block failed for %s: %s", issue_ref, err)
        mark_degraded(db, f"prelaunch-block:{err}")
        queue_status_transition(
            db,
            issue_ref,
            to_status="Blocked",
            from_statuses={"Ready"},
            blocked_reason=blocked_reason,
        )


def create_pr_for_execution_result(
    *,
    config: ConsumerConfig,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    codex_result: CodexResultPayload | None,
    session_status: str,
    failure_reason: str | None,
    subprocess_runner: SubprocessRunnerFn | None,
    gh_runner: GitHubRunnerFn | None,
    has_commits_on_branch: Callable[..., bool],
    validate_branch_publication: Callable[..., None],
    create_or_update_pr: Callable[..., str],
    pr_creation_outcome_factory: type[PrCreationOutcome],
    logger: LoggerLike,
) -> PrCreationOutcome:
    """Reuse or create a PR from claimed-session output."""
    return _session_completion_helpers.create_pr_for_execution_result(
        config=config,
        launch_context=launch_context,
        claimed_context=claimed_context,
        codex_result=codex_result,
        session_status=session_status,
        failure_reason=failure_reason,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        has_commits_on_branch=has_commits_on_branch,
        validate_branch_publication=validate_branch_publication,
        create_or_update_pr=create_or_update_pr,
        pr_creation_outcome_factory=pr_creation_outcome_factory,
        logger=logger,
    )


def transition_claimed_session_to_review(
    *,
    db: _review_handoff_helpers.ReviewHandoffStatePort,
    issue_ref: str,
    session_id: str,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | None,
    transition_issue_to_review: _review_handoff_helpers.TransitionIssueToReviewFn,
    record_successful_github_mutation: Callable[[ConsumerRuntimeStatePort], None],
    mark_degraded: Callable[[ConsumerRuntimeStatePort, str], None],
    queue_status_transition: _review_handoff_helpers.QueueStatusTransitionFn,
    logger: LoggerLike,
) -> None:
    """Move one claimed issue into Review or queue the transition on failure."""
    _review_handoff_helpers.transition_claimed_session_to_review(
        db=db,
        issue_ref=issue_ref,
        session_id=session_id,
        config=config,
        critical_path_config=critical_path_config,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        transition_issue_to_review=transition_issue_to_review,
        record_successful_github_mutation=record_successful_github_mutation,
        mark_degraded=mark_degraded,
        queue_status_transition=queue_status_transition,
        log_error=lambda err: logger.error("Review transition failed: %s", err),
    )


def post_claimed_session_verdict_marker(
    *,
    db: ConsumerRuntimeStatePort,
    pr_url: str,
    session_id: str,
    gh_runner: GitHubRunnerFn | None,
    post_pr_codex_verdict: _review_handoff_helpers.PostPrCodexVerdictFn,
    record_successful_github_mutation: Callable[[ConsumerRuntimeStatePort], None],
    mark_degraded: Callable[[ConsumerRuntimeStatePort, str], None],
    queue_verdict_marker: _review_handoff_helpers.QueueVerdictMarkerFn,
    logger: LoggerLike,
) -> None:
    """Post the codex verdict marker for a newly handed-off review PR."""
    _review_handoff_helpers.post_claimed_session_verdict_marker(
        db=db,
        pr_url=pr_url,
        session_id=session_id,
        gh_runner=gh_runner,
        post_pr_codex_verdict=post_pr_codex_verdict,
        record_successful_github_mutation=record_successful_github_mutation,
        mark_degraded=mark_degraded,
        queue_verdict_marker=queue_verdict_marker,
        log_error=lambda err: logger.error("PR codex verdict comment failed: %s", err),
    )


def queue_claimed_session_for_review(
    *,
    store: SessionStorePort,
    issue_ref: str,
    pr_url: str,
    session_id: str,
    queue_review_item: _review_handoff_helpers.QueueReviewItemFn,
) -> ReviewQueueEntry | None:
    """Queue one claimed session for immediate review handling."""
    return _review_handoff_helpers.queue_claimed_session_for_review(
        store=store,
        issue_ref=issue_ref,
        pr_url=pr_url,
        session_id=session_id,
        queue_review_item=queue_review_item,
    )


def run_immediate_review_handoff(
    *,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    store: SessionStorePort,
    queue_entry: ReviewQueueEntry,
    gh_runner: GitHubRunnerFn | None,
    db: ConsumerRuntimeStatePort,
    build_github_port_bundle: _review_handoff_helpers.BuildGitHubPortBundleFn,
    github_memo_factory: Callable[[], object],
    build_review_snapshots_for_queue_entries: (
        _review_handoff_helpers.BuildReviewSnapshotsForQueueEntriesFn
    ),
    review_rescue: _review_handoff_helpers.ReviewRescueFn,
    apply_review_queue_result: _review_handoff_helpers.ApplyReviewQueueResultFn,
    mark_degraded: Callable[[ConsumerRuntimeStatePort, str], None],
    gh_reason_code: Callable[[Exception], str],
    summary_factory: type[ReviewQueueDrainSummary],
    logger: LoggerLike,
) -> ReviewQueueDrainSummary:
    """Run immediate rescue for the just-opened review PR."""
    return _review_handoff_helpers.run_immediate_review_handoff(
        config=config,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        store=store,
        queue_entry=queue_entry,
        gh_runner=gh_runner,
        db=db,
        build_github_port_bundle=build_github_port_bundle,
        github_memo_factory=github_memo_factory,
        build_review_snapshots_for_queue_entries=build_review_snapshots_for_queue_entries,
        review_rescue=review_rescue,
        apply_review_queue_result=apply_review_queue_result,
        mark_degraded=mark_degraded,
        gh_reason_code=gh_reason_code,
        summary_factory=summary_factory,
        log_warning=lambda issue_ref, _detail, err: logger.warning(
            "Immediate review clearance failed for %s: %s",
            issue_ref,
            err,
        ),
    )


def handoff_execution_to_review(
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
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    gh_runner: GitHubRunnerFn | GhRunnerPort | None,
    transition_claimed_session_to_review: Callable[..., None],
    post_claimed_session_verdict_marker: Callable[..., None],
    queue_claimed_session_for_review: Callable[..., ReviewQueueEntry | None],
    run_immediate_review_handoff: Callable[..., ReviewQueueDrainSummary],
    record_metric: Callable[..., None],
) -> ReviewQueueDrainSummary:
    """Transition a claimed session into Review and perform immediate rescue."""
    gh_runner_fn = _gh_runner_callable(gh_runner)

    def _transition_claimed_session_to_review(
        *,
        db: ConsumerRuntimeStatePort,
        issue_ref: str,
        session_id: str,
        config: ConsumerConfig,
        critical_path_config: CriticalPathConfig,
        review_state_port: ReviewStatePort | None,
        board_port: BoardMutationPort | None,
    ) -> None:
        return transition_claimed_session_to_review(
            db=db,
            issue_ref=issue_ref,
            session_id=session_id,
            config=config,
            critical_path_config=critical_path_config,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner_fn,
        )

    def _post_claimed_session_verdict_marker(
        *,
        db: ConsumerRuntimeStatePort,
        pr_url: str,
        session_id: str,
    ) -> None:
        return post_claimed_session_verdict_marker(
            db=db,
            pr_url=pr_url,
            session_id=session_id,
            gh_runner=gh_runner_fn,
        )

    def _run_immediate_review_handoff(
        *,
        config: ConsumerConfig,
        critical_path_config: CriticalPathConfig,
        automation_config: BoardAutomationConfig,
        store: SessionStorePort,
        queue_entry: ReviewQueueEntry,
        db: ConsumerRuntimeStatePort,
    ) -> ReviewQueueDrainSummary:
        return run_immediate_review_handoff(
            config=config,
            critical_path_config=critical_path_config,
            automation_config=automation_config,
            store=store,
            queue_entry=queue_entry,
            gh_runner=gh_runner_fn,
            db=db,
        )

    return _handoff_execution_to_review_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=ReviewHandoffDeps(
            transition_claimed_session_to_review=_transition_claimed_session_to_review,
            post_claimed_session_verdict_marker=_post_claimed_session_verdict_marker,
            queue_claimed_session_for_review=queue_claimed_session_for_review,
            run_immediate_review_handoff=_run_immediate_review_handoff,
            record_metric=record_metric,
        ),
        launch_context=launch_context,
        session_id=session_id,
        pr_url=pr_url,
        session_status=session_status,
        session_store=session_store,
        review_state_port=review_state_port,
        board_port=board_port,
    )


def handle_non_review_execution_outcome(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    session_id: str,
    session_status: str,
    codex_result: CodexResultPayload | None,
    has_commits: bool,
    pr_port: PullRequestPort | None,
    board_port: BoardMutationPort | None,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    comment_poster: CommentPosterFn | None,
    subprocess_runner: SubprocessRunnerFn | None,
    gh_runner: GitHubRunnerFn | GhRunnerPort | None,
    verify_resolution_payload: Callable[..., ResolutionEvaluation],
    apply_resolution_action: Callable[..., str | None],
    return_issue_to_ready: Callable[..., None],
    record_successful_github_mutation: Callable[[ConsumerRuntimeStatePort], None],
    mark_degraded: Callable[[ConsumerRuntimeStatePort, str], None],
    queue_status_transition: Callable[..., None],
    record_metric: Callable[..., None],
    logger: LoggerLike,
) -> tuple[str, ResolutionEvaluation | None, str | None]:
    """Handle non-review outcomes for a claimed session."""
    gh_runner_port = _gh_runner_port(gh_runner)
    gh_runner_fn = _gh_runner_callable(gh_runner)
    github_bundle = None
    if pr_port is None or board_port is None:
        github_bundle = build_github_port_bundle(
            config.project_owner,
            config.project_number,
            config=prepared.cp_config,
            github_memo=prepared.github_memo,
            gh_runner=gh_runner_fn,
        )
    effective_pr_port = pr_port or (
        github_bundle.pull_requests if github_bundle is not None else None
    )
    effective_board_port = board_port or (
        github_bundle.board_mutations if github_bundle is not None else None
    )
    if effective_pr_port is None or effective_board_port is None:
        raise ValueError("pr_port and board_port are required for execution outcome")

    def _verify_resolution_payload(
        issue_ref: str,
        resolution: ResolutionPayload | None,
        *,
        config: ConsumerConfig,
        workflows: dict[str, WorkflowDefinition],
        pr_port: PullRequestPort,
    ) -> ResolutionEvaluation:
        return verify_resolution_payload(
            issue_ref,
            resolution,
            config=config,
            workflows=workflows,
            pr_port=pr_port,
            subprocess_runner=subprocess_runner,
            gh_runner=gh_runner_fn,
        )

    def _apply_resolution_action(
        issue_ref: str,
        resolution_evaluation: ResolutionEvaluation,
        *,
        session_id: str,
        db: ConsumerRuntimeStatePort,
        config: ConsumerConfig,
        critical_path_config: CriticalPathConfig,
        board_port: BoardMutationPort,
    ) -> str | None:
        return apply_resolution_action(
            issue_ref,
            resolution_evaluation,
            session_id=session_id,
            db=db,
            config=config,
            critical_path_config=critical_path_config,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_poster=comment_poster,
            gh_runner=gh_runner_fn,
        )

    def _return_issue_to_ready(
        issue_ref: str,
        config: CriticalPathConfig,
        project_owner: str,
        project_number: int,
        *,
        board_port: BoardMutationPort | None = None,
    ) -> None:
        return return_issue_to_ready(
            issue_ref,
            config,
            project_owner,
            project_number,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner_fn,
        )

    return _handle_non_review_execution_outcome_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=NonReviewOutcomeDeps(
            verify_resolution_payload=_verify_resolution_payload,
            apply_resolution_action=_apply_resolution_action,
            return_issue_to_ready=_return_issue_to_ready,
            record_successful_github_mutation=record_successful_github_mutation,
            mark_degraded=mark_degraded,
            queue_status_transition=queue_status_transition,
            record_metric=record_metric,
            log_ready_reset_failure=lambda err: logger.error(
                "Ready reset failed after non-PR session: %s",
                err,
            ),
        ),
        launch_context=launch_context,
        session_id=session_id,
        session_status=session_status,
        codex_result=codex_result,
        has_commits=has_commits,
        gh_runner=gh_runner_port or gh_runner_fn,
        pr_port=effective_pr_port,
        board_port=effective_board_port,
    )


def final_phase_for_claimed_session(
    *,
    launch_context: PreparedLaunchContext,
    execution_outcome: SessionExecutionOutcome,
) -> str:
    """Determine the final persisted phase for a claimed session."""
    return _session_completion_helpers.final_phase_for_claimed_session(
        launch_context=launch_context,
        execution_outcome=execution_outcome,
    )


def persist_claimed_session_completion(
    *,
    db: ConsumerRuntimeStatePort,
    session_id: str,
    issue_ref: str,
    execution_outcome: SessionExecutionOutcome,
    final_phase: str,
    complete_session: Callable[..., None],
) -> None:
    """Persist the final session record for a claimed execution outcome."""
    _session_completion_helpers.persist_claimed_session_completion(
        db=db,
        session_id=session_id,
        issue_ref=issue_ref,
        execution_outcome=execution_outcome,
        final_phase=final_phase,
        complete_session=complete_session,
    )


def post_claimed_session_result_comment(
    *,
    issue_ref: str,
    session_id: str,
    codex_result: CodexResultPayload | None,
    cp_config: CriticalPathConfig,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | None,
    post_result_comment: Callable[..., None],
    logger: LoggerLike,
) -> None:
    """Post the session result comment when Codex produced structured output."""
    _session_completion_helpers.post_claimed_session_result_comment(
        issue_ref=issue_ref,
        session_id=session_id,
        codex_result=codex_result,
        cp_config=cp_config,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        post_result_comment=post_result_comment,
        logger=logger,
    )


def maybe_escalate_claimed_session_failure(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    issue_ref: str,
    effective_max_retries: int,
    session_status: str,
    codex_result: CodexResultPayload | None,
    cp_config: CriticalPathConfig,
    board_info_resolver: BoardInfoResolverFn | None,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | None,
    escalate_to_claude: Callable[..., None],
    logger: LoggerLike,
) -> None:
    """Escalate terminal failed/timeout sessions once retry ceiling is reached."""
    _session_completion_helpers.maybe_escalate_claimed_session_failure(
        config=config,
        db=cast(_session_completion_helpers.RetryCountStatePort, db),
        issue_ref=issue_ref,
        effective_max_retries=effective_max_retries,
        session_status=session_status,
        codex_result=codex_result,
        cp_config=cp_config,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        escalate_to_claude=escalate_to_claude,
        logger=logger,
    )


def execute_claimed_session(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    process_runner: ProcessRunnerPort | SubprocessRunnerFn | None,
    file_reader: Callable[[Path], str] | None,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    pr_port: PullRequestPort | None,
    board_info_resolver: BoardInfoResolverFn | None,
    board_mutator: BoardStatusMutatorFn | None,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | GhRunnerPort | None,
    assemble_codex_prompt: Callable[..., str],
    run_codex_session: Callable[..., int],
    parse_codex_result: Callable[..., CodexResultPayload | None],
    session_status_from_codex_result: Callable[..., tuple[str, str | None]],
    create_pr_for_execution_result: Callable[..., PrCreationOutcome],
    handoff_execution_to_review: Callable[..., ReviewQueueDrainSummary],
    handle_non_review_execution_outcome: Callable[
        ...,
        tuple[str, ResolutionEvaluation | None, str | None],
    ],
) -> SessionExecutionOutcome:
    """Execute Codex for a claimed session and apply immediate board handoff."""
    gh_runner_port = _gh_runner_port(gh_runner)
    gh_runner_fn = _gh_runner_callable(gh_runner)
    process_runner_port = _process_runner_port(
        process_runner,
        gh_runner=gh_runner_fn,
    )
    github_bundle = None
    if pr_port is None or board_port is None:
        github_bundle = build_github_port_bundle(
            config.project_owner,
            config.project_number,
            config=prepared.cp_config,
            github_memo=prepared.github_memo,
            gh_runner=gh_runner_fn,
        )
    effective_pr_port = pr_port or (
        github_bundle.pull_requests if github_bundle is not None else None
    )
    effective_board_port = board_port or (
        github_bundle.board_mutations if github_bundle is not None else None
    )
    if effective_pr_port is None or effective_board_port is None:
        raise ValueError("pr_port and board_port are required for claimed execution")

    def _create_pr_for_execution_result(
        *,
        config: ConsumerConfig,
        launch_context: PreparedLaunchContext,
        claimed_context: ClaimedSessionContext,
        codex_result: CodexResultPayload | None,
        session_status: str,
        failure_reason: str | None,
        subprocess_runner: SubprocessRunnerFn | None,
        gh_runner: GitHubRunnerFn | None,
    ) -> PrCreationOutcome:
        return create_pr_for_execution_result(
            config=config,
            launch_context=launch_context,
            claimed_context=claimed_context,
            codex_result=codex_result,
            session_status=session_status,
            failure_reason=failure_reason,
            subprocess_runner=subprocess_runner,
            gh_runner=gh_runner,
        )

    def _handoff_execution_to_review(
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
    ) -> ReviewQueueDrainSummary:
        return handoff_execution_to_review(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            session_id=session_id,
            pr_url=pr_url,
            session_status=session_status,
            session_store=session_store,
            review_state_port=review_state_port,
            board_port=effective_board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner_fn,
        )

    def _handle_non_review_execution_outcome(
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext,
        session_id: str,
        session_status: str,
        codex_result: CodexResultPayload | None,
        has_commits: bool,
        gh_runner: GitHubRunnerFn | GhRunnerPort | None,
        pr_port: PullRequestPort,
        board_port: BoardMutationPort,
    ) -> tuple[str, ResolutionEvaluation | None, str | None]:
        return handle_non_review_execution_outcome(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            session_id=session_id,
            session_status=session_status,
            codex_result=codex_result,
            has_commits=has_commits,
            pr_port=effective_pr_port,
            board_port=effective_board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_poster=comment_poster,
            subprocess_runner=(
                process_runner_port.run if process_runner_port is not None else None
            ),
            gh_runner=gh_runner,
        )

    return _execute_claimed_session_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=ExecutionDeps(
            assemble_codex_prompt=assemble_codex_prompt,
            drain_requested=_drain_requested,
            run_codex_session=run_codex_session,
            parse_codex_result=parse_codex_result,
            session_status_from_codex_result=session_status_from_codex_result,
            create_pr_for_execution_result=_create_pr_for_execution_result,
            handoff_execution_to_review=_handoff_execution_to_review,
            handle_non_review_execution_outcome=_handle_non_review_execution_outcome,
            build_session_execution_outcome=SessionExecutionOutcome,
        ),
        launch_context=launch_context,
        claimed_context=claimed_context,
        session_store=build_session_store(cast(ConsumerDB, db)),
        gh_runner=gh_runner_port,
        process_runner=process_runner_port,
        file_reader=file_reader,
        review_state_port=review_state_port,
        board_port=effective_board_port,
        pr_port=effective_pr_port,
    )


def finalize_claimed_session(
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    execution_outcome: SessionExecutionOutcome,
    review_state_port: ReviewStatePort | None,
    board_info_resolver: BoardInfoResolverFn | None,
    comment_checker: CommentCheckerFn | None,
    comment_poster: CommentPosterFn | None,
    gh_runner: GitHubRunnerFn | GhRunnerPort | None,
    final_phase_for_claimed_session: Callable[..., str],
    persist_claimed_session_completion: Callable[..., None],
    post_claimed_session_result_comment: Callable[..., None],
    maybe_escalate_claimed_session_failure: Callable[..., None],
) -> CycleResult:
    """Persist final session state and return the cycle result."""
    gh_runner_fn = _gh_runner_callable(gh_runner)
    effective_comment_checker = comment_checker or (
        _comment_pr_helpers.comment_checker_from_review_state_port(review_state_port)
        if review_state_port is not None
        else None
    )

    def _post_claimed_session_result_comment(
        *,
        issue_ref: str,
        session_id: str,
        codex_result: CodexResultPayload | None,
        cp_config: CriticalPathConfig,
    ) -> None:
        return post_claimed_session_result_comment(
            issue_ref=issue_ref,
            session_id=session_id,
            codex_result=codex_result,
            cp_config=cp_config,
            comment_checker=effective_comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner_fn,
        )

    def _maybe_escalate_claimed_session_failure(
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        issue_ref: str,
        effective_max_retries: int,
        session_status: str,
        codex_result: CodexResultPayload | None,
        cp_config: CriticalPathConfig,
    ) -> None:
        return maybe_escalate_claimed_session_failure(
            config=config,
            db=db,
            issue_ref=issue_ref,
            effective_max_retries=effective_max_retries,
            session_status=session_status,
            codex_result=codex_result,
            cp_config=cp_config,
            board_info_resolver=board_info_resolver,
            comment_checker=effective_comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner_fn,
        )

    return _finalize_claimed_session_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=FinalizeClaimedSessionDeps(
            final_phase_for_claimed_session=final_phase_for_claimed_session,
            persist_claimed_session_completion=persist_claimed_session_completion,
            post_claimed_session_result_comment=_post_claimed_session_result_comment,
            maybe_escalate_claimed_session_failure=(
                _maybe_escalate_claimed_session_failure
            ),
        ),
        launch_context=launch_context,
        claimed_context=claimed_context,
        execution_outcome=execution_outcome,
        review_state_port=review_state_port,
    )
