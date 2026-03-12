"""Launch and execution wiring extracted from board_consumer."""

from __future__ import annotations

import logging
from typing import Any, Callable, cast

import startupai_controller.consumer_board_state_helpers as _board_state_helpers
import startupai_controller.consumer_execution_support_helpers as _execution_support_helpers
import startupai_controller.consumer_launch_support_wiring as _launch_support_wiring
import startupai_controller.consumer_resolution_helpers as _resolution_helpers
import startupai_controller.consumer_support_wiring as _support_wiring
from startupai_controller.control_plane_runtime import (
    _mark_degraded,
    _record_successful_github_mutation,
)
from startupai_controller.application.consumer.cycle import PreparedCycleDeps
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
from startupai_controller.application.consumer.launch import (
    ClaimLaunchDeps,
    PrepareLaunchDeps,
    ResolveLaunchDeps,
    claim_launch_context as _claim_launch_context_use_case,
    prepare_launch_candidate as _prepare_launch_candidate_use_case,
    resolve_launch_context_for_cycle as _resolve_launch_context_for_cycle_use_case,
)
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    PendingClaimContext,
    PrCreationOutcome,
    PreparedLaunchContext,
    SelectedLaunchCandidate,
    SessionExecutionOutcome,
)
from startupai_controller.domain.models import (
    CycleResult,
    ResolutionEvaluation,
    ReviewQueueDrainSummary,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.process_runner import GhRunnerPort
from startupai_controller.runtime.wiring import (
    build_gh_runner_port,
    build_process_runner_port,
    build_github_port_bundle,
    build_session_store,
    build_worktree_port,
)

logger = logging.getLogger("board-consumer")


def assemble_prepared_launch_context(
    issue_ref: str,
    *,
    candidate_prefix: str,
    owner: str,
    repo: str,
    number: int,
    title: str,
    context: dict[str, Any],
    session_kind: str,
    repair_pr_url: str | None,
    repair_branch_name: str | None,
    worktree_path: str,
    branch_name: str,
    workflow_definition: Any,
    effective_consumer_config: Any,
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
    config: Any,
    prepared: Any,
    db: Any,
    subprocess_runner: Any | None = None,
    review_state_port: Any | None = None,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Any | None = None,
    pr_port: Any | None = None,
    issue_context_port: Any | None = None,
    session_store: Any | None = None,
    worktree_port: Any | None = None,
) -> PreparedLaunchContext:
    """Prepare local launch state for an issue before board claim."""
    process_runner_port = (
        subprocess_runner
        if hasattr(subprocess_runner, "run")
        else build_process_runner_port(
            gh_runner=gh_runner,
            subprocess_runner=subprocess_runner,
        )
    )
    gh_port = (
        gh_runner
        if hasattr(gh_runner, "run_gh")
        else build_gh_runner_port(gh_runner=gh_runner)
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
        cp_config: Any,
        auto_config: Any,
        board_snapshot: Any,
        pr_port: Any,
    ) -> tuple[Any, ...]:
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
        snapshot: Any | None,
        config: Any,
        db: Any,
        issue_context_port: Any,
    ) -> tuple[Any, str]:
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
        config: Any,
        cp_config: Any,
        db: Any,
        session_store: Any | None = None,
        worktree_port: Any | None = None,
        subprocess_runner: Any | None = None,
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
        session_store=session_store or build_session_store(db),
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
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: PreparedLaunchContext | None,
    target_issue: str | None,
    dry_run: bool,
    review_state_port: Any | None,
    pr_port: Any | None,
    status_resolver: Callable[..., str] | None,
    subprocess_runner: Any | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Any | None,
    select_launch_candidate_for_cycle: Callable[
        ...,
        tuple[SelectedLaunchCandidate | None, CycleResult | None],
    ],
    prepare_selected_launch_candidate: Callable[
        ...,
        tuple[PreparedLaunchContext | None, CycleResult | None],
    ],
    logger: Any,
) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
    """Resolve or prepare launch-ready work for this cycle."""

    def _select_launch_candidate(
        *, config: Any, db: Any, prepared: Any, target_issue: str | None
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
        config: Any,
        db: Any,
        prepared: Any,
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
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: PreparedLaunchContext,
    slot_id: int,
    review_state_port: Any | None,
    board_port: Any | None,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Any | None,
    open_pending_claim_session: Callable[
        ...,
        tuple[PendingClaimContext | None, CycleResult | None],
    ],
    enforce_claim_retry_ceiling: Callable[..., CycleResult | None],
    attempt_launch_context_claim: Callable[
        ...,
        tuple[Any | None, CycleResult | None],
    ],
    mark_claimed_session_running: Callable[..., ClaimedSessionContext],
) -> tuple[ClaimedSessionContext | None, CycleResult | None]:
    """Claim board ownership and start a durable running session."""
    effective_comment_checker = comment_checker or (
        review_state_port.comment_exists if review_state_port is not None else None
    )
    effective_status_resolver = status_resolver or (
        review_state_port.get_issue_status if review_state_port is not None else None
    )

    def _enforce_claim_retry_ceiling(
        *,
        config: Any,
        db: Any,
        launch_context: PreparedLaunchContext,
        pending_claim: PendingClaimContext,
        cp_config: Any,
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
        config: Any,
        db: Any,
        prepared: Any,
        launch_context: PreparedLaunchContext,
        pending_claim: PendingClaimContext,
        slot_id: int,
    ) -> tuple[Any | None, CycleResult | None]:
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
        config: Any,
        db: Any,
        launch_context: PreparedLaunchContext,
        pending_claim: PendingClaimContext,
        slot_id: int,
        cp_config: Any,
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


def handoff_execution_to_review(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    session_id: str,
    pr_url: str,
    session_status: str,
    session_store: Any,
    review_state_port: Any | None,
    board_port: Any | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Any | None,
    transition_claimed_session_to_review: Callable[..., None],
    post_claimed_session_verdict_marker: Callable[..., None],
    queue_claimed_session_for_review: Callable[..., Any | None],
    run_immediate_review_handoff: Callable[..., Any],
    record_metric: Callable[..., None],
) -> Any:
    """Transition a claimed session into Review and perform immediate rescue."""

    def _transition_claimed_session_to_review(
        *,
        db: Any,
        issue_ref: str,
        session_id: str,
        config: Any,
        critical_path_config: Any,
        review_state_port: Any | None,
        board_port: Any | None,
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
            gh_runner=gh_runner,
        )

    def _post_claimed_session_verdict_marker(
        *,
        db: Any,
        pr_url: str,
        session_id: str,
    ) -> None:
        return post_claimed_session_verdict_marker(
            db=db,
            pr_url=pr_url,
            session_id=session_id,
            gh_runner=gh_runner,
        )

    def _run_immediate_review_handoff(
        *,
        config: Any,
        critical_path_config: Any,
        automation_config: Any,
        store: Any,
        queue_entry: Any,
        db: Any,
    ) -> Any:
        return run_immediate_review_handoff(
            config=config,
            critical_path_config=critical_path_config,
            automation_config=automation_config,
            store=store,
            queue_entry=queue_entry,
            gh_runner=gh_runner,
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
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: Any,
    session_id: str,
    session_status: str,
    codex_result: dict[str, Any] | None,
    has_commits: bool,
    pr_port: Any | None,
    board_port: Any | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_poster: Callable[..., None] | None,
    subprocess_runner: Callable[..., Any] | None,
    gh_runner: Any | None,
    verify_resolution_payload: Callable[..., Any],
    apply_resolution_action: Callable[..., str | None],
    return_issue_to_ready: Callable[..., None],
    record_successful_github_mutation: Callable[..., None],
    mark_degraded: Callable[..., None],
    queue_status_transition: Callable[..., None],
    record_metric: Callable[..., None],
    logger: Any,
) -> tuple[str, Any | None, str | None]:
    """Handle non-review outcomes for a claimed session."""
    if pr_port is None or board_port is None:
        raise ValueError("pr_port and board_port are required for execution outcome")

    def _verify_resolution_payload(
        issue_ref: str,
        resolution: dict[str, Any] | None,
        *,
        config: Any,
        workflows: dict[str, Any],
        pr_port: Any | None = None,
    ) -> Any:
        return verify_resolution_payload(
            issue_ref,
            resolution,
            config=config,
            workflows=workflows,
            pr_port=pr_port,
            subprocess_runner=subprocess_runner,
            gh_runner=gh_runner,
        )

    def _apply_resolution_action(
        issue_ref: str,
        resolution_evaluation: Any,
        *,
        session_id: str,
        db: Any,
        config: Any,
        critical_path_config: Any,
        board_port: Any | None = None,
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
            gh_runner=gh_runner,
        )

    def _return_issue_to_ready(
        issue_ref: str,
        config: Any,
        project_owner: str,
        project_number: int,
        *,
        board_port: Any | None = None,
        from_statuses: set[str] | None = None,
    ) -> None:
        return return_issue_to_ready(
            issue_ref,
            config,
            project_owner,
            project_number,
            board_port=board_port,
            from_statuses=from_statuses,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
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
        gh_runner=gh_runner,
        pr_port=pr_port,
        board_port=board_port,
    )


def execute_claimed_session(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    process_runner: Any | None,
    file_reader: Callable[..., Any] | None,
    review_state_port: Any | None,
    board_port: Any | None,
    pr_port: Any | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Any | None,
    assemble_codex_prompt: Callable[..., str],
    run_codex_session: Callable[..., int],
    parse_codex_result: Callable[..., dict[str, Any] | None],
    session_status_from_codex_result: Callable[..., tuple[str, str | None]],
    create_pr_for_execution_result: Callable[..., PrCreationOutcome],
    handoff_execution_to_review: Callable[..., ReviewQueueDrainSummary],
    handle_non_review_execution_outcome: Callable[
        ..., tuple[str, ResolutionEvaluation | None, str | None]
    ],
) -> SessionExecutionOutcome:
    """Execute Codex for a claimed session and apply immediate board handoff."""

    def _gh_runner_callable(
        gh_runner: Any | None,
    ) -> Callable[..., str] | None:
        if gh_runner is None:
            return None
        if hasattr(gh_runner, "run_gh"):
            return cast(GhRunnerPort, gh_runner).run_gh
        return cast(Callable[..., str], gh_runner)

    gh_runner_fn = _gh_runner_callable(gh_runner)
    github_bundle = (
        None
        if pr_port is not None and board_port is not None
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
    effective_board_port = board_port or (
        github_bundle.board_mutations if github_bundle is not None else None
    )

    def _create_pr_for_execution_result(
        *,
        config: Any,
        launch_context: PreparedLaunchContext,
        claimed_context: ClaimedSessionContext,
        codex_result: dict[str, Any] | None,
        session_status: str,
        failure_reason: str | None,
        subprocess_runner: Callable[..., Any] | None,
        gh_runner: Callable[..., str] | None,
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
        config: Any,
        db: Any,
        prepared: Any,
        launch_context: PreparedLaunchContext,
        session_id: str,
        pr_url: str,
        session_status: str,
        session_store: Any,
        review_state_port: Any | None,
        board_port: Any | None,
    ) -> ReviewQueueDrainSummary:
        del session_store
        return handoff_execution_to_review(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            session_id=session_id,
            pr_url=pr_url,
            session_status=session_status,
            review_state_port=review_state_port,
            board_port=effective_board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner_fn,
        )

    def _handle_non_review_execution_outcome(
        *,
        config: Any,
        db: Any,
        prepared: Any,
        launch_context: PreparedLaunchContext,
        session_id: str,
        session_status: str,
        codex_result: dict[str, Any] | None,
        has_commits: bool,
        gh_runner: Any | None,
        pr_port: Any | None,
        board_port: Any | None,
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
            pr_port=cast(PullRequestPort | None, effective_pr_port),
            board_port=cast(BoardMutationPort | None, effective_board_port),
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_poster=comment_poster,
            subprocess_runner=(
                process_runner.run if process_runner is not None else None
            ),
            gh_runner=gh_runner_fn,
        )

    return _execute_claimed_session_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=ExecutionDeps(
            assemble_codex_prompt=assemble_codex_prompt,
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
        session_store=build_session_store(db),
        gh_runner=gh_runner,
        process_runner=process_runner,
        file_reader=file_reader,
        review_state_port=review_state_port,
        board_port=effective_board_port,
        pr_port=effective_pr_port,
    )


def finalize_claimed_session(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    execution_outcome: SessionExecutionOutcome,
    review_state_port: Any | None,
    board_info_resolver: Callable[..., Any] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Any | None,
    final_phase_for_claimed_session: Callable[..., str],
    persist_claimed_session_completion: Callable[..., None],
    post_claimed_session_result_comment: Callable[..., None],
    maybe_escalate_claimed_session_failure: Callable[..., None],
) -> CycleResult:
    """Persist final session state and return the cycle result."""
    effective_comment_checker = comment_checker or (
        review_state_port.comment_exists if review_state_port is not None else None
    )

    def _post_claimed_session_result_comment(
        *,
        issue_ref: str,
        session_id: str,
        codex_result: dict[str, Any] | None,
        cp_config: Any,
    ) -> None:
        return post_claimed_session_result_comment(
            issue_ref=issue_ref,
            session_id=session_id,
            codex_result=codex_result,
            cp_config=cp_config,
            comment_checker=effective_comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )

    def _maybe_escalate_claimed_session_failure(
        *,
        config: Any,
        db: Any,
        issue_ref: str,
        effective_max_retries: int,
        session_status: str,
        codex_result: dict[str, Any] | None,
        cp_config: Any,
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
            gh_runner=gh_runner,
        )

    return _finalize_claimed_session_use_case(
        config=config,
        db=db,
        prepared=prepared,
        deps=FinalizeClaimedSessionDeps(
            final_phase_for_claimed_session=final_phase_for_claimed_session,
            persist_claimed_session_completion=persist_claimed_session_completion,
            post_claimed_session_result_comment=_post_claimed_session_result_comment,
            maybe_escalate_claimed_session_failure=_maybe_escalate_claimed_session_failure,
        ),
        launch_context=launch_context,
        claimed_context=claimed_context,
        execution_outcome=execution_outcome,
        review_state_port=review_state_port,
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
