"""Prepared-cycle orchestration for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    PreparedCycleContext,
    PreparedLaunchContext,
    SessionExecutionOutcome,
)
from startupai_controller.domain.models import CycleResult
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.process_runner import GhRunnerPort, ProcessRunnerPort
from startupai_controller.ports.review_state import ReviewStatePort

SubprocessRunnerFn = Callable[..., object]


class ResolveLaunchContextForCycleFn(Protocol):
    """Resolve or prepare launch-ready state for one cycle."""

    def __call__(
        self,
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext | None,
        target_issue: str | None,
        dry_run: bool,
        review_state_port: ReviewStatePort | None,
        pr_port: PullRequestPort | None,
        subprocess_runner: SubprocessRunnerFn | None,
        gh_runner: Callable[..., str] | None,
    ) -> tuple[PreparedLaunchContext | None, CycleResult | None]: ...


class ClaimLaunchContextFn(Protocol):
    """Claim board ownership for a prepared launch context."""

    def __call__(
        self,
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext,
        slot_id: int,
        review_state_port: ReviewStatePort | None,
        board_port: BoardMutationPort | None,
        gh_runner: Callable[..., str] | None,
    ) -> tuple[ClaimedSessionContext | None, CycleResult | None]: ...


class ExecuteClaimedSessionFn(Protocol):
    """Run Codex for a claimed launch session."""

    def __call__(
        self,
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext,
        claimed_context: ClaimedSessionContext,
        gh_runner: GhRunnerPort | None,
        process_runner: ProcessRunnerPort | None,
        file_reader: Callable[[Path], str] | None,
        review_state_port: ReviewStatePort | None,
        board_port: BoardMutationPort | None,
        pr_port: PullRequestPort | None,
    ) -> SessionExecutionOutcome: ...


class FinalizeClaimedSessionFn(Protocol):
    """Persist the final session state after execution."""

    def __call__(
        self,
        *,
        config: ConsumerConfig,
        db: ConsumerRuntimeStatePort,
        prepared: PreparedCycleContext,
        launch_context: PreparedLaunchContext,
        claimed_context: ClaimedSessionContext,
        execution_outcome: SessionExecutionOutcome,
        review_state_port: ReviewStatePort | None,
        gh_runner: Callable[..., str] | None,
    ) -> CycleResult: ...


@dataclass(frozen=True)
class PreparedCycleDeps:
    """Injected seams for the prepared-cycle orchestration."""

    claim_suppression_state: Callable[
        [ConsumerRuntimeStatePort],
        dict[str, str] | None,
    ]
    next_available_slot: Callable[[ConsumerRuntimeStatePort, int], int | None]
    resolve_launch_context_for_cycle: ResolveLaunchContextForCycleFn
    claim_launch_context: ClaimLaunchContextFn
    execute_claimed_session: ExecuteClaimedSessionFn
    finalize_claimed_session: FinalizeClaimedSessionFn


def run_prepared_cycle(
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    *,
    prepared: PreparedCycleContext,
    deps: PreparedCycleDeps,
    dry_run: bool = False,
    target_issue: str | None = None,
    launch_context: PreparedLaunchContext | None = None,
    slot_id_override: int | None = None,
    gh_runner: GhRunnerPort | None = None,
    process_runner: ProcessRunnerPort | None = None,
    file_reader: Callable[[Path], str] | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    pr_port: PullRequestPort | None = None,
) -> CycleResult:
    """Run the prepared claim/execute/finalize cycle."""
    config.poll_interval_seconds = prepared.effective_interval

    suppression_state = deps.claim_suppression_state(db)
    if suppression_state is not None and launch_context is None:
        return CycleResult(
            action="idle",
            reason=f"claim-suppressed:{suppression_state['scope']}",
        )

    if slot_id_override is None:
        if db.active_lease_count() >= prepared.global_limit:
            return CycleResult(action="idle", reason="lease-cap")
        slot_id = deps.next_available_slot(db, prepared.global_limit)
        if slot_id is None:
            return CycleResult(action="idle", reason="lease-cap")
    else:
        slot_id = slot_id_override

    launch_context, cycle_result = deps.resolve_launch_context_for_cycle(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        target_issue=target_issue,
        dry_run=dry_run,
        review_state_port=review_state_port,
        pr_port=pr_port,
        subprocess_runner=process_runner.run if process_runner is not None else None,
        gh_runner=gh_runner.run_gh if gh_runner is not None else None,
    )
    if cycle_result is not None:
        return cycle_result

    assert launch_context is not None
    claimed_context, cycle_result = deps.claim_launch_context(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        slot_id=slot_id,
        review_state_port=review_state_port,
        board_port=board_port,
        gh_runner=gh_runner.run_gh if gh_runner is not None else None,
    )
    if cycle_result is not None:
        return cycle_result

    assert claimed_context is not None
    execution_outcome = deps.execute_claimed_session(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        claimed_context=claimed_context,
        gh_runner=gh_runner,
        process_runner=process_runner,
        file_reader=file_reader,
        review_state_port=review_state_port,
        board_port=board_port,
        pr_port=pr_port,
    )

    return deps.finalize_claimed_session(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        claimed_context=claimed_context,
        execution_outcome=execution_outcome,
        review_state_port=review_state_port,
        gh_runner=gh_runner.run_gh if gh_runner is not None else None,
    )
