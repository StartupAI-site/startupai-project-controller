"""Prepared-cycle orchestration for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from startupai_controller.domain.models import CycleResult
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.process_runner import GhRunnerPort, ProcessRunnerPort
from startupai_controller.ports.review_state import ReviewStatePort


@dataclass(frozen=True)
class PreparedCycleDeps:
    """Injected seams for the prepared-cycle orchestration."""

    claim_suppression_state: Callable[[Any], dict[str, Any] | None]
    next_available_slot: Callable[[Any, int], int | None]
    resolve_launch_context_for_cycle: Callable[..., tuple[Any | None, CycleResult | None]]
    claim_launch_context: Callable[..., tuple[Any | None, CycleResult | None]]
    execute_claimed_session: Callable[..., Any]
    finalize_claimed_session: Callable[..., CycleResult]


def run_prepared_cycle(
    config: Any,
    db: Any,
    *,
    prepared: Any,
    deps: PreparedCycleDeps,
    dry_run: bool = False,
    target_issue: str | None = None,
    launch_context: Any | None = None,
    slot_id_override: int | None = None,
    gh_runner: GhRunnerPort | None = None,
    process_runner: ProcessRunnerPort | None = None,
    file_reader: Callable[[Path], str] | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    pr_port: PullRequestPort | None = None,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
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
        status_resolver=status_resolver,
        subprocess_runner=process_runner,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
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
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
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
        process_runner=process_runner,
        file_reader=file_reader,
        review_state_port=review_state_port,
        board_port=board_port,
        pr_port=pr_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )

    return deps.finalize_claimed_session(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        claimed_context=claimed_context,
        execution_outcome=execution_outcome,
        review_state_port=review_state_port,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )
