"""Launch and claim orchestration for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from startupai_controller.domain.models import CycleResult


@dataclass(frozen=True)
class ResolveLaunchDeps:
    """Injected seams for launch-context resolution."""

    select_launch_candidate_for_cycle: Callable[..., tuple[Any | None, CycleResult | None]]
    prepare_selected_launch_candidate: Callable[..., tuple[Any | None, CycleResult | None]]


def resolve_launch_context_for_cycle(
    *,
    config: Any,
    db: Any,
    prepared: Any,
    deps: ResolveLaunchDeps,
    launch_context: Any | None,
    target_issue: str | None,
    dry_run: bool,
    status_resolver: Callable[..., str] | None,
    subprocess_runner: Callable[..., Any] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
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
        status_resolver=status_resolver,
        gh_runner=gh_runner,
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
        subprocess_runner=subprocess_runner,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
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
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
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
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
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
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
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
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            cp_config=prepared.cp_config,
            gh_runner=gh_runner,
        ),
        None,
    )
