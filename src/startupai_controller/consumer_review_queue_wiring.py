"""Review-queue wiring helpers extracted from board_consumer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

import startupai_controller.consumer_review_queue_helpers as _review_queue_helpers


@dataclass(frozen=True)
class ReviewQueueWiringDeps:
    """Injected shell-facing wiring seams for review-queue processing."""

    prepared_batch_factory: Callable[..., Any]
    summary_factory: Callable[..., Any]
    prepared_due_processing_factory: Callable[..., Any]
    review_group_outcome_factory: Callable[..., Any]
    review_queue_processing_outcome_factory: Callable[..., Any]
    post_pr_codex_verdict: Callable[..., bool]
    review_rescue_fn: Callable[..., Any]
    escalate_to_claude: Callable[..., None]
    gh_reason_code: Callable[..., str]
    log_probe_warning: Callable[[Exception], None]
    log_pre_backfill_warning: Callable[[str, Exception], None]
    log_backfill_warning: Callable[[str, str, Exception], None]


def _shell_module():
    """Import the consumer shell lazily to avoid import cycles."""
    from startupai_controller import board_consumer_compat

    return board_consumer_compat


def build_review_queue_wiring_deps() -> ReviewQueueWiringDeps:
    """Build shell-facing review-queue wiring dependencies."""
    shell = _shell_module()
    return ReviewQueueWiringDeps(
        prepared_batch_factory=shell.PreparedReviewQueueBatch,
        summary_factory=shell.ReviewQueueDrainSummary,
        prepared_due_processing_factory=shell.PreparedDueReviewProcessing,
        review_group_outcome_factory=shell.ReviewGroupProcessingOutcome,
        review_queue_processing_outcome_factory=shell.ReviewQueueProcessingOutcome,
        post_pr_codex_verdict=shell._post_pr_codex_verdict,
        review_rescue_fn=shell.review_rescue,
        escalate_to_claude=shell._escalate_to_claude,
        gh_reason_code=shell.gh_reason_code,
        log_probe_warning=lambda err: shell.logger.warning(
            "Review queue wakeup probe failed: %s",
            err,
        ),
        log_pre_backfill_warning=lambda issue_ref, err: shell.logger.warning(
            "Pre-backfill verdict failed for %s: %s",
            issue_ref,
            err,
        ),
        log_backfill_warning=lambda issue_ref, session_id, err: shell.logger.warning(
            "Review verdict backfill failed for %s (%s): %s",
            issue_ref,
            session_id,
            err,
        ),
    )


def prepare_review_queue_batch(
    *,
    config: Any,
    store: Any,
    critical_path_config: Any,
    board_snapshot: Any,
    pr_port: Any,
    now: datetime,
    dry_run: bool,
    deps: ReviewQueueWiringDeps,
    review_queue_helpers: Any,
) -> tuple[Any | None, Any | None]:
    """Prepare the bounded review-queue workset for one drain cycle."""
    return review_queue_helpers.prepare_review_queue_batch(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        board_snapshot=board_snapshot,
        pr_port=pr_port,
        now=now,
        dry_run=dry_run,
        prepared_batch_factory=deps.prepared_batch_factory,
        summary_factory=deps.summary_factory,
        log_warning=deps.log_probe_warning,
    )


def prepare_review_queue_batch_from_shell(
    *,
    config: Any,
    store: Any,
    critical_path_config: Any,
    board_snapshot: Any,
    pr_port: Any,
    now: datetime,
    dry_run: bool,
) -> tuple[Any | None, Any | None]:
    """Prepare the bounded review-queue workset using live shell deps."""
    return prepare_review_queue_batch(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        board_snapshot=board_snapshot,
        pr_port=pr_port,
        now=now,
        dry_run=dry_run,
        deps=build_review_queue_wiring_deps(),
        review_queue_helpers=_review_queue_helpers,
    )


def prepare_due_review_processing(
    *,
    store: Any,
    automation_config: Any,
    pr_port: Any,
    prepared_batch: Any,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    deps: ReviewQueueWiringDeps,
    review_queue_helpers: Any,
) -> Any:
    """Prepare the changed due-review groups and snapshots for rescue processing."""
    return review_queue_helpers.prepare_due_review_processing(
        store=store,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        post_pr_codex_verdict=deps.post_pr_codex_verdict,
        prepared_due_processing_factory=deps.prepared_due_processing_factory,
        log_pre_backfill_warning=deps.log_pre_backfill_warning,
        log_backfill_warning=deps.log_backfill_warning,
    )


def prepare_due_review_processing_from_shell(
    *,
    store: Any,
    automation_config: Any,
    pr_port: Any,
    prepared_batch: Any,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> Any:
    """Prepare changed due-review groups and snapshots using live shell deps."""
    return prepare_due_review_processing(
        store=store,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        deps=build_review_queue_wiring_deps(),
        review_queue_helpers=_review_queue_helpers,
    )


def process_due_review_group(
    *,
    config: Any,
    store: Any,
    critical_path_config: Any,
    automation_config: Any,
    pr_port: Any,
    pr_repo: str,
    pr_number: int,
    entries: tuple[Any, ...],
    snapshot: Any | None,
    updated_snapshot: Any,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    deps: ReviewQueueWiringDeps,
    review_queue_helpers: Any,
) -> Any:
    """Process one due PR group from the review queue."""
    return review_queue_helpers.process_due_review_group(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=pr_port,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        snapshot=snapshot,
        updated_snapshot=updated_snapshot,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        review_group_outcome_factory=deps.review_group_outcome_factory,
        review_rescue_fn=deps.review_rescue_fn,
        escalate_to_claude=deps.escalate_to_claude,
    )


def process_due_review_group_from_shell(
    *,
    config: Any,
    store: Any,
    critical_path_config: Any,
    automation_config: Any,
    pr_port: Any,
    pr_repo: str,
    pr_number: int,
    entries: tuple[Any, ...],
    snapshot: Any | None,
    updated_snapshot: Any,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> Any:
    """Process one due PR group using live shell deps."""
    return process_due_review_group(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=pr_port,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        snapshot=snapshot,
        updated_snapshot=updated_snapshot,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        deps=build_review_queue_wiring_deps(),
        review_queue_helpers=_review_queue_helpers,
    )


def apply_review_queue_group_result(
    *,
    store: Any,
    critical_path_config: Any,
    project_owner: str,
    project_number: int,
    pr_port: Any,
    pr_repo: str,
    pr_number: int,
    entries: tuple[Any, ...],
    result: Any,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    deps: ReviewQueueWiringDeps,
    review_queue_helpers: Any,
) -> tuple[str, ...]:
    """Persist one review-group result and return escalated issue refs."""
    return review_queue_helpers.apply_review_queue_group_result(
        store=store,
        critical_path_config=critical_path_config,
        project_owner=project_owner,
        project_number=project_number,
        pr_port=pr_port,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        result=result,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        escalate_to_claude=deps.escalate_to_claude,
    )


def apply_review_queue_group_result_from_shell(
    *,
    store: Any,
    critical_path_config: Any,
    project_owner: str,
    project_number: int,
    pr_port: Any,
    pr_repo: str,
    pr_number: int,
    entries: tuple[Any, ...],
    result: Any,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> tuple[str, ...]:
    """Persist one review-group result using live shell deps."""
    return apply_review_queue_group_result(
        store=store,
        critical_path_config=critical_path_config,
        project_owner=project_owner,
        project_number=project_number,
        pr_port=pr_port,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        result=result,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        deps=build_review_queue_wiring_deps(),
        review_queue_helpers=_review_queue_helpers,
    )


def summarize_review_group_outcome(
    *,
    critical_path_config: Any,
    store: Any,
    project_owner: str,
    project_number: int,
    pr_repo: str,
    pr_number: int,
    entries: tuple[Any, ...],
    result: Any,
    updated_snapshot: Any,
    escalated: tuple[str, ...],
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    deps: ReviewQueueWiringDeps,
    review_queue_helpers: Any,
) -> Any:
    """Build the public outcome for one processed review group."""
    return review_queue_helpers.summarize_review_group_outcome(
        critical_path_config=critical_path_config,
        store=store,
        project_owner=project_owner,
        project_number=project_number,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        result=result,
        updated_snapshot=updated_snapshot,
        escalated=escalated,
        dry_run=dry_run,
        gh_runner=gh_runner,
        escalate_to_claude=deps.escalate_to_claude,
        review_group_outcome_factory=deps.review_group_outcome_factory,
    )


def summarize_review_group_outcome_from_shell(
    *,
    critical_path_config: Any,
    store: Any,
    project_owner: str,
    project_number: int,
    pr_repo: str,
    pr_number: int,
    entries: tuple[Any, ...],
    result: Any,
    updated_snapshot: Any,
    escalated: tuple[str, ...],
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> Any:
    """Build one review-group outcome using live shell deps."""
    return summarize_review_group_outcome(
        critical_path_config=critical_path_config,
        store=store,
        project_owner=project_owner,
        project_number=project_number,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        result=result,
        updated_snapshot=updated_snapshot,
        escalated=escalated,
        dry_run=dry_run,
        gh_runner=gh_runner,
        deps=build_review_queue_wiring_deps(),
        review_queue_helpers=_review_queue_helpers,
    )


def process_review_queue_due_groups(
    *,
    config: Any,
    store: Any,
    critical_path_config: Any,
    automation_config: Any,
    pr_port: Any,
    prepared_batch: Any,
    board_snapshot: Any,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    deps: ReviewQueueWiringDeps,
    review_queue_helpers: Any,
) -> Any:
    """Process the due PR groups for a prepared review-queue batch."""
    return review_queue_helpers.process_review_queue_due_groups(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        board_snapshot=board_snapshot,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        post_pr_codex_verdict=deps.post_pr_codex_verdict,
        review_rescue_fn=deps.review_rescue_fn,
        escalate_to_claude=deps.escalate_to_claude,
        prepared_due_processing_factory=deps.prepared_due_processing_factory,
        review_group_outcome_factory=deps.review_group_outcome_factory,
        review_queue_processing_outcome_factory=deps.review_queue_processing_outcome_factory,
        gh_reason_code=deps.gh_reason_code,
        log_pre_backfill_warning=deps.log_pre_backfill_warning,
        log_backfill_warning=deps.log_backfill_warning,
    )


def process_review_queue_due_groups_from_shell(
    *,
    config: Any,
    store: Any,
    critical_path_config: Any,
    automation_config: Any,
    pr_port: Any,
    prepared_batch: Any,
    board_snapshot: Any,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None = None,
) -> Any:
    """Process due review groups using live shell deps."""
    return process_review_queue_due_groups(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        board_snapshot=board_snapshot,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        deps=build_review_queue_wiring_deps(),
        review_queue_helpers=_review_queue_helpers,
    )


def run_review_rescue_for_group_from_shell(
    *,
    config: Any,
    critical_path_config: Any,
    automation_config: Any,
    pr_port: Any,
    pr_repo: str,
    pr_number: int,
    snapshot: Any,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> Any:
    """Run rescue logic for one due review group using live shell deps."""
    return _review_queue_helpers.run_review_rescue_for_group(
        config=config,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=pr_port,
        pr_repo=pr_repo,
        pr_number=pr_number,
        snapshot=snapshot,
        dry_run=dry_run,
        gh_runner=gh_runner,
        review_rescue_fn=build_review_queue_wiring_deps().review_rescue_fn,
    )


def seed_new_review_entries_from_shell(
    store: Any,
    review_refs: set[str],
    existing_refs: set[str],
    *,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[str]:
    """Seed queue entries for Review issues not yet tracked."""
    return _review_queue_helpers.seed_new_review_entries(
        store,
        review_refs,
        existing_refs,
        dry_run=dry_run,
        now=now,
    )
