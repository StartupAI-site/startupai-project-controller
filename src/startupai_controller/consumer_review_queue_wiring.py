"""Review-queue wiring helpers extracted from board_consumer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable


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
