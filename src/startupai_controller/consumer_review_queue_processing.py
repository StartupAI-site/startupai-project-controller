"""Review-queue processing helpers extracted from board_consumer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, cast

import startupai_controller.consumer_review_queue_drain_processing as _drain_processing
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    PreparedDueReviewProcessing,
    PreparedReviewQueueBatch,
    ReviewGroupProcessingOutcome,
    ReviewQueueProcessingOutcome,
)
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ReviewQueueDrainSummary,
    ReviewQueueEntry,
    ReviewRescueResult,
    ReviewSnapshot,
)
from startupai_controller.domain.review_queue_policy import (
    DEFAULT_REVIEW_QUEUE_BATCH_SIZE,
    requeue_or_escalate,
)
from startupai_controller.domain.verdict_policy import (
    is_pre_backfill_eligible,
    is_session_verdict_eligible,
    marker_already_present,
    verdict_marker_text,
)
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.runtime.wiring import GitHubPortBundle, GitHubRuntimeMemo
from startupai_controller.runtime.wiring import ConsumerDB
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
)

from startupai_controller.consumer_review_queue_state import (
    apply_review_queue_partial_failure,
    apply_review_queue_result,
    group_review_queue_entries_by_pr,
    partition_review_queue_entries_by_probe_change,
    prune_stale_review_entries,
    repark_unchanged_review_queue_entries,
    reconcile_review_queue_identity,
    review_scope_issue_refs,
    seed_new_review_entries,
    update_board_snapshot_statuses,
    wakeup_changed_review_queue_entries,
)


@dataclass(frozen=True)
class ReviewRescueExecution:
    """Result of executing rescue logic for one review PR."""

    result: ReviewRescueResult
    partial_failure: bool = False
    error: str | None = None


def build_review_snapshots_for_queue_entries(
    *,
    queue_entries: list[ReviewQueueEntry],
    review_refs: set[str],
    pr_port: PullRequestPort,
    trusted_codex_actors: frozenset[str],
) -> dict[tuple[str, int], ReviewSnapshot]:
    """Build one review snapshot per unique PR for queued review entries."""
    review_refs_by_pr: dict[tuple[str, int], list[str]] = {}
    for entry in queue_entries:
        if entry.issue_ref not in review_refs:
            continue
        review_refs_by_pr.setdefault((entry.pr_repo, entry.pr_number), []).append(
            entry.issue_ref
        )
    return pr_port.review_snapshots(
        {
            pr_key: tuple(sorted(set(refs)))
            for pr_key, refs in review_refs_by_pr.items()
        },
        trusted_codex_actors=trusted_codex_actors,
    )


def backfill_review_verdicts_from_snapshots(
    store: SessionStorePort,
    entries: list[ReviewQueueEntry],
    snapshots: dict[tuple[str, int], ReviewSnapshot],
    *,
    pr_port: PullRequestPort,
    post_pr_codex_verdict: Callable[..., bool],
    log_warning: Callable[[str, str, Exception], None],
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, ...]:
    """Backfill missing verdict markers using already-fetched PR comment payloads."""
    backfilled: list[str] = []
    posted_markers: dict[tuple[str, int], set[str]] = {}
    for entry in entries:
        snapshot = snapshots.get((entry.pr_repo, entry.pr_number))
        if snapshot is None:
            continue
        session = (
            store.get_session(entry.source_session_id)
            if entry.source_session_id
            else store.latest_session_for_issue(entry.issue_ref)
        )
        if session is None:
            continue
        if not is_session_verdict_eligible(
            session_status=session.status,
            session_phase=session.phase,
            session_pr_url=session.pr_url,
        ):
            continue
        assert session.pr_url is not None
        marker = verdict_marker_text(session.id)
        seen_markers = posted_markers.setdefault(
            (entry.pr_repo, entry.pr_number),
            {body for body in snapshot.pr_comment_bodies if isinstance(body, str)},
        )
        if marker_already_present(marker, seen_markers):
            continue
        try:
            if comment_poster is None and gh_runner is None:
                posted = pr_port.post_codex_verdict_if_missing(
                    session.pr_url,
                    session.id,
                )
            else:
                posted = post_pr_codex_verdict(
                    session.pr_url,
                    session.id,
                    comment_checker=lambda *args, **kwargs: False,
                    comment_poster=comment_poster,
                    gh_runner=gh_runner,
                )
        except Exception as err:
            log_warning(entry.issue_ref, session.id, err)
            continue
        if posted:
            seen_markers.add(marker)
            backfilled.append(entry.issue_ref)
    return tuple(backfilled)


def pre_backfill_verdicts_for_due_prs(
    store: SessionStorePort,
    due_items: list[ReviewQueueEntry],
    *,
    post_pr_codex_verdict: Callable[..., bool],
    pr_port: PullRequestPort | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    log_warning: Callable[[str, Exception], None],
) -> tuple[str, ...]:
    """Post missing verdicts before snapshot build for eligible review items."""
    backfilled: list[str] = []
    for entry in due_items:
        if not is_pre_backfill_eligible(
            last_result=entry.last_result,
            last_reason=entry.last_reason,
        ):
            continue
        try:
            session = (
                store.get_session(entry.source_session_id)
                if entry.source_session_id
                else store.latest_session_for_issue(entry.issue_ref)
            )
            if session is None:
                continue
            if not is_session_verdict_eligible(
                session_status=session.status,
                session_phase=session.phase,
                session_pr_url=session.pr_url,
                entry_pr_url=entry.pr_url,
            ):
                continue
            assert session.pr_url is not None
            if (
                pr_port is not None
                and comment_checker is None
                and comment_poster is None
                and gh_runner is None
            ):
                posted = pr_port.post_codex_verdict_if_missing(
                    session.pr_url,
                    session.id,
                )
            else:
                posted = post_pr_codex_verdict(
                    session.pr_url,
                    session.id,
                    comment_checker=comment_checker,
                    comment_poster=comment_poster,
                    gh_runner=gh_runner,
                )
            if posted:
                backfilled.append(entry.issue_ref)
        except Exception as err:
            log_warning(entry.issue_ref, err)
            continue
    return tuple(backfilled)


def prepare_review_queue_batch(
    *,
    config: ConsumerConfig,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    board_snapshot: CycleBoardSnapshot,
    pr_port: PullRequestPort,
    now: datetime,
    dry_run: bool,
    prepared_batch_factory: type[PreparedReviewQueueBatch],
    summary_factory: type[ReviewQueueDrainSummary],
    log_warning: Callable[[Exception], None],
    wakeup_changed_review_queue_entries_fn: (
        Callable[..., tuple[str, ...]] | None
    ) = None,
) -> tuple[PreparedReviewQueueBatch | None, ReviewQueueDrainSummary | None]:
    """Prepare the bounded review-queue workset for one drain cycle."""
    review_refs = frozenset(
        review_scope_issue_refs(
            config,
            critical_path_config,
            board_snapshot,
        )
    )
    existing_entries = store.list_review_queue_items()
    existing_refs = {entry.issue_ref for entry in existing_entries}

    removed = tuple(
        prune_stale_review_entries(
            store, review_refs, existing_entries, dry_run=dry_run
        )
    )
    seeded = tuple(
        seed_new_review_entries(
            store,
            review_refs,
            existing_refs,
            dry_run=dry_run,
            now=now,
        )
    )
    if not dry_run:
        reconcile_review_queue_identity(store, review_refs, now=now)

    queue_items = tuple(
        entry
        for entry in store.list_review_queue_items()
        if entry.issue_ref in review_refs
    )
    if not review_refs and not queue_items:
        return None, summary_factory(
            queued_count=0,
            due_count=0,
            removed=removed,
            skipped=("control-plane:no-review-items",),
        )

    try:
        (wakeup_changed_review_queue_entries_fn or wakeup_changed_review_queue_entries)(
            store,
            list(queue_items),
            now=now,
            pr_port=pr_port,
            dry_run=dry_run,
        )
    except GhQueryError as err:
        log_warning(err)

    if not dry_run:
        queue_items = tuple(
            entry
            for entry in store.list_review_queue_items()
            if entry.issue_ref in review_refs
        )

    queue_pr_groups = dict(group_review_queue_entries_by_pr(list(queue_items)))
    due_items = tuple(
        entry for entry in queue_items if entry.next_attempt_datetime() <= now
    )
    due_pr_groups_list = group_review_queue_entries_by_pr(list(due_items))[
        :DEFAULT_REVIEW_QUEUE_BATCH_SIZE
    ]
    due_items = tuple(
        entry for _pr_key, entries in due_pr_groups_list for entry in entries
    )
    due_pr_keys = {pr_key for pr_key, _entries in due_pr_groups_list}
    selected_snapshot_entries = tuple(
        entry
        for pr_key, entries in queue_pr_groups.items()
        if pr_key in due_pr_keys
        for entry in entries
    )

    return (
        prepared_batch_factory(
            review_refs=review_refs,
            queue_items=queue_items,
            due_items=due_items,
            due_pr_groups=tuple(
                (pr_key, tuple(entries)) for pr_key, entries in due_pr_groups_list
            ),
            selected_snapshot_entries=selected_snapshot_entries,
            seeded=seeded,
            removed=removed,
        ),
        None,
    )


def prepare_due_review_processing(
    *,
    store: SessionStorePort,
    automation_config: BoardAutomationConfig,
    pr_port: PullRequestPort,
    prepared_batch: PreparedReviewQueueBatch,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    post_pr_codex_verdict: Callable[..., bool],
    prepared_due_processing_factory: type[PreparedDueReviewProcessing],
    log_pre_backfill_warning: Callable[[str, Exception], None],
    log_backfill_warning: Callable[[str, str, Exception], None],
    pre_backfill_verdicts_for_due_prs_fn: Callable[..., tuple[str, ...]] | None = None,
    partition_review_queue_entries_by_probe_change_fn: (
        Callable[..., tuple[list[ReviewQueueEntry], list[ReviewQueueEntry]]] | None
    ) = None,
    repark_unchanged_review_queue_entries_fn: Callable[..., None] | None = None,
    build_review_snapshots_for_queue_entries_fn: (
        Callable[..., dict[tuple[str, int], ReviewSnapshot]] | None
    ) = None,
    backfill_review_verdicts_from_snapshots_fn: (
        Callable[..., tuple[str, ...]] | None
    ) = None,
) -> PreparedDueReviewProcessing:
    """Prepare the changed due-review groups and snapshots for rescue processing."""
    return _drain_processing.prepare_due_review_processing(
        store=store,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        prepared_due_processing_factory=prepared_due_processing_factory,
        pre_backfill_verdicts_for_due_prs_fn=(
            pre_backfill_verdicts_for_due_prs_fn
            or (
                lambda *args, **kwargs: pre_backfill_verdicts_for_due_prs(
                    *args,
                    post_pr_codex_verdict=post_pr_codex_verdict,
                    log_warning=log_pre_backfill_warning,
                    **kwargs,
                )
            )
        ),
        partition_review_queue_entries_by_probe_change_fn=(
            partition_review_queue_entries_by_probe_change_fn
            or partition_review_queue_entries_by_probe_change
        ),
        repark_unchanged_review_queue_entries_fn=(
            repark_unchanged_review_queue_entries_fn
            or repark_unchanged_review_queue_entries
        ),
        build_review_snapshots_for_queue_entries_fn=(
            build_review_snapshots_for_queue_entries_fn
            or build_review_snapshots_for_queue_entries
        ),
        backfill_review_verdicts_from_snapshots_fn=(
            backfill_review_verdicts_from_snapshots_fn
            or (
                lambda *args, **kwargs: backfill_review_verdicts_from_snapshots(
                    *args,
                    post_pr_codex_verdict=post_pr_codex_verdict,
                    log_warning=log_backfill_warning,
                    **kwargs,
                )
            )
        ),
    )


def run_review_rescue_for_group(
    *,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    pr_port: PullRequestPort,
    pr_repo: str,
    pr_number: int,
    snapshot: ReviewSnapshot,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    review_rescue_fn: Callable[..., ReviewRescueResult],
) -> ReviewRescueExecution:
    """Run rescue logic for one due review group."""
    try:
        return ReviewRescueExecution(
            result=review_rescue_fn(
                pr_repo=pr_repo,
                pr_number=pr_number,
                config=critical_path_config,
                automation_config=automation_config,
                project_owner=config.project_owner,
                project_number=config.project_number,
                dry_run=dry_run,
                snapshot=snapshot,
                gh_runner=gh_runner,
                pr_port=pr_port,
            )
        )
    except GhQueryError as err:
        return ReviewRescueExecution(
            result=ReviewRescueResult(pr_repo=pr_repo, pr_number=pr_number),
            partial_failure=True,
            error=str(err),
        )


def apply_review_queue_group_result(
    *,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    pr_port: PullRequestPort,
    pr_repo: str,
    pr_number: int,
    entries: tuple[ReviewQueueEntry, ...],
    result: ReviewRescueResult,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    escalate_to_claude: Callable[..., None],
) -> tuple[str, ...]:
    """Persist one review-group result and return escalated issue refs."""
    if dry_run:
        return ()

    escalated: list[str] = []
    state_digest = pr_port.review_state_digests([(pr_repo, pr_number)]).get(
        (pr_repo, pr_number)
    )
    for entry in entries:
        needs_escalation = apply_review_queue_result(
            store,
            entry,
            result,
            now=now,
            last_state_digest=state_digest,
        )
        if needs_escalation:
            escalate_to_claude(
                entry.issue_ref,
                critical_path_config,
                project_owner,
                project_number,
                reason=f"review queue blocked escalation: {result.blocked_reason}",
                gh_runner=gh_runner,
            )
            store.delete_review_queue_item(entry.issue_ref)
            escalated.append(entry.issue_ref)
    return tuple(escalated)


def summarize_review_group_outcome(
    *,
    critical_path_config: CriticalPathConfig,
    store: SessionStorePort,
    project_owner: str,
    project_number: int,
    pr_repo: str,
    pr_number: int,
    entries: tuple[ReviewQueueEntry, ...],
    result: ReviewRescueResult,
    updated_snapshot: CycleBoardSnapshot,
    escalated: tuple[str, ...],
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    escalate_to_claude: Callable[..., None],
    review_group_outcome_factory: type[ReviewGroupProcessingOutcome],
) -> ReviewGroupProcessingOutcome:
    """Build the public outcome for one processed review group."""
    escalated_refs = list(escalated)
    requeued: list[str] = []
    blocked: list[str] = []
    skipped: list[str] = []
    rerun: list[str] = []
    auto_merge_enabled: list[str] = []

    pr_ref = f"{pr_repo}#{pr_number}"
    if result.rerun_checks:
        rerun.append(f"{pr_ref}:{','.join(result.rerun_checks)}")
    elif result.auto_merge_enabled:
        auto_merge_enabled.append(pr_ref)
    elif result.requeued_refs:
        for issue_ref in result.requeued_refs:
            entry = next(
                (
                    candidate
                    for candidate in entries
                    if candidate.issue_ref == issue_ref
                ),
                None,
            )
            pr_url = entry.pr_url if entry is not None else ""
            requeue_count, _ = store.get_requeue_state(issue_ref)
            if requeue_or_escalate(requeue_count) == "escalate":
                if not dry_run:
                    escalate_to_claude(
                        issue_ref,
                        critical_path_config,
                        project_owner,
                        project_number,
                        reason=(
                            f"repair requeue ceiling ({requeue_count} cycles on same PR): "
                            f"{result.blocked_reason or 'persistent check failure / conflict'}"
                        ),
                        gh_runner=gh_runner,
                    )
                    store.delete_review_queue_item(issue_ref)
                escalated_refs.append(issue_ref)
            else:
                if not dry_run:
                    store.increment_requeue_count(issue_ref, pr_url)
                    store.delete_review_queue_item(issue_ref)
                requeued.append(issue_ref)
        requeued_this_group = [
            ref for ref in result.requeued_refs if ref not in escalated_refs
        ]
        if requeued_this_group:
            updated_snapshot = update_board_snapshot_statuses(
                updated_snapshot,
                critical_path_config,
                {ref: "Ready" for ref in requeued_this_group},
            )
    elif result.blocked_reason:
        blocked.append(f"{pr_ref}:{result.blocked_reason}")
    elif result.skipped_reason:
        skipped.append(f"{pr_ref}:{result.skipped_reason}")

    return review_group_outcome_factory(
        rerun=tuple(rerun),
        auto_merge_enabled=tuple(auto_merge_enabled),
        requeued=tuple(requeued),
        blocked=tuple(blocked),
        skipped=tuple(skipped),
        escalated=tuple(escalated_refs),
        updated_snapshot=updated_snapshot,
    )


def process_due_review_group(
    *,
    config: ConsumerConfig,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    pr_port: PullRequestPort,
    pr_repo: str,
    pr_number: int,
    entries: tuple[ReviewQueueEntry, ...],
    snapshot: ReviewSnapshot | None,
    updated_snapshot: CycleBoardSnapshot,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    review_group_outcome_factory: type[ReviewGroupProcessingOutcome],
    review_rescue_fn: Callable[..., ReviewRescueResult],
    escalate_to_claude: Callable[..., None],
) -> ReviewGroupProcessingOutcome:
    """Process one due PR group from the review queue."""

    def _run_review_rescue_for_group(
        *,
        config: ConsumerConfig,
        critical_path_config: CriticalPathConfig,
        automation_config: BoardAutomationConfig,
        pr_port: PullRequestPort,
        pr_repo: str,
        pr_number: int,
        snapshot: ReviewSnapshot,
        dry_run: bool,
        gh_runner: Callable[..., str] | None,
    ) -> ReviewRescueExecution:
        return run_review_rescue_for_group(
            config=config,
            critical_path_config=critical_path_config,
            automation_config=automation_config,
            pr_port=pr_port,
            pr_repo=pr_repo,
            pr_number=pr_number,
            snapshot=snapshot,
            dry_run=dry_run,
            gh_runner=gh_runner,
            review_rescue_fn=review_rescue_fn,
        )

    return _drain_processing.process_due_review_group(
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
        review_group_outcome_factory=review_group_outcome_factory,
        run_review_rescue_for_group_fn=cast(
            Callable[..., _drain_processing.ReviewRescueExecutionView],
            _run_review_rescue_for_group,
        ),
        apply_review_queue_group_result_fn=(
            lambda **kwargs: apply_review_queue_group_result(
                escalate_to_claude=escalate_to_claude,
                **kwargs,
            )
        ),
        summarize_review_group_outcome_fn=(
            lambda **kwargs: summarize_review_group_outcome(
                escalate_to_claude=escalate_to_claude,
                **kwargs,
            )
        ),
    )


def process_review_queue_due_groups(
    *,
    config: ConsumerConfig,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    pr_port: PullRequestPort,
    prepared_batch: PreparedReviewQueueBatch,
    board_snapshot: CycleBoardSnapshot,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    post_pr_codex_verdict: Callable[..., bool],
    review_rescue_fn: Callable[..., ReviewRescueResult],
    escalate_to_claude: Callable[..., None],
    prepared_due_processing_factory: type[PreparedDueReviewProcessing],
    review_group_outcome_factory: type[ReviewGroupProcessingOutcome],
    review_queue_processing_outcome_factory: type[ReviewQueueProcessingOutcome],
    gh_reason_code: Callable[[Exception], str],
    log_pre_backfill_warning: Callable[[str, Exception], None],
    log_backfill_warning: Callable[[str, str, Exception], None],
    pre_backfill_verdicts_for_due_prs_fn: Callable[..., tuple[str, ...]] | None = None,
    partition_review_queue_entries_by_probe_change_fn: (
        Callable[..., tuple[list[ReviewQueueEntry], list[ReviewQueueEntry]]] | None
    ) = None,
    repark_unchanged_review_queue_entries_fn: Callable[..., None] | None = None,
    build_review_snapshots_for_queue_entries_fn: (
        Callable[..., dict[tuple[str, int], ReviewSnapshot]] | None
    ) = None,
    backfill_review_verdicts_from_snapshots_fn: (
        Callable[..., tuple[str, ...]] | None
    ) = None,
) -> ReviewQueueProcessingOutcome:
    """Process the due PR groups for a prepared review-queue batch."""
    return _drain_processing.process_review_queue_due_groups(
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
        review_queue_processing_outcome_factory=review_queue_processing_outcome_factory,
        gh_reason_code=gh_reason_code,
        prepare_due_review_processing_fn=(
            lambda **kwargs: prepare_due_review_processing(
                post_pr_codex_verdict=post_pr_codex_verdict,
                prepared_due_processing_factory=prepared_due_processing_factory,
                log_pre_backfill_warning=log_pre_backfill_warning,
                log_backfill_warning=log_backfill_warning,
                pre_backfill_verdicts_for_due_prs_fn=(
                    pre_backfill_verdicts_for_due_prs_fn
                ),
                partition_review_queue_entries_by_probe_change_fn=(
                    partition_review_queue_entries_by_probe_change_fn
                ),
                repark_unchanged_review_queue_entries_fn=(
                    repark_unchanged_review_queue_entries_fn
                ),
                build_review_snapshots_for_queue_entries_fn=(
                    build_review_snapshots_for_queue_entries_fn
                ),
                backfill_review_verdicts_from_snapshots_fn=(
                    backfill_review_verdicts_from_snapshots_fn
                ),
                **kwargs,
            )
        ),
        process_due_review_group_fn=(
            lambda **kwargs: process_due_review_group(
                review_group_outcome_factory=review_group_outcome_factory,
                review_rescue_fn=review_rescue_fn,
                escalate_to_claude=escalate_to_claude,
                **kwargs,
            )
        ),
        apply_review_queue_partial_failure_fn=apply_review_queue_partial_failure,
    )


def drain_review_queue(
    config: ConsumerConfig,
    db: ConsumerDB,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    *,
    pr_port: PullRequestPort | None = None,
    session_store: SessionStorePort | None = None,
    board_snapshot: CycleBoardSnapshot | None = None,
    dry_run: bool = False,
    github_memo: GitHubRuntimeMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
    build_github_port_bundle: Callable[..., GitHubPortBundle],
    build_session_store: Callable[[ConsumerDB], SessionStorePort],
    github_memo_factory: Callable[[], GitHubRuntimeMemo],
    prepared_batch_factory: type[PreparedReviewQueueBatch],
    summary_factory: type[ReviewQueueDrainSummary],
    prepared_due_processing_factory: type[PreparedDueReviewProcessing],
    review_group_outcome_factory: type[ReviewGroupProcessingOutcome],
    review_queue_processing_outcome_factory: type[ReviewQueueProcessingOutcome],
    post_pr_codex_verdict: Callable[..., bool],
    review_rescue_fn: Callable[..., ReviewRescueResult],
    escalate_to_claude: Callable[..., None],
    gh_reason_code: Callable[[Exception], str],
    log_probe_warning: Callable[[Exception], None],
    log_pre_backfill_warning: Callable[[str, Exception], None],
    log_backfill_warning: Callable[[str, str, Exception], None],
    wakeup_changed_review_queue_entries_fn: (
        Callable[..., tuple[str, ...]] | None
    ) = None,
    pre_backfill_verdicts_for_due_prs_fn: Callable[..., tuple[str, ...]] | None = None,
    partition_review_queue_entries_by_probe_change_fn: (
        Callable[..., tuple[list[ReviewQueueEntry], list[ReviewQueueEntry]]] | None
    ) = None,
    repark_unchanged_review_queue_entries_fn: Callable[..., None] | None = None,
    build_review_snapshots_for_queue_entries_fn: (
        Callable[..., dict[tuple[str, int], ReviewSnapshot]] | None
    ) = None,
    backfill_review_verdicts_from_snapshots_fn: (
        Callable[..., tuple[str, ...]] | None
    ) = None,
) -> tuple[ReviewQueueDrainSummary, CycleBoardSnapshot]:
    """Process a bounded batch of queued Review items."""
    return _drain_processing.drain_review_queue(
        config,
        db,
        critical_path_config,
        automation_config,
        pr_port=pr_port,
        session_store=session_store,
        board_snapshot=board_snapshot,
        dry_run=dry_run,
        github_memo=github_memo,
        gh_runner=gh_runner,
        build_github_port_bundle=build_github_port_bundle,
        build_session_store=build_session_store,
        github_memo_factory=github_memo_factory,
        summary_factory=summary_factory,
        prepare_review_queue_batch_fn=(
            lambda **kwargs: prepare_review_queue_batch(
                prepared_batch_factory=prepared_batch_factory,
                summary_factory=summary_factory,
                log_warning=log_probe_warning,
                wakeup_changed_review_queue_entries_fn=(
                    wakeup_changed_review_queue_entries_fn
                ),
                **kwargs,
            )
        ),
        process_review_queue_due_groups_fn=(
            lambda **kwargs: process_review_queue_due_groups(
                post_pr_codex_verdict=post_pr_codex_verdict,
                review_rescue_fn=review_rescue_fn,
                escalate_to_claude=escalate_to_claude,
                prepared_due_processing_factory=prepared_due_processing_factory,
                review_group_outcome_factory=review_group_outcome_factory,
                review_queue_processing_outcome_factory=(
                    review_queue_processing_outcome_factory
                ),
                gh_reason_code=gh_reason_code,
                log_pre_backfill_warning=log_pre_backfill_warning,
                log_backfill_warning=log_backfill_warning,
                pre_backfill_verdicts_for_due_prs_fn=(
                    pre_backfill_verdicts_for_due_prs_fn
                ),
                partition_review_queue_entries_by_probe_change_fn=(
                    partition_review_queue_entries_by_probe_change_fn
                ),
                repark_unchanged_review_queue_entries_fn=(
                    repark_unchanged_review_queue_entries_fn
                ),
                build_review_snapshots_for_queue_entries_fn=(
                    build_review_snapshots_for_queue_entries_fn
                ),
                backfill_review_verdicts_from_snapshots_fn=(
                    backfill_review_verdicts_from_snapshots_fn
                ),
                **kwargs,
            )
        ),
    )
