"""Review-queue preparation helpers extracted from review-queue processing."""

from __future__ import annotations

from datetime import datetime
from typing import Callable

import startupai_controller.consumer_review_queue_drain_processing as _drain_processing
from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_types import (
    PreparedDueReviewProcessing,
    PreparedReviewQueueBatch,
)
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ReviewQueueDrainSummary,
    ReviewQueueEntry,
    ReviewSnapshot,
)
from startupai_controller.domain.review_queue_policy import (
    DEFAULT_REVIEW_QUEUE_BATCH_SIZE,
)
from startupai_controller.domain.verdict_policy import (
    is_pre_backfill_eligible,
    is_session_verdict_eligible,
    marker_already_present,
    verdict_marker_text,
)
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
)

from startupai_controller.consumer_review_queue_state import (
    group_review_queue_entries_by_pr,
    partition_review_queue_entries_by_probe_change,
    prune_stale_review_entries,
    repark_unchanged_review_queue_entries,
    reconcile_review_queue_identity,
    review_scope_issue_refs,
    seed_new_review_entries,
    wakeup_changed_review_queue_entries,
)


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
