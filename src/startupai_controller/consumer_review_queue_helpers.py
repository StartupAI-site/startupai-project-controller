"""Review-queue helper cluster extracted from board_consumer."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Callable

from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ProjectItemSnapshot,
    ReviewQueueDrainSummary,
    ReviewQueueEntry,
    ReviewRescueResult,
    ReviewSnapshot,
)
from startupai_controller.domain.repair_policy import parse_pr_url
from startupai_controller.domain.review_queue_policy import (
    DEFAULT_REVIEW_QUEUE_BATCH_SIZE,
    DEFAULT_REVIEW_QUEUE_RETRY_SECONDS,
    REVIEW_QUEUE_STABLE_RESULTS,
    blocked_streak_needs_escalation,
    requeue_or_escalate,
    review_queue_retry_seconds_for_partial_failure,
    review_queue_retry_seconds_for_result,
)
from startupai_controller.domain.scheduling_policy import snapshot_to_issue_ref
from startupai_controller.domain.verdict_policy import (
    is_pre_backfill_eligible,
    is_session_verdict_eligible,
    marker_already_present,
    verdict_marker_text,
)
from startupai_controller.validate_critical_path_promotion import (
    GhQueryError,
    parse_issue_ref,
)
from startupai_controller.runtime.wiring import gh_reason_code as _gh_reason_code


@dataclass(frozen=True)
class ReviewRescueExecution:
    """Result of executing rescue logic for one review PR."""

    result: ReviewRescueResult
    partial_failure: bool = False
    error: str | None = None


def group_review_queue_entries_by_pr(
    entries: list[ReviewQueueEntry],
) -> list[tuple[tuple[str, int], tuple[ReviewQueueEntry, ...]]]:
    """Group review queue rows by PR while preserving earliest-due ordering."""
    groups: dict[tuple[str, int], list[ReviewQueueEntry]] = {}
    for entry in entries:
        groups.setdefault((entry.pr_repo, entry.pr_number), []).append(entry)
    return sorted(
        (
            key,
            tuple(
                sorted(
                    grouped,
                    key=lambda item: (
                        item.next_attempt_at,
                        item.enqueued_at,
                        item.issue_ref,
                    ),
                )
            ),
        )
        for key, grouped in groups.items()
    )


def review_queue_next_attempt_at(
    *,
    now: datetime | None = None,
    delay_seconds: int = DEFAULT_REVIEW_QUEUE_RETRY_SECONDS,
) -> str:
    """Return the next scheduled review-queue attempt timestamp."""
    current = now or datetime.now(timezone.utc)
    return (current + timedelta(seconds=delay_seconds)).isoformat()


def apply_review_queue_result(
    store: Any,
    entry: ReviewQueueEntry,
    result: Any,
    *,
    now: datetime | None = None,
    retry_seconds: int | None = None,
    last_state_digest: str | None = None,
) -> bool:
    """Persist the outcome of processing one review-queue entry."""
    current = now or datetime.now(timezone.utc)
    effective_state_digest = (
        entry.last_state_digest if last_state_digest is None else last_state_digest
    )
    effective_retry_seconds = (
        retry_seconds
        if retry_seconds is not None
        else review_queue_retry_seconds_for_result(result)
    )
    next_attempt_at = review_queue_next_attempt_at(
        now=current,
        delay_seconds=effective_retry_seconds,
    )

    if result.requeued_refs:
        return False

    if result.auto_merge_enabled:
        store.update_review_queue_item(
            entry.issue_ref,
            next_attempt_at=next_attempt_at,
            last_result="auto_merge_enabled",
            last_reason=None,
            last_state_digest=effective_state_digest,
            blocked_streak=0,
            blocked_class=None,
            now=current,
        )
        return False

    if result.rerun_checks:
        store.update_review_queue_item(
            entry.issue_ref,
            next_attempt_at=next_attempt_at,
            last_result="rerun_checks",
            last_reason=",".join(result.rerun_checks),
            last_state_digest=effective_state_digest,
            blocked_streak=0,
            blocked_class=None,
            now=current,
        )
        return False

    if result.blocked_reason:
        new_class, new_streak, needs_escalation = blocked_streak_needs_escalation(
            result.blocked_reason,
            entry.blocked_streak,
            entry.blocked_class,
        )
        store.update_review_queue_item(
            entry.issue_ref,
            next_attempt_at=next_attempt_at,
            last_result="blocked",
            last_reason=result.blocked_reason,
            last_state_digest=effective_state_digest,
            blocked_streak=new_streak,
            blocked_class=new_class,
            now=current,
        )
        return needs_escalation

    if result.skipped_reason:
        store.update_review_queue_item(
            entry.issue_ref,
            next_attempt_at=next_attempt_at,
            last_result="skipped",
            last_reason=result.skipped_reason,
            last_state_digest=effective_state_digest,
            blocked_streak=0,
            blocked_class=None,
            now=current,
        )
        return False

    store.update_review_queue_item(
        entry.issue_ref,
        next_attempt_at=next_attempt_at,
        last_result="processed",
        last_reason=None,
        last_state_digest=effective_state_digest,
        blocked_streak=0,
        blocked_class=None,
        now=current,
    )
    return False


def apply_review_queue_partial_failure(
    store: Any,
    entries: list[ReviewQueueEntry],
    *,
    config: Any,
    error: str | None,
    gh_reason_code: Callable[[str | Exception], str] = _gh_reason_code,
    now: datetime | None = None,
) -> None:
    """Back off queued review entries after a partial-failure cycle."""
    if not entries:
        return
    current = now or datetime.now(timezone.utc)
    next_attempt_at = review_queue_next_attempt_at(
        now=current,
        delay_seconds=review_queue_retry_seconds_for_partial_failure(
            config.rate_limit_cooldown_seconds,
            gh_reason_code(error) if error else None,
        ),
    )
    for entry in entries:
        store.update_review_queue_item(
            entry.issue_ref,
            next_attempt_at=next_attempt_at,
            last_result="partial_failure",
            last_reason=error,
            last_state_digest=entry.last_state_digest,
            blocked_streak=0,
            blocked_class=None,
            now=current,
        )


def review_scope_issue_refs(
    config: Any,
    critical_path_config: Any,
    board_snapshot: CycleBoardSnapshot,
) -> tuple[str, ...]:
    """Return governed review issue refs for the consumer executor."""
    refs: list[str] = []
    for snapshot in board_snapshot.items_with_status("Review"):
        issue_ref = snapshot_to_issue_ref(
            snapshot.issue_ref, critical_path_config.issue_prefixes
        )
        if issue_ref is None:
            continue
        if parse_issue_ref(issue_ref).prefix not in config.repo_prefixes:
            continue
        if snapshot.executor.strip().lower() != config.executor:
            continue
        refs.append(issue_ref)
    return tuple(refs)


def queue_review_item(
    store: Any,
    issue_ref: str,
    pr_url: str,
    *,
    session_id: str | None = None,
    now: datetime | None = None,
) -> ReviewQueueEntry | None:
    """Persist one review item for bounded follow-up processing."""
    parsed = parse_pr_url(pr_url)
    if parsed is None:
        return None
    owner, repo, pr_number = parsed
    store.enqueue_review_item(
        issue_ref,
        pr_url=pr_url,
        pr_repo=f"{owner}/{repo}",
        pr_number=pr_number,
        source_session_id=session_id,
        next_attempt_at=(now or datetime.now(timezone.utc)).isoformat(),
        now=now,
    )
    return store.get_review_queue_item(issue_ref)


def prune_stale_review_entries(
    store: Any,
    review_refs: set[str],
    existing_entries: list[ReviewQueueEntry],
    *,
    dry_run: bool = False,
) -> list[str]:
    """Remove queue entries for issues no longer in Review scope."""
    removed: list[str] = []
    for entry in existing_entries:
        if entry.issue_ref in review_refs:
            continue
        if not dry_run:
            store.delete_review_queue_item(entry.issue_ref)
        removed.append(entry.issue_ref)
    return removed


def seed_new_review_entries(
    store: Any,
    review_refs: set[str],
    existing_refs: set[str],
    *,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[str]:
    """Seed queue entries for Review issues not yet tracked."""
    seeded: list[str] = []
    for issue_ref in sorted(review_refs):
        if issue_ref in existing_refs:
            continue
        latest_session = store.latest_session_for_issue(issue_ref)
        if latest_session is None or not latest_session.pr_url:
            continue
        if not dry_run:
            if (
                queue_review_item(
                    store,
                    issue_ref,
                    latest_session.pr_url,
                    session_id=latest_session.id,
                    now=now,
                )
                is None
            ):
                continue
        seeded.append(issue_ref)
    return seeded


def reconcile_review_queue_identity(
    store: Any,
    review_refs: set[str],
    *,
    now: datetime | None = None,
) -> None:
    """Reconcile queue rows and requeue counters against current PR identity."""
    for entry in store.list_review_queue_items():
        if entry.issue_ref not in review_refs:
            continue
        current_pr_url = entry.pr_url
        latest_session = store.latest_session_for_issue(entry.issue_ref)
        if latest_session is not None and latest_session.pr_url:
            current_pr_url = latest_session.pr_url
        if current_pr_url != entry.pr_url:
            parsed = parse_pr_url(current_pr_url)
            if parsed is None:
                continue
            owner, repo, pr_number = parsed
            store.enqueue_review_item(
                entry.issue_ref,
                pr_url=current_pr_url,
                pr_repo=f"{owner}/{repo}",
                pr_number=pr_number,
                source_session_id=(
                    latest_session.id
                    if latest_session is not None
                    else entry.source_session_id
                ),
                next_attempt_at=entry.next_attempt_at,
                now=now,
            )
        _count, stored_pr_url = store.get_requeue_state(entry.issue_ref)
        if stored_pr_url is not None and stored_pr_url != current_pr_url:
            store.reset_requeue_count(entry.issue_ref)


def review_queue_state_probe_candidates(
    entries: list[ReviewQueueEntry],
) -> list[ReviewQueueEntry]:
    """Return review entries whose PR state can be tracked cheaply via digest."""
    return [
        entry for entry in entries if entry.last_result in REVIEW_QUEUE_STABLE_RESULTS
    ]


def partition_review_queue_entries_by_probe_change(
    entries: list[ReviewQueueEntry],
    *,
    pr_port: Any,
) -> tuple[list[ReviewQueueEntry], list[ReviewQueueEntry]]:
    """Split queued review entries into changed vs unchanged probe state."""
    unchanged: list[ReviewQueueEntry] = []
    changed: list[ReviewQueueEntry] = []
    numbers_by_repo: dict[str, list[int]] = {}
    for entry in entries:
        if not (
            entry.last_state_digest and entry.last_result in REVIEW_QUEUE_STABLE_RESULTS
        ):
            changed.append(entry)
            continue
        numbers_by_repo.setdefault(entry.pr_repo, []).append(entry.pr_number)

    digests = pr_port.review_state_digests(
        [
            (pr_repo, pr_number)
            for pr_repo, pr_numbers in sorted(numbers_by_repo.items())
            for pr_number in sorted(set(pr_numbers))
        ]
    )

    for entry in entries:
        if not (
            entry.last_state_digest and entry.last_result in REVIEW_QUEUE_STABLE_RESULTS
        ):
            continue
        digest = digests.get((entry.pr_repo, entry.pr_number))
        if digest and digest == entry.last_state_digest:
            unchanged.append(entry)
        else:
            changed.append(entry)
    return changed, unchanged


def repark_unchanged_review_queue_entries(
    store: Any,
    entries: list[ReviewQueueEntry],
    *,
    now: datetime,
) -> None:
    """Re-schedule unchanged review entries without rehydrating the full PR state."""
    for entry in entries:
        synthetic_result = SimpleNamespace(
            rerun_checks=(),
            auto_merge_enabled=entry.last_result == "auto_merge_enabled",
            requeued_refs=(),
            blocked_reason=(
                entry.last_reason if entry.last_result == "blocked" else None
            ),
            skipped_reason=(
                entry.last_reason if entry.last_result == "skipped" else None
            ),
        )
        retry_seconds = review_queue_retry_seconds_for_result(synthetic_result)
        if entry.last_result == "partial_failure":
            retry_seconds = max(DEFAULT_REVIEW_QUEUE_RETRY_SECONDS, retry_seconds)
        apply_review_queue_result(
            store,
            entry,
            synthetic_result,
            now=now,
            retry_seconds=retry_seconds,
        )


def wakeup_changed_review_queue_entries(
    store: Any,
    entries: list[ReviewQueueEntry],
    *,
    now: datetime,
    pr_port: Any,
    dry_run: bool = False,
) -> tuple[str, ...]:
    """Promote parked review entries whose lightweight PR state has changed."""
    candidates = [
        entry
        for entry in review_queue_state_probe_candidates(entries)
        if entry.last_state_digest and entry.next_attempt_datetime() > now
    ]
    if not candidates:
        return ()
    changed, _unchanged = partition_review_queue_entries_by_probe_change(
        candidates,
        pr_port=pr_port,
    )
    if not changed:
        return ()
    if not dry_run:
        next_attempt_at = now.isoformat()
        for entry in changed:
            store.reschedule_review_queue_item(
                entry.issue_ref,
                next_attempt_at=next_attempt_at,
                now=now,
            )
    return tuple(entry.issue_ref for entry in changed)


def build_review_snapshots_for_queue_entries(
    *,
    queue_entries: list[ReviewQueueEntry],
    review_refs: set[str],
    pr_port: Any,
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
    store: Any,
    entries: list[ReviewQueueEntry],
    snapshots: dict[tuple[str, int], ReviewSnapshot],
    *,
    pr_port: Any,
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
    store: Any,
    due_items: list[ReviewQueueEntry],
    *,
    post_pr_codex_verdict: Callable[..., bool],
    pr_port: Any | None = None,
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


def update_board_snapshot_statuses(
    board_snapshot: CycleBoardSnapshot,
    critical_path_config: Any,
    status_updates: dict[str, str],
) -> CycleBoardSnapshot:
    """Return a new snapshot with the requested issue status overrides."""
    if not status_updates:
        return board_snapshot
    items: list[ProjectItemSnapshot] = []
    for snapshot in board_snapshot.items:
        issue_ref = snapshot_to_issue_ref(
            snapshot.issue_ref, critical_path_config.issue_prefixes
        )
        if issue_ref is None or issue_ref not in status_updates:
            items.append(snapshot)
            continue
        items.append(replace(snapshot, status=status_updates[issue_ref]))
    by_status: dict[str, list[ProjectItemSnapshot]] = {}
    for snapshot in items:
        by_status.setdefault(snapshot.status, []).append(snapshot)
    return CycleBoardSnapshot(
        items=tuple(items),
        by_status={status: tuple(group) for status, group in by_status.items()},
    )


def prepare_review_queue_batch(
    *,
    config: Any,
    store: Any,
    critical_path_config: Any,
    board_snapshot: CycleBoardSnapshot,
    pr_port: Any,
    now: datetime,
    dry_run: bool,
    prepared_batch_factory: Callable[..., Any],
    summary_factory: Callable[..., Any],
    log_warning: Callable[[Exception], None],
    wakeup_changed_review_queue_entries_fn: (
        Callable[..., tuple[str, ...]] | None
    ) = None,
) -> tuple[Any | None, Any | None]:
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
    store: Any,
    automation_config: Any,
    pr_port: Any,
    prepared_batch: Any,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    post_pr_codex_verdict: Callable[..., bool],
    prepared_due_processing_factory: Callable[..., Any],
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
) -> Any:
    """Prepare the changed due-review groups and snapshots for rescue processing."""
    due_items = list(prepared_batch.due_items)
    due_pr_groups = [
        (pr_key, list(entries)) for pr_key, entries in prepared_batch.due_pr_groups
    ]
    selected_snapshot_entries = list(prepared_batch.selected_snapshot_entries)

    pre_backfilled: tuple[str, ...] = ()
    if not dry_run and due_items:
        if pre_backfill_verdicts_for_due_prs_fn is not None:
            pre_backfilled = pre_backfill_verdicts_for_due_prs_fn(
                store,
                due_items,
                pr_port=pr_port,
                gh_runner=gh_runner,
            )
        else:
            pre_backfilled = pre_backfill_verdicts_for_due_prs(
                store,
                due_items,
                post_pr_codex_verdict=post_pr_codex_verdict,
                pr_port=pr_port,
                gh_runner=gh_runner,
                log_warning=log_pre_backfill_warning,
            )

    verdict_backfilled: tuple[str, ...] = ()
    partial_failure = False
    error: str | None = None
    snapshots: dict[tuple[str, int], ReviewSnapshot] = {}

    if due_pr_groups:
        try:
            changed_due_items, unchanged_due_items = (
                partition_review_queue_entries_by_probe_change_fn
                or partition_review_queue_entries_by_probe_change
            )(
                due_items,
                pr_port=pr_port,
            )
            if unchanged_due_items and not dry_run:
                (
                    repark_unchanged_review_queue_entries_fn
                    or repark_unchanged_review_queue_entries
                )(
                    store,
                    unchanged_due_items,
                    now=now,
                )
            due_pr_keys = {
                (entry.pr_repo, entry.pr_number) for entry in changed_due_items
            }
            due_pr_groups = [
                (pr_key, entries)
                for pr_key, entries in due_pr_groups
                if pr_key in due_pr_keys
            ]
            selected_snapshot_entries = [
                entry
                for entry in selected_snapshot_entries
                if (entry.pr_repo, entry.pr_number) in due_pr_keys
            ]
            due_items = changed_due_items
            if due_pr_groups:
                snapshots = (
                    build_review_snapshots_for_queue_entries_fn
                    or build_review_snapshots_for_queue_entries
                )(
                    queue_entries=selected_snapshot_entries,
                    review_refs=set(prepared_batch.review_refs),
                    pr_port=pr_port,
                    trusted_codex_actors=frozenset(
                        automation_config.trusted_codex_actors
                    ),
                )
        except GhQueryError as err:
            partial_failure = True
            error = str(err)

        if not dry_run and not partial_failure:
            if backfill_review_verdicts_from_snapshots_fn is not None:
                secondary_backfilled = backfill_review_verdicts_from_snapshots_fn(
                    store,
                    due_items,
                    snapshots,
                    pr_port=pr_port,
                    gh_runner=gh_runner,
                )
            else:
                secondary_backfilled = backfill_review_verdicts_from_snapshots(
                    store,
                    due_items,
                    snapshots,
                    pr_port=pr_port,
                    post_pr_codex_verdict=post_pr_codex_verdict,
                    gh_runner=gh_runner,
                    log_warning=log_backfill_warning,
                )
            verdict_backfilled = tuple(
                dict.fromkeys(pre_backfilled + secondary_backfilled)
            )

    return prepared_due_processing_factory(
        due_items=tuple(due_items),
        due_pr_groups=tuple(
            (pr_key, tuple(entries)) for pr_key, entries in due_pr_groups
        ),
        snapshots=snapshots,
        verdict_backfilled=verdict_backfilled,
        partial_failure=partial_failure,
        error=error,
    )


def run_review_rescue_for_group(
    *,
    config: Any,
    critical_path_config: Any,
    automation_config: Any,
    pr_port: Any,
    pr_repo: str,
    pr_number: int,
    snapshot: ReviewSnapshot,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    review_rescue_fn: Callable[..., Any],
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
            result=ReviewRescueResult(),
            partial_failure=True,
            error=str(err),
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
    critical_path_config: Any,
    store: Any,
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
    review_group_outcome_factory: Callable[..., Any],
) -> Any:
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
            entry = next((e for e in entries if e.issue_ref == issue_ref), None)
            pr_url = entry.pr_url if entry is not None else ""
            requeue_count, _ = store.get_requeue_state(issue_ref)
            if requeue_or_escalate(requeue_count) == "escalate":
                if not dry_run:
                    escalate_to_claude(
                        issue_ref,
                        critical_path_config,
                        project_owner,
                        project_number,
                        reason=f"repair requeue ceiling ({requeue_count} cycles on same PR): "
                        f"{result.blocked_reason or 'persistent check failure / conflict'}",
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
    config: Any,
    store: Any,
    critical_path_config: Any,
    automation_config: Any,
    pr_port: Any,
    pr_repo: str,
    pr_number: int,
    entries: tuple[ReviewQueueEntry, ...],
    snapshot: ReviewSnapshot | None,
    updated_snapshot: CycleBoardSnapshot,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    review_group_outcome_factory: Callable[..., Any],
    review_rescue_fn: Callable[..., Any],
    escalate_to_claude: Callable[..., None],
) -> Any:
    """Process one due PR group from the review queue."""
    if snapshot is None:
        return review_group_outcome_factory(
            rerun=(),
            auto_merge_enabled=(),
            requeued=(),
            blocked=(),
            skipped=(),
            escalated=(),
            updated_snapshot=updated_snapshot,
            partial_failure=True,
            error=f"missing-review-snapshot:{pr_repo}#{pr_number}",
        )

    rescue_result = run_review_rescue_for_group(
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
    if rescue_result.partial_failure:
        return review_group_outcome_factory(
            rerun=(),
            auto_merge_enabled=(),
            requeued=(),
            blocked=(),
            skipped=(),
            escalated=(),
            updated_snapshot=updated_snapshot,
            partial_failure=True,
            error=rescue_result.error,
        )

    escalated = apply_review_queue_group_result(
        store=store,
        critical_path_config=critical_path_config,
        project_owner=config.project_owner,
        project_number=config.project_number,
        pr_port=pr_port,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        result=rescue_result.result,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        escalate_to_claude=escalate_to_claude,
    )
    return summarize_review_group_outcome(
        critical_path_config=critical_path_config,
        store=store,
        project_owner=config.project_owner,
        project_number=config.project_number,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        result=rescue_result.result,
        updated_snapshot=updated_snapshot,
        escalated=escalated,
        dry_run=dry_run,
        gh_runner=gh_runner,
        escalate_to_claude=escalate_to_claude,
        review_group_outcome_factory=review_group_outcome_factory,
    )


def process_review_queue_due_groups(
    *,
    config: Any,
    store: Any,
    critical_path_config: Any,
    automation_config: Any,
    pr_port: Any,
    prepared_batch: Any,
    board_snapshot: CycleBoardSnapshot,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    post_pr_codex_verdict: Callable[..., bool],
    review_rescue_fn: Callable[..., Any],
    escalate_to_claude: Callable[..., None],
    prepared_due_processing_factory: Callable[..., Any],
    review_group_outcome_factory: Callable[..., Any],
    review_queue_processing_outcome_factory: Callable[..., Any],
    gh_reason_code: Callable[[str | Exception], str],
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
) -> Any:
    """Process the due PR groups for a prepared review-queue batch."""
    prepared_due_processing = prepare_due_review_processing(
        store=store,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        post_pr_codex_verdict=post_pr_codex_verdict,
        prepared_due_processing_factory=prepared_due_processing_factory,
        log_pre_backfill_warning=log_pre_backfill_warning,
        log_backfill_warning=log_backfill_warning,
        pre_backfill_verdicts_for_due_prs_fn=pre_backfill_verdicts_for_due_prs_fn,
        partition_review_queue_entries_by_probe_change_fn=partition_review_queue_entries_by_probe_change_fn,
        repark_unchanged_review_queue_entries_fn=repark_unchanged_review_queue_entries_fn,
        build_review_snapshots_for_queue_entries_fn=build_review_snapshots_for_queue_entries_fn,
        backfill_review_verdicts_from_snapshots_fn=backfill_review_verdicts_from_snapshots_fn,
    )

    rerun: list[str] = []
    auto_merge_enabled: list[str] = []
    requeued: list[str] = []
    blocked: list[str] = []
    skipped: list[str] = []
    escalated: list[str] = []
    partial_failure = prepared_due_processing.partial_failure
    error = prepared_due_processing.error
    updated_snapshot = board_snapshot

    for (pr_repo, pr_number), entries in prepared_due_processing.due_pr_groups:
        if partial_failure:
            break
        group_outcome = process_due_review_group(
            config=config,
            store=store,
            critical_path_config=critical_path_config,
            automation_config=automation_config,
            pr_port=pr_port,
            pr_repo=pr_repo,
            pr_number=pr_number,
            entries=entries,
            snapshot=prepared_due_processing.snapshots.get((pr_repo, pr_number)),
            updated_snapshot=updated_snapshot,
            now=now,
            dry_run=dry_run,
            gh_runner=gh_runner,
            review_group_outcome_factory=review_group_outcome_factory,
            review_rescue_fn=review_rescue_fn,
            escalate_to_claude=escalate_to_claude,
        )
        if group_outcome.partial_failure:
            partial_failure = True
            error = group_outcome.error
            break
        rerun.extend(group_outcome.rerun)
        auto_merge_enabled.extend(group_outcome.auto_merge_enabled)
        requeued.extend(group_outcome.requeued)
        blocked.extend(group_outcome.blocked)
        skipped.extend(group_outcome.skipped)
        escalated.extend(group_outcome.escalated)
        updated_snapshot = group_outcome.updated_snapshot

    if partial_failure and not dry_run:
        apply_review_queue_partial_failure(
            store,
            list(prepared_due_processing.due_items),
            config=config,
            error=error,
            gh_reason_code=gh_reason_code,
            now=now,
        )

    return review_queue_processing_outcome_factory(
        due_count=len(prepared_due_processing.due_items),
        verdict_backfilled=prepared_due_processing.verdict_backfilled,
        rerun=tuple(rerun),
        auto_merge_enabled=tuple(auto_merge_enabled),
        requeued=tuple(requeued),
        blocked=tuple(blocked),
        skipped=tuple(skipped),
        escalated=tuple(escalated),
        partial_failure=partial_failure,
        error=error,
        updated_snapshot=updated_snapshot,
    )


def drain_review_queue(
    config: Any,
    db: Any,
    critical_path_config: Any,
    automation_config: Any | None,
    *,
    pr_port: Any | None = None,
    session_store: Any | None = None,
    board_snapshot: CycleBoardSnapshot | None = None,
    dry_run: bool = False,
    github_memo: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
    build_github_port_bundle: Callable[..., Any],
    build_session_store: Callable[[Any], Any],
    github_memo_factory: Callable[[], Any],
    prepared_batch_factory: Callable[..., Any],
    summary_factory: Callable[..., Any],
    prepared_due_processing_factory: Callable[..., Any],
    review_group_outcome_factory: Callable[..., Any],
    review_queue_processing_outcome_factory: Callable[..., Any],
    post_pr_codex_verdict: Callable[..., bool],
    review_rescue_fn: Callable[..., Any],
    escalate_to_claude: Callable[..., None],
    gh_reason_code: Callable[[str | Exception], str],
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
    memo = github_memo or github_memo_factory()
    if automation_config is None:
        return summary_factory(), (
            board_snapshot
            if board_snapshot is not None
            else build_github_port_bundle(
                config.project_owner,
                config.project_number,
                config=critical_path_config,
                github_memo=memo,
                gh_runner=gh_runner,
            ).review_state.build_board_snapshot()
        )

    effective_board_snapshot = (
        board_snapshot
        or build_github_port_bundle(
            config.project_owner,
            config.project_number,
            config=critical_path_config,
            github_memo=memo,
            gh_runner=gh_runner,
        ).review_state.build_board_snapshot()
    )

    store = session_store or build_session_store(db)
    now = datetime.now(timezone.utc)
    effective_pr_port = (
        pr_port
        or build_github_port_bundle(
            config.project_owner,
            config.project_number,
            config=critical_path_config,
            github_memo=memo,
            gh_runner=gh_runner,
        ).pull_requests
    )

    prepared_batch, empty_summary = prepare_review_queue_batch(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        board_snapshot=effective_board_snapshot,
        pr_port=effective_pr_port,
        now=now,
        dry_run=dry_run,
        prepared_batch_factory=prepared_batch_factory,
        summary_factory=summary_factory,
        log_warning=log_probe_warning,
        wakeup_changed_review_queue_entries_fn=wakeup_changed_review_queue_entries_fn,
    )
    if empty_summary is not None:
        return empty_summary, effective_board_snapshot
    assert prepared_batch is not None

    processing_outcome = process_review_queue_due_groups(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=effective_pr_port,
        prepared_batch=prepared_batch,
        board_snapshot=effective_board_snapshot,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        post_pr_codex_verdict=post_pr_codex_verdict,
        review_rescue_fn=review_rescue_fn,
        escalate_to_claude=escalate_to_claude,
        prepared_due_processing_factory=prepared_due_processing_factory,
        review_group_outcome_factory=review_group_outcome_factory,
        review_queue_processing_outcome_factory=review_queue_processing_outcome_factory,
        gh_reason_code=gh_reason_code,
        log_pre_backfill_warning=log_pre_backfill_warning,
        log_backfill_warning=log_backfill_warning,
        pre_backfill_verdicts_for_due_prs_fn=pre_backfill_verdicts_for_due_prs_fn,
        partition_review_queue_entries_by_probe_change_fn=partition_review_queue_entries_by_probe_change_fn,
        repark_unchanged_review_queue_entries_fn=repark_unchanged_review_queue_entries_fn,
        build_review_snapshots_for_queue_entries_fn=build_review_snapshots_for_queue_entries_fn,
        backfill_review_verdicts_from_snapshots_fn=backfill_review_verdicts_from_snapshots_fn,
    )

    summary = summary_factory(
        queued_count=len(prepared_batch.queue_items),
        due_count=processing_outcome.due_count,
        seeded=prepared_batch.seeded,
        removed=prepared_batch.removed,
        verdict_backfilled=processing_outcome.verdict_backfilled,
        rerun=processing_outcome.rerun,
        auto_merge_enabled=processing_outcome.auto_merge_enabled,
        requeued=processing_outcome.requeued,
        blocked=processing_outcome.blocked,
        skipped=processing_outcome.skipped,
        escalated=processing_outcome.escalated,
        partial_failure=processing_outcome.partial_failure,
        error=processing_outcome.error,
    )
    return summary, processing_outcome.updated_snapshot
