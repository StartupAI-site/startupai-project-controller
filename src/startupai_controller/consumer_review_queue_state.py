"""Review-queue state helpers extracted from board_consumer."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Callable, Protocol, cast

from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ProjectItemSnapshot,
    ReviewQueueEntry,
    SessionInfo,
)
from startupai_controller.domain.repair_policy import parse_pr_url
from startupai_controller.domain.review_queue_policy import (
    DEFAULT_REVIEW_QUEUE_RETRY_SECONDS,
    REVIEW_QUEUE_STABLE_RESULTS,
    blocked_streak_needs_escalation,
    review_queue_retry_seconds_for_partial_failure,
    review_queue_retry_seconds_for_result,
)
from startupai_controller.domain.scheduling_policy import snapshot_to_issue_ref
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.runtime.wiring import gh_reason_code as _gh_reason_code
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    parse_issue_ref,
)


class ReviewQueueResultView(Protocol):
    """Minimal queue-result shape consumed by review queue persistence."""

    @property
    def rerun_checks(self) -> tuple[str, ...]: ...

    @property
    def auto_merge_enabled(self) -> bool: ...

    @property
    def requeued_refs(self) -> tuple[str, ...]: ...

    @property
    def blocked_reason(self) -> str | None: ...

    @property
    def queue_reason(self) -> str | None: ...

    @property
    def skipped_reason(self) -> str | None: ...

    @property
    def terminal_reason(self) -> str | None: ...

    @property
    def check_names(self) -> tuple[str, ...]: ...

    @property
    def last_result(self) -> str | None: ...


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
    store: SessionStorePort,
    entry: ReviewQueueEntry,
    result: ReviewQueueResultView,
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

    result_requeued_refs = getattr(result, "requeued_refs", ())
    result_terminal_reason = getattr(result, "terminal_reason", None)
    result_auto_merge_enabled = getattr(result, "auto_merge_enabled", False)
    result_rerun_checks = getattr(result, "rerun_checks", ())
    result_blocked_reason = getattr(result, "blocked_reason", None)
    result_queue_reason = getattr(result, "queue_reason", None)
    result_skipped_reason = getattr(result, "skipped_reason", None)
    result_check_names = getattr(result, "check_names", ())
    result_last_result = getattr(result, "last_result", None)

    if result_requeued_refs:
        return False

    if result_terminal_reason:
        store.append_review_rescue_event(
            entry.issue_ref,
            pr_repo=entry.pr_repo,
            pr_number=entry.pr_number,
            result_kind=result_last_result or "terminal",
            reason=result_terminal_reason,
            now=current,
        )
        store.delete_review_queue_item(entry.issue_ref)
        return False

    if result_auto_merge_enabled:
        store.append_review_rescue_event(
            entry.issue_ref,
            pr_repo=entry.pr_repo,
            pr_number=entry.pr_number,
            result_kind=result_last_result or "success",
            reason="auto-merge-enabled",
            now=current,
        )
        store.delete_review_queue_item(entry.issue_ref)
        return False

    if result_rerun_checks:
        store.update_review_queue_item(
            entry.issue_ref,
            next_attempt_at=next_attempt_at,
            last_result="rerun_checks",
            last_reason=",".join(result_rerun_checks),
            last_state_digest=effective_state_digest,
            last_check_names=(),
            copilot_missing_since=None,
            blocked_streak=0,
            blocked_class=None,
            now=current,
        )
        return False

    if result_blocked_reason:
        persisted_reason = result_queue_reason or result_blocked_reason
        new_class, new_streak, needs_escalation = blocked_streak_needs_escalation(
            persisted_reason,
            entry.blocked_streak,
            entry.blocked_class,
        )
        store.update_review_queue_item(
            entry.issue_ref,
            next_attempt_at=next_attempt_at,
            last_result=result_last_result or "blocked",
            last_reason=persisted_reason,
            last_state_digest=effective_state_digest,
            last_check_names=result_check_names,
            copilot_missing_since=(
                entry.copilot_missing_since or current.isoformat()
                if persisted_reason == "missing-copilot-review"
                else None
            ),
            blocked_streak=new_streak,
            blocked_class=new_class,
            now=current,
        )
        return needs_escalation

    if result_skipped_reason:
        if result_skipped_reason == "auto-merge-already-enabled":
            store.append_review_rescue_event(
                entry.issue_ref,
                pr_repo=entry.pr_repo,
                pr_number=entry.pr_number,
                result_kind=result_last_result or "terminal",
                reason="auto-merge-already-enabled-terminal",
                now=current,
            )
            store.delete_review_queue_item(entry.issue_ref)
            return False
        store.update_review_queue_item(
            entry.issue_ref,
            next_attempt_at=next_attempt_at,
            last_result=result_last_result or "skipped",
            last_reason=result_skipped_reason,
            last_state_digest=effective_state_digest,
            last_check_names=(),
            copilot_missing_since=None,
            blocked_streak=0,
            blocked_class=None,
            now=current,
        )
        return False

    store.update_review_queue_item(
        entry.issue_ref,
        next_attempt_at=next_attempt_at,
        last_result=result_last_result or "processed",
        last_reason=None,
        last_state_digest=effective_state_digest,
        last_check_names=(),
        copilot_missing_since=None,
        blocked_streak=0,
        blocked_class=None,
        now=current,
    )
    return False


def apply_review_queue_partial_failure(
    store: SessionStorePort,
    entries: list[ReviewQueueEntry],
    *,
    config: ConsumerConfig,
    error: str | None,
    gh_reason_code: Callable[[Exception], str] = _gh_reason_code,
    now: datetime | None = None,
) -> None:
    """Back off queued review entries after a partial-failure cycle."""
    if not entries:
        return
    current = now or datetime.now(timezone.utc)
    reason_code = gh_reason_code(Exception(error)) if error else None
    next_attempt_at = review_queue_next_attempt_at(
        now=current,
        delay_seconds=review_queue_retry_seconds_for_partial_failure(
            config.rate_limit_cooldown_seconds,
            reason_code,
        ),
    )
    for entry in entries:
        store.update_review_queue_item(
            entry.issue_ref,
            next_attempt_at=next_attempt_at,
            last_result="partial_failure",
            last_reason=error,
            last_state_digest=entry.last_state_digest,
            last_check_names=(),
            copilot_missing_since=None,
            blocked_streak=0,
            blocked_class=None,
            now=current,
        )


def review_scope_issue_refs(
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
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
    store: SessionStorePort,
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
    store: SessionStorePort,
    review_refs: set[str] | frozenset[str],
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


def _explicit_requeue_for_pr(
    store: SessionStorePort,
    issue_ref: str,
    pr_url: str,
) -> bool:
    """Return True when the current PR was explicitly requeued for coding."""
    count, stored_pr_url = store.get_requeue_state(issue_ref)
    return count > 0 and stored_pr_url == pr_url


def review_entry_matches_latest_session(
    entry: ReviewQueueEntry,
    latest_session: SessionInfo | None,
) -> bool:
    """Return True when a review queue row matches the latest successful PR session."""
    if (
        latest_session is None
        or latest_session.status != "success"
        or entry.source_session_id is None
    ):
        return False
    current_pr_url = latest_session.pr_url or latest_session.repair_pr_url
    if not current_pr_url:
        return False
    return (
        entry.source_session_id == latest_session.id and entry.pr_url == current_pr_url
    )


def local_review_owned_issue_refs(
    store: SessionStorePort,
    existing_entries: list[ReviewQueueEntry],
    *,
    board_status_by_issue: dict[str, str],
) -> tuple[str, ...]:
    """Return locally owned review refs that should survive snapshot churn."""
    refs: list[str] = []
    for entry in existing_entries:
        latest_session = store.latest_session_for_issue(entry.issue_ref)
        current_pr_url = (
            (latest_session.pr_url or latest_session.repair_pr_url)
            if latest_session is not None
            else entry.pr_url
        )
        if (
            latest_session is None
            or latest_session.status != "success"
            or not current_pr_url
        ):
            continue
        if board_status_by_issue.get(entry.issue_ref) in {"Done", "Blocked"}:
            continue
        if review_entry_matches_latest_session(entry, latest_session):
            refs.append(entry.issue_ref)
            continue
        if _explicit_requeue_for_pr(store, entry.issue_ref, current_pr_url):
            continue
        refs.append(entry.issue_ref)
    return tuple(sorted(set(refs)))


def seed_new_review_entries(
    store: SessionStorePort,
    review_refs: set[str] | frozenset[str],
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
    store: SessionStorePort,
    review_refs: set[str] | frozenset[str],
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
    pr_port: PullRequestPort,
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
    store: SessionStorePort,
    entries: list[ReviewQueueEntry],
    *,
    now: datetime,
) -> None:
    """Re-schedule unchanged review entries without rehydrating the full PR state."""
    for entry in entries:
        synthetic_result = cast(
            ReviewQueueResultView,
            SimpleNamespace(
                rerun_checks=(),
                auto_merge_enabled=entry.last_result == "auto_merge_enabled",
                requeued_refs=(),
                blocked_reason=(
                    entry.last_reason if entry.last_result == "blocked" else None
                ),
                queue_reason=(
                    entry.last_reason if entry.last_result == "blocked" else None
                ),
                skipped_reason=(
                    entry.last_reason if entry.last_result == "skipped" else None
                ),
                terminal_reason=None,
                check_names=entry.last_check_names,
                last_result=entry.last_result,
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
    store: SessionStorePort,
    entries: list[ReviewQueueEntry],
    *,
    now: datetime,
    pr_port: PullRequestPort,
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


def update_board_snapshot_statuses(
    board_snapshot: CycleBoardSnapshot,
    critical_path_config: CriticalPathConfig,
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
