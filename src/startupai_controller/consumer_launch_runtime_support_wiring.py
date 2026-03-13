"""Launch/runtime support wiring extracted from consumer_support_wiring."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path
import subprocess
from typing import Callable, Protocol, cast

from startupai_controller.application.consumer.daemon import (
    DaemonRuntime,
    WorkerExecutor,
    WorkerFuture,
)
import startupai_controller.consumer_runtime_support_wiring as _runtime_support_wiring
import startupai_controller.consumer_runtime_wiring as _runtime_wiring
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_context_helpers import (
    IssueContextCacheEntryPort,
    IssueContextCacheStorePort,
    fetch_issue_context as _fetch_issue_context_helper,
    hydrate_issue_context as _hydrate_issue_context_helper,
    issue_context_cache_is_fresh as _issue_context_cache_is_fresh_helper,
    snapshot_for_issue as _snapshot_for_issue_helper,
)
from startupai_controller.consumer_types import (
    ActiveWorkerTask,
    IssueContextPayload,
    PreparedCycleContext,
    PreparedLaunchContext,
)
from startupai_controller.consumer_worktree_helpers import (
    list_repo_worktrees as _list_repo_worktrees_helper,
    worktree_is_clean as _worktree_is_clean_helper,
    worktree_ownership_is_safe as _worktree_ownership_is_safe_helper,
)
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    CycleResult,
    ProjectItemSnapshot,
)
from startupai_controller.domain.review_queue_policy import (
    parse_iso8601_timestamp as _parse_iso8601_timestamp,
)
from startupai_controller.domain.scheduling_policy import (
    snapshot_to_issue_ref as _snapshot_to_issue_ref,
)
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.issue_context import IssueContextPort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.ports.worktrees import WorktreePort
from startupai_controller.runtime.wiring import (
    _run_gh,
    build_github_port_bundle,
    build_worktree_port,
    open_consumer_db,
)
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

SubprocessRunnerFn = Callable[..., subprocess.CompletedProcess[str]]
LegacyDaemonKwargs = dict[str, object]


class WorkerDbPort(ConsumerRuntimeStatePort, Protocol):
    """Runtime worker DB surface used by isolated worker execution."""

    def close(self) -> None: ...


def hydrate_issue_context(
    issue_ref: str,
    *,
    owner: str,
    repo: str,
    number: int,
    snapshot: ProjectItemSnapshot | None,
    config: ConsumerConfig,
    db: IssueContextCacheStorePort,
    issue_context_port: IssueContextPort | None = None,
    gh_runner: Callable[..., str] | None = None,
    now: datetime | None = None,
) -> IssueContextPayload:
    """Return locally ready issue context, refreshing the cache when needed."""

    def _fetch_issue_context(
        owner: str,
        repo: str,
        number: int,
        *,
        issue_context_port: IssueContextPort | None = None,
        gh_runner: Callable[..., str] | None = None,
    ) -> IssueContextPayload:
        return _fetch_issue_context_helper(
            owner,
            repo,
            number,
            build_github_port_bundle=build_github_port_bundle,
            issue_context_port=issue_context_port,
            gh_runner=gh_runner,
        )

    def _issue_context_cache_is_fresh(
        cached: IssueContextCacheEntryPort | None,
        *,
        snapshot_updated_at: str,
        now: datetime,
    ) -> bool:
        return _issue_context_cache_is_fresh_helper(
            cached,
            snapshot_updated_at=snapshot_updated_at,
            now=now,
            parse_iso8601_timestamp=_parse_iso8601_timestamp,
        )

    def _record_metric(
        db: IssueContextCacheStorePort,
        config: ConsumerConfig,
        metric_name: str,
        *,
        issue_ref: str | None = None,
        payload: dict[str, object] | None = None,
        now: datetime | None = None,
    ) -> None:
        _runtime_support_wiring.record_metric(
            db,
            config,
            metric_name,
            issue_ref=issue_ref,
            payload=payload,
            now=now,
        )

    return _hydrate_issue_context_helper(
        issue_ref,
        owner=owner,
        repo=repo,
        number=number,
        snapshot=snapshot,
        config=config,
        db=db,
        fetch_issue_context=_fetch_issue_context,
        issue_context_cache_is_fresh=_issue_context_cache_is_fresh,
        record_metric=_record_metric,
        issue_context_port=issue_context_port,
        gh_runner=gh_runner,
        now=now,
    )


def snapshot_for_issue(
    board_snapshot: CycleBoardSnapshot,
    issue_ref: str,
    config: CriticalPathConfig,
) -> ProjectItemSnapshot | None:
    """Return the thin board snapshot row for an issue ref."""
    return _snapshot_for_issue_helper(
        board_snapshot,
        issue_ref,
        config,
        snapshot_to_issue_ref=_snapshot_to_issue_ref,
    )


def list_repo_worktrees(
    repo_root: Path | str,
    *,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
) -> list[tuple[str, str]]:
    """Return (worktree_path, branch_name) pairs for a repo root."""
    return _list_repo_worktrees_helper(
        repo_root,
        build_worktree_port=build_worktree_port,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )


def worktree_is_clean(
    worktree_path: str,
    *,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: SubprocessRunnerFn | None = None,
) -> bool:
    """Return True when a worktree has no local changes."""
    return _worktree_is_clean_helper(
        worktree_path,
        build_worktree_port=build_worktree_port,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )


def worktree_ownership_is_safe(
    store: SessionStorePort,
    issue_ref: str,
    worktree_path: str,
) -> bool:
    """Return True when a clean worktree is safe to adopt for an issue."""
    return _worktree_ownership_is_safe_helper(store, issue_ref, worktree_path)


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
        issue_ref = _snapshot_to_issue_ref(
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


def run_worker_cycle(
    config: ConsumerConfig,
    *,
    target_issue: str,
    slot_id: int,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext | None = None,
    dry_run: bool = False,
    runtime: DaemonRuntime | None = None,
    di_kwargs: LegacyDaemonKwargs | None = None,
) -> CycleResult:
    """Execute one issue in an isolated worker DB connection."""
    worker_db = cast(WorkerDbPort, open_consumer_db(config.db_path))
    worker_config = replace(config)
    try:
        return _runtime_wiring.run_one_cycle_live(
            worker_config,
            worker_db,
            dry_run=dry_run,
            target_issue=target_issue,
            prepared=prepared,
            launch_context=launch_context,
            slot_id_override=slot_id,
            skip_control_plane=True,
            runtime=runtime,
            di_kwargs=di_kwargs,
        )
    finally:
        worker_db.close()


def prepare_multi_worker_launch_context(
    candidate: str,
    *,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    prepared: PreparedCycleContext,
    dry_run: bool,
    runtime: DaemonRuntime | None = None,
    di_kwargs: LegacyDaemonKwargs | None = None,
) -> tuple[PreparedLaunchContext | None, bool]:
    """Prepare launch context for one candidate."""
    return _runtime_wiring.prepare_multi_worker_launch_context(
        candidate,
        config=config,
        db=db,
        prepared=prepared,
        dry_run=dry_run,
        runtime=runtime,
        di_kwargs=di_kwargs,
    )


def submit_multi_worker_task(
    executor: WorkerExecutor,
    active_tasks: dict[WorkerFuture, ActiveWorkerTask],
    *,
    config: ConsumerConfig,
    candidate: str,
    slot_id: int,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext | None,
    dry_run: bool,
    runtime: DaemonRuntime | None = None,
    di_kwargs: LegacyDaemonKwargs | None = None,
) -> None:
    """Submit one prepared candidate to a worker slot."""
    _runtime_wiring.submit_multi_worker_task(
        executor,
        active_tasks,
        config=config,
        candidate=candidate,
        slot_id=slot_id,
        prepared=prepared,
        launch_context=launch_context,
        dry_run=dry_run,
        runtime=runtime,
        di_kwargs=di_kwargs,
    )


def dispatch_multi_worker_launches(
    executor: WorkerExecutor,
    config: ConsumerConfig,
    db: ConsumerRuntimeStatePort,
    *,
    prepared: PreparedCycleContext,
    available_slots: list[int],
    active_issue_refs: set[str],
    active_tasks: dict[WorkerFuture, ActiveWorkerTask],
    dry_run: bool,
    runtime: DaemonRuntime | None = None,
    di_kwargs: LegacyDaemonKwargs | None = None,
) -> int:
    """Launch as many ready candidates as the current hydration budget allows."""
    return _runtime_wiring.dispatch_multi_worker_launches(
        executor,
        config,
        db,
        prepared=prepared,
        available_slots=available_slots,
        active_issue_refs=active_issue_refs,
        active_tasks=active_tasks,
        dry_run=dry_run,
        runtime=runtime,
        di_kwargs=di_kwargs,
    )
