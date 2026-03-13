"""Shell-facing support wiring extracted from board_consumer."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path
import subprocess
from typing import Any, Callable, cast

import startupai_controller.consumer_runtime_support_wiring as _runtime_support_wiring
import startupai_controller.consumer_runtime_wiring as _runtime_wiring
from startupai_controller.consumer_context_helpers import (
    fetch_issue_context as _fetch_issue_context_helper,
    hydrate_issue_context as _hydrate_issue_context_helper,
    issue_context_cache_is_fresh as _issue_context_cache_is_fresh_helper,
    snapshot_for_issue as _snapshot_for_issue_helper,
)
from startupai_controller import consumer_resolution_helpers as _resolution_helpers
from startupai_controller.consumer_types import IssueContextPayload
from startupai_controller.consumer_worktree_helpers import (
    list_repo_worktrees as _list_repo_worktrees_helper,
    worktree_is_clean as _worktree_is_clean_helper,
    worktree_ownership_is_safe as _worktree_ownership_is_safe_helper,
)
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.ports.worktrees import WorktreePort
from startupai_controller.domain.repair_policy import parse_pr_url as _parse_pr_url
from startupai_controller.domain.models import CycleBoardSnapshot, ResolutionEvaluation
from startupai_controller.domain.resolution_policy import (
    NON_AUTO_CLOSE_RESOLUTION_KINDS,
    normalize_resolution_payload,
    resolution_allows_autoclose,
    resolution_has_meaningful_signal,
)
from startupai_controller.domain.review_queue_policy import (
    parse_iso8601_timestamp as _parse_iso8601_timestamp,
)
from startupai_controller.domain.scheduling_policy import (
    snapshot_to_issue_ref as _snapshot_to_issue_ref,
)
from startupai_controller.runtime.wiring import (
    _run_gh,
    build_github_port_bundle,
    build_worktree_port,
    open_consumer_db,
)
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    parse_issue_ref,
)

SubprocessRunnerFn = Callable[..., subprocess.CompletedProcess[str]]

record_metric = _runtime_support_wiring.record_metric
clear_claim_suppression = _runtime_support_wiring.clear_claim_suppression
activate_claim_suppression = _runtime_support_wiring.activate_claim_suppression
default_admission_summary = _runtime_support_wiring.default_admission_summary
claim_suppression_state = _runtime_support_wiring.claim_suppression_state
maybe_activate_claim_suppression = (
    _runtime_support_wiring.maybe_activate_claim_suppression
)
queue_status_transition = _runtime_support_wiring.queue_status_transition
queue_verdict_marker = _runtime_support_wiring.queue_verdict_marker
load_admission_summary = _runtime_support_wiring.load_admission_summary
next_available_slot = _runtime_support_wiring.next_available_slot
session_retry_state = _runtime_support_wiring.session_retry_state
complete_session = _runtime_support_wiring.complete_session


def verify_resolution_payload(
    issue_ref: str,
    resolution: dict[str, Any] | None,
    *,
    config: Any,
    workflows: dict[str, Any],
    pr_port: Any | None = None,
    subprocess_runner: Callable[..., Any] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> Any:
    """Verify a structured resolution payload against canonical main."""
    return _resolution_helpers.verify_resolution_payload(
        issue_ref,
        resolution,
        config=config,
        workflows=workflows,
        pr_port=pr_port,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        build_resolution_evaluation=ResolutionEvaluation,
        normalize_resolution_payload=cast(
            Callable[[dict[str, Any] | None], dict[str, Any] | None],
            normalize_resolution_payload,
        ),
        resolution_has_meaningful_signal=cast(
            Callable[[dict[str, Any]], bool],
            resolution_has_meaningful_signal,
        ),
        resolution_allows_autoclose=cast(
            Callable[[dict[str, Any]], bool],
            resolution_allows_autoclose,
        ),
        non_auto_close_resolution_kinds=NON_AUTO_CLOSE_RESOLUTION_KINDS,
        repo_root_for_issue_ref_fn=lambda cfg, ref: _resolution_helpers.repo_root_for_issue_ref(
            cfg,
            ref,
            parse_issue_ref=parse_issue_ref,
            config_error_type=ConfigError,
        ),
        resolution_validation_command_fn=lambda issue_ref, normalized, **kwargs: _resolution_helpers.resolution_validation_command(
            issue_ref,
            normalized,
            parse_issue_ref=parse_issue_ref,
            **kwargs,
        ),
        resolution_evidence_payload_fn=lambda repo_root, normalized, validation_command, **kwargs: _resolution_helpers.resolution_evidence_payload(
            repo_root,
            normalized,
            validation_command,
            verify_code_refs_on_main_fn=_resolution_helpers.verify_code_refs_on_main,
            commit_reachable_from_origin_main_fn=_resolution_helpers.commit_reachable_from_origin_main,
            pr_is_merged_fn=lambda pr_url, **pr_kwargs: _resolution_helpers.pr_is_merged(
                pr_url,
                parse_pr_url=_parse_pr_url,
                run_gh=_run_gh,
                **pr_kwargs,
            ),
            **kwargs,
        ),
        resolution_is_strong_fn=lambda normalized, evidence: _resolution_helpers.resolution_is_strong(
            normalized,
            evidence,
            resolution_allows_autoclose=cast(
                Callable[[dict[str, Any]], bool],
                resolution_allows_autoclose,
            ),
        ),
        resolution_blocked_reason_fn=lambda normalized, evidence: _resolution_helpers.resolution_blocked_reason(
            normalized,
            evidence,
            resolution_allows_autoclose=cast(
                Callable[[dict[str, Any]], bool],
                resolution_allows_autoclose,
            ),
        ),
    )


def resolution_evidence_payload(
    repo_root: Any,
    normalized: dict[str, Any],
    validation_command: str,
    *,
    pr_port: Any | None = None,
    subprocess_runner: Callable[..., Any] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> dict[str, Any]:
    """Collect deterministic evidence for resolution verification."""
    return _resolution_helpers.resolution_evidence_payload(
        repo_root,
        normalized,
        validation_command,
        pr_port=pr_port,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        verify_code_refs_on_main_fn=_resolution_helpers.verify_code_refs_on_main,
        commit_reachable_from_origin_main_fn=_resolution_helpers.commit_reachable_from_origin_main,
        pr_is_merged_fn=_resolution_helpers.pr_is_merged,
    )


def hydrate_issue_context(
    issue_ref: str,
    *,
    owner: str,
    repo: str,
    number: int,
    snapshot: Any | None,
    config: Any,
    db: Any,
    issue_context_port: Any | None = None,
    gh_runner: Callable[..., str] | None = None,
    now: datetime | None = None,
) -> IssueContextPayload:
    """Return locally ready issue context, refreshing the cache when needed."""
    return _hydrate_issue_context_helper(
        issue_ref,
        owner=owner,
        repo=repo,
        number=number,
        snapshot=snapshot,
        config=config,
        db=db,
        fetch_issue_context=lambda owner, repo, number, **kwargs: _fetch_issue_context_helper(
            owner,
            repo,
            number,
            build_github_port_bundle=build_github_port_bundle,
            issue_context_port=kwargs.get("issue_context_port"),
            gh_runner=kwargs.get("gh_runner"),
        ),
        issue_context_cache_is_fresh=lambda cached, **kwargs: _issue_context_cache_is_fresh_helper(
            cached,
            snapshot_updated_at=kwargs["snapshot_updated_at"],
            now=kwargs["now"],
            parse_iso8601_timestamp=_parse_iso8601_timestamp,
        ),
        record_metric=record_metric,
        issue_context_port=issue_context_port,
        gh_runner=gh_runner,
        now=now,
    )


def snapshot_for_issue(
    board_snapshot: CycleBoardSnapshot,
    issue_ref: str,
    config: Any,
) -> Any | None:
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
    critical_path_config: Any,
    status_updates: dict[str, str],
) -> CycleBoardSnapshot:
    """Return a new snapshot with the requested issue status overrides."""
    if not status_updates:
        return board_snapshot
    items: list[Any] = []
    for snapshot in board_snapshot.items:
        issue_ref = _snapshot_to_issue_ref(
            snapshot.issue_ref, critical_path_config.issue_prefixes
        )
        if issue_ref is None or issue_ref not in status_updates:
            items.append(snapshot)
            continue
        items.append(replace(snapshot, status=status_updates[issue_ref]))
    by_status: dict[str, list[Any]] = {}
    for snapshot in items:
        by_status.setdefault(snapshot.status, []).append(snapshot)
    return CycleBoardSnapshot(
        items=tuple(items),
        by_status={status: tuple(group) for status, group in by_status.items()},
    )


def run_worker_cycle(
    config: Any,
    *,
    target_issue: str,
    slot_id: int,
    prepared: Any,
    launch_context: Any | None = None,
    dry_run: bool = False,
    runtime: Any | None = None,
    di_kwargs: dict[str, Any] | None = None,
) -> Any:
    """Execute one issue in an isolated worker DB connection."""
    worker_db = open_consumer_db(config.db_path)
    worker_config = replace(config)
    try:
        return _runtime_wiring.run_one_cycle_live(
            worker_config,
            cast(ConsumerRuntimeStatePort, worker_db),
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
    config: Any,
    db: Any,
    prepared: Any,
    dry_run: bool,
    runtime: Any | None = None,
    di_kwargs: dict[str, Any] | None = None,
) -> tuple[Any | None, bool]:
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
    executor: Any,
    active_tasks: dict[Any, Any],
    *,
    config: Any,
    candidate: str,
    slot_id: int,
    prepared: Any,
    launch_context: Any | None,
    dry_run: bool,
    runtime: Any | None = None,
    di_kwargs: dict[str, Any] | None = None,
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
    executor: Any,
    config: Any,
    db: Any,
    *,
    prepared: Any,
    available_slots: list[int],
    active_issue_refs: set[str],
    active_tasks: dict[Any, Any],
    dry_run: bool,
    runtime: Any | None = None,
    di_kwargs: dict[str, Any] | None = None,
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
