"""Shell-facing support wiring extracted from board_consumer."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import startupai_controller.consumer_runtime_wiring as _runtime_wiring
import startupai_controller.consumer_selection_retry_wiring as _selection_retry_wiring
from startupai_controller.board_graph import admission_watermarks
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
from startupai_controller.control_plane_runtime import (
    CONTROL_KEY_CLAIM_SUPPRESSED_REASON,
    CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE,
    CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL,
    CONTROL_KEY_LAST_ADMISSION_SUMMARY,
    CONTROL_KEY_LAST_RATE_LIMIT_AT,
    _mark_degraded,
    _record_control_timestamp,
)
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.domain.repair_policy import parse_pr_url as _parse_pr_url
from startupai_controller.domain.models import CycleBoardSnapshot, ResolutionEvaluation
from startupai_controller.domain.resolution_policy import (
    NON_AUTO_CLOSE_RESOLUTION_KINDS,
    normalize_resolution_payload,
    resolution_allows_autoclose,
    resolution_has_meaningful_signal,
)
from startupai_controller.domain.review_queue_policy import (
    is_retryable_failure_reason as _is_retryable_failure_reason,
    parse_iso8601_timestamp as _parse_iso8601_timestamp,
    retry_delay_seconds as _retry_delay_seconds,
    session_retry_due_at as _session_retry_due_at,
)
from startupai_controller.domain.scheduling_policy import (
    snapshot_to_issue_ref as _snapshot_to_issue_ref,
)
from startupai_controller.runtime.wiring import (
    _run_gh,
    build_github_port_bundle,
    build_worktree_port,
    gh_reason_code,
    open_consumer_db,
)
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    parse_issue_ref,
)


def record_metric(
    db: Any,
    config: Any,
    event_type: str,
    *,
    issue_ref: str | None = None,
    payload: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> None:
    """Persist a metric event when SLO metrics are enabled."""
    if not config.slo_metrics_enabled:
        return
    db.record_metric_event(
        event_type,
        issue_ref=issue_ref,
        payload=payload,
        now=now,
    )


def clear_claim_suppression(db: Any) -> None:
    """Clear active claim suppression state."""
    db.set_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL, None)
    db.set_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_REASON, None)
    db.set_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE, None)


def activate_claim_suppression(
    db: Any,
    config: Any,
    *,
    scope: str,
    error: Exception,
    now: datetime | None = None,
) -> datetime:
    """Persist a rate-limit-driven claim suppression window."""
    current = now or datetime.now(timezone.utc)
    reset_epoch = getattr(error, "rate_limit_reset_at", None)
    if isinstance(reset_epoch, int) and reset_epoch > 0:
        until = datetime.fromtimestamp(reset_epoch, tz=timezone.utc)
    else:
        until = current + timedelta(seconds=config.rate_limit_cooldown_seconds)
    reason = f"{gh_reason_code(error)}:{error}"
    db.set_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL, until.isoformat())
    db.set_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_REASON, reason)
    db.set_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE, scope)
    _record_control_timestamp(db, CONTROL_KEY_LAST_RATE_LIMIT_AT, now=current)
    _mark_degraded(db, f"rate_limit_suppressed:{scope}:{reason}")
    record_metric(
        db,
        config,
        "claim_suppressed",
        payload={"scope": scope, "reason": reason, "until": until.isoformat()},
        now=current,
    )
    return until


def default_admission_summary(
    automation_config: Any | None,
) -> dict[str, Any]:
    """Return a stable empty admission summary when none is persisted yet."""
    if automation_config is None:
        floor = 0
        cap = 0
        enabled = False
    else:
        floor, cap = admission_watermarks(
            automation_config.global_concurrency,
            floor_multiplier=automation_config.admission.ready_floor_multiplier,
            cap_multiplier=automation_config.admission.ready_cap_multiplier,
        )
        enabled = automation_config.admission.enabled
    return {
        "enabled": enabled,
        "ready_count": 0,
        "ready_floor": floor,
        "ready_cap": cap,
        "needed": 0,
        "scanned_backlog": 0,
        "eligible_count": 0,
        "admitted": [],
        "resolved": [],
        "blocked": [],
        "skip_reason_counts": {},
        "top_candidates": [],
        "top_skipped": [],
        "partial_failure": False,
        "error": None,
        "controller_owned_admission_rejections": 0,
    }


def claim_suppression_state(
    db: ConsumerRuntimeStatePort,
    *,
    now: datetime | None = None,
) -> dict[str, str] | None:
    """Return active claim suppression state, clearing expired windows."""
    current = now or datetime.now(timezone.utc)
    until_raw = db.get_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL)
    if not until_raw:
        return None
    until = _parse_iso8601_timestamp(until_raw)
    if until is None or until <= current:
        clear_claim_suppression(db)
        return None
    return {
        "until": until.isoformat(),
        "reason": db.get_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_REASON) or "",
        "scope": db.get_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE) or "",
    }


def maybe_activate_claim_suppression(
    db: Any,
    config: Any,
    *,
    scope: str,
    error: Exception,
    now: datetime | None = None,
) -> bool:
    """Activate suppression for GitHub rate-limit errors when configured."""
    if not config.rate_limit_pause_enabled:
        return False
    if gh_reason_code(error) != "rate_limit":
        return False
    activate_claim_suppression(
        db,
        config,
        scope=scope,
        error=error,
        now=now,
    )
    return True


def queue_status_transition(
    db: Any,
    issue_ref: str,
    *,
    to_status: str,
    from_statuses: set[str],
    blocked_reason: str | None = None,
) -> None:
    """Queue a board status mutation for replay after GitHub recovery."""
    payload: dict[str, Any] = {
        "issue_ref": issue_ref,
        "to_status": to_status,
        "from_statuses": sorted(from_statuses),
    }
    if blocked_reason is not None:
        payload["blocked_reason"] = blocked_reason
    db.queue_deferred_action(issue_ref, "set_status", payload)


def queue_verdict_marker(
    db: Any,
    pr_url: str,
    session_id: str,
) -> None:
    """Queue a missing PR verdict marker for replay."""
    db.queue_deferred_action(
        pr_url,
        "post_verdict_marker",
        {"pr_url": pr_url, "session_id": session_id},
    )


def load_admission_summary(
    control_state: dict[str, str],
    automation_config: Any | None,
) -> dict[str, Any]:
    """Return the last persisted admission summary or a stable default."""
    raw = control_state.get(CONTROL_KEY_LAST_ADMISSION_SUMMARY)
    if not raw:
        return default_admission_summary(automation_config)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = default_admission_summary(automation_config)
        payload["error"] = "invalid-persisted-admission-summary"
        return payload
    if not isinstance(payload, dict):
        payload = default_admission_summary(automation_config)
        payload["error"] = "invalid-persisted-admission-summary"
    return payload


def next_available_slot(db: ConsumerRuntimeStatePort, limit: int) -> int | None:
    """Return the next available execution slot id, or None if saturated."""
    occupied = db.active_slot_ids()
    for slot_id in range(1, limit + 1):
        if slot_id not in occupied:
            return slot_id
    return None


def session_retry_state(
    session: Any,
    *,
    config: Any,
    workflows: dict[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return retry metadata for a session."""
    return _selection_retry_wiring.session_retry_state(
        session,
        config=config,
        workflows=workflows,
        parse_issue_ref=parse_issue_ref,
        effective_retry_backoff=_selection_retry_wiring.effective_retry_backoff,
        session_retry_due_at=_session_retry_due_at,
        retry_delay_seconds=_retry_delay_seconds,
        now=now,
    )


def complete_session(
    db: Any,
    session_id: str,
    issue_ref: str,
    *,
    status: str,
    failure_reason: str | None = None,
    completed_at: str | None = None,
    **fields: Any,
) -> int:
    """Persist a terminal session update and return its retry count."""
    retry_count = _selection_retry_wiring.next_retry_count(
        db,
        issue_ref,
        current_session_id=session_id,
        failure_reason=failure_reason,
        is_retryable_failure_reason=_is_retryable_failure_reason,
    )
    db.update_session(
        session_id,
        status=status,
        completed_at=completed_at or datetime.now(timezone.utc).isoformat(),
        failure_reason=failure_reason,
        retry_count=retry_count,
        **fields,
    )
    return retry_count


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
        normalize_resolution_payload=normalize_resolution_payload,
        resolution_has_meaningful_signal=resolution_has_meaningful_signal,
        resolution_allows_autoclose=resolution_allows_autoclose,
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
            resolution_allows_autoclose=resolution_allows_autoclose,
        ),
        resolution_blocked_reason_fn=lambda normalized, evidence: _resolution_helpers.resolution_blocked_reason(
            normalized,
            evidence,
            resolution_allows_autoclose=resolution_allows_autoclose,
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
    repo_root: Any,
    *,
    worktree_port: Any | None = None,
    subprocess_runner: Callable[..., Any] | None = None,
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
    worktree_port: Any | None = None,
    subprocess_runner: Callable[..., Any] | None = None,
) -> bool:
    """Return True when a worktree has no local changes."""
    return _worktree_is_clean_helper(
        worktree_path,
        build_worktree_port=build_worktree_port,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )


def worktree_ownership_is_safe(
    store: Any,
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
