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
    hydrate_issue_context as _hydrate_issue_context_helper,
)
from startupai_controller import consumer_resolution_helpers as _resolution_helpers
from startupai_controller.control_plane_runtime import CONTROL_KEY_LAST_ADMISSION_SUMMARY
from startupai_controller.domain.models import CycleBoardSnapshot


def _shell_module():
    """Import the consumer shell lazily to avoid import cycles."""
    from startupai_controller import board_consumer

    return board_consumer


def activate_claim_suppression(
    db: Any,
    config: Any,
    *,
    scope: str,
    error: Exception,
    now: datetime | None = None,
) -> datetime:
    """Persist a rate-limit-driven claim suppression window."""
    shell = _shell_module()
    current = now or datetime.now(timezone.utc)
    reset_epoch = getattr(error, "rate_limit_reset_at", None)
    if isinstance(reset_epoch, int) and reset_epoch > 0:
        until = datetime.fromtimestamp(reset_epoch, tz=timezone.utc)
    else:
        until = current + timedelta(seconds=config.rate_limit_cooldown_seconds)
    reason = f"{shell.gh_reason_code(error)}:{error}"
    db.set_control_value(shell.CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL, until.isoformat())
    db.set_control_value(shell.CONTROL_KEY_CLAIM_SUPPRESSED_REASON, reason)
    db.set_control_value(shell.CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE, scope)
    shell._record_control_timestamp(db, shell.CONTROL_KEY_LAST_RATE_LIMIT_AT, now=current)
    shell._mark_degraded(db, f"rate_limit_suppressed:{scope}:{reason}")
    shell._record_metric(
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
    db: Any,
    *,
    now: datetime | None = None,
) -> dict[str, str] | None:
    """Return active claim suppression state, clearing expired windows."""
    shell = _shell_module()
    current = now or datetime.now(timezone.utc)
    until_raw = db.get_control_value(shell.CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL)
    if not until_raw:
        return None
    until = shell._parse_iso8601_timestamp(until_raw)
    if until is None or until <= current:
        shell._clear_claim_suppression(db)
        return None
    return {
        "until": until.isoformat(),
        "reason": db.get_control_value(shell.CONTROL_KEY_CLAIM_SUPPRESSED_REASON) or "",
        "scope": db.get_control_value(shell.CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE) or "",
    }


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


def next_available_slot(db: Any, limit: int) -> int | None:
    """Return the next available execution slot id, or None if saturated."""
    occupied = db.active_slot_ids()
    for slot_id in range(1, limit + 1):
        if slot_id not in occupied:
            return slot_id
    return None


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
    shell = _shell_module()
    retry_count = _selection_retry_wiring.next_retry_count(
        db,
        issue_ref,
        current_session_id=session_id,
        failure_reason=failure_reason,
        is_retryable_failure_reason=shell._is_retryable_failure_reason,
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
    shell = _shell_module()
    return _resolution_helpers.verify_resolution_payload(
        issue_ref,
        resolution,
        config=config,
        workflows=workflows,
        pr_port=pr_port,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        build_resolution_evaluation=shell.ResolutionEvaluation,
        normalize_resolution_payload=shell.normalize_resolution_payload,
        resolution_has_meaningful_signal=shell.resolution_has_meaningful_signal,
        resolution_allows_autoclose=shell.resolution_allows_autoclose,
        non_auto_close_resolution_kinds=shell.NON_AUTO_CLOSE_RESOLUTION_KINDS,
        repo_root_for_issue_ref_fn=shell._repo_root_for_issue_ref,
        resolution_validation_command_fn=shell._resolution_validation_command,
        resolution_evidence_payload_fn=shell._resolution_evidence_payload,
        resolution_is_strong_fn=shell._resolution_is_strong,
        resolution_blocked_reason_fn=shell._resolution_blocked_reason,
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
    shell = _shell_module()
    return _resolution_helpers.resolution_evidence_payload(
        repo_root,
        normalized,
        validation_command,
        pr_port=pr_port,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
        verify_code_refs_on_main_fn=shell._verify_code_refs_on_main,
        commit_reachable_from_origin_main_fn=shell._commit_reachable_from_origin_main,
        pr_is_merged_fn=shell._pr_is_merged,
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
) -> dict[str, Any]:
    """Return locally ready issue context, refreshing the cache when needed."""
    shell = _shell_module()
    return _hydrate_issue_context_helper(
        issue_ref,
        owner=owner,
        repo=repo,
        number=number,
        snapshot=snapshot,
        config=config,
        db=db,
        fetch_issue_context=shell._fetch_issue_context,
        issue_context_cache_is_fresh=shell._issue_context_cache_is_fresh,
        record_metric=shell._record_metric,
        issue_context_port=issue_context_port,
        gh_runner=gh_runner,
        now=now,
    )


def update_board_snapshot_statuses(
    board_snapshot: CycleBoardSnapshot,
    critical_path_config: Any,
    status_updates: dict[str, str],
) -> CycleBoardSnapshot:
    """Return a new snapshot with the requested issue status overrides."""
    shell = _shell_module()
    if not status_updates:
        return board_snapshot
    items: list[Any] = []
    for snapshot in board_snapshot.items:
        issue_ref = shell._snapshot_to_issue_ref(
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
    di_kwargs: dict[str, Any] | None = None,
) -> Any:
    """Execute one issue in an isolated worker DB connection."""
    shell = _shell_module()
    return shell._run_worker_cycle_use_case(
        config,
        target_issue=target_issue,
        slot_id=slot_id,
        prepared=prepared,
        launch_context=launch_context,
        dry_run=dry_run,
        di_kwargs=di_kwargs,
        open_consumer_db=shell.open_consumer_db,
        run_one_cycle=shell.run_one_cycle,
    )


def prepare_multi_worker_launch_context(
    candidate: str,
    *,
    config: Any,
    db: Any,
    prepared: Any,
    dry_run: bool,
    di_kwargs: dict[str, Any],
) -> tuple[Any | None, bool]:
    """Prepare launch context for one candidate."""
    shell = _shell_module()
    return _runtime_wiring.prepare_multi_worker_launch_context(
        candidate,
        config=config,
        db=db,
        prepared=prepared,
        dry_run=dry_run,
        di_kwargs=di_kwargs,
        shell=shell,
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
    di_kwargs: dict[str, Any],
) -> None:
    """Submit one prepared candidate to a worker slot."""
    shell = _shell_module()
    _runtime_wiring.submit_multi_worker_task(
        executor,
        active_tasks,
        config=config,
        candidate=candidate,
        slot_id=slot_id,
        prepared=prepared,
        launch_context=launch_context,
        dry_run=dry_run,
        di_kwargs=di_kwargs,
        shell=shell,
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
    di_kwargs: dict[str, Any],
) -> int:
    """Launch as many ready candidates as the current hydration budget allows."""
    shell = _shell_module()
    return _runtime_wiring.dispatch_multi_worker_launches(
        executor,
        config,
        db,
        prepared=prepared,
        available_slots=available_slots,
        active_issue_refs=active_issue_refs,
        active_tasks=active_tasks,
        dry_run=dry_run,
        di_kwargs=di_kwargs,
        shell=shell,
    )
