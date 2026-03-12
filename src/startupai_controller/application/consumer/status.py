"""Status payload assembly for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from startupai_controller.ports.review_state import ReviewStatePort


@dataclass(frozen=True)
class CollectStatusPayloadDeps:
    """Injected seams for consumer status collection."""

    config_error_type: type[Exception]
    load_automation_config: Callable[[Any], Any]
    apply_automation_runtime: Callable[[Any, Any | None], None]
    current_main_workflows: Callable[..., tuple[dict[str, Any], dict[str, Any], int]]
    read_workflow_snapshot: Callable[[Any], Any | None]
    parse_issue_ref: Callable[[str], Any]
    load_admission_summary: Callable[[dict[str, str], Any | None], dict[str, Any]]
    control_plane_health_summary: Callable[..., Any]
    drain_requested: Callable[[Any], bool]
    workflow_status_payload: Callable[[Any], dict[str, Any]]
    session_retry_state: Callable[..., dict[str, Any]]
    parse_iso8601_timestamp: Callable[[str], datetime | None]
    control_keys: dict[str, str]


def _local_review_summary(db: Any) -> dict[str, Any]:
    """Build review summary from local consumer state only."""
    review_refs = db.latest_review_issue_refs()
    return {
        "count": len(review_refs),
        "refs": review_refs,
        "source": "local",
    }


def _github_review_summary(
    config: Any,
    db: Any,
    *,
    deps: CollectStatusPayloadDeps,
    review_state_port: ReviewStatePort,
) -> dict[str, Any]:
    """Build review summary from GitHub with local fallback."""
    try:
        review_refs: list[str] = []
        for snapshot in review_state_port.list_issues_by_status("Review"):
            issue_ref = snapshot.issue_ref
            if deps.parse_issue_ref(issue_ref).prefix not in config.repo_prefixes:
                continue
            if snapshot.executor.strip().lower() != config.executor:
                continue
            review_refs.append(issue_ref)
        return {
            "count": len(review_refs),
            "refs": review_refs,
            "source": "github",
        }
    except Exception as error:
        fallback = _local_review_summary(db)
        fallback["source"] = "local-fallback"
        fallback["error"] = str(error)
        return fallback


def _local_review_queue_summary(
    db: Any,
    *,
    now: datetime,
) -> dict[str, Any]:
    """Return bounded local review-queue diagnostics."""
    entries = db.list_review_queue_items()
    due_count = db.due_review_queue_count(now=now)
    return {
        "count": len(entries),
        "due_count": due_count,
        "refs": [entry.issue_ref for entry in entries[:10]],
    }


def _metric_window_payload(
    db: Any,
    *,
    hours: int,
    now: datetime,
) -> dict[str, Any]:
    """Return one rolling SLO/throughput window summary."""
    since = now - timedelta(hours=hours)
    counts = db.count_metric_events_since(since)
    occupied_slot_seconds = db.occupied_slot_seconds_since(since, now=now)
    hydration_total = counts.get("context_cache_hit", 0) + counts.get(
        "context_cache_miss", 0
    )
    claim_attempted = counts.get("claim_attempted", 0)
    durable_starts = counts.get("worker_durable_start", 0)
    return {
        "hours": hours,
        "candidate_selected": counts.get("candidate_selected", 0),
        "claim_attempted": claim_attempted,
        "claim_suppressed": counts.get("claim_suppressed", 0),
        "durable_starts": durable_starts,
        "startup_failures": counts.get("worker_start_failed", 0),
        "review_transitions": counts.get("session_transition_review", 0),
        "done_transitions": counts.get("session_transition_done", 0),
        "occupied_slot_seconds": occupied_slot_seconds,
        "occupied_slots_per_hour": occupied_slot_seconds / float(hours * 3600),
        "cache_hits": counts.get("context_cache_hit", 0),
        "cache_misses": counts.get("context_cache_miss", 0),
        "cache_hit_rate": (
            counts.get("context_cache_hit", 0) / hydration_total
            if hydration_total
            else None
        ),
        "worktree_reused": counts.get("worktree_reused", 0),
        "worktree_blocked": counts.get("worktree_blocked", 0),
        "rate_limit_events": counts.get("claim_suppressed", 0),
        "durable_start_reliability": (
            durable_starts / claim_attempted if claim_attempted else None
        ),
    }


def _ready_pressure_hours(
    events: list[Any],
    *,
    minimum_ready: int,
    since: datetime,
    now: datetime,
    parse_iso8601_timestamp: Callable[[str], datetime | None],
) -> float:
    """Approximate hours where the queue had at least minimum ready items."""
    if not events:
        return 0.0
    observations: list[tuple[datetime, int]] = []
    for event in events:
        created_at = parse_iso8601_timestamp(event.created_at)
        if created_at is None:
            continue
        observations.append(
            (
                created_at,
                int(event.payload.get("ready_for_executor", 0) or 0),
            )
        )
    if not observations:
        return 0.0
    observations.sort(key=lambda item: item[0])
    total_seconds = 0.0
    for index, (started_at, ready_count) in enumerate(observations):
        window_start = max(started_at, since)
        next_started_at = (
            observations[index + 1][0] if index + 1 < len(observations) else now
        )
        window_end = min(next_started_at, now)
        if ready_count < minimum_ready or window_end <= window_start:
            continue
        total_seconds += (window_end - window_start).total_seconds()
    return total_seconds / 3600.0


def _augment_slo_window_payload(
    db: Any,
    payload: dict[str, Any],
    *,
    hours: int,
    now: datetime,
    parse_iso8601_timestamp: Callable[[str], datetime | None],
) -> dict[str, Any]:
    """Attach queue-opportunity normalized throughput metrics to a window."""
    since = now - timedelta(hours=hours)
    observations = db.metric_events_since(
        since,
        event_types=("cycle_observation",),
    )
    ready_hours_ge_1 = _ready_pressure_hours(
        observations,
        minimum_ready=1,
        since=since,
        now=now,
        parse_iso8601_timestamp=parse_iso8601_timestamp,
    )
    ready_hours_ge_4 = _ready_pressure_hours(
        observations,
        minimum_ready=4,
        since=since,
        now=now,
        parse_iso8601_timestamp=parse_iso8601_timestamp,
    )
    occupied_slots_per_ready_hour_ge_1 = (
        payload["occupied_slot_seconds"] / (ready_hours_ge_1 * 3600.0)
        if ready_hours_ge_1 > 0
        else None
    )
    occupied_slots_per_ready_hour_ge_4 = (
        payload["occupied_slot_seconds"] / (ready_hours_ge_4 * 3600.0)
        if ready_hours_ge_4 > 0
        else None
    )
    return {
        **payload,
        "ready_hours_ge_1": ready_hours_ge_1,
        "ready_hours_ge_4": ready_hours_ge_4,
        "occupied_slots_per_ready_hour_ge_1": occupied_slots_per_ready_hour_ge_1,
        "occupied_slots_per_ready_hour_ge_4": occupied_slots_per_ready_hour_ge_4,
    }


def _lane_wip_limits_payload(auto_config: Any | None) -> dict[str, dict[str, int]]:
    """Render lane WIP limits grouped by executor."""
    if auto_config is None:
        return {}
    lane_wip_limits: dict[str, dict[str, int]] = {}
    for executor in auto_config.execution_authority_executors:
        executor_limits = auto_config.wip_limits.get(executor, {})
        lane_wip_limits[executor] = {
            repo_prefix: executor_limits.get(repo_prefix, 1)
            for repo_prefix in auto_config.execution_authority_repos
        }
    return lane_wip_limits


def _worker_status_payload(worker: Any) -> dict[str, Any]:
    """Render one active worker payload."""
    return {
        "id": worker.id,
        "issue_ref": worker.issue_ref,
        "slot_id": worker.slot_id,
        "phase": worker.phase,
        "status": worker.status,
        "session_kind": worker.session_kind,
        "repair_pr_url": worker.repair_pr_url,
        "branch_reconcile_state": worker.branch_reconcile_state,
        "branch_reconcile_error": worker.branch_reconcile_error,
        "pr_url": worker.pr_url,
        "resolution_kind": worker.resolution_kind,
        "verification_class": worker.verification_class,
        "resolution_action": worker.resolution_action,
        "done_reason": worker.done_reason,
    }


def _throughput_status_payload(
    throughput_1h: dict[str, Any],
    throughput_24h: dict[str, Any],
) -> dict[str, Any]:
    """Render throughput payload for status JSON."""
    return {
        "baseline_status": "pending-soak",
        "windows": {"1h": throughput_1h, "24h": throughput_24h},
    }


def _reliability_status_payload(
    throughput_1h: dict[str, Any],
    throughput_24h: dict[str, Any],
) -> dict[str, Any]:
    """Render reliability payload for status JSON."""
    return {
        "durable_start_reliability_1h": throughput_1h["durable_start_reliability"],
        "durable_start_reliability_24h": throughput_24h["durable_start_reliability"],
        "startup_failures_1h": throughput_1h["startup_failures"],
        "startup_failures_24h": throughput_24h["startup_failures"],
    }


def _context_cache_status_payload(
    throughput_1h: dict[str, Any],
    throughput_24h: dict[str, Any],
) -> dict[str, Any]:
    """Render issue-context cache payload for status JSON."""
    return {
        "hit_rate_1h": throughput_1h["cache_hit_rate"],
        "hit_rate_24h": throughput_24h["cache_hit_rate"],
        "hits_1h": throughput_1h["cache_hits"],
        "hits_24h": throughput_24h["cache_hits"],
        "misses_1h": throughput_1h["cache_misses"],
        "misses_24h": throughput_24h["cache_misses"],
    }


def _worktree_reuse_status_payload(
    throughput_1h: dict[str, Any],
    throughput_24h: dict[str, Any],
) -> dict[str, Any]:
    """Render worktree reuse payload for status JSON."""
    return {
        "reused_1h": throughput_1h["worktree_reused"],
        "reused_24h": throughput_24h["worktree_reused"],
        "blocked_1h": throughput_1h["worktree_blocked"],
        "blocked_24h": throughput_24h["worktree_blocked"],
    }


def _recent_session_status_payload(
    session: Any,
    *,
    config: Any,
    workflows: dict[str, Any],
    now: datetime,
    session_retry_state: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    """Render one recent session payload for status JSON."""
    return {
        "id": session.id,
        "issue_ref": session.issue_ref,
        "status": session.status,
        "executor": session.executor,
        "slot_id": session.slot_id,
        "phase": session.phase,
        "session_kind": session.session_kind,
        "repair_pr_url": session.repair_pr_url,
        "branch_reconcile_state": session.branch_reconcile_state,
        "branch_reconcile_error": session.branch_reconcile_error,
        "started_at": session.started_at,
        "completed_at": session.completed_at,
        "pr_url": session.pr_url,
        "resolution_kind": session.resolution_kind,
        "verification_class": session.verification_class,
        "resolution_action": session.resolution_action,
        "done_reason": session.done_reason,
        **session_retry_state(
            session,
            config=config,
            workflows=workflows,
            now=now,
        ),
    }


def _load_status_runtime(
    config: Any,
    *,
    deps: CollectStatusPayloadDeps,
) -> tuple[Any | None, dict[str, Any], dict[str, Any], int, str | None]:
    """Load automation config and persisted workflow status for status reporting."""
    try:
        auto_config = deps.load_automation_config(config.automation_config_path)
    except deps.config_error_type:
        auto_config = None
    deps.apply_automation_runtime(config, auto_config)

    main_workflows, workflow_statuses, effective_interval = deps.current_main_workflows(
        config,
        persist_snapshot=False,
    )
    persisted_snapshot = deps.read_workflow_snapshot(config.workflow_state_path)
    last_reload_at = (
        persisted_snapshot.generated_at if persisted_snapshot is not None else None
    )
    return (
        auto_config,
        main_workflows,
        workflow_statuses,
        effective_interval,
        last_reload_at,
    )


def _collect_status_runtime_state(
    config: Any,
    *,
    auto_config: Any | None,
    local_only: bool,
    status_now: datetime,
    db: Any,
    review_state_port: ReviewStatePort | None,
    deps: CollectStatusPayloadDeps,
) -> dict[str, Any]:
    """Collect DB-backed runtime state for status reporting."""
    leases = db.active_lease_count()
    slots = sorted(db.active_slot_ids())
    workers = db.active_workers()
    sessions = db.recent_sessions(limit=10)
    control_state = db.control_state_snapshot()
    deferred_action_count = db.deferred_action_count()
    oldest_deferred_action_age_seconds = db.oldest_deferred_action_age_seconds()
    review_summary = (
        _local_review_summary(db)
        if local_only or review_state_port is None
        else _github_review_summary(
            config, db, deps=deps, review_state_port=review_state_port
        )
    )
    review_queue = _local_review_queue_summary(db, now=status_now)
    admission_summary = deps.load_admission_summary(control_state, auto_config)
    throughput_1h = _augment_slo_window_payload(
        db,
        _metric_window_payload(db, hours=1, now=status_now),
        hours=1,
        now=status_now,
        parse_iso8601_timestamp=deps.parse_iso8601_timestamp,
    )
    throughput_24h = _augment_slo_window_payload(
        db,
        _metric_window_payload(db, hours=24, now=status_now),
        hours=24,
        now=status_now,
        parse_iso8601_timestamp=deps.parse_iso8601_timestamp,
    )

    return {
        "leases": leases,
        "slots": slots,
        "workers": workers,
        "sessions": sessions,
        "control_state": control_state,
        "deferred_action_count": deferred_action_count,
        "oldest_deferred_action_age_seconds": oldest_deferred_action_age_seconds,
        "review_summary": review_summary,
        "review_queue": review_queue,
        "admission_summary": admission_summary,
        "throughput_1h": throughput_1h,
        "throughput_24h": throughput_24h,
    }


def _build_status_payload(
    config: Any,
    *,
    auto_config: Any | None,
    workflow_statuses: dict[str, Any],
    main_workflows: dict[str, Any],
    effective_interval: int,
    last_reload_at: str | None,
    status_now: datetime,
    leases: int,
    slots: list[int],
    workers: list[Any],
    sessions: list[Any],
    control_state: dict[str, str],
    deferred_action_count: int,
    oldest_deferred_action_age_seconds: float | None,
    review_summary: dict[str, Any],
    review_queue: dict[str, Any],
    admission_summary: dict[str, Any],
    throughput_1h: dict[str, Any],
    throughput_24h: dict[str, Any],
    local_only: bool,
    deps: CollectStatusPayloadDeps,
) -> dict[str, Any]:
    """Assemble the final JSON-serializable status payload."""
    degraded_key = deps.control_keys["degraded"]
    degraded_reason_key = deps.control_keys["degraded_reason"]
    suppressed_until_key = deps.control_keys["claim_suppressed_until"]
    suppressed_reason_key = deps.control_keys["claim_suppressed_reason"]
    suppressed_scope_key = deps.control_keys["claim_suppressed_scope"]
    last_rate_limit_key = deps.control_keys["last_rate_limit_at"]
    last_board_sync_key = deps.control_keys["last_successful_board_sync_at"]
    last_mutation_key = deps.control_keys["last_successful_github_mutation_at"]

    degraded = control_state.get(degraded_key) == "true"
    claim_suppressed_until = control_state.get(suppressed_until_key)
    claim_suppressed_reason = control_state.get(suppressed_reason_key)
    claim_suppressed_scope = control_state.get(suppressed_scope_key)
    last_rate_limit_at = control_state.get(last_rate_limit_key)
    control_plane_health = deps.control_plane_health_summary(
        control_state,
        deferred_action_count=deferred_action_count,
        oldest_deferred_action_age_seconds=oldest_deferred_action_age_seconds,
        poll_interval_seconds=effective_interval,
    )
    lane_wip_limits = _lane_wip_limits_payload(auto_config)

    return {
        "active_leases": leases,
        "active_slots": slots,
        "workers": [_worker_status_payload(worker) for worker in workers],
        "repo_prefixes": list(config.repo_prefixes),
        "global_concurrency": config.global_concurrency,
        "lane_wip_limits": lane_wip_limits,
        "poll_interval_seconds": effective_interval,
        "drain_requested": deps.drain_requested(config.drain_path),
        "drain_path": str(config.drain_path),
        "workflow_state_path": str(config.workflow_state_path),
        "workflow_last_reload_at": last_reload_at,
        "degraded": degraded,
        "degraded_reason": control_state.get(degraded_reason_key),
        "claim_suppressed_until": claim_suppressed_until,
        "claim_suppressed_reason": claim_suppressed_reason,
        "claim_suppressed_scope": claim_suppressed_scope,
        "last_rate_limit_at": last_rate_limit_at,
        "last_successful_board_sync_at": control_state.get(last_board_sync_key),
        "last_successful_github_mutation_at": control_state.get(last_mutation_key),
        "deferred_action_count": deferred_action_count,
        "oldest_deferred_action_age_seconds": oldest_deferred_action_age_seconds,
        "control_plane_health": control_plane_health,
        "throughput_metrics": _throughput_status_payload(throughput_1h, throughput_24h),
        "reliability_metrics": _reliability_status_payload(
            throughput_1h, throughput_24h
        ),
        "context_cache_metrics": _context_cache_status_payload(
            throughput_1h, throughput_24h
        ),
        "worktree_reuse_metrics": _worktree_reuse_status_payload(
            throughput_1h, throughput_24h
        ),
        "review_summary": review_summary,
        "review_queue": review_queue,
        "admission": admission_summary,
        "repo_workflows": {
            repo_prefix: deps.workflow_status_payload(status)
            for repo_prefix, status in workflow_statuses.items()
        },
        "recent_sessions": [
            _recent_session_status_payload(
                session,
                config=config,
                workflows=main_workflows,
                now=status_now,
                session_retry_state=deps.session_retry_state,
            )
            for session in sessions
        ],
        "local_only": local_only,
    }


def collect_status_payload(
    config: Any,
    *,
    local_only: bool = False,
    db: Any,
    review_state_port: ReviewStatePort | None,
    deps: CollectStatusPayloadDeps,
) -> dict[str, Any]:
    """Collect consumer status as a JSON-serializable payload."""
    (
        auto_config,
        main_workflows,
        workflow_statuses,
        effective_interval,
        last_reload_at,
    ) = _load_status_runtime(config, deps=deps)
    status_now = datetime.now(timezone.utc)
    status_state = _collect_status_runtime_state(
        config,
        auto_config=auto_config,
        local_only=local_only,
        status_now=status_now,
        db=db,
        review_state_port=review_state_port,
        deps=deps,
    )
    return _build_status_payload(
        config,
        auto_config=auto_config,
        workflow_statuses=workflow_statuses,
        main_workflows=main_workflows,
        effective_interval=effective_interval,
        last_reload_at=last_reload_at,
        status_now=status_now,
        local_only=local_only,
        deps=deps,
        **status_state,
    )
