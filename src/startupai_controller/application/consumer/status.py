"""Status payload assembly for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Protocol, TypedDict

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_workflow import (
    WorkflowDefinition,
    WorkflowRepoStatus,
    WorkflowStateSnapshot,
)
from startupai_controller.domain.models import SessionInfo
from startupai_controller.payload_types import (
    AdmissionSummaryPayload,
    ControlPlaneHealthPayload,
    ObjectPayload,
    SessionRetryStatePayload,
    StatusPayload,
    WorkflowStatusPayload,
)
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.status_store import MetricEventView, StatusStorePort
from startupai_controller.validate_critical_path_promotion import IssueRef

ReviewSummaryPayload = ObjectPayload
ReviewQueueSummaryPayload = ObjectPayload
RecentSessionStatusPayload = ObjectPayload
WorkerStatusPayload = ObjectPayload
DrainBlockerPayload = ObjectPayload
EXECUTION_PROGRESS_STUCK_SECONDS = 120


class MetricWindowPayload(TypedDict):
    """Rolling metric window emitted in the status payload."""

    hours: int
    candidate_selected: int
    claim_attempted: int
    claim_suppressed: int
    durable_starts: int
    startup_failures: int
    review_transitions: int
    done_transitions: int
    occupied_slot_seconds: float
    occupied_slots_per_hour: float
    cache_hits: int
    cache_misses: int
    cache_hit_rate: float | None
    worktree_reused: int
    worktree_blocked: int
    rate_limit_events: int
    durable_start_reliability: float | None
    ready_hours_ge_1: float
    ready_hours_ge_4: float
    occupied_slots_per_ready_hour_ge_1: float | None
    occupied_slots_per_ready_hour_ge_4: float | None


class TransportWindowPayload(TypedDict):
    """Rolling transport-observation summary emitted in status/report outputs."""

    hours: int
    observations: int
    graphql_requests: int
    rest_requests: int
    total_requests: int
    retry_attempts: int
    cli_fallbacks: int
    latency_le_250_ms: int
    latency_le_1000_ms: int
    latency_gt_1000_ms: int
    error_counts: ObjectPayload


class CurrentMainWorkflowsFn(Protocol):
    """Load the canonical workflow definitions/statuses for status reporting."""

    def __call__(
        self,
        config: ConsumerConfig,
        *,
        persist_snapshot: bool = True,
    ) -> tuple[dict[str, WorkflowDefinition], dict[str, WorkflowRepoStatus], int]:
        """Return main-checkout workflows, statuses, and effective interval."""
        ...


class ControlPlaneHealthSummaryFn(Protocol):
    """Summarize current control-plane health for status JSON."""

    def __call__(
        self,
        control_state: dict[str, str],
        *,
        deferred_action_count: int,
        oldest_deferred_action_age_seconds: float | None,
        poll_interval_seconds: int,
    ) -> ControlPlaneHealthPayload:
        """Return the machine-readable health summary."""
        ...


class SessionRetryStateFn(Protocol):
    """Render retry metadata for one recent session."""

    def __call__(
        self,
        session: SessionInfo,
        *,
        config: ConsumerConfig,
        workflows: dict[str, WorkflowDefinition],
        now: datetime | None = None,
    ) -> SessionRetryStatePayload:
        """Return retry-state fields for one session."""
        ...


class StatusControlKeys(TypedDict):
    """Control-plane state keys consumed by the status payload."""

    degraded: str
    degraded_reason: str
    claim_suppressed_until: str
    claim_suppressed_reason: str
    claim_suppressed_scope: str
    last_rate_limit_at: str
    last_successful_board_sync_at: str
    last_successful_github_mutation_at: str


@dataclass(frozen=True)
class CollectedStatusRuntimeState:
    """DB-backed runtime state consumed when assembling status JSON."""

    leases: int
    slots: list[int]
    workers: list[SessionInfo]
    sessions: list[SessionInfo]
    control_state: dict[str, str]
    deferred_action_count: int
    oldest_deferred_action_age_seconds: float | None
    review_summary: ReviewSummaryPayload
    review_queue: ReviewQueueSummaryPayload
    admission_summary: AdmissionSummaryPayload
    throughput_1h: MetricWindowPayload
    throughput_24h: MetricWindowPayload
    transport_1h: TransportWindowPayload
    transport_24h: TransportWindowPayload


@dataclass(frozen=True)
class CollectStatusPayloadDeps:
    """Injected seams for consumer status collection."""

    config_error_type: type[Exception]
    load_automation_config: Callable[[Path], BoardAutomationConfig]
    apply_automation_runtime: Callable[
        [ConsumerConfig, BoardAutomationConfig | None], None
    ]
    current_main_workflows: CurrentMainWorkflowsFn
    read_workflow_snapshot: Callable[[Path], WorkflowStateSnapshot | None]
    parse_issue_ref: Callable[[str], IssueRef]
    load_admission_summary: Callable[
        [dict[str, str], BoardAutomationConfig | None], AdmissionSummaryPayload
    ]
    control_plane_health_summary: ControlPlaneHealthSummaryFn
    drain_requested: Callable[[Path], bool]
    read_drain_requested_at: Callable[[Path], str | None]
    workflow_status_payload: Callable[[WorkflowRepoStatus], WorkflowStatusPayload]
    session_retry_state: SessionRetryStateFn
    parse_iso8601_timestamp: Callable[[str], datetime | None]
    control_keys: StatusControlKeys


def _local_review_summary(db: StatusStorePort) -> ReviewSummaryPayload:
    """Build review summary from local consumer state only."""
    review_refs = db.latest_review_issue_refs()
    return {
        "count": len(review_refs),
        "refs": review_refs,
        "source": "local",
    }


def _canonical_review_issue_ref(
    config: ConsumerConfig,
    issue_ref: str,
    *,
    parse_issue_ref: Callable[[str], IssueRef],
) -> str | None:
    """Normalize GitHub-qualified review refs at the status boundary only."""
    default_repo_names = {
        "app": "app.startupai-site",
        "crew": "startupai-crew",
        "site": "startupai.site",
    }
    try:
        parse_issue_ref(issue_ref)
        return issue_ref
    except Exception:
        pass
    if "/" not in issue_ref or "#" not in issue_ref:
        return None
    repo_name, number_text = issue_ref.rsplit("#", maxsplit=1)
    if not number_text.isdigit():
        return None
    repo_prefix = next(
        (
            prefix
            for prefix, repo_root in config.repo_roots.items()
            if repo_name
            in {
                f"{config.project_owner}/{repo_root.name}",
                f"{config.project_owner}/{default_repo_names.get(prefix, repo_root.name)}",
            }
        ),
        None,
    )
    if repo_prefix is None:
        return None
    canonical = f"{repo_prefix}#{number_text}"
    try:
        parse_issue_ref(canonical)
    except Exception:
        return None
    return canonical


def _github_review_summary(
    config: ConsumerConfig,
    db: StatusStorePort,
    *,
    deps: CollectStatusPayloadDeps,
    review_state_port: ReviewStatePort,
) -> ReviewSummaryPayload:
    """Build review summary from GitHub with local fallback."""
    try:
        review_refs: list[str] = []
        for snapshot in review_state_port.list_issues_by_status("Review"):
            issue_ref = _canonical_review_issue_ref(
                config,
                snapshot.issue_ref,
                parse_issue_ref=deps.parse_issue_ref,
            )
            if issue_ref is None:
                continue
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
    db: StatusStorePort,
    *,
    now: datetime,
) -> ReviewQueueSummaryPayload:
    """Return bounded local review-queue diagnostics."""
    entries = db.list_review_queue_items()
    due_count = db.due_review_queue_count(now=now)
    return {
        "count": len(entries),
        "due_count": due_count,
        "refs": [entry.issue_ref for entry in entries[:10]],
    }


def _metric_window_payload(
    db: StatusStorePort,
    *,
    hours: int,
    now: datetime,
) -> MetricWindowPayload:
    """Return one rolling SLO/throughput window summary."""
    since = now - timedelta(hours=hours)
    counts = db.count_metric_events_since(since)
    occupied_slot_seconds = float(db.occupied_slot_seconds_since(since, now=now))
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
        "ready_hours_ge_1": 0.0,
        "ready_hours_ge_4": 0.0,
        "occupied_slots_per_ready_hour_ge_1": None,
        "occupied_slots_per_ready_hour_ge_4": None,
    }


def _ready_pressure_hours(
    events: list[MetricEventView],
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
    db: StatusStorePort,
    payload: MetricWindowPayload,
    *,
    hours: int,
    now: datetime,
    parse_iso8601_timestamp: Callable[[str], datetime | None],
) -> MetricWindowPayload:
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
    augmented: MetricWindowPayload = {
        **payload,
        "ready_hours_ge_1": ready_hours_ge_1,
        "ready_hours_ge_4": ready_hours_ge_4,
        "occupied_slots_per_ready_hour_ge_1": occupied_slots_per_ready_hour_ge_1,
        "occupied_slots_per_ready_hour_ge_4": occupied_slots_per_ready_hour_ge_4,
    }
    return augmented


def _lane_wip_limits_payload(
    auto_config: BoardAutomationConfig | None,
) -> dict[str, dict[str, int]]:
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


def _active_seconds(started_at: str | None, *, now: datetime) -> int | None:
    """Return active duration in seconds when a start timestamp is present."""
    if not started_at:
        return None
    started = datetime.fromisoformat(started_at)
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    return max(0, int((now - started).total_seconds()))


def _parse_iso8601_best_effort(value: str) -> datetime | None:
    """Parse one ISO-8601 timestamp and return None on failure."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _external_execution_started(session: SessionInfo) -> bool:
    """Return True once the claimed session crossed into Codex execution."""
    return session.phase == "executing"


def _drain_wait_class(
    session: SessionInfo,
    *,
    drain_requested: bool,
) -> str:
    """Classify why a session is still active during a drain window."""
    if session.status != "running":
        return "not_draining"
    if not drain_requested:
        return "not_draining"
    if _external_execution_started(session):
        return "finishing_inflight_execution"
    return "pre_execution_abort_pending"


def _effective_drain_observed_at(
    session: SessionInfo,
    *,
    drain_requested_at: str | None,
) -> str | None:
    """Return the session-local drain timestamp, falling back to the sentinel."""
    return session.drain_observed_at or drain_requested_at


def _shutdown_class(
    session: SessionInfo,
    *,
    now: datetime,
    drain_requested: bool,
    drain_requested_at: str | None,
    parse_iso8601_timestamp: Callable[[str], datetime | None],
) -> str:
    """Classify why an active session is still blocking shutdown."""
    if session.status != "running" or not drain_requested:
        return "not_draining"
    if not _external_execution_started(session):
        return "stuck_waiting_on_controller_cleanup"
    progress_at = None
    if session.last_execution_progress_at:
        progress_at = parse_iso8601_timestamp(session.last_execution_progress_at)
    if progress_at is None and session.started_at:
        progress_at = parse_iso8601_timestamp(session.started_at)
    if progress_at is None:
        return "finishing_inflight_execution"
    effective_drain_observed_at = _effective_drain_observed_at(
        session,
        drain_requested_at=drain_requested_at,
    )
    if effective_drain_observed_at is not None:
        drain_observed_at = parse_iso8601_timestamp(effective_drain_observed_at)
        if drain_observed_at is not None and progress_at < drain_observed_at:
            progress_at = drain_observed_at
    if (now - progress_at).total_seconds() > EXECUTION_PROGRESS_STUCK_SECONDS:
        return "stuck_waiting_on_external_execution"
    return "finishing_inflight_execution"


def _drain_abort_reason(session: SessionInfo) -> str | None:
    """Return the structured drain abort reason when this session was drained."""
    if session.failure_reason == "drain_requested_pre_execution":
        return session.failure_reason
    if session.done_reason == "drain_requested_pre_execution":
        return session.done_reason
    return None


def _worker_status_payload(
    worker: SessionInfo,
    *,
    now: datetime,
    drain_requested: bool,
    drain_requested_at: str | None = None,
    parse_iso8601_timestamp: Callable[
        [str], datetime | None
    ] = _parse_iso8601_best_effort,
) -> WorkerStatusPayload:
    """Render one active worker payload."""
    return {
        "id": worker.id,
        "issue_ref": worker.issue_ref,
        "slot_id": worker.slot_id,
        "phase": worker.phase,
        "status": worker.status,
        "started_at": worker.started_at,
        "active_seconds": _active_seconds(worker.started_at, now=now),
        "external_execution_started": _external_execution_started(worker),
        "drain_wait_class": _drain_wait_class(
            worker,
            drain_requested=drain_requested,
        ),
        "drain_observed_at": _effective_drain_observed_at(
            worker,
            drain_requested_at=drain_requested_at,
        ),
        "last_execution_progress_at": worker.last_execution_progress_at,
        "shutdown_class": _shutdown_class(
            worker,
            now=now,
            drain_requested=drain_requested,
            drain_requested_at=drain_requested_at,
            parse_iso8601_timestamp=parse_iso8601_timestamp,
        ),
        "drain_abort_reason": _drain_abort_reason(worker),
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
    throughput_1h: MetricWindowPayload,
    throughput_24h: MetricWindowPayload,
) -> ObjectPayload:
    """Render throughput payload for status JSON."""
    return {
        "baseline_status": "pending-soak",
        "windows": {"1h": throughput_1h, "24h": throughput_24h},
    }


def _reliability_status_payload(
    throughput_1h: MetricWindowPayload,
    throughput_24h: MetricWindowPayload,
) -> ObjectPayload:
    """Render reliability payload for status JSON."""
    return {
        "durable_start_reliability_1h": throughput_1h["durable_start_reliability"],
        "durable_start_reliability_24h": throughput_24h["durable_start_reliability"],
        "startup_failures_1h": throughput_1h["startup_failures"],
        "startup_failures_24h": throughput_24h["startup_failures"],
    }


def _context_cache_status_payload(
    throughput_1h: MetricWindowPayload,
    throughput_24h: MetricWindowPayload,
) -> ObjectPayload:
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
    throughput_1h: MetricWindowPayload,
    throughput_24h: MetricWindowPayload,
) -> ObjectPayload:
    """Render worktree reuse payload for status JSON."""
    return {
        "reused_1h": throughput_1h["worktree_reused"],
        "reused_24h": throughput_24h["worktree_reused"],
        "blocked_1h": throughput_1h["worktree_blocked"],
        "blocked_24h": throughput_24h["worktree_blocked"],
    }


def _transport_window_payload(
    db: StatusStorePort,
    *,
    hours: int,
    now: datetime,
) -> TransportWindowPayload:
    """Summarize persisted transport observations over a rolling window."""
    since = now - timedelta(hours=hours)
    observations = db.metric_events_since(
        since,
        event_types=("github_transport_observation",),
    )
    error_counts: dict[str, int] = {}
    graphql_requests = 0
    rest_requests = 0
    retry_attempts = 0
    cli_fallbacks = 0
    latency_le_250_ms = 0
    latency_le_1000_ms = 0
    latency_gt_1000_ms = 0
    for observation in observations:
        payload = observation.payload
        graphql_requests += int(payload.get("graphql_requests", 0) or 0)
        rest_requests += int(payload.get("rest_requests", 0) or 0)
        retry_attempts += int(payload.get("retry_attempts", 0) or 0)
        cli_fallbacks += int(payload.get("cli_fallbacks", 0) or 0)
        latency_le_250_ms += int(payload.get("latency_le_250_ms", 0) or 0)
        latency_le_1000_ms += int(payload.get("latency_le_1000_ms", 0) or 0)
        latency_gt_1000_ms += int(payload.get("latency_gt_1000_ms", 0) or 0)
        raw_error_counts = payload.get("error_counts", {})
        if isinstance(raw_error_counts, dict):
            for key, value in raw_error_counts.items():
                error_counts[str(key)] = error_counts.get(str(key), 0) + int(value or 0)
    return {
        "hours": hours,
        "observations": len(observations),
        "graphql_requests": graphql_requests,
        "rest_requests": rest_requests,
        "total_requests": graphql_requests + rest_requests,
        "retry_attempts": retry_attempts,
        "cli_fallbacks": cli_fallbacks,
        "latency_le_250_ms": latency_le_250_ms,
        "latency_le_1000_ms": latency_le_1000_ms,
        "latency_gt_1000_ms": latency_gt_1000_ms,
        "error_counts": dict(sorted(error_counts.items())),
    }


def _transport_status_payload(
    transport_1h: TransportWindowPayload,
    transport_24h: TransportWindowPayload,
) -> ObjectPayload:
    """Render transport-observation payload for status JSON."""
    return {"windows": {"1h": transport_1h, "24h": transport_24h}}


def _recent_session_status_payload(
    session: SessionInfo,
    *,
    config: ConsumerConfig,
    workflows: dict[str, WorkflowDefinition],
    now: datetime,
    drain_requested: bool,
    drain_requested_at: str | None = None,
    parse_iso8601_timestamp: Callable[
        [str], datetime | None
    ] = _parse_iso8601_best_effort,
    session_retry_state: SessionRetryStateFn,
) -> RecentSessionStatusPayload:
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
        "active_seconds": _active_seconds(session.started_at, now=now),
        "completed_at": session.completed_at,
        "external_execution_started": _external_execution_started(session),
        "drain_wait_class": _drain_wait_class(
            session,
            drain_requested=drain_requested,
        ),
        "drain_observed_at": _effective_drain_observed_at(
            session,
            drain_requested_at=drain_requested_at,
        ),
        "last_execution_progress_at": session.last_execution_progress_at,
        "shutdown_class": _shutdown_class(
            session,
            now=now,
            drain_requested=drain_requested,
            drain_requested_at=drain_requested_at,
            parse_iso8601_timestamp=parse_iso8601_timestamp,
        ),
        "drain_abort_reason": _drain_abort_reason(session),
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


def _drain_blockers_payload(
    workers: list[WorkerStatusPayload],
    *,
    drain_requested: bool,
) -> list[DrainBlockerPayload]:
    """Return active drain blockers only while drain is in progress."""
    if not drain_requested:
        return []
    blockers: list[DrainBlockerPayload] = []
    for worker in workers:
        blockers.append(
            {
                "issue_ref": worker["issue_ref"],
                "session_id": worker["id"],
                "phase": worker["phase"],
                "external_execution_started": worker["external_execution_started"],
                "active_seconds": worker["active_seconds"],
                "drain_observed_at": worker.get("drain_observed_at"),
                "last_execution_progress_at": worker.get("last_execution_progress_at"),
                "shutdown_class": worker.get("shutdown_class"),
            }
        )
    return blockers


def _load_status_runtime(
    config: ConsumerConfig,
    *,
    deps: CollectStatusPayloadDeps,
) -> tuple[
    BoardAutomationConfig | None,
    dict[str, WorkflowDefinition],
    dict[str, WorkflowRepoStatus],
    int,
    str | None,
]:
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
    config: ConsumerConfig,
    *,
    auto_config: BoardAutomationConfig | None,
    local_only: bool,
    status_now: datetime,
    db: StatusStorePort,
    review_state_port: ReviewStatePort | None,
    deps: CollectStatusPayloadDeps,
) -> CollectedStatusRuntimeState:
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
    transport_1h = _transport_window_payload(db, hours=1, now=status_now)
    transport_24h = _transport_window_payload(db, hours=24, now=status_now)

    return CollectedStatusRuntimeState(
        leases=leases,
        slots=slots,
        workers=workers,
        sessions=sessions,
        control_state=control_state,
        deferred_action_count=deferred_action_count,
        oldest_deferred_action_age_seconds=oldest_deferred_action_age_seconds,
        review_summary=review_summary,
        review_queue=review_queue,
        admission_summary=admission_summary,
        throughput_1h=throughput_1h,
        throughput_24h=throughput_24h,
        transport_1h=transport_1h,
        transport_24h=transport_24h,
    )


def _build_status_payload(
    config: ConsumerConfig,
    *,
    auto_config: BoardAutomationConfig | None,
    workflow_statuses: dict[str, WorkflowRepoStatus],
    main_workflows: dict[str, WorkflowDefinition],
    effective_interval: int,
    last_reload_at: str | None,
    status_now: datetime,
    leases: int,
    slots: list[int],
    workers: list[SessionInfo],
    sessions: list[SessionInfo],
    control_state: dict[str, str],
    deferred_action_count: int,
    oldest_deferred_action_age_seconds: float | None,
    review_summary: ReviewSummaryPayload,
    review_queue: ReviewQueueSummaryPayload,
    admission_summary: AdmissionSummaryPayload,
    throughput_1h: MetricWindowPayload,
    throughput_24h: MetricWindowPayload,
    transport_1h: TransportWindowPayload,
    transport_24h: TransportWindowPayload,
    local_only: bool,
    deps: CollectStatusPayloadDeps,
) -> StatusPayload:
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
    drain_requested = deps.drain_requested(config.drain_path)
    drain_requested_at = deps.read_drain_requested_at(config.drain_path)
    control_plane_health = deps.control_plane_health_summary(
        control_state,
        deferred_action_count=deferred_action_count,
        oldest_deferred_action_age_seconds=oldest_deferred_action_age_seconds,
        poll_interval_seconds=effective_interval,
    )
    lane_wip_limits = _lane_wip_limits_payload(auto_config)
    worker_payloads = [
        _worker_status_payload(
            worker,
            now=status_now,
            drain_requested=drain_requested,
            drain_requested_at=drain_requested_at,
            parse_iso8601_timestamp=deps.parse_iso8601_timestamp,
        )
        for worker in workers
    ]

    return {
        "active_leases": leases,
        "active_slots": slots,
        "workers": worker_payloads,
        "drain_blockers": _drain_blockers_payload(
            worker_payloads,
            drain_requested=drain_requested,
        ),
        "repo_prefixes": list(config.repo_prefixes),
        "global_concurrency": config.global_concurrency,
        "lane_wip_limits": lane_wip_limits,
        "poll_interval_seconds": effective_interval,
        "drain_requested": drain_requested,
        "drain_requested_at": drain_requested_at,
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
        "transport_metrics": _transport_status_payload(transport_1h, transport_24h),
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
                drain_requested=drain_requested,
                drain_requested_at=drain_requested_at,
                parse_iso8601_timestamp=deps.parse_iso8601_timestamp,
                session_retry_state=deps.session_retry_state,
            )
            for session in sessions
        ],
        "local_only": local_only,
    }


def collect_status_payload(
    config: ConsumerConfig,
    *,
    local_only: bool = False,
    db: StatusStorePort,
    review_state_port: ReviewStatePort | None,
    deps: CollectStatusPayloadDeps,
) -> StatusPayload:
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
        leases=status_state.leases,
        slots=status_state.slots,
        workers=status_state.workers,
        sessions=status_state.sessions,
        control_state=status_state.control_state,
        deferred_action_count=status_state.deferred_action_count,
        oldest_deferred_action_age_seconds=status_state.oldest_deferred_action_age_seconds,
        review_summary=status_state.review_summary,
        review_queue=status_state.review_queue,
        admission_summary=status_state.admission_summary,
        throughput_1h=status_state.throughput_1h,
        throughput_24h=status_state.throughput_24h,
        transport_1h=status_state.transport_1h,
        transport_24h=status_state.transport_24h,
    )
