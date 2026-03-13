"""Runtime/control support wiring extracted from consumer_support_wiring."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Protocol

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.board_graph import admission_watermarks
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.consumer_workflow import WorkflowDefinition
from startupai_controller.consumer_selection_retry_wiring import (
    effective_retry_backoff as _effective_retry_backoff,
    next_retry_count as _next_retry_count,
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
from startupai_controller.domain.models import SessionInfo
from startupai_controller.domain.review_queue_policy import (
    is_retryable_failure_reason as _is_retryable_failure_reason,
    parse_iso8601_timestamp as _parse_iso8601_timestamp,
    retry_delay_seconds as _retry_delay_seconds,
    session_retry_due_at as _session_retry_due_at,
)
from startupai_controller.ports.consumer_runtime_state import ConsumerRuntimeStatePort
from startupai_controller.validate_critical_path_promotion import parse_issue_ref
from startupai_controller.runtime.wiring import gh_reason_code

MetricPayload = dict[str, object]
AdmissionSummaryPayload = dict[str, object]
SessionRetryStatePayload = dict[str, object]
SessionUpdateFieldValue = str | int | None


class MetricRecorderPort(Protocol):
    """Persistence port for SLO metric events."""

    def record_metric_event(
        self,
        event_type: str,
        *,
        issue_ref: str | None = None,
        payload: MetricPayload | None = None,
        now: datetime | None = None,
    ) -> int:
        """Persist one metric event and return its row id."""
        ...


class ClaimSuppressionStatePort(ConsumerRuntimeStatePort, MetricRecorderPort, Protocol):
    """Runtime state operations needed for claim-suppression control."""

    def set_control_value(self, key: str, value: str | None) -> None:
        """Persist or clear one control-state key."""
        ...


class DeferredActionStorePort(Protocol):
    """Persistence port for deferred GitHub mutations."""

    def queue_deferred_action(
        self,
        scope_key: str,
        action_type: str,
        payload: dict[str, object],
        *,
        now: datetime | None = None,
    ) -> int:
        """Queue one deferred action for later replay."""
        ...


class CompletionSessionStorePort(Protocol):
    """Session persistence operations used for terminal session updates."""

    def latest_session_for_issue(
        self,
        issue_ref: str,
        *,
        exclude_session_id: str | None = None,
    ) -> SessionInfo | None:
        """Return the latest session for an issue, optionally excluding one id."""
        ...

    def update_session(
        self,
        session_id: str,
        **fields: SessionUpdateFieldValue,
    ) -> None:
        """Persist session field updates."""
        ...


def record_metric(
    db: MetricRecorderPort,
    config: ConsumerConfig,
    event_type: str,
    *,
    issue_ref: str | None = None,
    payload: MetricPayload | None = None,
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


def clear_claim_suppression(db: ClaimSuppressionStatePort) -> None:
    """Clear active claim suppression state."""
    db.set_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL, None)
    db.set_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_REASON, None)
    db.set_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE, None)


def activate_claim_suppression(
    db: ClaimSuppressionStatePort,
    config: ConsumerConfig,
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
    automation_config: BoardAutomationConfig | None,
) -> AdmissionSummaryPayload:
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
    db: ClaimSuppressionStatePort,
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
    db: ClaimSuppressionStatePort,
    config: ConsumerConfig,
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
    db: DeferredActionStorePort,
    issue_ref: str,
    *,
    to_status: str,
    from_statuses: set[str],
    blocked_reason: str | None = None,
) -> None:
    """Queue a board status mutation for replay after GitHub recovery."""
    payload: dict[str, object] = {
        "issue_ref": issue_ref,
        "to_status": to_status,
        "from_statuses": sorted(from_statuses),
    }
    if blocked_reason is not None:
        payload["blocked_reason"] = blocked_reason
    db.queue_deferred_action(issue_ref, "set_status", payload)


def queue_verdict_marker(
    db: DeferredActionStorePort,
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
    automation_config: BoardAutomationConfig | None,
) -> AdmissionSummaryPayload:
    """Return the last persisted admission summary or a stable default."""
    raw = control_state.get(CONTROL_KEY_LAST_ADMISSION_SUMMARY)
    if not raw:
        return default_admission_summary(automation_config)
    try:
        payload_obj: object = json.loads(raw)
    except json.JSONDecodeError:
        payload = default_admission_summary(automation_config)
        payload["error"] = "invalid-persisted-admission-summary"
        return payload
    if not isinstance(payload_obj, dict):
        payload = default_admission_summary(automation_config)
        payload["error"] = "invalid-persisted-admission-summary"
        return payload
    return {str(key): value for key, value in payload_obj.items()}


def next_available_slot(db: ConsumerRuntimeStatePort, limit: int) -> int | None:
    """Return the next available execution slot id, or None if saturated."""
    occupied = db.active_slot_ids()
    for slot_id in range(1, limit + 1):
        if slot_id not in occupied:
            return slot_id
    return None


def session_retry_state(
    session: SessionInfo,
    *,
    config: ConsumerConfig,
    workflows: dict[str, WorkflowDefinition],
    now: datetime | None = None,
) -> SessionRetryStatePayload:
    """Return retry metadata for a session."""
    current = now or datetime.now(timezone.utc)
    repo_prefix = session.repo_prefix
    if repo_prefix is None:
        try:
            repo_prefix = parse_issue_ref(session.issue_ref).prefix
        except ValueError:
            repo_prefix = None
    workflow = workflows.get(repo_prefix) if repo_prefix is not None else None
    base_seconds, max_seconds = _effective_retry_backoff(config, workflow)
    due_at = _session_retry_due_at(
        session,
        base_seconds=base_seconds,
        max_seconds=max_seconds,
    )
    retry_delay_value: int | None = None
    retry_remaining_seconds: int | None = None
    if due_at is not None:
        retry_count = session.retry_count or 1
        retry_delay_value = _retry_delay_seconds(
            retry_count,
            base_seconds=base_seconds,
            max_seconds=max_seconds,
        )
        retry_remaining_seconds = max(0, int((due_at - current).total_seconds()))
    return {
        "failure_reason": session.failure_reason,
        "retry_count": session.retry_count,
        "retryable": due_at is not None,
        "retry_backoff_base_seconds": base_seconds,
        "retry_backoff_max_seconds": max_seconds,
        "retry_delay_seconds": retry_delay_value,
        "next_retry_at": due_at.isoformat() if due_at is not None else None,
        "retry_remaining_seconds": retry_remaining_seconds,
    }


def complete_session(
    db: CompletionSessionStorePort,
    session_id: str,
    issue_ref: str,
    *,
    status: str,
    failure_reason: str | None = None,
    completed_at: str | None = None,
    **fields: SessionUpdateFieldValue,
) -> int:
    """Persist a terminal session update and return its retry count."""
    retry_count = _next_retry_count(
        db,
        issue_ref,
        current_session_id=session_id,
        failure_reason=failure_reason,
        is_retryable_failure_reason=_is_retryable_failure_reason,
    )
    update_fields: dict[str, SessionUpdateFieldValue] = {
        "status": status,
        "completed_at": completed_at or datetime.now(timezone.utc).isoformat(),
        "failure_reason": failure_reason,
        "retry_count": retry_count,
    }
    update_fields.update(fields)
    db.update_session(session_id, **update_fields)
    return retry_count
