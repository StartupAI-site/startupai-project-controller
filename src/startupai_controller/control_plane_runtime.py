"""Shared control-plane runtime helpers used by consumer and control-plane shells."""

from __future__ import annotations

from datetime import datetime, timezone
import json

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.ports.control_plane_state import (
    ControlPlaneStatePort,
    ControlValueStorePort,
)
from startupai_controller.consumer_workflow import (
    WorkflowDefinition,
    WorkflowRepoStatus,
    effective_poll_interval,
    load_repo_workflows,
    snapshot_from_statuses,
    write_workflow_snapshot,
)
from startupai_controller.payload_types import (
    AdmissionSummaryPayload,
    ControlPlaneHealthPayload,
)
from startupai_controller.runtime.wiring import clear_github_runtime_caches

CONTROL_KEY_DEGRADED = "degraded"
CONTROL_KEY_DEGRADED_REASON = "degraded_reason"
CONTROL_KEY_LAST_SUCCESSFUL_BOARD_SYNC_AT = "last_successful_board_sync_at"
CONTROL_KEY_LAST_SUCCESSFUL_GITHUB_MUTATION_AT = "last_successful_github_mutation_at"
CONTROL_KEY_LAST_ADMISSION_SUMMARY = "last_admission_summary"
CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL = "claim_suppressed_until"
CONTROL_KEY_CLAIM_SUPPRESSED_REASON = "claim_suppressed_reason"
CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE = "claim_suppressed_scope"
CONTROL_KEY_LAST_RATE_LIMIT_AT = "last_rate_limit_at"


def _record_control_timestamp(
    db: ControlValueStorePort,
    key: str,
    *,
    now: datetime | None = None,
) -> None:
    """Persist an ISO timestamp in the consumer control-plane state."""
    db.set_control_value(key, (now or datetime.now(timezone.utc)).isoformat())


def _record_successful_board_sync(
    db: ControlValueStorePort,
    *,
    now=None,
) -> None:
    """Persist the latest successful board-sync timestamp."""
    _record_control_timestamp(db, CONTROL_KEY_LAST_SUCCESSFUL_BOARD_SYNC_AT, now=now)


def _record_successful_github_mutation(
    db: ControlValueStorePort,
    *,
    now=None,
) -> None:
    """Persist the latest successful GitHub mutation timestamp."""
    clear_github_runtime_caches()
    _record_control_timestamp(
        db,
        CONTROL_KEY_LAST_SUCCESSFUL_GITHUB_MUTATION_AT,
        now=now,
    )


def _mark_degraded(
    db: ControlValueStorePort,
    reason: str,
) -> None:
    """Enter degraded mode with a machine-readable reason."""
    db.set_control_value(CONTROL_KEY_DEGRADED, "true")
    db.set_control_value(CONTROL_KEY_DEGRADED_REASON, reason)


def _persist_admission_summary(
    db: ControlValueStorePort,
    summary: AdmissionSummaryPayload,
) -> None:
    """Persist the latest admission summary for local-only status surfaces."""
    db.set_control_value(
        CONTROL_KEY_LAST_ADMISSION_SUMMARY,
        json.dumps(summary, sort_keys=True),
    )


def _clear_degraded(db: ControlValueStorePort) -> None:
    """Clear degraded mode after a successful control-plane cycle."""
    db.set_control_value(CONTROL_KEY_DEGRADED, "false")
    db.set_control_value(CONTROL_KEY_DEGRADED_REASON, None)


def _apply_automation_runtime(
    config: ConsumerConfig,
    automation_config: BoardAutomationConfig | None,
) -> None:
    """Apply automation-config runtime controls to the consumer config."""
    if automation_config is None:
        return
    config.repo_prefixes = automation_config.execution_authority_repos
    config.global_concurrency = automation_config.global_concurrency
    config.deferred_replay_enabled = automation_config.deferred_replay_enabled
    config.multi_worker_enabled = automation_config.multi_worker_enabled
    config.issue_context_cache_enabled = automation_config.issue_context_cache_enabled
    config.issue_context_cache_ttl_seconds = (
        automation_config.issue_context_cache_ttl_seconds
    )
    config.launch_hydration_concurrency = automation_config.launch_hydration_concurrency
    config.rate_limit_pause_enabled = automation_config.rate_limit_pause_enabled
    config.rate_limit_cooldown_seconds = automation_config.rate_limit_cooldown_seconds
    config.worktree_reuse_enabled = automation_config.worktree_reuse_enabled
    config.slo_metrics_enabled = automation_config.slo_metrics_enabled


def _control_plane_health_summary(
    control_state: dict[str, str],
    *,
    deferred_action_count: int,
    oldest_deferred_action_age_seconds: float | None,
    poll_interval_seconds: int,
) -> ControlPlaneHealthPayload:
    """Classify consumer control-plane health into stable machine states."""
    degraded = control_state.get(CONTROL_KEY_DEGRADED) == "true"
    degraded_reason = control_state.get(CONTROL_KEY_DEGRADED_REASON) or ""
    base_reason = degraded_reason.split(":", maxsplit=1)[0] or "none"
    stuck_threshold_seconds = max(poll_interval_seconds * 2, poll_interval_seconds + 60)

    if not degraded and deferred_action_count == 0:
        return {"health": "healthy", "reason_code": "none"}

    if (
        oldest_deferred_action_age_seconds is not None
        and oldest_deferred_action_age_seconds > stuck_threshold_seconds
    ):
        return {
            "health": "degraded_stuck",
            "reason_code": "deferred_backlog" if deferred_action_count else base_reason,
        }

    if degraded:
        return {"health": "degraded_recovering", "reason_code": base_reason}

    return {
        "health": "degraded_recovering",
        "reason_code": "deferred_replay_pending",
    }


def _current_main_workflows(
    config: ConsumerConfig,
    *,
    persist_snapshot: bool = True,
) -> tuple[dict[str, WorkflowDefinition], dict[str, WorkflowRepoStatus], int]:
    """Load repo-owned workflow contracts from canonical main checkouts."""
    workflows, statuses = load_repo_workflows(
        config.repo_prefixes,
        config.repo_roots,
        filename=config.workflow_filename,
    )
    effective_interval = effective_poll_interval(
        workflows,
        default_seconds=config.poll_interval_seconds,
    )
    if persist_snapshot:
        snapshot = snapshot_from_statuses(
            statuses,
            effective_poll_interval_seconds=effective_interval,
        )
        write_workflow_snapshot(config.workflow_state_path, snapshot)
    return workflows, statuses, effective_interval
