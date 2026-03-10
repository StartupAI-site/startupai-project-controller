#!/usr/bin/env python3
"""Board consumer daemon — polls Ready items, claims for codex, executes.

Execution authority: codex-only, multi-repo, local daemon with truthful WIP.

CLI:
    board_consumer.py run [--interval N] [--db-path PATH] [--dry-run] [--verbose]
    board_consumer.py one-shot [--issue ISSUE_REF] [--db-path PATH] [--dry-run] [--verbose]
    board_consumer.py status [--db-path PATH] [--json] [--local-only]
    board_consumer.py report-slo [--db-path PATH] [--json]
    board_consumer.py serve-status [--db-path PATH] [--host HOST] [--port PORT]
    board_consumer.py drain [--db-path PATH]
    board_consumer.py resume [--db-path PATH]

Exit codes: 0 success, 2 no-op, 3 config error, 4 API error.
"""

from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable


from startupai_controller.board_automation import (
    BoardAutomationConfig,
    admission_summary_payload,
    admit_backlog_items,
    mark_issues_done,
    _set_blocked_with_reason,
    claim_ready_issue,
    load_automation_config,
    review_rescue,
    route_protected_queue_executors,
    sync_review_state,
)
from startupai_controller.board_graph import (
    _ready_snapshot_rank,
    _resolve_issue_coordinates,
    admission_watermarks,
)
from startupai_controller.consumer_workflow import (
    DEFAULT_WORKFLOW_FILENAME,
    WorkflowConfigError,
    WorkflowDefinition,
    default_repo_roots,
    effective_poll_interval,
    load_repo_workflows,
    load_worktree_workflow,
    read_workflow_snapshot,
    render_workflow_prompt,
    snapshot_from_statuses,
    workflow_status_payload,
    write_workflow_snapshot,
)
from startupai_controller.adapters.github_cli import (  # canonical adapter surface (ADR-002)
    CycleBoardSnapshot,
    CycleGitHubMemo,
    GitHubCliAdapter,
    LinkedIssue,
    _ProjectItemSnapshot,
    _comment_exists,
    build_cycle_board_snapshot,
    _list_project_items_by_status,
    _marker_for,
    _post_comment,
    _run_gh,
    _set_status_if_changed,
    _snapshot_to_issue_ref,
    clear_cycle_board_snapshot_cache,
    close_issue,
    enable_pull_request_automerge,
    gh_reason_code,
    rerun_actions_run,
)
from startupai_controller.adapters.github_http_adapter import (  # canonical: transport stats
    begin_request_stats,
    end_request_stats,
)
from startupai_controller.adapters.local_process import LocalProcessAdapter
from startupai_controller.adapters.sqlite_store import ConsumerDB
from startupai_controller.adapters.sqlite_store import (  # canonical: adapter-internal types
    MetricEvent,
    RecoveredLease,
)
from startupai_controller.adapters.sqlite_store import SqliteSessionStore
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.issue_context import IssueContextPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.ports.worktrees import WorktreePort
from startupai_controller.domain.resolution_policy import (
    NON_AUTO_CLOSE_RESOLUTION_KINDS,
    build_resolution_comment,
    normalize_resolution_payload,
    resolution_allows_autoclose,
    resolution_has_meaningful_signal,
)
from startupai_controller.domain.models import (
    ClaimReadyResult,
    CycleResult,
    IssueSnapshot,
    OpenPullRequestMatch,
    IssueContext,
    RepairBranchReconcileOutcome,
    ResolutionEvaluation,
    ReviewQueueDrainSummary,
    ReviewQueueEntry,
    ReviewSnapshot,
    SessionInfo,
    WorktreeEntry,
)
from startupai_controller.domain.repair_policy import (
    MARKER_PREFIX,
    parse_pr_url as _parse_pr_url,
)
from startupai_controller.domain.review_queue_policy import (
    DEFAULT_REVIEW_QUEUE_BATCH_SIZE,
    DEFAULT_REVIEW_QUEUE_RETRY_SECONDS,
    ESCALATION_CEILING_AUTOMERGE,
    ESCALATION_CEILING_DEFAULT,
    ESCALATION_CEILING_FAILED,
    ESCALATION_CEILING_STABLE,
    ESCALATION_CEILING_TRANSIENT,
    MAX_REQUEUE_CYCLES,
    RETRYABLE_FAILURE_REASONS,
    REVIEW_QUEUE_AUTOMERGE_RETRY_SECONDS,
    REVIEW_QUEUE_FAILED_RETRY_SECONDS,
    REVIEW_QUEUE_PENDING_AUTOMERGE_RETRY_SECONDS,
    REVIEW_QUEUE_PENDING_RETRY_SECONDS,
    REVIEW_QUEUE_SKIPPED_RETRY_SECONDS,
    REVIEW_QUEUE_STABLE_BLOCKED_RETRY_SECONDS,
    REVIEW_QUEUE_STABLE_RESULTS,
    blocked_streak_needs_escalation as _blocked_streak_needs_escalation,
    blocker_class as _blocker_class,
    effective_retry_backoff as _effective_retry_backoff_primitives,
    escalation_ceiling_for_blocker_class as _escalation_ceiling_for_blocker_class,
    requeue_or_escalate as _requeue_or_escalate,
    is_retryable_failure_reason as _is_retryable_failure_reason,
    parse_iso8601_timestamp as _parse_iso8601_timestamp,
    retry_delay_seconds as _retry_delay_seconds,
    review_queue_retry_seconds_for_blocked_reason as _review_queue_retry_seconds_for_blocked_reason,
    review_queue_retry_seconds_for_result as _review_queue_retry_seconds_for_result,
    review_queue_retry_seconds_for_skipped_reason as _review_queue_retry_seconds_for_skipped_reason,
    session_retry_due_at as _session_retry_due_at,
)
from startupai_controller.domain.verdict_policy import (
    is_pre_backfill_eligible as _is_pre_backfill_eligible,
    is_session_verdict_eligible as _is_session_verdict_eligible,
    marker_already_present as _marker_already_present,
    verdict_comment_body as _verdict_comment_body,
    verdict_marker_text as _verdict_marker_text,
)
from startupai_controller.domain.launch_policy import (
    classify_pr_candidates as _classify_pr_candidates_pure,
    launch_session_kind as _launch_session_kind,
    reconcile_in_progress_decision,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    ConfigError,
    GhQueryError,
    evaluate_ready_promotion,
    in_any_critical_path,
    load_config,
    parse_issue_ref,
)

logger = logging.getLogger("board-consumer")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CONFIG_PATH = str(_REPO_ROOT / "config" / "critical-paths.json")
DEFAULT_AUTOMATION_CONFIG_PATH = str(
    _REPO_ROOT / "config" / "board-automation-config.json"
)
DEFAULT_DB_PATH = Path.home() / ".local" / "share" / "startupai" / "consumer.db"
DEFAULT_OUTPUT_DIR = Path.home() / ".local" / "share" / "startupai" / "outputs"
DEFAULT_DRAIN_PATH = Path.home() / ".local" / "share" / "startupai" / "consumer.drain"
DEFAULT_WORKFLOW_STATE_PATH = (
    Path.home() / ".local" / "share" / "startupai" / "workflow-state.json"
)
DEFAULT_STATUS_HOST = "127.0.0.1"
DEFAULT_STATUS_PORT = 8765
DEFAULT_SCHEMA_PATH = _REPO_ROOT / "config" / "codex_session_result.schema.json"
CONTROL_KEY_DEGRADED = "degraded"
CONTROL_KEY_DEGRADED_REASON = "degraded_reason"
CONTROL_KEY_LAST_SUCCESSFUL_BOARD_SYNC_AT = "last_successful_board_sync_at"
CONTROL_KEY_LAST_SUCCESSFUL_GITHUB_MUTATION_AT = "last_successful_github_mutation_at"
CONTROL_KEY_LAST_ADMISSION_SUMMARY = "last_admission_summary"
CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL = "claim_suppressed_until"
CONTROL_KEY_CLAIM_SUPPRESSED_REASON = "claim_suppressed_reason"
CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE = "claim_suppressed_scope"
CONTROL_KEY_LAST_RATE_LIMIT_AT = "last_rate_limit_at"
# Review queue constants: re-exported from domain.review_queue_policy


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@dataclass
class ConsumerConfig:
    """Runtime configuration for the board consumer daemon."""

    critical_paths_path: Path
    automation_config_path: Path
    project_owner: str = "StartupAI-site"
    project_number: int = 1
    db_path: Path = field(default_factory=lambda: DEFAULT_DB_PATH)
    schema_path: Path = field(default_factory=lambda: DEFAULT_SCHEMA_PATH)
    output_dir: Path = field(default_factory=lambda: DEFAULT_OUTPUT_DIR)
    drain_path: Path = field(default_factory=lambda: DEFAULT_DRAIN_PATH)
    workflow_state_path: Path = field(default_factory=lambda: DEFAULT_WORKFLOW_STATE_PATH)
    poll_interval_seconds: int = 180
    codex_timeout_seconds: int = 1800
    heartbeat_expiry_seconds: int = 3600
    max_retries: int = 3
    retry_backoff_base_seconds: int = 30
    retry_backoff_seconds: int = 300
    repo_prefixes: tuple[str, ...] = ("crew",)
    executor: str = "codex"
    global_concurrency: int = 1
    deferred_replay_enabled: bool = True
    multi_worker_enabled: bool = False
    validation_cmd: str = "uv run pytest tests/ -v --tb=short"
    workflow_filename: str = DEFAULT_WORKFLOW_FILENAME
    repo_roots: dict[str, Path] = field(default_factory=default_repo_roots)
    issue_context_cache_enabled: bool = True
    issue_context_cache_ttl_seconds: int = 900
    launch_hydration_concurrency: int = 1
    rate_limit_pause_enabled: bool = True
    rate_limit_cooldown_seconds: int = 300
    worktree_reuse_enabled: bool = True
    slo_metrics_enabled: bool = True


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


# CycleResult: re-exported from domain.models
# ReviewQueueDrainSummary: re-exported from domain.models


@dataclass(frozen=True)
class PreparedCycleContext:
    """Preflight context reused across worker launches in one daemon tick."""

    cp_config: CriticalPathConfig
    auto_config: BoardAutomationConfig | None
    main_workflows: dict[str, WorkflowDefinition]
    workflow_statuses: dict[str, Any]
    dispatchable_repo_prefixes: tuple[str, ...]
    effective_interval: int
    global_limit: int
    board_snapshot: CycleBoardSnapshot
    github_memo: CycleGitHubMemo
    admission_summary: dict[str, Any]
    review_queue_summary: ReviewQueueDrainSummary = field(
        default_factory=ReviewQueueDrainSummary
    )
    timings_ms: dict[str, int] = field(default_factory=dict)
    github_request_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class ActiveWorkerTask:
    """Bookkeeping for one asynchronously executing worker slot."""

    issue_ref: str
    slot_id: int
    launched_at: str


@dataclass(frozen=True)
class PreparedLaunchContext:
    """Locally prepared work that is safe to claim and launch."""

    issue_ref: str
    repo_prefix: str
    owner: str
    repo: str
    number: int
    title: str
    issue_context: dict[str, Any]
    session_kind: str
    repair_pr_url: str | None
    repair_branch_name: str | None
    worktree_path: str
    branch_name: str
    workflow_definition: WorkflowDefinition
    effective_consumer_config: ConsumerConfig
    dependency_summary: str
    branch_reconcile_state: str | None = None
    branch_reconcile_error: str | None = None


@dataclass(frozen=True)
class ClaimedSessionContext:
    """Claimed and started local session ready for Codex execution."""

    session_id: str
    effective_max_retries: int
    slot_id: int


@dataclass(frozen=True)
class SessionExecutionOutcome:
    """Outcome of executing a claimed local session."""

    session_status: str
    failure_reason: str | None
    pr_url: str | None
    has_commits: bool
    codex_result: dict[str, Any] | None
    should_transition_to_review: bool
    immediate_review_summary: ReviewQueueDrainSummary
    resolution_evaluation: ResolutionEvaluation | None = None
    done_reason: str | None = None


@dataclass(frozen=True)
class PrCreationOutcome:
    """PR creation/salvage result for a claimed session."""

    pr_url: str | None
    has_commits: bool
    session_status: str
    failure_reason: str | None


@dataclass(frozen=True)
class PreparedReviewQueueBatch:
    """Prepared review-queue workset for one drain cycle."""

    review_refs: frozenset[str]
    queue_items: tuple[ReviewQueueEntry, ...]
    due_items: tuple[ReviewQueueEntry, ...]
    due_pr_groups: tuple[tuple[tuple[str, int], tuple[ReviewQueueEntry, ...]], ...]
    selected_snapshot_entries: tuple[ReviewQueueEntry, ...]
    seeded: tuple[str, ...]
    removed: tuple[str, ...]


@dataclass(frozen=True)
class ReviewQueueProcessingOutcome:
    """Processed review-queue results for a prepared batch."""

    due_count: int
    verdict_backfilled: tuple[str, ...]
    rerun: tuple[str, ...]
    auto_merge_enabled: tuple[str, ...]
    requeued: tuple[str, ...]
    blocked: tuple[str, ...]
    skipped: tuple[str, ...]
    escalated: tuple[str, ...]
    partial_failure: bool
    error: str | None
    updated_snapshot: CycleBoardSnapshot


@dataclass(frozen=True)
class PreparedDueReviewProcessing:
    """Prepared changed due-review groups ready for rescue processing."""

    due_items: tuple[ReviewQueueEntry, ...]
    due_pr_groups: tuple[tuple[tuple[str, int], tuple[ReviewQueueEntry, ...]], ...]
    snapshots: dict[tuple[str, int], ReviewSnapshot]
    verdict_backfilled: tuple[str, ...]
    partial_failure: bool
    error: str | None


@dataclass(frozen=True)
class ReviewGroupProcessingOutcome:
    """Outcome of processing one due PR group from the review queue."""

    rerun: tuple[str, ...]
    auto_merge_enabled: tuple[str, ...]
    requeued: tuple[str, ...]
    blocked: tuple[str, ...]
    skipped: tuple[str, ...]
    escalated: tuple[str, ...]
    updated_snapshot: CycleBoardSnapshot
    partial_failure: bool = False
    error: str | None = None


@dataclass(frozen=True)
class CycleRuntimeContext:
    """Cycle-scoped runtime wiring and configuration."""

    session_store: SessionStorePort
    cp_config: CriticalPathConfig
    auto_config: BoardAutomationConfig | None
    main_workflows: dict[str, WorkflowDefinition]
    workflow_statuses: dict[str, Any]
    dispatchable_repo_prefixes: tuple[str, ...]
    effective_interval: int
    global_limit: int
    github_memo: CycleGitHubMemo
    pr_port: PullRequestPort


@dataclass(frozen=True)
class SelectedLaunchCandidate:
    """A Ready issue selected for launch preparation."""

    issue_ref: str
    repo_prefix: str
    main_workflow: WorkflowDefinition


# RepairBranchReconcileOutcome: re-exported from domain.models
# ResolutionEvaluation: re-exported from domain.models


class WorktreePrepareError(RuntimeError):
    """Raised when a worktree cannot be safely prepared for launch."""

    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        self.detail = detail
        super().__init__(detail)


def _record_control_timestamp(
    db: ConsumerDB,
    key: str,
    *,
    now: datetime | None = None,
) -> None:
    """Persist an ISO timestamp in the consumer control plane state."""
    db.set_control_value(key, (now or datetime.now(timezone.utc)).isoformat())


def _record_successful_board_sync(
    db: ConsumerDB,
    *,
    now: datetime | None = None,
) -> None:
    """Persist the latest successful board-sync timestamp."""
    _record_control_timestamp(db, CONTROL_KEY_LAST_SUCCESSFUL_BOARD_SYNC_AT, now=now)


def _record_successful_github_mutation(
    db: ConsumerDB,
    *,
    now: datetime | None = None,
) -> None:
    """Persist the latest successful GitHub mutation timestamp."""
    clear_cycle_board_snapshot_cache()
    _record_control_timestamp(
        db, CONTROL_KEY_LAST_SUCCESSFUL_GITHUB_MUTATION_AT, now=now
    )


def _record_metric(
    db: ConsumerDB,
    config: ConsumerConfig,
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


def _clear_claim_suppression(db: ConsumerDB) -> None:
    """Clear active claim suppression state."""
    db.set_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL, None)
    db.set_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_REASON, None)
    db.set_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE, None)


def _claim_suppression_state(
    db: ConsumerDB,
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
        _clear_claim_suppression(db)
        return None
    return {
        "until": until.isoformat(),
        "reason": db.get_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_REASON) or "",
        "scope": db.get_control_value(CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE) or "",
    }


def _activate_claim_suppression(
    db: ConsumerDB,
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
    _record_metric(
        db,
        config,
        "claim_suppressed",
        payload={"scope": scope, "reason": reason, "until": until.isoformat()},
        now=current,
    )
    return until


def _maybe_activate_claim_suppression(
    db: ConsumerDB,
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
    _activate_claim_suppression(db, config, scope=scope, error=error, now=now)
    return True


def _mark_degraded(
    db: ConsumerDB,
    reason: str,
) -> None:
    """Enter degraded mode with a machine-readable reason."""
    db.set_control_value(CONTROL_KEY_DEGRADED, "true")
    db.set_control_value(CONTROL_KEY_DEGRADED_REASON, reason)


def _default_admission_summary(
    automation_config: BoardAutomationConfig | None,
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


def _persist_admission_summary(
    db: ConsumerDB,
    summary: dict[str, Any],
) -> None:
    """Persist the latest admission summary for local-only status surfaces."""
    db.set_control_value(
        CONTROL_KEY_LAST_ADMISSION_SUMMARY,
        json.dumps(summary, sort_keys=True),
    )


def _load_admission_summary(
    control_state: dict[str, str],
    automation_config: BoardAutomationConfig | None,
) -> dict[str, Any]:
    """Return the last persisted admission summary or a stable default."""
    raw = control_state.get(CONTROL_KEY_LAST_ADMISSION_SUMMARY)
    if not raw:
        return _default_admission_summary(automation_config)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = _default_admission_summary(automation_config)
        payload["error"] = "invalid-persisted-admission-summary"
        return payload
    if not isinstance(payload, dict):
        payload = _default_admission_summary(automation_config)
        payload["error"] = "invalid-persisted-admission-summary"
    return payload


def _clear_degraded(db: ConsumerDB) -> None:
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
    config.launch_hydration_concurrency = (
        automation_config.launch_hydration_concurrency
    )
    config.rate_limit_pause_enabled = automation_config.rate_limit_pause_enabled
    config.rate_limit_cooldown_seconds = (
        automation_config.rate_limit_cooldown_seconds
    )
    config.worktree_reuse_enabled = automation_config.worktree_reuse_enabled
    config.slo_metrics_enabled = automation_config.slo_metrics_enabled


def _control_plane_health_summary(
    control_state: dict[str, str],
    *,
    deferred_action_count: int,
    oldest_deferred_action_age_seconds: float | None,
    poll_interval_seconds: int,
) -> dict[str, Any]:
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


def _queue_status_transition(
    db: ConsumerDB,
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


def _queue_verdict_marker(
    db: ConsumerDB,
    pr_url: str,
    session_id: str,
) -> None:
    """Queue a missing PR verdict marker for replay."""
    db.queue_deferred_action(
        pr_url,
        "post_verdict_marker",
        {"pr_url": pr_url, "session_id": session_id},
    )


def _current_main_workflows(
    config: ConsumerConfig,
    *,
    persist_snapshot: bool = True,
) -> tuple[dict[str, WorkflowDefinition], dict[str, Any], int]:
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


# _parse_iso8601_timestamp: imported from domain.review_queue_policy
# _is_retryable_failure_reason: imported from domain.review_queue_policy
# _retry_delay_seconds: imported from domain.review_queue_policy
# _session_retry_due_at: imported from domain.review_queue_policy


def _effective_retry_backoff(
    config: ConsumerConfig,
    workflow: WorkflowDefinition | None,
) -> tuple[int, int]:
    """Return effective retry backoff (base, max) in seconds.

    Thin wrapper that destructures config/workflow for the domain function.
    """
    runtime = workflow.runtime if workflow is not None else None
    return _effective_retry_backoff_primitives(
        base_seconds=(
            runtime.retry_backoff_base_seconds
            if runtime is not None and runtime.retry_backoff_base_seconds is not None
            else None
        ),
        max_seconds=(
            runtime.retry_backoff_seconds
            if runtime is not None and runtime.retry_backoff_seconds is not None
            else None
        ),
        config_base=config.retry_backoff_base_seconds,
        config_max=config.retry_backoff_seconds,
    )


def _next_retry_count(
    db: ConsumerDB,
    issue_ref: str,
    *,
    current_session_id: str,
    failure_reason: str | None,
) -> int:
    """Return the next retry attempt count for a terminal session."""
    if not _is_retryable_failure_reason(failure_reason):
        return 0
    previous = db.latest_session_for_issue(
        issue_ref,
        exclude_session_id=current_session_id,
    )
    if previous is None:
        return 1
    if previous.failure_reason is None:
        if previous.status not in {"failed", "timeout"}:
            return 1
        return max(previous.retry_count, 1) + 1
    if not _is_retryable_failure_reason(previous.failure_reason):
        return 1
    return max(previous.retry_count, 1) + 1


def _complete_session(
    db: ConsumerDB,
    session_id: str,
    issue_ref: str,
    *,
    status: str,
    failure_reason: str | None = None,
    completed_at: str | None = None,
    **fields: Any,
) -> int:
    """Persist a terminal session update and return its retry count."""
    retry_count = _next_retry_count(
        db,
        issue_ref,
        current_session_id=session_id,
        failure_reason=failure_reason,
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


def _retry_backoff_active(
    db: ConsumerDB,
    issue_ref: str,
    *,
    base_seconds: int,
    max_seconds: int,
) -> bool:
    """Return True when a recent failed attempt is still cooling down."""
    latest = db.latest_session_for_issue(issue_ref)
    if latest is None:
        return False
    due_at = _session_retry_due_at(
        latest,
        base_seconds=base_seconds,
        max_seconds=max_seconds,
    )
    if due_at is None:
        return False
    return datetime.now(timezone.utc) < due_at


def _session_retry_state(
    session: SessionInfo,
    *,
    config: ConsumerConfig,
    workflows: dict[str, WorkflowDefinition],
    now: datetime | None = None,
) -> dict[str, Any]:
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
    retry_delay_seconds: int | None = None
    retry_remaining_seconds: int | None = None
    if due_at is not None:
        retry_count = session.retry_count or 1
        retry_delay_seconds = _retry_delay_seconds(
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
        "retry_delay_seconds": retry_delay_seconds,
        "next_retry_at": due_at.isoformat() if due_at is not None else None,
        "retry_remaining_seconds": retry_remaining_seconds,
    }


def _repo_root_for_issue_ref(config: ConsumerConfig, issue_ref: str) -> Path:
    """Return the canonical main-checkout root for an issue ref."""
    repo_prefix = parse_issue_ref(issue_ref).prefix
    root = config.repo_roots.get(repo_prefix)
    if root is None:
        raise ConfigError(f"Missing repo root for {issue_ref}")
    return root


def _verify_code_refs_on_main(
    repo_root: Path,
    code_refs: list[str],
) -> tuple[bool, list[str]]:
    """Verify that every referenced path exists on canonical main."""
    if not code_refs:
        return False, []
    missing: list[str] = []
    resolved_root = repo_root.resolve()
    for ref in code_refs:
        candidate = (repo_root / ref).resolve()
        try:
            candidate.relative_to(resolved_root)
        except ValueError:
            missing.append(ref)
            continue
        if not candidate.exists():
            missing.append(ref)
    return len(missing) == 0, missing


def _run_validation_on_main(
    repo_root: Path,
    command: str,
    *,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> tuple[bool, int | None, str]:
    """Run the repo validation command against canonical main."""
    if not command.strip():
        return False, None, "missing-validation-command"
    runner = subprocess_runner or (lambda args, **kw: subprocess.run(args, **kw))
    result = runner(
        ["bash", "-lc", command],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
    )
    detail = (result.stderr or result.stdout or "").strip()
    return result.returncode == 0, result.returncode, detail


def _commit_reachable_from_origin_main(
    repo_root: Path,
    commit_sha: str,
    *,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> bool:
    """Return True when a commit is reachable from origin/main."""
    runner = subprocess_runner or (lambda args, **kw: subprocess.run(args, **kw))
    result = runner(
        [
            "git",
            "-C",
            str(repo_root),
            "merge-base",
            "--is-ancestor",
            commit_sha,
            "origin/main",
        ],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def _pr_is_merged(
    pr_url: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Return True when a PR URL points at a merged pull request."""
    parsed = _parse_pr_url(pr_url)
    if parsed is None:
        return False
    owner, repo, pr_number = parsed
    output = _run_gh(
        [
            "pr",
            "view",
            str(pr_number),
            "--repo",
            f"{owner}/{repo}",
            "--json",
            "state,mergedAt",
        ],
        gh_runner=gh_runner,
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError:
        return False
    merged_at = payload.get("mergedAt")
    state = str(payload.get("state") or "").strip().upper()
    return bool(merged_at) or state == "MERGED"


def _verify_resolution_payload(
    issue_ref: str,
    resolution: dict[str, Any] | None,
    *,
    config: ConsumerConfig,
    workflows: dict[str, WorkflowDefinition],
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ResolutionEvaluation:
    """Verify a structured resolution payload against canonical main."""
    normalized = normalize_resolution_payload(resolution)
    if normalized is None:
        return ResolutionEvaluation(
            resolution_kind=None,
            verification_class="failed",
            final_action="blocked_for_resolution_review",
            summary="Successful no-op session returned no structured resolution evidence.",
            evidence={},
            blocked_reason="resolution-review-required:no-structured-resolution",
        )

    kind = str(normalized["kind"])
    summary = str(normalized["summary"] or "").strip()
    repo_root = _repo_root_for_issue_ref(config, issue_ref)
    workflow = workflows.get(parse_issue_ref(issue_ref).prefix)
    validation_command = (
        str(normalized["validation_command"]).strip()
        if normalized["validation_command"]
        else (
            workflow.runtime.validation_cmd
            if workflow is not None and workflow.runtime.validation_cmd is not None
            else config.validation_cmd
        )
    )

    code_refs = list(normalized["code_refs"])
    commit_shas = list(normalized["commit_shas"])
    pr_urls = list(normalized["pr_urls"])
    code_refs_ok, missing_code_refs = _verify_code_refs_on_main(repo_root, code_refs)
    reachable_commits = [
        sha
        for sha in commit_shas
        if _commit_reachable_from_origin_main(
            repo_root,
            sha,
            subprocess_runner=subprocess_runner,
        )
    ]
    merged_pr_urls = [
        pr_url
        for pr_url in pr_urls
        if _pr_is_merged(pr_url, gh_runner=gh_runner)
    ]
    validation_ok, validation_exit_code, validation_detail = _run_validation_on_main(
        repo_root,
        validation_command,
        subprocess_runner=subprocess_runner,
    )
    evidence = {
        "code_refs": code_refs,
        "missing_code_refs": missing_code_refs,
        "commit_shas": commit_shas,
        "reachable_commit_shas": reachable_commits,
        "pr_urls": pr_urls,
        "merged_pr_urls": merged_pr_urls,
        "validated_on_main_claim": bool(normalized["validated_on_main"]),
        "validation_command": validation_command,
        "validation_exit_code": validation_exit_code,
        "validation_detail": validation_detail[:500] if validation_detail else "",
        "acceptance_criteria_met": bool(normalized["acceptance_criteria_met"]),
        "acceptance_criteria_notes": normalized["acceptance_criteria_notes"],
        "equivalence_claim": normalized["equivalence_claim"],
    }

    if kind in NON_AUTO_CLOSE_RESOLUTION_KINDS:
        return ResolutionEvaluation(
            resolution_kind=kind,
            verification_class="weak",
            final_action="blocked_for_resolution_review",
            summary=summary or f"Resolution `{kind}` requires review.",
            evidence=evidence,
            blocked_reason=f"resolution-review-required:{kind}",
        )

    has_reference_evidence = bool(code_refs or commit_shas or pr_urls)
    auto_close_allowed = resolution_allows_autoclose(normalized)
    strong = all(
        [
            auto_close_allowed,
            has_reference_evidence,
            code_refs_ok,
            bool(reachable_commits or merged_pr_urls),
            bool(normalized["validated_on_main"]),
            validation_ok,
            bool(normalized["acceptance_criteria_met"]),
        ]
    )
    if strong:
        return ResolutionEvaluation(
            resolution_kind=kind,
            verification_class="strong",
            final_action="closed_as_already_resolved",
            summary=summary or "Verified existing implementation already satisfies the issue.",
            evidence=evidence,
        )

    verification_class = (
        "ambiguous"
        if resolution_has_meaningful_signal(normalized)
        else "failed"
    )
    if not auto_close_allowed:
        blocked_reason = "resolution-review-required:unsupported-resolution-kind"
    elif not has_reference_evidence:
        blocked_reason = "resolution-review-required:missing-evidence"
    elif not code_refs_ok:
        blocked_reason = "resolution-review-required:missing-code-refs"
    elif not (reachable_commits or merged_pr_urls):
        blocked_reason = "resolution-review-required:unverified-main-evidence"
    elif not normalized["validated_on_main"] or not validation_ok:
        blocked_reason = "resolution-review-required:validation-failed"
    elif not normalized["acceptance_criteria_met"]:
        blocked_reason = "resolution-review-required:acceptance-not-met"
    else:
        blocked_reason = "resolution-review-required:ambiguous"

    return ResolutionEvaluation(
        resolution_kind=kind,
        verification_class=verification_class,
        final_action="blocked_for_resolution_review",
        summary=summary or "Resolution evidence was not strong enough to auto-close.",
        evidence=evidence,
        blocked_reason=blocked_reason,
    )


def _queue_issue_comment(
    db: ConsumerDB,
    issue_ref: str,
    body: str,
) -> None:
    """Queue an issue comment for replay after GitHub recovery."""
    db.queue_deferred_action(
        issue_ref,
        "post_issue_comment",
        {"issue_ref": issue_ref, "body": body},
    )


def _queue_issue_close(
    db: ConsumerDB,
    issue_ref: str,
) -> None:
    """Queue an issue close mutation for replay."""
    db.queue_deferred_action(
        issue_ref,
        "close_issue",
        {"issue_ref": issue_ref},
    )


def _set_issue_handoff_target(
    issue_ref: str,
    target: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    board_info_resolver: Callable[..., Any] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set the board Handoff To field for an issue."""
    from startupai_controller.adapters.github_cli import _set_single_select_field
    from startupai_controller.promote_ready import _query_issue_board_info

    resolve_info = board_info_resolver or _query_issue_board_info
    info = resolve_info(issue_ref, config, project_owner, project_number)
    if info.status == "NOT_ON_BOARD":
        return
    _set_single_select_field(
        info.project_id,
        info.item_id,
        "Handoff To",
        target,
        gh_runner=gh_runner,
    )


def _apply_resolution_action(
    issue_ref: str,
    evaluation: ResolutionEvaluation,
    *,
    session_id: str | None,
    db: ConsumerDB,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Apply the verified resolution decision to the board and issue."""
    owner, repo, number = _resolve_issue_coordinates(issue_ref, critical_path_config)
    comment_body = build_resolution_comment(
        issue_ref=issue_ref,
        session_id=session_id,
        resolution_kind=evaluation.resolution_kind or "unknown",
        summary=evaluation.summary,
        verification_class=evaluation.verification_class,
        final_action=evaluation.final_action,
        evidence=evaluation.evidence,
    )

    if evaluation.final_action == "closed_as_already_resolved":
        try:
            mark_issues_done(
                [LinkedIssue(owner=owner, repo=repo, number=number, ref=issue_ref)],
                critical_path_config,
                config.project_owner,
                config.project_number,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                gh_runner=gh_runner,
            )
            _record_successful_github_mutation(db)
        except (GhQueryError, Exception) as err:
            _mark_degraded(db, f"resolution-done:{gh_reason_code(err)}:{err}")
            _queue_status_transition(
                db,
                issue_ref,
                to_status="Done",
                from_statuses={"Backlog", "In Progress", "Ready"},
            )
        try:
            poster = comment_poster or _post_comment
            poster(owner, repo, number, comment_body, gh_runner=gh_runner)
            _record_successful_github_mutation(db)
        except (GhQueryError, Exception) as err:
            _mark_degraded(db, f"resolution-comment:{gh_reason_code(err)}:{err}")
            _queue_issue_comment(db, issue_ref, comment_body)
        try:
            close_issue(owner, repo, number, gh_runner=gh_runner)
            _record_successful_github_mutation(db)
        except (GhQueryError, Exception) as err:
            _mark_degraded(db, f"resolution-close:{gh_reason_code(err)}:{err}")
            _queue_issue_close(db, issue_ref)
        return "already_resolved"

    blocked_reason = evaluation.blocked_reason or "resolution-review-required"
    try:
        _set_blocked_with_reason(
            issue_ref,
            blocked_reason,
            critical_path_config,
            config.project_owner,
            config.project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        _set_issue_handoff_target(
            issue_ref,
            "claude",
            critical_path_config,
            config.project_owner,
            config.project_number,
            board_info_resolver=board_info_resolver,
            gh_runner=gh_runner,
        )
        _record_successful_github_mutation(db)
    except (GhQueryError, Exception) as err:
        _mark_degraded(db, f"resolution-blocked:{gh_reason_code(err)}:{err}")
        _queue_status_transition(
            db,
            issue_ref,
            to_status="Blocked",
            from_statuses={"Backlog", "In Progress", "Ready"},
            blocked_reason=blocked_reason,
        )
    try:
        poster = comment_poster or _post_comment
        poster(owner, repo, number, comment_body, gh_runner=gh_runner)
        _record_successful_github_mutation(db)
    except (GhQueryError, Exception) as err:
        _mark_degraded(db, f"resolution-comment:{gh_reason_code(err)}:{err}")
        _queue_issue_comment(db, issue_ref, comment_body)
    return "resolution_review"


def _run_workspace_hooks(
    commands: tuple[str, ...],
    *,
    worktree_path: str,
    issue_ref: str,
    branch_name: str,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> None:
    """Run repo-owned workspace hook commands in the claimed worktree."""
    port = worktree_port or LocalProcessAdapter(subprocess_runner=subprocess_runner)
    port.run_workspace_hooks(
        commands,
        worktree_path=worktree_path,
        issue_ref=issue_ref,
        branch_name=branch_name,
    )


# ---------------------------------------------------------------------------
# Helper: select best candidate
# ---------------------------------------------------------------------------


def _select_best_candidate(
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    executor: str = "codex",
    this_repo_prefix: str | None = None,
    repo_prefixes: tuple[str, ...] = ("crew",),
    automation_config: BoardAutomationConfig | None = None,
    status_resolver: Callable[..., str] | None = None,
    ready_items: tuple[_ProjectItemSnapshot, ...] | None = None,
    github_memo: CycleGitHubMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
    issue_filter: Callable[[str], bool] | None = None,
) -> str | None:
    """Select highest-priority Ready issue for executor using agreed ranking.

    Ranking: critical-path first -> Priority (P0 > P1 > P2 > P3) -> oldest number.
    Skips items with unmet graph dependencies.
    Returns issue_ref (e.g. "crew#84") or None.
    """
    if this_repo_prefix is not None:
        repo_prefixes = (this_repo_prefix,)
    ready_items = ready_items or tuple(
        _list_project_items_by_status(
            "Ready", project_owner, project_number, gh_runner=gh_runner
        )
    )
    eligible: list[_ProjectItemSnapshot] = []
    for snapshot in ready_items:
        if snapshot.executor.strip().lower() != executor:
            continue
        ref = _snapshot_to_issue_ref(snapshot, config)
        if ref is None:
            continue
        if parse_issue_ref(ref).prefix not in repo_prefixes:
            continue
        if issue_filter is not None and not issue_filter(ref):
            continue
        if in_any_critical_path(config, ref):
            is_ready = None
            if github_memo is not None:
                is_ready = github_memo.dependency_ready.get(ref)
            if is_ready is None:
                val_code, _ = evaluate_ready_promotion(
                    issue_ref=ref,
                    config=config,
                    project_owner=project_owner,
                    project_number=project_number,
                    status_resolver=status_resolver,
                    require_in_graph=True,
                )
                is_ready = val_code == 0
                if github_memo is not None:
                    github_memo.dependency_ready[ref] = is_ready
            if not is_ready:
                continue
        eligible.append(snapshot)

    if not eligible:
        return None

    eligible.sort(key=lambda s: _ready_snapshot_rank(s, config))
    best_ref = _snapshot_to_issue_ref(eligible[0], config)
    return best_ref


# ---------------------------------------------------------------------------
# Helper: fetch issue context
# ---------------------------------------------------------------------------


def _fetch_issue_context(
    owner: str,
    repo: str,
    number: int,
    *,
    issue_context_port: IssueContextPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> dict[str, Any]:
    """Read issue title, body, and labels via the issue-context boundary."""
    port = issue_context_port or GitHubCliAdapter(
        project_owner="",
        project_number=0,
        gh_runner=gh_runner,
    )
    context = port.get_issue_context(owner, repo, number)
    return {
        "title": context.title,
        "body": context.body,
        "labels": list(context.labels),
        "updated_at": context.updated_at,
    }


def _snapshot_for_issue(
    board_snapshot: CycleBoardSnapshot,
    issue_ref: str,
    config: CriticalPathConfig,
) -> _ProjectItemSnapshot | None:
    """Return the thin board snapshot row for an issue ref."""
    for snapshot in board_snapshot.items:
        if _snapshot_to_issue_ref(snapshot, config) == issue_ref:
            return snapshot
    return None


def _issue_context_cache_is_fresh(
    cached: Any,
    *,
    snapshot_updated_at: str,
    now: datetime,
) -> bool:
    """Return True when cached issue context is safe to reuse."""
    if cached is None:
        return False
    expires_at = _parse_iso8601_timestamp(getattr(cached, "expires_at", None))
    if expires_at is None or expires_at <= now:
        return False
    if snapshot_updated_at and getattr(cached, "issue_updated_at", "") != snapshot_updated_at:
        return False
    return True


def _hydrate_issue_context(
    issue_ref: str,
    *,
    owner: str,
    repo: str,
    number: int,
    snapshot: _ProjectItemSnapshot | None,
    config: ConsumerConfig,
    db: ConsumerDB,
    issue_context_port: IssueContextPort | None = None,
    gh_runner: Callable[..., str] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return locally ready issue context, refreshing the cache when needed."""
    current = now or datetime.now(timezone.utc)
    cached = db.get_issue_context(issue_ref) if config.issue_context_cache_enabled else None
    snapshot_updated_at = snapshot.issue_updated_at if snapshot is not None else ""
    if _issue_context_cache_is_fresh(
        cached,
        snapshot_updated_at=snapshot_updated_at,
        now=current,
    ):
        _record_metric(
            db,
            config,
            "context_cache_hit",
            issue_ref=issue_ref,
            now=current,
        )
        return {
            "title": cached.title,
            "body": cached.body,
            "labels": cached.labels,
            "updated_at": cached.issue_updated_at,
        }

    _record_metric(
        db,
        config,
        "context_cache_miss",
        issue_ref=issue_ref,
        payload={"stale": cached is not None},
        now=current,
    )
    _record_metric(
        db,
        config,
        "context_hydration_started",
        issue_ref=issue_ref,
        now=current,
    )
    context = _fetch_issue_context(
        owner,
        repo,
        number,
        issue_context_port=issue_context_port,
        gh_runner=gh_runner,
    )
    context.setdefault("title", snapshot.title if snapshot is not None else f"issue-{number}")
    context.setdefault("body", "")
    labels = context.get("labels")
    if not isinstance(labels, list):
        labels = []
    context["labels"] = [str(label) for label in labels if str(label)]
    issue_updated_at = str(context.get("updated_at") or snapshot_updated_at or current.isoformat())
    context["updated_at"] = issue_updated_at
    fetched_at = current.isoformat()
    expires_at = (current + timedelta(seconds=config.issue_context_cache_ttl_seconds)).isoformat()
    if config.issue_context_cache_enabled:
        db.set_issue_context(
            issue_ref,
            owner=owner,
            repo=repo,
            number=number,
            title=str(context.get("title") or ""),
            body=str(context.get("body") or ""),
            labels=list(context["labels"]),
            issue_updated_at=issue_updated_at,
            fetched_at=fetched_at,
            expires_at=expires_at,
        )
    _record_metric(
        db,
        config,
        "context_hydration_succeeded",
        issue_ref=issue_ref,
        payload={"cached_until": expires_at},
        now=current,
    )
    return context


def _list_repo_worktrees(
    repo_root: Path,
    *,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> list[tuple[str, str]]:
    """Return (worktree_path, branch_name) pairs for a repo root."""
    port = worktree_port or LocalProcessAdapter(subprocess_runner=subprocess_runner)
    return [(entry.path, entry.branch_name) for entry in port.list_worktrees(str(repo_root))]


def _worktree_is_clean(
    worktree_path: str,
    *,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> bool:
    """Return True when a worktree has no local changes."""
    port = worktree_port or LocalProcessAdapter(subprocess_runner=subprocess_runner)
    return port.is_clean(worktree_path)


def _worktree_ownership_is_safe(
    store: SessionStorePort,
    issue_ref: str,
    worktree_path: str,
) -> bool:
    """Return True when a clean worktree is safe to adopt for an issue."""
    for worker in store.active_workers():
        if worker.worktree_path == worktree_path and worker.issue_ref != issue_ref:
            return False
    latest = store.latest_session_for_worktree(worktree_path)
    if latest is None:
        return True
    return latest.issue_ref == issue_ref


def _prepare_worktree(
    issue_ref: str,
    title: str,
    config: ConsumerConfig,
    db: ConsumerDB,
    *,
    branch_name_override: str | None = None,
    session_store: SessionStorePort | None = None,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> tuple[str, str]:
    """Create or safely adopt a worktree for an issue."""
    parsed = parse_issue_ref(issue_ref)
    store = session_store or SqliteSessionStore(db)
    port = worktree_port or LocalProcessAdapter(subprocess_runner=subprocess_runner)
    if config.worktree_reuse_enabled:
        repo_root = config.repo_roots.get(parsed.prefix)
        if repo_root is None:
            raise WorktreePrepareError(
                "unknown_repo_prefix",
                f"unknown repo prefix for worktree prep: {parsed.prefix}",
            )
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:40]
        target_branch = branch_name_override or f"feat/{parsed.number}-{slug}"
        try:
            worktree_records = _list_repo_worktrees(
                repo_root,
                worktree_port=port,
            )
        except RuntimeError as err:
            logger.warning(
                "Worktree reuse lookup failed for %s (%s); falling back to create",
                issue_ref,
                err,
            )
            worktree_records = []
        for worktree_path, branch_name in worktree_records:
            if branch_name != target_branch:
                continue
            if not _worktree_is_clean(worktree_path, worktree_port=port):
                raise WorktreePrepareError(
                    "worktree_in_use",
                    f"existing worktree is dirty for {target_branch}: {worktree_path}",
                )
            if not _worktree_ownership_is_safe(store, issue_ref, worktree_path):
                raise WorktreePrepareError(
                    "worktree_in_use",
                    f"existing worktree ownership is ambiguous for {target_branch}: {worktree_path}",
                )
            port.fast_forward_existing(worktree_path, target_branch)
            _record_metric(
                db,
                config,
                "worktree_reused",
                issue_ref=issue_ref,
                payload={"worktree_path": worktree_path, "branch_name": target_branch},
            )
            return worktree_path, target_branch

    return _create_worktree(
        issue_ref,
        title,
        config,
        branch_name_override=branch_name_override,
        worktree_port=port,
        subprocess_runner=subprocess_runner,
    )


# ---------------------------------------------------------------------------
# Helper: create worktree
# ---------------------------------------------------------------------------


def _create_worktree(
    issue_ref: str,
    title: str,
    config: ConsumerConfig,
    *,
    branch_name_override: str | None = None,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> tuple[str, str]:
    """Create a worktree for the issue. Returns (worktree_path, branch_name).

    Shells out to wt-create.sh.
    """
    port = worktree_port or LocalProcessAdapter(subprocess_runner=subprocess_runner)
    entry = port.create_issue_worktree(
        issue_ref,
        title,
        branch_name_override=branch_name_override,
    )
    return entry.path, entry.branch_name


def _fast_forward_existing_worktree(
    worktree_path: str,
    branch: str,
    *,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> None:
    """Fast-forward a clean reused worktree to the remote branch head when possible."""
    port = worktree_port or LocalProcessAdapter(subprocess_runner=subprocess_runner)
    port.fast_forward_existing(worktree_path, branch)


def _git_command_detail(result: subprocess.CompletedProcess[str]) -> str:
    """Return the most useful human-readable detail from a git subprocess result."""
    return result.stderr.strip() or result.stdout.strip() or "unknown-error"


def _reconcile_repair_branch(
    worktree_path: str,
    branch: str,
    *,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> RepairBranchReconcileOutcome:
    """Reconcile a repair branch against its remote and origin/main."""
    port = worktree_port or LocalProcessAdapter(subprocess_runner=subprocess_runner)
    return port.reconcile_repair_branch(worktree_path, branch)


# ---------------------------------------------------------------------------
# Helper: assemble codex prompt
# ---------------------------------------------------------------------------


def _assemble_codex_prompt(
    issue_context: dict[str, Any],
    issue_ref: str,
    config: CriticalPathConfig,
    consumer_config: ConsumerConfig,
    worktree_path: str,
    branch_name: str,
    *,
    dependency_summary: str = "",
    workflow_definition: WorkflowDefinition | None = None,
    session_kind: str = "new_work",
    repair_pr_url: str | None = None,
    branch_reconcile_state: str | None = None,
    branch_reconcile_error: str | None = None,
) -> str:
    """Build the codex execution prompt from ADR-018 contract.

    Pure function — no I/O.
    """
    parsed = parse_issue_ref(issue_ref)
    owner, repo, number = _resolve_issue_coordinates(issue_ref, config)
    title = issue_context.get("title", f"Issue #{parsed.number}")
    body = issue_context.get("body", "")

    # Extract acceptance criteria from issue body
    acceptance = _extract_acceptance_criteria(body)

    prompt = f"""\
Issue: {title} (#{number})
Repository: {owner}/{repo}
Base branch: main

Working directory: {worktree_path}
Branch: {branch_name}

Dependency summary:
{dependency_summary or "(No graph dependencies.)"}
(All listed predecessors are Done.)

Acceptance criteria:
{acceptance or "(See issue body for details.)"}

Constraints:
- You are working in an EXISTING worktree at the path above on the branch above.
  Do NOT create a new worktree, branch, or checkout.
- Do not modify board state, issue state, or project fields.
- Do not open or create pull requests — PR lifecycle is consumer-owned.
- Validate your work: {consumer_config.validation_cmd}
- Commit changes and push to origin/{branch_name}.
- If the issue is already satisfied on main and no code changes are needed, set
  `resolution` with concrete code refs, merged PRs or commits, and the exact
  validation result on canonical main. Do not leave `resolution` null in a
  successful no-op case.
- Return ONLY JSON matching the provided schema. Populate every schema field;
  use null or [] when applicable. No prose, no markdown."""

    if session_kind == "repair":
        prompt += (
            "\n\nRepair context:\n"
            f"- Existing PR: {repair_pr_url or '(unknown)'}\n"
            f"- Branch reconcile state: {branch_reconcile_state or 'not-run'}\n"
        )
        if branch_reconcile_error:
            prompt += f"- Branch reconcile error: {branch_reconcile_error}\n"
        prompt += (
            "- This is an in-place repair of an existing PR branch.\n"
            "- First make the branch cleanly mergeable with main.\n"
            "- If the branch currently has merge conflicts from origin/main, "
            "resolve them before running final validation.\n"
        )

    if workflow_definition is not None:
        workflow_context = {
            "issue_ref": issue_ref,
            "issue_title": title,
            "repository": f"{owner}/{repo}",
            "worktree_path": worktree_path,
            "branch_name": branch_name,
            "dependency_summary": dependency_summary or "(No graph dependencies.)",
            "acceptance_criteria": acceptance or "(See issue body for details.)",
            "validation_cmd": consumer_config.validation_cmd,
        }
        rendered = render_workflow_prompt(workflow_definition, workflow_context)
        prompt = f"{prompt}\n\nRepository workflow instructions:\n{rendered}"

    return prompt


def _extract_acceptance_criteria(body: str) -> str:
    """Extract acceptance criteria section from issue body."""
    from startupai_controller.domain.repair_policy import extract_acceptance_criteria
    return extract_acceptance_criteria(body)


def _deterministic_branch_pattern(issue_ref: str) -> re.Pattern[str]:
    """Return the canonical issue branch pattern for PR adoption."""
    from startupai_controller.domain.repair_policy import deterministic_branch_pattern
    parsed = parse_issue_ref(issue_ref)
    return deterministic_branch_pattern(parsed.number)


def _repo_to_prefix_for_repo(repo: str) -> str:
    """Best-effort repo name to board prefix mapping."""
    from startupai_controller.domain.repair_policy import repo_to_prefix_for_repo
    return repo_to_prefix_for_repo(repo)


def _consumer_provenance_marker(
    *,
    session_id: str,
    issue_ref: str,
    repo_prefix: str,
    branch_name: str,
    executor: str,
) -> str:
    """Build a machine-readable provenance marker for issues and PRs."""
    from startupai_controller.domain.repair_policy import consumer_provenance_marker
    return consumer_provenance_marker(
        session_id=session_id,
        issue_ref=issue_ref,
        repo_prefix=repo_prefix,
        branch_name=branch_name,
        executor=executor,
    )


def _parse_consumer_provenance(text: str) -> dict[str, str] | None:
    """Parse the consumer provenance marker from text."""
    from startupai_controller.domain.repair_policy import parse_consumer_provenance
    return parse_consumer_provenance(text)


# ---------------------------------------------------------------------------
# Helper: run codex session
# ---------------------------------------------------------------------------


def _run_codex_session(
    worktree_path: str,
    prompt: str,
    schema_path: Path,
    output_path: Path,
    timeout_seconds: int,
    *,
    heartbeat_fn: Callable[[], None] | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> int:
    """Run codex exec with timeout wrapper. Returns exit code.

    Exit 124 = timeout (from coreutils timeout command).
    """
    codex_cmd = _resolve_cli_command("codex")
    args = [
        "timeout",
        str(timeout_seconds),
        codex_cmd,
        "exec",
        "-C", worktree_path,
        "--full-auto",
        "--output-schema", str(schema_path),
        "-o", str(output_path),
        prompt,
    ]
    if subprocess_runner is not None:
        result = subprocess_runner(
            args,
            capture_output=True,
            text=True,
        )
    else:
        proc_args = args[2:]
        with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as stdout_log, tempfile.TemporaryFile(
            mode="w+t",
            encoding="utf-8",
        ) as stderr_log:
            process = subprocess.Popen(
                proc_args,
                stdout=stdout_log,
                stderr=stderr_log,
                text=True,
            )
            deadline = time.monotonic() + timeout_seconds
            while True:
                if heartbeat_fn is not None:
                    heartbeat_fn()
                rc = process.poll()
                if rc is not None:
                    stdout_log.flush()
                    stderr_log.flush()
                    stdout_log.seek(0)
                    stderr_log.seek(0)
                    result = subprocess.CompletedProcess(
                        args=proc_args,
                        returncode=rc,
                        stdout=stdout_log.read(),
                        stderr=stderr_log.read(),
                    )
                    break
                if time.monotonic() >= deadline:
                    process.kill()
                    process.wait()
                    stdout_log.flush()
                    stderr_log.flush()
                    stdout_log.seek(0)
                    stderr_log.seek(0)
                    result = subprocess.CompletedProcess(
                        args=proc_args,
                        returncode=124,
                        stdout=stdout_log.read(),
                        stderr=stderr_log.read(),
                    )
                    break
                time.sleep(15)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        if detail:
            logger.error("codex exec failed (exit %s): %s", result.returncode, detail)
        else:
            logger.error("codex exec failed (exit %s) with no output", result.returncode)
    elif not output_path.exists():
        logger.error("codex exec exited 0 but produced no output file: %s", output_path)
    return result.returncode


def _resolve_cli_command(command: str) -> str:
    """Resolve a CLI binary without relying on interactive shell PATH setup."""
    resolved = shutil.which(command)
    if resolved:
        return resolved

    home = Path.home()
    candidates = [
        home / ".local" / "bin" / command,
        home / ".local" / "share" / "pnpm" / command,
        home / ".npm-global" / "bin" / command,
        Path("/usr/local/bin") / command,
        Path("/usr/bin") / command,
    ]
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)

    return command


def _drain_requested(path: Path) -> bool:
    """Return True when a graceful drain has been requested."""
    return path.exists()


def _request_drain(path: Path) -> None:
    """Create the drain sentinel file used for graceful maintenance."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(datetime.now(timezone.utc).isoformat(), encoding="utf-8")


def _clear_drain(path: Path) -> bool:
    """Remove the drain sentinel file if present."""
    if not path.exists():
        return False
    path.unlink()
    return True


# ---------------------------------------------------------------------------
# Helper: parse codex result
# ---------------------------------------------------------------------------


def _parse_codex_result(
    output_path: Path,
    *,
    file_reader: Callable[[Path], str] | None = None,
) -> dict[str, Any] | None:
    """Parse CodexSessionResult JSON from output file. Returns None on failure."""
    reader = file_reader or (lambda p: p.read_text(encoding="utf-8"))
    try:
        text = reader(output_path)
        return json.loads(text)
    except (OSError, json.JSONDecodeError):
        return None


# ---------------------------------------------------------------------------
# Helper: create or update PR
# ---------------------------------------------------------------------------


def _create_or_update_pr(
    worktree_path: str,
    branch: str,
    issue_number: int,
    owner: str,
    repo: str,
    title: str,
    config: ConsumerConfig | None = None,
    issue_ref: str | None = None,
    session_id: str = "legacy-session",
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Ensure a PR exists for the branch. Returns PR URL.

    1. Check if PR already exists (codex may have created one).
    2. If yes -> verify body contains required fields; gh pr edit if needed.
    3. If no -> gh pr create with required body fields.
    """
    resolved_issue_ref = issue_ref or f"{_repo_to_prefix_for_repo(repo)}#{issue_number}"

    # Check for existing PR
    try:
        existing = _run_gh(
            [
                "pr",
                "view",
                branch,
                "--repo",
                f"{owner}/{repo}",
                "--json",
                "url,body",
            ],
            gh_runner=gh_runner,
        )
        pr_data = json.loads(existing)
        pr_url = pr_data.get("url", "")
        body = pr_data.get("body", "")

        # Validate required body fields
        needs_edit = False
        required_lines = {
            "Lead Agent:": "Lead Agent: codex",
            "Handoff:": "Handoff: none",
            f"Closes #{issue_number}": f"Closes #{issue_number}",
            f"issue={resolved_issue_ref}": f"issue={resolved_issue_ref}",
        }
        for marker, full_line in required_lines.items():
            if marker not in body:
                needs_edit = True
                break

        if needs_edit:
            new_body = _build_pr_body(
                title,
                issue_number,
                issue_ref=resolved_issue_ref,
                session_id=session_id,
                repo_prefix=parse_issue_ref(resolved_issue_ref).prefix,
                branch_name=branch,
            )
            _run_gh(
                [
                    "pr",
                    "edit",
                    branch,
                    "--repo",
                    f"{owner}/{repo}",
                    "--body",
                    new_body,
                ],
                gh_runner=gh_runner,
            )

        return pr_url

    except (GhQueryError, json.JSONDecodeError, Exception):
        pass  # No existing PR — create one

    # Create PR
    body = _build_pr_body(
        title,
        issue_number,
        issue_ref=resolved_issue_ref,
        session_id=session_id,
        repo_prefix=parse_issue_ref(resolved_issue_ref).prefix,
        branch_name=branch,
    )
    output = _run_gh(
        [
            "pr",
            "create",
            "--repo",
            f"{owner}/{repo}",
            "--head",
            branch,
            "--title",
            f"{title} (#{issue_number})",
            "--body",
            body,
        ],
        gh_runner=gh_runner,
    )
    # gh pr create outputs the PR URL
    return output.strip()


def _build_pr_body(
    title: str,
    issue_number: int,
    *,
    issue_ref: str = "crew#0",
    session_id: str = "legacy-session",
    repo_prefix: str = "crew",
    branch_name: str = "feat/0-legacy",
) -> str:
    """Build PR body with required tag-contract fields."""
    marker = _consumer_provenance_marker(
        session_id=session_id,
        issue_ref=issue_ref,
        repo_prefix=repo_prefix,
        branch_name=branch_name,
        executor="codex",
    )
    return (
        f"## Summary\n\n"
        f"Automated implementation for #{issue_number}.\n\n"
        f"Closes #{issue_number}\n\n"
        f"Lead Agent: codex\n"
        f"Handoff: none\n\n"
        f"{marker}\n"
    )


def _post_consumer_claim_comment(
    issue_ref: str,
    session_id: str,
    repo_prefix: str,
    branch_name: str,
    executor: str,
    config: CriticalPathConfig,
    *,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Post a deterministic claim provenance marker on the issue."""
    owner, repo, number = _resolve_issue_coordinates(issue_ref, config)
    marker = _consumer_provenance_marker(
        session_id=session_id,
        issue_ref=issue_ref,
        repo_prefix=repo_prefix,
        branch_name=branch_name,
        executor=executor,
    )
    checker = comment_checker or _comment_exists
    if checker(owner, repo, number, marker, gh_runner=gh_runner):
        return
    body = "\n".join(
        [
            marker,
            f"Local consumer claimed `{issue_ref}` for `{executor}` execution.",
            f"Branch: `{branch_name}`",
            f"Session: `{session_id}`",
        ]
    )
    poster = comment_poster or _post_comment
    poster(owner, repo, number, body, gh_runner=gh_runner)


# OpenPullRequestMatch: re-exported from domain.models


def _list_open_pr_candidates(
    owner: str,
    repo: str,
    issue_number: int,
    *,
    pr_port: PullRequestPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[OpenPullRequestMatch]:
    """Return open PRs that reference an issue number in the repository."""
    port = pr_port or GitHubCliAdapter(
        project_owner="",
        project_number=0,
        gh_runner=gh_runner,
    )
    payload = port.list_open_prs_for_issue(f"{owner}/{repo}", issue_number)
    matches: list[OpenPullRequestMatch] = []
    for item in payload:
        body = item.body
        matches.append(
            OpenPullRequestMatch(
                url=item.url,
                number=item.number,
                author=item.author,
                body=body,
                branch_name=item.head_ref_name,
                provenance=_parse_consumer_provenance(body),
            )
        )
    return matches


def _classify_open_pr_candidates(
    issue_ref: str,
    owner: str,
    repo: str,
    issue_number: int,
    automation_config: BoardAutomationConfig,
    *,
    expected_branch: str | None = None,
    pr_port: PullRequestPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, OpenPullRequestMatch | None, str]:
    """Classify open PRs for an issue as adoptable, ambiguous, non-local, or none."""
    candidates = _list_open_pr_candidates(
        owner,
        repo,
        issue_number,
        pr_port=pr_port,
        gh_runner=gh_runner,
    )
    return _classify_pr_candidates_pure(
        issue_ref,
        candidates,
        trusted_authors=automation_config.trusted_local_authors,
        expected_branch=expected_branch,
        issue_number=issue_number,
    )


# ---------------------------------------------------------------------------
# Helper: post result comment
# ---------------------------------------------------------------------------


def _post_result_comment(
    issue_ref: str,
    result: dict[str, Any],
    session_id: str,
    config: CriticalPathConfig,
    *,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Post a machine-marker result comment on the issue."""
    marker = _marker_for("consumer-result", issue_ref)
    owner, repo, number = _resolve_issue_coordinates(issue_ref, config)

    checker = comment_checker or _comment_exists
    # Don't check for duplicates — each cycle posts a new result

    outcome = result.get("outcome", "unknown")
    summary = result.get("summary", "No summary provided.")
    tests_run = result.get("tests_run")
    tests_passed = result.get("tests_passed")
    changed_files = result.get("changed_files", [])
    pr_url = result.get("pr_url")
    duration = result.get("duration_seconds")
    resolution = normalize_resolution_payload(result.get("resolution"))

    lines = [
        marker,
        f"**Consumer result**: `{outcome}` (session: `{session_id}`)",
        "",
        f"> {summary}",
    ]
    if tests_run is not None:
        lines.append(f"\nTests: {tests_passed}/{tests_run} passed")
    if changed_files:
        lines.append(f"\nChanged files: {len(changed_files)}")
    if pr_url:
        lines.append(f"\nPR: {pr_url}")
    if resolution is not None:
        lines.append(
            "\nResolution: "
            f"{resolution['kind']} ({resolution['equivalence_claim']})"
        )
    if duration is not None:
        lines.append(f"\nDuration: {duration:.0f}s")

    body = "\n".join(lines)
    poster = comment_poster or _post_comment
    poster(owner, repo, number, body, gh_runner=gh_runner)


# ---------------------------------------------------------------------------
# Helper: post Codex verdict marker on the PR
# ---------------------------------------------------------------------------


def _post_pr_codex_verdict(
    pr_url: str,
    session_id: str,
    *,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Post the machine-readable codex pass verdict required for auto-merge."""
    parsed = _parse_pr_url(pr_url)
    if parsed is None:
        raise GhQueryError(f"Invalid PR URL for codex verdict: {pr_url}")

    owner, repo, pr_number = parsed
    marker = _verdict_marker_text(session_id)
    checker = comment_checker or _comment_exists
    if checker(owner, repo, pr_number, marker, gh_runner=gh_runner):
        return False

    body = _verdict_comment_body(session_id
    )
    poster = comment_poster or _post_comment
    poster(owner, repo, pr_number, body, gh_runner=gh_runner)
    return True


# ---------------------------------------------------------------------------
# Helper: backfill missing Codex verdict markers for review sessions
# ---------------------------------------------------------------------------


def _backfill_review_verdicts(
    db: ConsumerDB,
    *,
    session_limit: int = 50,
    review_refs: tuple[str, ...] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, ...]:
    """Re-post missing codex verdict markers for successful review sessions."""
    backfilled: list[str] = []
    scoped_review_refs = set(review_refs or ())
    for session in db.recent_sessions(limit=session_limit):
        if session.status != "success":
            continue
        if session.phase != "review":
            continue
        if not session.pr_url:
            continue
        if scoped_review_refs and session.issue_ref not in scoped_review_refs:
            continue
        try:
            posted = _post_pr_codex_verdict(
                session.pr_url,
                session.id,
                comment_checker=comment_checker,
                comment_poster=comment_poster,
                gh_runner=gh_runner,
            )
        except (GhQueryError, Exception) as err:
            logger.warning(
                "Review verdict backfill failed for %s (%s): %s",
                session.issue_ref,
                session.id,
                err,
            )
            continue
        if posted:
            backfilled.append(session.issue_ref)
    return tuple(backfilled)


def _group_review_queue_entries_by_pr(
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


def _repark_unchanged_review_queue_entries(
    store: SessionStorePort,
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
            blocked_reason=entry.last_reason if entry.last_result == "blocked" else None,
            skipped_reason=entry.last_reason if entry.last_result == "skipped" else None,
        )
        retry_seconds = _review_queue_retry_seconds_for_result(synthetic_result)
        if entry.last_result == "partial_failure":
            retry_seconds = max(DEFAULT_REVIEW_QUEUE_RETRY_SECONDS, retry_seconds)
        _apply_review_queue_result(
            store,
            entry,
            synthetic_result,
            now=now,
            retry_seconds=retry_seconds,
        )


def _review_queue_state_probe_candidates(
    entries: list[ReviewQueueEntry],
) -> list[ReviewQueueEntry]:
    """Return review entries whose PR state can be tracked cheaply via digest."""
    return [
        entry
        for entry in entries
        if entry.last_result in REVIEW_QUEUE_STABLE_RESULTS
    ]


def _partition_review_queue_entries_by_probe_change(
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
        if (
            entry.last_state_digest
            and entry.last_result in REVIEW_QUEUE_STABLE_RESULTS
            and digest
            and digest == entry.last_state_digest
        ):
            unchanged.append(entry)
        else:
            changed.append(entry)
    return changed, unchanged


def _wakeup_changed_review_queue_entries(
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
        for entry in _review_queue_state_probe_candidates(entries)
        if entry.last_state_digest and entry.next_attempt_datetime() > now
    ]
    if not candidates:
        return ()
    changed, _unchanged = _partition_review_queue_entries_by_probe_change(
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


def _build_review_snapshots_for_queue_entries(
    *,
    queue_entries: list[ReviewQueueEntry],
    review_refs: set[str],
    pr_port: PullRequestPort,
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


def _backfill_review_verdicts_from_snapshots(
    store: SessionStorePort,
    entries: list[ReviewQueueEntry],
    snapshots: dict[tuple[str, int], ReviewSnapshot],
    *,
    pr_port: PullRequestPort,
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
        if not _is_session_verdict_eligible(
            session_status=session.status,
            session_phase=session.phase,
            session_pr_url=session.pr_url,
        ):
            continue
        marker = _verdict_marker_text(session.id)
        seen_markers = posted_markers.setdefault(
            (entry.pr_repo, entry.pr_number),
            {
                body
                for body in snapshot.pr_comment_bodies
                if isinstance(body, str)
            },
        )
        if _marker_already_present(marker, seen_markers):
            continue
        try:
            if comment_poster is None and gh_runner is None:
                posted = pr_port.post_codex_verdict_if_missing(
                    session.pr_url,
                    session.id,
                )
            else:
                posted = _post_pr_codex_verdict(
                    session.pr_url,
                    session.id,
                    comment_checker=lambda *args, **kwargs: False,
                    comment_poster=comment_poster,
                    gh_runner=gh_runner,
                )
        except (GhQueryError, Exception) as err:
            logger.warning(
                "Review verdict backfill failed for %s (%s): %s",
                entry.issue_ref,
                session.id,
                err,
            )
            continue
        if posted:
            seen_markers.add(marker)
            backfilled.append(entry.issue_ref)
    return tuple(backfilled)


def _pre_backfill_verdicts_for_due_prs(
    store: SessionStorePort,
    due_items: list[ReviewQueueEntry],
    *,
    pr_port: PullRequestPort | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, ...]:
    """Post missing verdicts BEFORE snapshot build for verdict-blocked and newly seeded entries."""
    backfilled: list[str] = []
    for entry in due_items:
        if not _is_pre_backfill_eligible(
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
            if not _is_session_verdict_eligible(
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
                posted = _post_pr_codex_verdict(
                    session.pr_url,
                    session.id,
                    comment_checker=comment_checker,
                    comment_poster=comment_poster,
                    gh_runner=gh_runner,
                )
            if posted:
                backfilled.append(entry.issue_ref)
        except Exception as err:
            logger.warning(
                "Pre-backfill verdict failed for %s: %s",
                entry.issue_ref,
                err,
            )
            continue
    return tuple(backfilled)


def _review_scope_issue_refs(
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    board_snapshot: CycleBoardSnapshot,
) -> tuple[str, ...]:
    """Return governed review issue refs for the consumer executor."""
    refs: list[str] = []
    for snapshot in board_snapshot.items_with_status("Review"):
        issue_ref = _snapshot_to_issue_ref(snapshot, critical_path_config)
        if issue_ref is None:
            continue
        if parse_issue_ref(issue_ref).prefix not in config.repo_prefixes:
            continue
        if snapshot.executor.strip().lower() != config.executor:
            continue
        refs.append(issue_ref)
    return tuple(refs)


def _review_queue_next_attempt_at(
    *,
    now: datetime | None = None,
    delay_seconds: int = DEFAULT_REVIEW_QUEUE_RETRY_SECONDS,
) -> str:
    """Return the next scheduled review-queue attempt timestamp."""
    current = now or datetime.now(timezone.utc)
    return (current + timedelta(seconds=delay_seconds)).isoformat()


# _review_queue_retry_seconds_for_blocked_reason: imported from domain.review_queue_policy
# _review_queue_retry_seconds_for_skipped_reason: imported from domain.review_queue_policy
# _review_queue_retry_seconds_for_result: imported from domain.review_queue_policy


def _review_queue_retry_seconds_for_partial_failure(
    config: ConsumerConfig,
    error: str | None,
) -> int:
    """Return the retry delay after a partial review-queue failure.

    Thin wrapper: resolves reason code then delegates to domain function.
    """
    from startupai_controller.domain.review_queue_policy import (
        review_queue_retry_seconds_for_partial_failure,
    )
    reason_code = gh_reason_code(error) if error else None
    return review_queue_retry_seconds_for_partial_failure(
        config.rate_limit_cooldown_seconds,
        reason_code,
    )


def _queue_review_item(
    store: SessionStorePort,
    issue_ref: str,
    pr_url: str,
    *,
    session_id: str | None = None,
    now: datetime | None = None,
) -> ReviewQueueEntry | None:
    """Persist one review item for bounded follow-up processing."""
    parsed = _parse_pr_url(pr_url)
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


# ---------------------------------------------------------------------------
# Blocked-streak escalation policy: imported from domain.review_queue_policy
# _blocker_class, _escalation_ceiling_for_blocker_class, ESCALATION_CEILING_*
# ---------------------------------------------------------------------------


def _apply_review_queue_result(
    store: SessionStorePort,
    entry: ReviewQueueEntry,
    result: Any,
    *,
    now: datetime | None = None,
    retry_seconds: int | None = None,
    last_state_digest: str | None = None,
) -> bool:
    """Persist the outcome of processing one review-queue entry.

    Returns True when blocked-streak escalation is needed (caller handles).
    Requeued results are NOT handled here — see _drain_review_queue().
    """
    current = now or datetime.now(timezone.utc)
    effective_state_digest = (
        entry.last_state_digest if last_state_digest is None else last_state_digest
    )
    effective_retry_seconds = (
        retry_seconds
        if retry_seconds is not None
        else _review_queue_retry_seconds_for_result(result)
    )
    next_attempt_at = _review_queue_next_attempt_at(
        now=current,
        delay_seconds=effective_retry_seconds,
    )

    if result.requeued_refs:
        # Requeue handling is done entirely in _drain_review_queue()
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
        new_class, new_streak, needs_escalation = _blocked_streak_needs_escalation(
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


def _apply_review_queue_partial_failure(
    store: SessionStorePort,
    entries: list[ReviewQueueEntry],
    *,
    config: ConsumerConfig,
    error: str | None,
    now: datetime | None = None,
) -> None:
    """Back off queued review entries after a partial-failure cycle."""
    if not entries:
        return
    current = now or datetime.now(timezone.utc)
    next_attempt_at = _review_queue_next_attempt_at(
        now=current,
        delay_seconds=_review_queue_retry_seconds_for_partial_failure(config, error),
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


def _update_board_snapshot_statuses(
    board_snapshot: CycleBoardSnapshot,
    critical_path_config: CriticalPathConfig,
    status_updates: dict[str, str],
) -> CycleBoardSnapshot:
    """Return a new snapshot with the requested issue status overrides."""
    if not status_updates:
        return board_snapshot
    items: list[_ProjectItemSnapshot] = []
    for snapshot in board_snapshot.items:
        issue_ref = _snapshot_to_issue_ref(snapshot, critical_path_config)
        if issue_ref is None or issue_ref not in status_updates:
            items.append(snapshot)
            continue
        items.append(replace(snapshot, status=status_updates[issue_ref]))
    by_status: dict[str, list[_ProjectItemSnapshot]] = {}
    for snapshot in items:
        by_status.setdefault(snapshot.status, []).append(snapshot)
    return CycleBoardSnapshot(
        items=tuple(items),
        by_status={status: tuple(group) for status, group in by_status.items()},
    )


def _prepare_review_queue_batch(
    *,
    config: ConsumerConfig,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    board_snapshot: CycleBoardSnapshot,
    pr_port: PullRequestPort,
    now: datetime,
    dry_run: bool,
) -> tuple[PreparedReviewQueueBatch | None, ReviewQueueDrainSummary | None]:
    """Prepare the bounded review-queue workset for one drain cycle."""
    review_refs = frozenset(
        _review_scope_issue_refs(
            config,
            critical_path_config,
            board_snapshot,
        )
    )
    existing_entries = store.list_review_queue_items()
    existing_refs = {entry.issue_ref for entry in existing_entries}

    removed = tuple(
        _prune_stale_review_entries(store, review_refs, existing_entries, dry_run=dry_run)
    )
    seeded = tuple(
        _seed_new_review_entries(
            store,
            review_refs,
            existing_refs,
            dry_run=dry_run,
            now=now,
        )
    )
    if not dry_run:
        _reconcile_review_queue_identity(store, review_refs, now=now)

    queue_items = tuple(
        entry
        for entry in store.list_review_queue_items()
        if entry.issue_ref in review_refs
    )
    if not review_refs and not queue_items:
        return None, ReviewQueueDrainSummary(
            queued_count=0,
            due_count=0,
            removed=removed,
            skipped=("control-plane:no-review-items",),
        )

    try:
        _wakeup_changed_review_queue_entries(
            store,
            list(queue_items),
            now=now,
            pr_port=pr_port,
            dry_run=dry_run,
        )
    except GhQueryError as err:
        logger.warning("Review queue wakeup probe failed: %s", err)

    if not dry_run:
        queue_items = tuple(
            entry
            for entry in store.list_review_queue_items()
            if entry.issue_ref in review_refs
        )

    queue_pr_groups = dict(_group_review_queue_entries_by_pr(list(queue_items)))
    due_items = tuple(
        entry for entry in queue_items if entry.next_attempt_datetime() <= now
    )
    due_pr_groups_list = _group_review_queue_entries_by_pr(list(due_items))[
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
        PreparedReviewQueueBatch(
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


def _prepare_due_review_processing(
    *,
    store: SessionStorePort,
    automation_config: BoardAutomationConfig,
    pr_port: PullRequestPort,
    prepared_batch: PreparedReviewQueueBatch,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> PreparedDueReviewProcessing:
    """Prepare the changed due-review groups and snapshots for rescue processing."""
    due_items = list(prepared_batch.due_items)
    due_pr_groups = [
        (pr_key, list(entries)) for pr_key, entries in prepared_batch.due_pr_groups
    ]
    selected_snapshot_entries = list(prepared_batch.selected_snapshot_entries)

    pre_backfilled: tuple[str, ...] = ()
    if not dry_run and due_items:
        pre_backfilled = _pre_backfill_verdicts_for_due_prs(
            store,
            due_items,
            pr_port=pr_port,
            gh_runner=gh_runner,
        )

    verdict_backfilled: tuple[str, ...] = ()
    partial_failure = False
    error: str | None = None
    snapshots: dict[tuple[str, int], ReviewSnapshot] = {}

    if due_pr_groups:
        try:
            changed_due_items, unchanged_due_items = (
                _partition_review_queue_entries_by_probe_change(
                    due_items,
                    pr_port=pr_port,
                )
            )
            if unchanged_due_items and not dry_run:
                _repark_unchanged_review_queue_entries(
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
                snapshots = _build_review_snapshots_for_queue_entries(
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
            secondary_backfilled = _backfill_review_verdicts_from_snapshots(
                store,
                due_items,
                snapshots,
                pr_port=pr_port,
                gh_runner=gh_runner,
            )
            verdict_backfilled = tuple(
                dict.fromkeys(pre_backfilled + secondary_backfilled)
            )

    return PreparedDueReviewProcessing(
        due_items=tuple(due_items),
        due_pr_groups=tuple(
            (pr_key, tuple(entries)) for pr_key, entries in due_pr_groups
        ),
        snapshots=snapshots,
        verdict_backfilled=verdict_backfilled,
        partial_failure=partial_failure,
        error=error,
    )


def _process_due_review_group(
    *,
    config: ConsumerConfig,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    pr_port: PullRequestPort,
    pr_repo: str,
    pr_number: int,
    entries: tuple[ReviewQueueEntry, ...],
    snapshot: ReviewSnapshot | None,
    updated_snapshot: CycleBoardSnapshot,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> ReviewGroupProcessingOutcome:
    """Process one due PR group from the review queue."""
    if snapshot is None:
        return ReviewGroupProcessingOutcome(
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

    try:
        result = review_rescue(
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
    except GhQueryError as err:
        return ReviewGroupProcessingOutcome(
            rerun=(),
            auto_merge_enabled=(),
            requeued=(),
            blocked=(),
            skipped=(),
            escalated=(),
            updated_snapshot=updated_snapshot,
            partial_failure=True,
            error=str(err),
        )

    escalated: list[str] = []
    requeued: list[str] = []
    blocked: list[str] = []
    skipped: list[str] = []
    rerun: list[str] = []
    auto_merge_enabled: list[str] = []

    if not dry_run:
        state_digest = pr_port.review_state_digests([(pr_repo, pr_number)]).get(
            (pr_repo, pr_number)
        )
        for entry in entries:
            needs_escalation = _apply_review_queue_result(
                store,
                entry,
                result,
                now=now,
                last_state_digest=state_digest,
            )
            if needs_escalation:
                _escalate_to_claude(
                    entry.issue_ref,
                    critical_path_config,
                    config.project_owner,
                    config.project_number,
                    reason=f"review queue blocked escalation: {result.blocked_reason}",
                    gh_runner=gh_runner,
                )
                store.delete_review_queue_item(entry.issue_ref)
                escalated.append(entry.issue_ref)

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
            if _requeue_or_escalate(requeue_count) == "escalate":
                if not dry_run:
                    _escalate_to_claude(
                        issue_ref,
                        critical_path_config,
                        config.project_owner,
                        config.project_number,
                        reason=f"repair requeue ceiling ({requeue_count} cycles on same PR): "
                        f"{result.blocked_reason or 'persistent check failure / conflict'}",
                        gh_runner=gh_runner,
                    )
                    store.delete_review_queue_item(issue_ref)
                escalated.append(issue_ref)
            else:
                if not dry_run:
                    store.increment_requeue_count(issue_ref, pr_url)
                    store.delete_review_queue_item(issue_ref)
                requeued.append(issue_ref)
        requeued_this_group = [ref for ref in result.requeued_refs if ref not in escalated]
        if requeued_this_group:
            updated_snapshot = _update_board_snapshot_statuses(
                updated_snapshot,
                critical_path_config,
                {ref: "Ready" for ref in requeued_this_group},
            )
    elif result.blocked_reason:
        blocked.append(f"{pr_ref}:{result.blocked_reason}")
    elif result.skipped_reason:
        skipped.append(f"{pr_ref}:{result.skipped_reason}")

    return ReviewGroupProcessingOutcome(
        rerun=tuple(rerun),
        auto_merge_enabled=tuple(auto_merge_enabled),
        requeued=tuple(requeued),
        blocked=tuple(blocked),
        skipped=tuple(skipped),
        escalated=tuple(escalated),
        updated_snapshot=updated_snapshot,
    )


def _process_review_queue_due_groups(
    *,
    config: ConsumerConfig,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    pr_port: PullRequestPort,
    prepared_batch: PreparedReviewQueueBatch,
    board_snapshot: CycleBoardSnapshot,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None = None,
) -> ReviewQueueProcessingOutcome:
    """Process the due PR groups for a prepared review-queue batch."""
    prepared_due_processing = _prepare_due_review_processing(
        store=store,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
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
        group_outcome = _process_due_review_group(
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
        _apply_review_queue_partial_failure(
            store,
            list(prepared_due_processing.due_items),
            config=config,
            error=error,
            now=now,
        )

    return ReviewQueueProcessingOutcome(
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


# ---------------------------------------------------------------------------
# _drain_review_queue sub-functions: focused extraction from god-function
# ---------------------------------------------------------------------------


def _prune_stale_review_entries(
    store: SessionStorePort,
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


def _seed_new_review_entries(
    store: SessionStorePort,
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
            if _queue_review_item(
                store,
                issue_ref,
                latest_session.pr_url,
                session_id=latest_session.id,
                now=now,
            ) is None:
                continue
        seeded.append(issue_ref)
    return seeded


def _reconcile_review_queue_identity(
    store: SessionStorePort,
    review_refs: set[str],
    *,
    now: datetime | None = None,
) -> None:
    """Reconcile queue rows and requeue counters against current PR identity.

    When the active session's PR URL changes, update the queue entry and
    reset the requeue counter to avoid false escalation.
    """
    for entry in store.list_review_queue_items():
        if entry.issue_ref not in review_refs:
            continue
        current_pr_url = entry.pr_url
        latest_session = store.latest_session_for_issue(entry.issue_ref)
        if latest_session is not None and latest_session.pr_url:
            current_pr_url = latest_session.pr_url
        if current_pr_url != entry.pr_url:
            parsed = _parse_pr_url(current_pr_url)
            if parsed is None:
                continue
            owner, repo, pr_number = parsed
            store.enqueue_review_item(
                entry.issue_ref,
                pr_url=current_pr_url,
                pr_repo=f"{owner}/{repo}",
                pr_number=pr_number,
                source_session_id=(
                    latest_session.id if latest_session is not None else entry.source_session_id
                ),
                next_attempt_at=entry.next_attempt_at,
                now=now,
            )
        _count, stored_pr_url = store.get_requeue_state(entry.issue_ref)
        if stored_pr_url is not None and stored_pr_url != current_pr_url:
            store.reset_requeue_count(entry.issue_ref)


def _drain_review_queue(
    config: ConsumerConfig,
    db: ConsumerDB,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    *,
    pr_port: PullRequestPort | None = None,
    session_store: SessionStorePort | None = None,
    board_snapshot: CycleBoardSnapshot,
    dry_run: bool = False,
    github_memo: CycleGitHubMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[ReviewQueueDrainSummary, CycleBoardSnapshot]:
    """Process a bounded batch of queued Review items."""
    if automation_config is None:
        return ReviewQueueDrainSummary(), board_snapshot

    store = session_store or SqliteSessionStore(db)
    now = datetime.now(timezone.utc)
    memo = github_memo or CycleGitHubMemo()
    effective_pr_port = pr_port or GitHubCliAdapter(
        project_owner=config.project_owner,
        project_number=config.project_number,
        config=critical_path_config,
        github_memo=memo,
        gh_runner=gh_runner,
    )

    prepared_batch, empty_summary = _prepare_review_queue_batch(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        board_snapshot=board_snapshot,
        pr_port=effective_pr_port,
        now=now,
        dry_run=dry_run,
    )
    if empty_summary is not None:
        return empty_summary, board_snapshot
    assert prepared_batch is not None

    processing_outcome = _process_review_queue_due_groups(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=effective_pr_port,
        prepared_batch=prepared_batch,
        board_snapshot=board_snapshot,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
    )

    summary = ReviewQueueDrainSummary(
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


def _replay_deferred_actions(
    db: ConsumerDB,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    *,
    pr_port: PullRequestPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[int, ...]:
    """Replay queued control-plane actions after GitHub recovery."""
    replayed: list[int] = []
    for action in db.list_deferred_actions():
        payload = action.payload
        try:
            if action.action_type == "set_status":
                issue_ref = str(payload["issue_ref"])
                to_status = str(payload["to_status"])
                from_statuses = {
                    str(value) for value in payload.get("from_statuses", [])
                }
                blocked_reason = payload.get("blocked_reason")
                if to_status == "Blocked":
                    _set_blocked_with_reason(
                        issue_ref,
                        str(blocked_reason or "deferred-control-plane"),
                        critical_path_config,
                        config.project_owner,
                        config.project_number,
                        gh_runner=gh_runner,
                    )
                elif to_status == "Review":
                    _transition_issue_to_review(
                        issue_ref,
                        critical_path_config,
                        config.project_owner,
                        config.project_number,
                        board_info_resolver=board_info_resolver,
                        board_mutator=board_mutator,
                        gh_runner=gh_runner,
                    )
                elif to_status == "In Progress":
                    _transition_issue_to_in_progress(
                        issue_ref,
                        critical_path_config,
                        config.project_owner,
                        config.project_number,
                        from_statuses=from_statuses,
                        review_state_port=review_state_port,
                        board_port=board_port,
                        board_info_resolver=board_info_resolver,
                        board_mutator=board_mutator,
                        gh_runner=gh_runner,
                    )
                elif to_status == "Ready":
                    _return_issue_to_ready(
                        issue_ref,
                        critical_path_config,
                        config.project_owner,
                        config.project_number,
                        from_statuses=from_statuses,
                        review_state_port=review_state_port,
                        board_port=board_port,
                        board_info_resolver=board_info_resolver,
                        board_mutator=board_mutator,
                        gh_runner=gh_runner,
                    )
                else:
                    raise GhQueryError(
                        f"Unsupported deferred status target: {to_status}"
                    )
            elif action.action_type == "post_verdict_marker":
                _post_pr_codex_verdict(
                    str(payload["pr_url"]),
                    str(payload["session_id"]),
                    comment_checker=comment_checker,
                    comment_poster=comment_poster,
                    gh_runner=gh_runner,
                )
            elif action.action_type == "post_issue_comment":
                issue_ref = str(payload["issue_ref"])
                owner, repo, number = _resolve_issue_coordinates(
                    issue_ref,
                    critical_path_config,
                )
                if board_port is not None:
                    board_port.post_issue_comment(
                        f"{owner}/{repo}",
                        number,
                        str(payload["body"]),
                    )
                else:
                    poster = comment_poster or _post_comment
                    poster(
                        owner,
                        repo,
                        number,
                        str(payload["body"]),
                        gh_runner=gh_runner,
                    )
            elif action.action_type == "close_issue":
                issue_ref = str(payload["issue_ref"])
                owner, repo, number = _resolve_issue_coordinates(
                    issue_ref,
                    critical_path_config,
                )
                if board_port is not None:
                    board_port.close_issue(f"{owner}/{repo}", number)
                else:
                    close_issue(owner, repo, number, gh_runner=gh_runner)
            elif action.action_type == "rerun_check":
                if pr_port is not None:
                    if not pr_port.rerun_failed_check(
                        str(payload["pr_repo"]),
                        str(payload.get("check_name") or ""),
                        int(payload["run_id"]),
                    ):
                        raise GhQueryError(
                            f"Failed rerunning check for {payload['pr_repo']} run {payload['run_id']}"
                        )
                else:
                    rerun_actions_run(
                        str(payload["pr_repo"]),
                        int(payload["run_id"]),
                        gh_runner=gh_runner,
                    )
            elif action.action_type == "enable_automerge":
                if pr_port is not None:
                    pr_port.enable_automerge(
                        str(payload["pr_repo"]),
                        int(payload["pr_number"]),
                    )
                else:
                    enable_pull_request_automerge(
                        str(payload["pr_repo"]),
                        int(payload["pr_number"]),
                        gh_runner=gh_runner,
                    )
            else:
                raise GhQueryError(
                    f"Unsupported deferred action type: {action.action_type}"
                )
        except GhQueryError:
            raise
        except Exception as error:
            raise GhQueryError(
                f"Deferred action {action.id} failed: {error}"
            ) from error

        db.delete_deferred_action(action.id)
        _record_successful_github_mutation(db)
        replayed.append(action.id)

    if replayed:
        _clear_degraded(db)
    return tuple(replayed)


# ---------------------------------------------------------------------------
# Helper: advance board state after successful PR creation
# ---------------------------------------------------------------------------


def _transition_issue_to_review(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Move a successfully submitted issue from In Progress to Review."""
    code, message = sync_review_state(
        event_kind="pr_ready_for_review",
        issue_ref=issue_ref,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    if code in (0, 2):
        return
    raise GhQueryError(f"Failed moving {issue_ref} to Review: {message}")


def _transition_issue_to_in_progress(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    from_statuses: set[str] | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Move an actively running local repair back into In Progress."""
    if review_state_port is not None and board_port is not None:
        old_status = review_state_port.get_issue_status(issue_ref)
        if old_status in (from_statuses or {"Review"}):
            board_port.set_issue_status(issue_ref, "In Progress")
            changed = True
        else:
            changed = False
    else:
        changed, old_status = _set_status_if_changed(
            issue_ref,
            from_statuses or {"Review"},
            "In Progress",
            config,
            project_owner,
            project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
    if changed or old_status == "In Progress":
        return
    raise GhQueryError(
        f"Failed moving {issue_ref} to In Progress: current status={old_status}"
    )


def _return_issue_to_ready(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    from_statuses: set[str] | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Move a non-running claimed issue back to Ready so the lane stays truthful."""
    if review_state_port is not None and board_port is not None:
        old_status = review_state_port.get_issue_status(issue_ref)
        if old_status in (from_statuses or {"In Progress"}):
            board_port.set_issue_status(issue_ref, "Ready")
            changed = True
        else:
            changed = False
    else:
        changed, old_status = _set_status_if_changed(
            issue_ref,
            from_statuses or {"In Progress"},
            "Ready",
            config,
            project_owner,
            project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
    if changed or old_status == "Ready":
        return
    raise GhQueryError(
        f"Failed moving {issue_ref} back to Ready: current status={old_status}"
    )


def _reconcile_active_repair_review_items(
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    *,
    active_repair_issue_refs: set[str],
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_snapshot: CycleBoardSnapshot | None,
    issue_ref_for_snapshot: Callable[[_ProjectItemSnapshot | IssueSnapshot], str | None],
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    dry_run: bool,
) -> list[str]:
    """Return active repair items that should move from Review back to In Progress."""
    moved_in_progress: list[str] = []
    review_items = (
        board_snapshot.items_with_status("Review")
        if board_snapshot is not None
        else review_state_port.list_issues_by_status("Review")
    )
    for snapshot in review_items:
        issue_ref = issue_ref_for_snapshot(snapshot)
        if issue_ref is None:
            continue
        parsed = parse_issue_ref(issue_ref)
        if parsed.prefix not in consumer_config.repo_prefixes:
            continue
        if snapshot.executor.strip().lower() != consumer_config.executor:
            continue
        if issue_ref not in active_repair_issue_refs:
            continue

        if not dry_run:
            _transition_issue_to_in_progress(
                issue_ref,
                critical_path_config,
                consumer_config.project_owner,
                consumer_config.project_number,
                review_state_port=review_state_port,
                board_port=board_port,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                gh_runner=gh_runner,
            )
        moved_in_progress.append(issue_ref)
    return moved_in_progress


def _reconcile_single_in_progress_item(
    issue_ref: str,
    *,
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    store: SessionStorePort,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    dry_run: bool,
) -> str:
    """Reconcile one stale In Progress item and return its target lane."""
    owner, repo, number = _resolve_issue_coordinates(issue_ref, critical_path_config)
    latest_session = store.latest_session_for_issue(issue_ref)
    expected_branch = latest_session.branch_name if latest_session else None
    classification, pr_match, reason = _classify_open_pr_candidates(
        issue_ref,
        owner,
        repo,
        number,
        automation_config,
        expected_branch=expected_branch,
        pr_port=pr_port,
        gh_runner=gh_runner,
    )
    target = reconcile_in_progress_decision(
        classification,
        has_latest_session=latest_session is not None,
        session_kind=latest_session.session_kind if latest_session else None,
        session_status=latest_session.status if latest_session else None,
    )

    if target == "ready":
        if classification == "adoptable" and pr_match is not None:
            if latest_session is not None and not dry_run:
                store.update_session(latest_session.id, pr_url=pr_match.url)
        if not dry_run:
            _return_issue_to_ready(
                issue_ref,
                critical_path_config,
                consumer_config.project_owner,
                consumer_config.project_number,
                from_statuses={"In Progress"},
                review_state_port=review_state_port,
                board_port=board_port,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                gh_runner=gh_runner,
            )
        return "ready"

    if target == "review":
        if latest_session is not None and pr_match is not None and not dry_run:
            store.update_session(
                latest_session.id,
                pr_url=pr_match.url,
                phase="review",
            )
        if not dry_run:
            _transition_issue_to_review(
                issue_ref,
                critical_path_config,
                consumer_config.project_owner,
                consumer_config.project_number,
                review_state_port=review_state_port,
                board_port=board_port,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                gh_runner=gh_runner,
            )
        return "review"

    if not dry_run:
        _set_blocked_with_reason(
            issue_ref,
            f"execution-authority:{reason}",
            critical_path_config,
            consumer_config.project_owner,
            consumer_config.project_number,
            review_state_port=review_state_port,
            board_port=board_port,
            gh_runner=gh_runner,
        )
    return "blocked"


def _reconcile_stale_in_progress_items(
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    *,
    store: SessionStorePort,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    board_snapshot: CycleBoardSnapshot | None,
    issue_ref_for_snapshot: Callable[[_ProjectItemSnapshot | IssueSnapshot], str | None],
    active_issue_refs: set[str],
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    dry_run: bool,
) -> tuple[list[str], list[str], list[str]]:
    """Reconcile stale In Progress items back to their truthful lanes."""
    moved_ready: list[str] = []
    moved_review: list[str] = []
    moved_blocked: list[str] = []
    in_progress = (
        board_snapshot.items_with_status("In Progress")
        if board_snapshot is not None
        else review_state_port.list_issues_by_status("In Progress")
    )
    for snapshot in in_progress:
        issue_ref = issue_ref_for_snapshot(snapshot)
        if issue_ref is None:
            continue
        parsed = parse_issue_ref(issue_ref)
        if parsed.prefix not in consumer_config.repo_prefixes:
            continue
        if snapshot.executor.strip().lower() != consumer_config.executor:
            continue
        if issue_ref in active_issue_refs:
            continue

        target = _reconcile_single_in_progress_item(
            issue_ref,
            consumer_config=consumer_config,
            critical_path_config=critical_path_config,
            automation_config=automation_config,
            store=store,
            pr_port=pr_port,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
            dry_run=dry_run,
        )
        if target == "ready":
            moved_ready.append(issue_ref)
        elif target == "review":
            moved_review.append(issue_ref)
        else:
            moved_blocked.append(issue_ref)
    return moved_ready, moved_review, moved_blocked


def _recover_interrupted_sessions(
    config: ConsumerConfig,
    db: ConsumerDB,
    *,
    automation_config: BoardAutomationConfig | None = None,
    pr_port: PullRequestPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[RecoveredLease]:
    """Recover leases left behind by a previous interrupted daemon process."""
    recovered = db.recover_interrupted_leases()
    if not recovered:
        return []

    try:
        cp_config = load_config(config.critical_paths_path)
    except ConfigError as err:
        logger.error("Interrupted-session recovery skipped: %s", err)
        return recovered
    effective_pr_port = pr_port or GitHubCliAdapter(
        project_owner=config.project_owner,
        project_number=config.project_number,
        config=cp_config,
        gh_runner=gh_runner,
    )
    effective_review_state_port = review_state_port or effective_pr_port
    effective_board_port = board_port or effective_pr_port

    for lease in recovered:
        try:
            owner, repo, number = _resolve_issue_coordinates(lease.issue_ref, cp_config)
            try:
                classification, pr_match, _reason = _classify_open_pr_candidates(
                    lease.issue_ref,
                    owner,
                    repo,
                    number,
                    automation_config or load_automation_config(config.automation_config_path),
                    expected_branch=lease.branch_name,
                    pr_port=effective_pr_port,
                    gh_runner=gh_runner,
                )
            except GhQueryError:
                classification, pr_match = ("none", None)
            pr_url = lease.pr_url or (pr_match.url if pr_match is not None else None)
            if lease.session_kind == "repair":
                _return_issue_to_ready(
                    lease.issue_ref,
                    cp_config,
                    config.project_owner,
                    config.project_number,
                    from_statuses={"In Progress", "Review"},
                    review_state_port=effective_review_state_port,
                    board_port=effective_board_port,
                    board_info_resolver=board_info_resolver,
                    board_mutator=board_mutator,
                    gh_runner=gh_runner,
                )
            elif pr_url or classification == "adoptable":
                _transition_issue_to_review(
                    lease.issue_ref,
                    cp_config,
                    config.project_owner,
                    config.project_number,
                    review_state_port=effective_review_state_port,
                    board_port=effective_board_port,
                    board_info_resolver=board_info_resolver,
                    board_mutator=board_mutator,
                    gh_runner=gh_runner,
                )
            elif classification == "none":
                _return_issue_to_ready(
                    lease.issue_ref,
                    cp_config,
                    config.project_owner,
                    config.project_number,
                    review_state_port=effective_review_state_port,
                    board_port=effective_board_port,
                    board_info_resolver=board_info_resolver,
                    board_mutator=board_mutator,
                    gh_runner=gh_runner,
                )
            else:
                _set_blocked_with_reason(
                    lease.issue_ref,
                    f"execution-authority:{classification}",
                    cp_config,
                    config.project_owner,
                    config.project_number,
                    review_state_port=effective_review_state_port,
                    board_port=effective_board_port,
                    gh_runner=gh_runner,
                )
        except (GhQueryError, Exception) as err:
            logger.error(
                "Interrupted-session board recovery failed for %s: %s",
                lease.issue_ref,
                err,
            )

    return recovered


def _initialize_cycle_runtime(
    config: ConsumerConfig,
    db: ConsumerDB,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> CycleRuntimeContext:
    """Build cycle-scoped runtime wiring and effective config."""
    session_store = SqliteSessionStore(db)

    cp_config = load_config(config.critical_paths_path)
    try:
        auto_config = load_automation_config(config.automation_config_path)
    except ConfigError as err:
        logger.warning("Automation config error (proceeding without): %s", err)
        auto_config = None
    _apply_automation_runtime(config, auto_config)

    main_workflows, workflow_statuses, effective_interval = _current_main_workflows(
        config
    )
    dispatchable_repo_prefixes = tuple(
        repo_prefix
        for repo_prefix in config.repo_prefixes
        if workflow_statuses[repo_prefix].available
    )
    config.poll_interval_seconds = effective_interval
    github_memo = CycleGitHubMemo()
    pr_port = GitHubCliAdapter(
        project_owner=config.project_owner,
        project_number=config.project_number,
        config=cp_config,
        github_memo=github_memo,
        gh_runner=gh_runner,
    )
    global_limit = (
        auto_config.global_concurrency
        if auto_config is not None
        else config.global_concurrency
    )

    return CycleRuntimeContext(
        session_store=session_store,
        cp_config=cp_config,
        auto_config=auto_config,
        main_workflows=main_workflows,
        workflow_statuses=workflow_statuses,
        dispatchable_repo_prefixes=dispatchable_repo_prefixes,
        effective_interval=effective_interval,
        global_limit=global_limit,
        github_memo=github_memo,
        pr_port=pr_port,
    )


def _run_deferred_replay_phase(
    config: ConsumerConfig,
    db: ConsumerDB,
    runtime: CycleRuntimeContext,
    *,
    timings_ms: dict[str, int],
    board_info_resolver: Callable | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    dry_run: bool,
) -> None:
    """Replay deferred actions for the cycle when enabled."""
    if not config.deferred_replay_enabled or dry_run:
        return
    phase_started = time.monotonic()
    replayed_actions = _replay_deferred_actions(
        db,
        config,
        runtime.cp_config,
        pr_port=runtime.pr_port,
        review_state_port=runtime.pr_port,
        board_port=runtime.pr_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )
    timings_ms["deferred_replay"] = int((time.monotonic() - phase_started) * 1000)
    if replayed_actions:
        logger.info("Replayed deferred control-plane actions: %s", replayed_actions)


def _load_board_snapshot_phase(
    config: ConsumerConfig,
    *,
    timings_ms: dict[str, int],
    gh_runner: Callable[..., str] | None,
) -> CycleBoardSnapshot:
    """Load the cycle board snapshot."""
    phase_started = time.monotonic()
    board_snapshot = build_cycle_board_snapshot(
        config.project_owner,
        config.project_number,
        gh_runner=gh_runner,
    )
    timings_ms["board_snapshot"] = int((time.monotonic() - phase_started) * 1000)
    return board_snapshot


def _run_executor_routing_phase(
    config: ConsumerConfig,
    db: ConsumerDB,
    runtime: CycleRuntimeContext,
    *,
    board_snapshot: CycleBoardSnapshot,
    timings_ms: dict[str, int],
    gh_runner: Callable[..., str] | None,
    dry_run: bool,
) -> None:
    """Normalize executor routing for the protected queue."""
    phase_started = time.monotonic()
    routing_decision = route_protected_queue_executors(
        runtime.cp_config,
        runtime.auto_config,
        config.project_owner,
        config.project_number,
        dry_run=dry_run,
        board_snapshot=board_snapshot,
        gh_runner=gh_runner,
    )
    timings_ms["executor_routing"] = int((time.monotonic() - phase_started) * 1000)
    if routing_decision.routed:
        if not dry_run:
            _record_successful_github_mutation(db)
        logger.info(
            "Executor routing normalized protected queue: %s",
            routing_decision.routed,
        )


def _run_reconciliation_phase(
    config: ConsumerConfig,
    db: ConsumerDB,
    runtime: CycleRuntimeContext,
    *,
    board_snapshot: CycleBoardSnapshot,
    timings_ms: dict[str, int],
    board_info_resolver: Callable | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Run truthful board reconciliation for the cycle."""
    phase_started = time.monotonic()
    reconciliation = _reconcile_board_truth(
        config,
        runtime.cp_config,
        runtime.auto_config,
        db,
        session_store=runtime.session_store,
        pr_port=runtime.pr_port,
        review_state_port=runtime.pr_port,
        board_port=runtime.pr_port,
        board_snapshot=board_snapshot,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    timings_ms["reconciliation"] = int((time.monotonic() - phase_started) * 1000)
    _record_successful_board_sync(db)
    _clear_degraded(db)
    if (
        reconciliation.moved_ready
        or reconciliation.moved_in_progress
        or reconciliation.moved_review
        or reconciliation.moved_blocked
    ):
        logger.info("Board reconciliation: %s", reconciliation)


def _run_review_queue_phase(
    config: ConsumerConfig,
    db: ConsumerDB,
    runtime: CycleRuntimeContext,
    *,
    board_snapshot: CycleBoardSnapshot,
    timings_ms: dict[str, int],
    gh_runner: Callable[..., str] | None,
    dry_run: bool,
) -> tuple[ReviewQueueDrainSummary, CycleBoardSnapshot]:
    """Drain the review queue for the current cycle."""
    phase_started = time.monotonic()
    review_queue_summary, updated_snapshot = _drain_review_queue(
        config,
        db,
        runtime.cp_config,
        runtime.auto_config,
        pr_port=runtime.pr_port,
        session_store=runtime.session_store,
        board_snapshot=board_snapshot,
        dry_run=dry_run,
        github_memo=runtime.github_memo,
        gh_runner=gh_runner,
    )
    timings_ms["review_queue"] = int((time.monotonic() - phase_started) * 1000)
    if review_queue_summary.error:
        logger.warning("Review queue partial failure: %s", review_queue_summary.error)
        if not dry_run:
            _mark_degraded(
                db,
                f"review-queue:partial-failure:{review_queue_summary.error}",
            )
    if (
        review_queue_summary.seeded
        or review_queue_summary.removed
        or review_queue_summary.verdict_backfilled
        or review_queue_summary.rerun
        or review_queue_summary.auto_merge_enabled
        or review_queue_summary.requeued
        or review_queue_summary.blocked
        or review_queue_summary.skipped
    ):
        logger.info("Review queue: %s", review_queue_summary)
    return review_queue_summary, updated_snapshot


def _run_admission_phase(
    config: ConsumerConfig,
    db: ConsumerDB,
    runtime: CycleRuntimeContext,
    *,
    board_snapshot: CycleBoardSnapshot,
    timings_ms: dict[str, int],
    gh_runner: Callable[..., str] | None,
    dry_run: bool,
) -> dict[str, Any]:
    """Run backlog admission for the current cycle."""
    phase_started = time.monotonic()
    admission_decision = admit_backlog_items(
        runtime.cp_config,
        runtime.auto_config,
        config.project_owner,
        config.project_number,
        dispatchable_repo_prefixes=runtime.dispatchable_repo_prefixes,
        active_lease_issue_refs=tuple(db.active_lease_issue_refs()),
        dry_run=dry_run,
        board_snapshot=board_snapshot,
        github_memo=runtime.github_memo,
        gh_runner=gh_runner,
    )
    timings_ms["admission"] = int((time.monotonic() - phase_started) * 1000)
    admission_summary = admission_summary_payload(
        admission_decision,
        enabled=bool(
            runtime.auto_config is not None and runtime.auto_config.admission.enabled
        ),
    )
    if admission_decision.admitted:
        if not dry_run:
            _record_successful_github_mutation(db)
        logger.info("Backlog admission admitted: %s", list(admission_decision.admitted))
    if admission_decision.partial_failure and admission_decision.error:
        logger.warning("Backlog admission partial failure: %s", admission_decision.error)
    if not dry_run:
        _persist_admission_summary(db, admission_summary)
    return admission_summary


def _execute_prepare_cycle_phases(
    config: ConsumerConfig,
    db: ConsumerDB,
    runtime: CycleRuntimeContext,
    *,
    dry_run: bool = False,
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[CycleBoardSnapshot, ReviewQueueDrainSummary, dict[str, Any], dict[str, int]]:
    """Execute the preflight phases for one cycle."""
    timings_ms: dict[str, int] = {}

    _run_deferred_replay_phase(
        config,
        db,
        runtime,
        timings_ms=timings_ms,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )
    board_snapshot = _load_board_snapshot_phase(
        config,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
    )
    _run_executor_routing_phase(
        config,
        db,
        runtime,
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )
    _run_reconciliation_phase(
        config,
        db,
        runtime,
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    review_queue_summary, board_snapshot = _run_review_queue_phase(
        config,
        db,
        runtime,
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )
    admission_summary = _run_admission_phase(
        config,
        db,
        runtime,
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )

    return board_snapshot, review_queue_summary, admission_summary, timings_ms


def _prepare_cycle(
    config: ConsumerConfig,
    db: ConsumerDB,
    *,
    dry_run: bool = False,
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> PreparedCycleContext:
    """Run control-plane preflight once for a daemon tick."""
    request_stats_token = begin_request_stats()
    expired = db.expire_stale_leases(config.heartbeat_expiry_seconds)
    if expired:
        logger.info("Expired stale leases: %s", expired)

    runtime = _initialize_cycle_runtime(
        config,
        db,
        gh_runner=gh_runner,
    )
    (
        board_snapshot,
        review_queue_summary,
        admission_summary,
        timings_ms,
    ) = _execute_prepare_cycle_phases(
        config,
        db,
        runtime,
        dry_run=dry_run,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )

    request_stats = end_request_stats(request_stats_token)
    github_request_counts = {
        "graphql": request_stats.graphql,
        "rest": request_stats.rest,
    }
    total_preflight_ms = sum(timings_ms.values())
    if total_preflight_ms >= 5000:
        logger.info(
            "Preflight timings ms=%s github_requests=%s",
            timings_ms,
            github_request_counts,
        )
    if not dry_run:
        ready_for_executor = 0
        for snapshot in board_snapshot.items_with_status("Ready"):
            if snapshot.executor.strip().lower() != config.executor:
                continue
            issue_ref = _snapshot_to_issue_ref(snapshot, runtime.cp_config)
            if issue_ref is None:
                continue
            if (
                parse_issue_ref(issue_ref).prefix
                not in runtime.dispatchable_repo_prefixes
            ):
                continue
            ready_for_executor += 1
        _record_metric(
            db,
            config,
            "cycle_observation",
            payload={
                "ready_for_executor": ready_for_executor,
                "active_leases": db.active_lease_count(),
                "global_limit": runtime.global_limit,
                "degraded": db.get_control_value(CONTROL_KEY_DEGRADED) == "true",
            },
        )

    return PreparedCycleContext(
        cp_config=runtime.cp_config,
        auto_config=runtime.auto_config,
        main_workflows=runtime.main_workflows,
        workflow_statuses=runtime.workflow_statuses,
        dispatchable_repo_prefixes=runtime.dispatchable_repo_prefixes,
        effective_interval=runtime.effective_interval,
        global_limit=runtime.global_limit,
        board_snapshot=board_snapshot,
        github_memo=runtime.github_memo,
        admission_summary=admission_summary,
        review_queue_summary=review_queue_summary,
        timings_ms=timings_ms,
        github_request_counts=github_request_counts,
    )


def _select_candidate_for_cycle(
    config: ConsumerConfig,
    db: ConsumerDB,
    prepared: PreparedCycleContext,
    *,
    target_issue: str | None = None,
    status_resolver: Callable[..., str] | None = None,
    gh_runner: Callable[..., str] | None = None,
    excluded_issue_refs: set[str] | None = None,
) -> str | None:
    """Select the next eligible issue for one slot in this cycle."""
    excluded = excluded_issue_refs or set()

    def issue_filter(issue_ref: str) -> bool:
        if issue_ref in excluded:
            return False
        repo_prefix = parse_issue_ref(issue_ref).prefix
        workflow = prepared.main_workflows.get(repo_prefix)
        if workflow is None:
            return False
        base_seconds, max_seconds = _effective_retry_backoff(
            config,
            workflow,
        )
        return not _retry_backoff_active(
            db,
            issue_ref,
            base_seconds=base_seconds,
            max_seconds=max_seconds,
        )

    if target_issue:
        if target_issue in excluded:
            return None
        return target_issue

    if not prepared.dispatchable_repo_prefixes:
        return None

    return _select_best_candidate(
        prepared.cp_config,
        config.project_owner,
        config.project_number,
        executor=config.executor,
        repo_prefixes=prepared.dispatchable_repo_prefixes,
        automation_config=prepared.auto_config,
        status_resolver=status_resolver,
        ready_items=prepared.board_snapshot.items_with_status("Ready"),
        github_memo=prepared.github_memo,
        gh_runner=gh_runner,
        issue_filter=issue_filter,
    )


# ---------------------------------------------------------------------------
# Helper: escalate to claude
# ---------------------------------------------------------------------------


def _escalate_to_claude(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    reason: str = "",
    *,
    board_info_resolver: Callable | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Block the issue for Claude handoff and post one escalation comment."""
    from startupai_controller.adapters.github_cli import _set_single_select_field
    from startupai_controller.promote_ready import _query_issue_board_info

    marker = _marker_for("consumer-escalation", issue_ref)
    owner, repo, number = _resolve_issue_coordinates(issue_ref, config)
    blocked_reason = reason or "max retries exceeded"

    _set_blocked_with_reason(
        issue_ref,
        blocked_reason,
        config,
        project_owner,
        project_number,
        board_info_resolver=board_info_resolver,
        gh_runner=gh_runner,
    )

    # Set Handoff To field
    resolve_info = board_info_resolver or _query_issue_board_info
    info = resolve_info(issue_ref, config, project_owner, project_number)

    if info.status != "NOT_ON_BOARD":
        try:
            _set_single_select_field(
                info.project_id,
                info.item_id,
                "Handoff To",
                "claude",
                gh_runner=gh_runner,
            )
        except (GhQueryError, Exception):
            logger.warning("Failed to set Handoff To field for %s", issue_ref)

    checker = comment_checker or _comment_exists
    if checker(owner, repo, number, marker, gh_runner=gh_runner):
        return

    # Post escalation comment
    body = (
        f"{marker}\n"
        f"**Escalation**: codex execution exhausted retries.\n\n"
        f"Reason: {blocked_reason}\n\n"
        f"Handoff To: `claude`"
    )
    poster = comment_poster or _post_comment
    poster(owner, repo, number, body, gh_runner=gh_runner)


# ---------------------------------------------------------------------------
# Helper: build dependency summary
# ---------------------------------------------------------------------------


def _build_dependency_summary(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    status_resolver: Callable[..., str] | None = None,
) -> str:
    """Build a human-readable dependency summary for the codex prompt."""
    from startupai_controller.validate_critical_path_promotion import direct_predecessors

    if not in_any_critical_path(config, issue_ref):
        return "(Not in critical path.)"

    preds = direct_predecessors(config, issue_ref)
    if not preds:
        return "(No predecessors.)"

    lines = []
    for pred in sorted(preds):
        lines.append(f"- {pred} (Done)")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helper: check for commits in worktree
# ---------------------------------------------------------------------------


def _has_commits_on_branch(
    worktree_path: str,
    branch: str,
    *,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> bool:
    """Check if there are commits on branch beyond main."""
    runner = subprocess_runner or (
        lambda args, **kw: subprocess.run(args, **kw)
    )
    result = runner(
        ["git", "-C", worktree_path, "log", "main..HEAD", "--oneline"],
        capture_output=True,
        text=True,
    )
    return bool(result.stdout.strip())


def _next_available_slot(db: ConsumerDB, limit: int) -> int | None:
    """Return the next available execution slot id, or None if saturated."""
    occupied = db.active_slot_ids()
    for slot_id in range(1, limit + 1):
        if slot_id not in occupied:
            return slot_id
    return None


@dataclass(frozen=True)
class ReconciliationResult:
    """Summary of consumer-led board truth reconciliation."""

    moved_ready: tuple[str, ...] = ()
    moved_in_progress: tuple[str, ...] = ()
    moved_review: tuple[str, ...] = ()
    moved_blocked: tuple[str, ...] = ()


def _reconcile_board_truth(
    consumer_config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    db: ConsumerDB,
    *,
    session_store: SqliteSessionStore | None = None,
    pr_port: PullRequestPort | None = None,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    dry_run: bool = False,
    board_snapshot: CycleBoardSnapshot | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ReconciliationResult:
    """Make board `In Progress` truthful against local consumer state."""
    if automation_config is None:
        return ReconciliationResult()

    store = session_store or SqliteSessionStore(db)
    active_workers = store.active_workers()
    active_issue_refs = {worker.issue_ref for worker in active_workers}
    active_repair_issue_refs = {
        worker.issue_ref
        for worker in active_workers
        if worker.session_kind == "repair"
    }
    moved_ready: list[str] = []
    moved_in_progress: list[str] = []
    moved_review: list[str] = []
    moved_blocked: list[str] = []
    effective_pr_port = pr_port or GitHubCliAdapter(
        project_owner=consumer_config.project_owner,
        project_number=consumer_config.project_number,
        config=critical_path_config,
        gh_runner=gh_runner,
    )
    effective_review_state_port = review_state_port or effective_pr_port
    effective_board_port = board_port or effective_pr_port

    def _issue_ref_for_snapshot(
        snapshot: _ProjectItemSnapshot | IssueSnapshot,
    ) -> str | None:
        if isinstance(snapshot, IssueSnapshot):
            return snapshot.issue_ref
        return _snapshot_to_issue_ref(snapshot, critical_path_config)

    moved_in_progress.extend(
        _reconcile_active_repair_review_items(
            consumer_config,
            critical_path_config,
            active_repair_issue_refs=active_repair_issue_refs,
            review_state_port=effective_review_state_port,
            board_port=effective_board_port,
            board_snapshot=board_snapshot,
            issue_ref_for_snapshot=_issue_ref_for_snapshot,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
            dry_run=dry_run,
        )
    )
    ready_refs, review_refs, blocked_refs = _reconcile_stale_in_progress_items(
        consumer_config,
        critical_path_config,
        automation_config,
        store=store,
        pr_port=effective_pr_port,
        review_state_port=effective_review_state_port,
        board_port=effective_board_port,
        board_snapshot=board_snapshot,
        issue_ref_for_snapshot=_issue_ref_for_snapshot,
        active_issue_refs=active_issue_refs,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )
    moved_ready.extend(ready_refs)
    moved_review.extend(review_refs)
    moved_blocked.extend(blocked_refs)

    return ReconciliationResult(
        moved_ready=tuple(moved_ready),
        moved_in_progress=tuple(moved_in_progress),
        moved_review=tuple(moved_review),
        moved_blocked=tuple(moved_blocked),
    )


def _block_prelaunch_issue(
    issue_ref: str,
    blocked_reason: str,
    *,
    config: ConsumerConfig,
    cp_config: CriticalPathConfig,
    db: ConsumerDB,
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Move a launch-unready issue to Blocked before claim."""
    try:
        _set_blocked_with_reason(
            issue_ref,
            blocked_reason,
            cp_config,
            config.project_owner,
            config.project_number,
            gh_runner=gh_runner,
        )
        _record_successful_github_mutation(db)
    except (GhQueryError, Exception) as err:
        logger.error("Prelaunch block failed for %s: %s", issue_ref, err)
        _mark_degraded(db, f"prelaunch-block:{err}")
        _queue_status_transition(
            db,
            issue_ref,
            to_status="Blocked",
            from_statuses={"Ready"},
            blocked_reason=blocked_reason,
        )


# ---------------------------------------------------------------------------
# _prepare_launch_candidate sub-functions: focused extraction from god-function
# ---------------------------------------------------------------------------


def _setup_launch_worktree(
    issue_ref: str,
    title: str,
    session_kind: str,
    repair_branch_name: str | None,
    *,
    config: ConsumerConfig,
    cp_config: CriticalPathConfig,
    db: ConsumerDB,
    session_store: SessionStorePort | None = None,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, str, str | None, str | None]:
    """Set up worktree and reconcile repair branch if needed.

    Returns (worktree_path, branch_name, reconcile_state, reconcile_error).
    Raises WorktreePrepareError on blocking failure.
    """
    try:
        worktree_path, branch_name = _prepare_worktree(
            issue_ref,
            title,
            config,
            db,
            branch_name_override=repair_branch_name,
            session_store=session_store,
            worktree_port=worktree_port,
            subprocess_runner=subprocess_runner,
        )
    except WorktreePrepareError as err:
        _record_metric(
            db,
            config,
            "worktree_blocked",
            issue_ref=issue_ref,
            payload={"reason": err.reason_code, "detail": err.detail},
        )
        blocked_reason = (
            err.reason_code
            if err.reason_code == "worktree_in_use"
            else f"workspace_prepare:{err.detail}"
        )
        _block_prelaunch_issue(
            issue_ref,
            blocked_reason,
            config=config,
            cp_config=cp_config,
            db=db,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        raise
    except RuntimeError as err:
        _record_metric(
            db,
            config,
            "worktree_blocked",
            issue_ref=issue_ref,
            payload={"reason": "workspace_error", "detail": str(err)},
        )
        blocked_reason = f"workspace_prepare:{err}"
        _block_prelaunch_issue(
            issue_ref,
            blocked_reason,
            config=config,
            cp_config=cp_config,
            db=db,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        raise WorktreePrepareError("workspace_error", str(err)) from err

    branch_reconcile_state: str | None = None
    branch_reconcile_error: str | None = None
    if session_kind == "repair":
        reconcile_outcome = _reconcile_repair_branch(
            worktree_path,
            branch_name,
            worktree_port=worktree_port,
            subprocess_runner=subprocess_runner,
        )
        branch_reconcile_state = reconcile_outcome.state
        branch_reconcile_error = reconcile_outcome.error
        if branch_reconcile_state == "reconcile_setup_failed":
            blocked_reason = (
                "repair-branch-reconcile:"
                f"{branch_reconcile_error or 'unknown-error'}"
            )
            _block_prelaunch_issue(
                issue_ref,
                blocked_reason,
                config=config,
                cp_config=cp_config,
                db=db,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                gh_runner=gh_runner,
            )
            _record_metric(
                db,
                config,
                "worker_start_failed",
                issue_ref=issue_ref,
                payload={"reason": "repair_reconcile_error", "detail": blocked_reason},
            )
            raise WorktreePrepareError("repair_reconcile_error", blocked_reason)

    return worktree_path, branch_name, branch_reconcile_state, branch_reconcile_error


def _resolve_launch_runtime(
    candidate_prefix: str,
    worktree_path: str,
    *,
    config: ConsumerConfig,
    prepared: PreparedCycleContext,
) -> tuple[Any, ConsumerConfig]:
    """Load worktree workflow and compute effective runtime config.

    Returns (workflow_definition, effective_consumer_config).
    """
    workflow_definition = load_worktree_workflow(
        candidate_prefix,
        Path(worktree_path),
        filename=config.workflow_filename,
    )

    main_workflow = prepared.main_workflows.get(candidate_prefix)
    effective_max_retries = (
        main_workflow.runtime.max_retries
        if main_workflow is not None and main_workflow.runtime.max_retries is not None
        else config.max_retries
    )
    effective_validation_cmd = (
        workflow_definition.runtime.validation_cmd
        if workflow_definition.runtime.validation_cmd is not None
        else config.validation_cmd
    )
    effective_timeout_seconds = (
        workflow_definition.runtime.codex_timeout_seconds
        if workflow_definition.runtime.codex_timeout_seconds is not None
        else config.codex_timeout_seconds
    )
    effective_consumer_config = replace(
        config,
        validation_cmd=effective_validation_cmd,
        codex_timeout_seconds=effective_timeout_seconds,
        max_retries=effective_max_retries,
    )
    return workflow_definition, effective_consumer_config


def _prepare_launch_candidate(
    issue_ref: str,
    *,
    config: ConsumerConfig,
    prepared: PreparedCycleContext,
    db: ConsumerDB,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    pr_port: PullRequestPort | None = None,
    issue_context_port: IssueContextPort | None = None,
    session_store: SessionStorePort | None = None,
    worktree_port: WorktreePort | None = None,
) -> PreparedLaunchContext:
    """Prepare local launch state for an issue before board claim."""
    cp_config = prepared.cp_config
    auto_config = prepared.auto_config
    store = session_store or SqliteSessionStore(db)
    effective_pr_port = pr_port or GitHubCliAdapter(
        project_owner=config.project_owner,
        project_number=config.project_number,
        config=cp_config,
        github_memo=prepared.github_memo,
        gh_runner=gh_runner,
    )
    effective_issue_context_port = issue_context_port or effective_pr_port
    effective_worktree_port = worktree_port or LocalProcessAdapter(
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
    )
    candidate_prefix = parse_issue_ref(issue_ref).prefix
    owner, repo, number = _resolve_issue_coordinates(issue_ref, cp_config)
    snapshot = _snapshot_for_issue(prepared.board_snapshot, issue_ref, cp_config)

    repair_pr_url: str | None = None
    repair_branch_name: str | None = None
    classification: str | None = None
    pr_match: OpenPullRequestMatch | None = None
    session_kind = "new_work"
    if auto_config is not None:
        classification, pr_match, _reason = _classify_open_pr_candidates(
            issue_ref,
            owner,
            repo,
            number,
            auto_config,
            pr_port=effective_pr_port,
            gh_runner=gh_runner,
        )
        session_kind = _launch_session_kind(classification, pr_match)
        if session_kind == "repair" and pr_match is not None:
            repair_pr_url = pr_match.url
            repair_branch_name = pr_match.branch_name

    context = _hydrate_issue_context(
        issue_ref,
        owner=owner,
        repo=repo,
        number=number,
        snapshot=snapshot,
        config=config,
        db=db,
        issue_context_port=effective_issue_context_port,
        gh_runner=gh_runner,
    )
    title = str(context.get("title") or (snapshot.title if snapshot is not None else f"issue-{number}"))

    # Phase 2: Set up worktree and reconcile repair branch
    worktree_path, branch_name, branch_reconcile_state, branch_reconcile_error = (
        _setup_launch_worktree(
            issue_ref,
            title,
            session_kind,
            repair_branch_name,
        config=config,
        cp_config=cp_config,
        db=db,
        session_store=store,
        worktree_port=effective_worktree_port,
        subprocess_runner=subprocess_runner,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
    )

    # Phase 3: Resolve workflow and effective config
    workflow_definition, effective_consumer_config = _resolve_launch_runtime(
        candidate_prefix,
        worktree_path,
        config=config,
        prepared=prepared,
    )

    _run_workspace_hooks(
        workflow_definition.runtime.workspace_hooks.get("after_create", ()),
        worktree_path=worktree_path,
        issue_ref=issue_ref,
        branch_name=branch_name,
        worktree_port=effective_worktree_port,
        subprocess_runner=subprocess_runner,
    )
    _run_workspace_hooks(
        workflow_definition.runtime.workspace_hooks.get("before_run", ()),
        worktree_path=worktree_path,
        issue_ref=issue_ref,
        branch_name=branch_name,
        worktree_port=effective_worktree_port,
        subprocess_runner=subprocess_runner,
    )

    dependency_summary = _build_dependency_summary(
        issue_ref,
        cp_config,
        config.project_owner,
        config.project_number,
        status_resolver=status_resolver,
    )
    return PreparedLaunchContext(
        issue_ref=issue_ref,
        repo_prefix=candidate_prefix,
        owner=owner,
        repo=repo,
        number=number,
        title=title,
        issue_context=context,
        session_kind=session_kind,
        repair_pr_url=repair_pr_url,
        repair_branch_name=repair_branch_name,
        worktree_path=worktree_path,
        branch_name=branch_name,
        workflow_definition=workflow_definition,
        effective_consumer_config=effective_consumer_config,
        dependency_summary=dependency_summary,
        branch_reconcile_state=branch_reconcile_state,
        branch_reconcile_error=branch_reconcile_error,
    )


def _select_launch_candidate_for_cycle(
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    prepared: PreparedCycleContext,
    target_issue: str | None,
    status_resolver: Callable[..., str] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[SelectedLaunchCandidate | None, CycleResult | None]:
    """Select a launch candidate and validate its immediate launchability."""
    try:
        candidate = _select_candidate_for_cycle(
            config,
            db,
            prepared,
            target_issue=target_issue,
            status_resolver=status_resolver,
            gh_runner=gh_runner,
        )
    except GhQueryError as err:
        logger.error("Ready-item selection failed: %s", err)
        if not _maybe_activate_claim_suppression(
            db,
            config,
            scope="preflight",
            error=err,
        ):
            _mark_degraded(db, f"selection-error:{gh_reason_code(err)}:{err}")
        return None, CycleResult(action="error", reason=f"selection-error:{err}")
    except Exception as err:
        logger.exception("Unexpected selection failure")
        return None, CycleResult(
            action="error", reason=f"selection-unexpected-error:{err}"
        )

    if not candidate:
        idle_reason = (
            "no-dispatchable-repos"
            if not prepared.dispatchable_repo_prefixes
            else "no-ready-for-executor"
        )
        return None, CycleResult(action="idle", reason=idle_reason)

    candidate_prefix = parse_issue_ref(candidate).prefix
    main_workflow = prepared.main_workflows.get(candidate_prefix)
    if main_workflow is None:
        status = prepared.workflow_statuses.get(candidate_prefix)
        reason = status.disabled_reason if status is not None else "workflow-missing"
        return None, CycleResult(
            action="idle",
            issue_ref=candidate,
            reason=f"repo-dispatch-disabled:{reason}",
        )

    base_seconds, max_seconds = _effective_retry_backoff(config, main_workflow)
    if _retry_backoff_active(
        db,
        candidate,
        base_seconds=base_seconds,
        max_seconds=max_seconds,
    ):
        return None, CycleResult(
            action="idle",
            issue_ref=candidate,
            reason=f"retry-backoff:{base_seconds}:{max_seconds}",
        )

    return (
        SelectedLaunchCandidate(
            issue_ref=candidate,
            repo_prefix=candidate_prefix,
            main_workflow=main_workflow,
        ),
        None,
    )


def _prepare_selected_launch_candidate(
    *,
    selected_candidate: SelectedLaunchCandidate,
    config: ConsumerConfig,
    db: ConsumerDB,
    prepared: PreparedCycleContext,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
    """Prepare the selected candidate into launch-ready local context."""
    candidate = selected_candidate.issue_ref
    _record_metric(db, config, "candidate_selected", issue_ref=candidate)
    try:
        return (
            _prepare_launch_candidate(
                candidate,
                config=config,
                prepared=prepared,
                db=db,
                subprocess_runner=subprocess_runner,
                status_resolver=status_resolver,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                gh_runner=gh_runner,
            ),
            None,
        )
    except GhQueryError as err:
        _record_metric(
            db,
            config,
            "context_hydration_failed",
            issue_ref=candidate,
            payload={"reason": gh_reason_code(err), "detail": str(err)},
        )
        if _maybe_activate_claim_suppression(
            db,
            config,
            scope="hydration",
            error=err,
        ):
            return None, CycleResult(
                action="idle",
                issue_ref=candidate,
                reason="claim-suppressed:hydration",
            )
        _mark_degraded(db, f"launch-prep:{gh_reason_code(err)}:{err}")
        return None, CycleResult(
            action="error",
            issue_ref=candidate,
            reason=f"launch-prep:{err}",
        )
    except WorkflowConfigError as err:
        _block_prelaunch_issue(
            candidate,
            f"workflow-config:{err}",
            config=config,
            cp_config=prepared.cp_config,
            db=db,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        _record_metric(
            db,
            config,
            "worker_start_failed",
            issue_ref=candidate,
            payload={"reason": "workflow_config_error", "detail": str(err)},
        )
        return None, CycleResult(
            action="error",
            issue_ref=candidate,
            reason=f"workflow-config:{err}",
        )
    except WorktreePrepareError as err:
        _record_metric(
            db,
            config,
            "worker_start_failed",
            issue_ref=candidate,
            payload={"reason": err.reason_code, "detail": err.detail},
        )
        reason = (
            err.detail
            if err.reason_code == "repair_reconcile_error"
            else f"{err.reason_code}:{err.detail}"
        )
        return None, CycleResult(
            action="error",
            issue_ref=candidate,
            reason=reason,
        )
    except RuntimeError as err:
        _block_prelaunch_issue(
            candidate,
            f"workflow-hook:{err}",
            config=config,
            cp_config=prepared.cp_config,
            db=db,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        _record_metric(
            db,
            config,
            "worker_start_failed",
            issue_ref=candidate,
            payload={"reason": "workflow_hook_error", "detail": str(err)},
        )
        return None, CycleResult(
            action="error",
            issue_ref=candidate,
            reason=f"workflow-hook:{err}",
        )


def _resolve_launch_context_for_cycle(
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext | None,
    target_issue: str | None,
    dry_run: bool,
    status_resolver: Callable[..., str] | None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None,
    board_info_resolver: Callable | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[PreparedLaunchContext | None, CycleResult | None]:
    """Resolve or prepare launch-ready work for this cycle."""
    if launch_context is not None:
        return launch_context, None

    selected_candidate, cycle_result = _select_launch_candidate_for_cycle(
        config=config,
        db=db,
        prepared=prepared,
        target_issue=target_issue,
        status_resolver=status_resolver,
        gh_runner=gh_runner,
    )
    if cycle_result is not None:
        return None, cycle_result

    if dry_run:
        logger.info(
            "[dry-run] Would prepare, claim, and execute: %s",
            selected_candidate.issue_ref,
        )
        return None, CycleResult(
            action="claimed",
            issue_ref=selected_candidate.issue_ref,
            reason="dry-run",
        )

    return _prepare_selected_launch_candidate(
        selected_candidate=selected_candidate,
        config=config,
        db=db,
        prepared=prepared,
        subprocess_runner=subprocess_runner,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )


@dataclass(frozen=True)
class PendingClaimContext:
    """Session state prepared for board claim after local launch prep."""

    session_id: str
    effective_max_retries: int


def _open_pending_claim_session(
    *,
    db: ConsumerDB,
    launch_context: PreparedLaunchContext,
    executor: str,
    slot_id: int,
) -> tuple[PendingClaimContext | None, CycleResult | None]:
    """Create the session record and acquire the lease for a launch-ready issue."""
    candidate = launch_context.issue_ref
    session_id = db.create_session(
        candidate,
        repo_prefix=launch_context.repo_prefix,
        executor=executor,
        slot_id=slot_id,
        phase="launch_ready",
        session_kind=launch_context.session_kind,
        repair_pr_url=launch_context.repair_pr_url,
    )
    db.update_session(session_id, provenance_id=session_id, phase="launch_ready")
    now = datetime.now(timezone.utc)
    try:
        lease_acquired = db.acquire_lease(candidate, session_id, slot_id=slot_id, now=now)
    except TypeError:
        lease_acquired = db.acquire_lease(candidate, session_id, now=now)
    if not lease_acquired:
        _complete_session(
            db,
            session_id,
            candidate,
            status="aborted",
            failure_reason="lease_conflict",
        )
        return None, CycleResult(
            action="idle",
            issue_ref=candidate,
            session_id=session_id,
            reason="lease-conflict",
        )
    return PendingClaimContext(
        session_id=session_id,
        effective_max_retries=launch_context.effective_consumer_config.max_retries,
    ), None


def _enforce_claim_retry_ceiling(
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    launch_context: PreparedLaunchContext,
    pending_claim: PendingClaimContext,
    cp_config: CriticalPathConfig,
    board_info_resolver: Callable | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> CycleResult | None:
    """Abort and escalate if the issue already exhausted its retry ceiling."""
    candidate = launch_context.issue_ref
    retries = db.count_retries(candidate)
    if retries < pending_claim.effective_max_retries:
        return None
    db.release_lease(candidate)
    _complete_session(
        db,
        pending_claim.session_id,
        candidate,
        status="failed",
        failure_reason="max_retries_exceeded",
    )
    try:
        _escalate_to_claude(
            candidate,
            cp_config,
            config.project_owner,
            config.project_number,
            reason=f"max retries ({pending_claim.effective_max_retries}) exceeded",
            board_info_resolver=board_info_resolver,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )
    except (GhQueryError, Exception) as err:
        logger.error("Escalation failed: %s", err)
    return CycleResult(
        action="error",
        issue_ref=candidate,
        session_id=pending_claim.session_id,
        reason="max-retries-exceeded",
    )


def _attempt_launch_context_claim(
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    pending_claim: PendingClaimContext,
    slot_id: int,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[ClaimReadyResult | None, CycleResult | None]:
    """Claim board ownership for a launch-ready issue."""
    candidate = launch_context.issue_ref
    _record_metric(
        db,
        config,
        "claim_attempted",
        issue_ref=candidate,
        payload={"slot_id": slot_id},
    )
    try:
        claim_result = claim_ready_issue(
            prepared.cp_config,
            config.project_owner,
            config.project_number,
            executor=config.executor,
            issue_ref=candidate,
            all_prefixes=True,
            automation_config=prepared.auto_config,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
            status_resolver=status_resolver,
        )
    except GhQueryError as err:
        logger.error("Claim failed after launch prep for %s: %s", candidate, err)
        if not _maybe_activate_claim_suppression(
            db,
            config,
            scope="claim",
            error=err,
        ):
            _mark_degraded(db, f"claim-error:{gh_reason_code(err)}:{err}")
        db.release_lease(candidate)
        _complete_session(
            db,
            pending_claim.session_id,
            candidate,
            status="failed",
            failure_reason="api_error",
        )
        _record_metric(
            db,
            config,
            "worker_start_failed",
            issue_ref=candidate,
            payload={"reason": "claim_error", "detail": str(err)},
        )
        return None, CycleResult(
            action="error",
            issue_ref=candidate,
            session_id=pending_claim.session_id,
            reason=f"claim-error:{err}",
        )
    except Exception as err:
        logger.exception("Unexpected claim failure after launch prep for %s", candidate)
        db.release_lease(candidate)
        _complete_session(
            db,
            pending_claim.session_id,
            candidate,
            status="failed",
            failure_reason="consumer_error",
        )
        _record_metric(
            db,
            config,
            "worker_start_failed",
            issue_ref=candidate,
            payload={"reason": "claim_unexpected_error", "detail": str(err)},
        )
        return None, CycleResult(
            action="error",
            issue_ref=candidate,
            session_id=pending_claim.session_id,
            reason=f"claim-unexpected-error:{err}",
        )

    if claim_result.claimed:
        return claim_result, None

    db.release_lease(candidate)
    terminal_status = (
        "aborted"
        if claim_result.reason in {"wip-limit", "status-not-ready:In Progress"}
        else "failed"
    )
    failure_reason = {
        "wip-limit": "claim_rejected_wip_limit",
        "status-not-ready:In Progress": "claim_rejected_status_changed",
    }.get(claim_result.reason, "claim_rejected")
    _complete_session(
        db,
        pending_claim.session_id,
        candidate,
        status=terminal_status,
        failure_reason=failure_reason,
    )
    _record_metric(
        db,
        config,
        "worker_start_failed",
        issue_ref=candidate,
        payload={"reason": failure_reason},
    )
    return None, CycleResult(
        action="idle",
        issue_ref=candidate,
        session_id=pending_claim.session_id,
        reason=f"claim-rejected:{claim_result.reason}",
    )


def _mark_claimed_session_running(
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    launch_context: PreparedLaunchContext,
    pending_claim: PendingClaimContext,
    slot_id: int,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    cp_config: CriticalPathConfig,
    gh_runner: Callable[..., str] | None,
) -> ClaimedSessionContext:
    """Persist the durable-start state and post the claim marker."""
    candidate = launch_context.issue_ref
    _record_successful_github_mutation(db)
    _record_metric(db, config, "claim_succeeded", issue_ref=candidate)
    db.update_session(
        pending_claim.session_id,
        status="running",
        slot_id=slot_id,
        worktree_path=launch_context.worktree_path,
        branch_name=launch_context.branch_name,
        phase="running",
        started_at=datetime.now(timezone.utc).isoformat(),
        session_kind=launch_context.session_kind,
        repair_pr_url=launch_context.repair_pr_url,
        branch_reconcile_state=launch_context.branch_reconcile_state,
        branch_reconcile_error=launch_context.branch_reconcile_error,
    )
    _record_metric(
        db,
        config,
        "worker_durable_start",
        issue_ref=candidate,
        payload={"slot_id": slot_id, "worktree_path": launch_context.worktree_path},
    )
    try:
        _post_consumer_claim_comment(
            candidate,
            pending_claim.session_id,
            launch_context.repo_prefix,
            launch_context.branch_name,
            config.executor,
            cp_config,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )
    except (GhQueryError, Exception) as err:
        logger.error("Consumer claim marker failed: %s", err)
    return ClaimedSessionContext(
        session_id=pending_claim.session_id,
        effective_max_retries=pending_claim.effective_max_retries,
        slot_id=slot_id,
    )


def _claim_launch_context(
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    slot_id: int,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[ClaimedSessionContext | None, CycleResult | None]:
    """Claim board ownership and start a durable running session."""
    cp_config = prepared.cp_config
    candidate = launch_context.issue_ref
    pending_claim, cycle_result = _open_pending_claim_session(
        db=db,
        launch_context=launch_context,
        executor=config.executor,
        slot_id=slot_id,
    )
    if cycle_result is not None:
        return None, cycle_result

    retry_ceiling_result = _enforce_claim_retry_ceiling(
        config=config,
        db=db,
        launch_context=launch_context,
        pending_claim=pending_claim,
        cp_config=cp_config,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )
    if retry_ceiling_result is not None:
        return None, retry_ceiling_result

    _claim_result, cycle_result = _attempt_launch_context_claim(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        pending_claim=pending_claim,
        slot_id=slot_id,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )
    if cycle_result is not None:
        return None, cycle_result

    return (
        _mark_claimed_session_running(
            config=config,
            db=db,
            launch_context=launch_context,
            pending_claim=pending_claim,
            slot_id=slot_id,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            cp_config=cp_config,
            gh_runner=gh_runner,
        ),
        None,
    )


def _session_status_from_codex_result(
    exit_code: int,
    codex_result: dict[str, Any] | None,
) -> tuple[str, str | None]:
    """Map Codex exit/result into session status and failure reason."""
    if exit_code == 0 and codex_result and codex_result.get("outcome") == "success":
        return "success", None
    if exit_code == 124:
        return "timeout", "timeout"
    if codex_result and codex_result.get("outcome") in {"failed", "blocked"}:
        return "failed", "validation_failed"
    return "failed", "codex_error"


def _create_pr_for_execution_result(
    *,
    config: ConsumerConfig,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    codex_result: dict[str, Any] | None,
    session_status: str,
    failure_reason: str | None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None,
    gh_runner: Callable[..., str] | None,
) -> PrCreationOutcome:
    """Reuse or create a PR from claimed-session output."""
    pr_url = codex_result.get("pr_url") if codex_result else None
    has_commits = False
    updated_session_status = session_status
    updated_failure_reason = failure_reason

    try:
        has_commits = _has_commits_on_branch(
            launch_context.worktree_path,
            launch_context.branch_name,
            subprocess_runner=subprocess_runner,
        )
        if has_commits:
            pr_url = _create_or_update_pr(
                launch_context.worktree_path,
                launch_context.branch_name,
                launch_context.number,
                launch_context.owner,
                launch_context.repo,
                launch_context.title,
                config,
                issue_ref=launch_context.issue_ref,
                session_id=claimed_context.session_id,
                gh_runner=gh_runner,
            )
    except (GhQueryError, Exception) as err:
        logger.error("PR creation failed: %s", err)
        if updated_session_status == "success":
            updated_session_status = "failed"
        updated_failure_reason = "pr_error"

    return PrCreationOutcome(
        pr_url=pr_url,
        has_commits=has_commits,
        session_status=updated_session_status,
        failure_reason=updated_failure_reason,
    )


def _handoff_execution_to_review(
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    session_id: str,
    pr_url: str,
    session_status: str,
    board_info_resolver: Callable | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> ReviewQueueDrainSummary:
    """Transition a claimed session into Review and perform immediate rescue."""
    cp_config = prepared.cp_config
    auto_config = prepared.auto_config
    candidate = launch_context.issue_ref

    try:
        _transition_issue_to_review(
            candidate,
            cp_config,
            config.project_owner,
            config.project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        _record_successful_github_mutation(db)
    except (GhQueryError, Exception) as err:
        logger.error("Review transition failed: %s", err)
        _mark_degraded(db, f"review-transition:{err}")
        _queue_status_transition(
            db,
            candidate,
            to_status="Review",
            from_statuses={"In Progress"},
        )
        db.update_session(session_id, phase="review")
    _record_metric(db, config, "session_transition_review", issue_ref=candidate)

    immediate_review_summary = ReviewQueueDrainSummary()
    if session_status != "success":
        return immediate_review_summary

    handoff_store = SqliteSessionStore(db)
    try:
        _post_pr_codex_verdict(
            pr_url,
            session_id,
            gh_runner=gh_runner,
        )
        _record_successful_github_mutation(db)
    except (GhQueryError, Exception) as err:
        logger.error("PR codex verdict comment failed: %s", err)
        _mark_degraded(db, f"verdict-marker:{err}")
        _queue_verdict_marker(db, pr_url, session_id)
    queue_entry = _queue_review_item(
        handoff_store,
        candidate,
        pr_url,
        session_id=session_id,
    )
    if queue_entry is None or auto_config is None:
        return immediate_review_summary

    try:
        review_memo = CycleGitHubMemo()
        handoff_pr_port = GitHubCliAdapter(
            project_owner=config.project_owner,
            project_number=config.project_number,
            config=cp_config,
            github_memo=review_memo,
            gh_runner=gh_runner,
        )
        queue_entries = [
            entry
            for entry in handoff_store.list_review_queue_items()
            if entry.pr_repo == queue_entry.pr_repo
            and entry.pr_number == queue_entry.pr_number
        ]
        review_snapshots = _build_review_snapshots_for_queue_entries(
            queue_entries=queue_entries,
            review_refs={entry.issue_ref for entry in queue_entries},
            pr_port=handoff_pr_port,
            trusted_codex_actors=frozenset(auto_config.trusted_codex_actors),
        )
        snapshot = review_snapshots.get((queue_entry.pr_repo, queue_entry.pr_number))
        result = review_rescue(
            pr_repo=queue_entry.pr_repo,
            pr_number=queue_entry.pr_number,
            config=cp_config,
            automation_config=auto_config,
            project_owner=config.project_owner,
            project_number=config.project_number,
            dry_run=False,
            snapshot=snapshot,
            gh_runner=gh_runner,
        )
        state_digest = handoff_pr_port.review_state_digests(
            [(queue_entry.pr_repo, queue_entry.pr_number)]
        ).get((queue_entry.pr_repo, queue_entry.pr_number))
        for entry in queue_entries or [queue_entry]:
            _apply_review_queue_result(
                handoff_store,
                entry,
                result,
                last_state_digest=state_digest,
            )
        return ReviewQueueDrainSummary(
            queued_count=len(queue_entries) or 1,
            due_count=len(queue_entries) or 1,
            rerun=(
                (
                    f"{queue_entry.pr_repo}#{queue_entry.pr_number}:{','.join(result.rerun_checks)}",
                )
                if result.rerun_checks
                else ()
            ),
            auto_merge_enabled=(
                (f"{queue_entry.pr_repo}#{queue_entry.pr_number}",)
                if result.auto_merge_enabled
                else ()
            ),
            requeued=result.requeued_refs,
            blocked=(
                (f"{queue_entry.pr_repo}#{queue_entry.pr_number}:{result.blocked_reason}",)
                if result.blocked_reason
                else ()
            ),
            skipped=(
                (f"{queue_entry.pr_repo}#{queue_entry.pr_number}:{result.skipped_reason}",)
                if result.skipped_reason
                else ()
            ),
        )
    except GhQueryError as err:
        logger.warning("Immediate review clearance failed for %s: %s", candidate, err)
        _mark_degraded(db, f"review-queue:{gh_reason_code(err)}:{err}")
        return immediate_review_summary


def _handle_non_review_execution_outcome(
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    session_id: str,
    session_status: str,
    codex_result: dict[str, Any] | None,
    has_commits: bool,
    board_info_resolver: Callable | None,
    board_mutator: Callable[..., None] | None,
    comment_poster: Callable[..., None] | None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[str, ResolutionEvaluation | None, str | None]:
    """Handle non-review outcomes for a claimed session."""
    cp_config = prepared.cp_config
    candidate = launch_context.issue_ref
    resolution_evaluation: ResolutionEvaluation | None = None
    done_reason: str | None = None
    updated_session_status = session_status

    if session_status == "success" and not has_commits:
        resolution_evaluation = _verify_resolution_payload(
            candidate,
            codex_result.get("resolution") if codex_result else None,
            config=launch_context.effective_consumer_config,
            workflows=prepared.main_workflows,
            subprocess_runner=subprocess_runner,
            gh_runner=gh_runner,
        )
        done_reason = _apply_resolution_action(
            candidate,
            resolution_evaluation,
            session_id=session_id,
            db=db,
            config=config,
            critical_path_config=cp_config,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )
        if done_reason == "already_resolved":
            _record_metric(db, config, "session_transition_done", issue_ref=candidate)
        return updated_session_status, resolution_evaluation, done_reason

    try:
        _return_issue_to_ready(
            candidate,
            cp_config,
            config.project_owner,
            config.project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        _record_successful_github_mutation(db)
    except (GhQueryError, Exception) as err:
        logger.error("Ready reset failed after non-PR session: %s", err)
        _mark_degraded(db, f"ready-reset:{err}")
        _queue_status_transition(
            db,
            candidate,
            to_status="Ready",
            from_statuses={"In Progress", "Review"},
        )
    if session_status == "failed" and not has_commits and codex_result is None:
        updated_session_status = "aborted"
    return updated_session_status, resolution_evaluation, done_reason


def _final_phase_for_claimed_session(
    *,
    launch_context: PreparedLaunchContext,
    execution_outcome: SessionExecutionOutcome,
) -> str:
    """Determine the final persisted phase for a claimed session."""
    review_requeued = (
        execution_outcome.should_transition_to_review
        and launch_context.issue_ref in execution_outcome.immediate_review_summary.requeued
    )
    final_phase = (
        "completed"
        if review_requeued
        else (
            "review"
            if execution_outcome.should_transition_to_review
            else "completed"
        )
    )
    if execution_outcome.session_status in {"failed", "timeout"} and not execution_outcome.pr_url:
        final_phase = "blocked"
    if (
        execution_outcome.resolution_evaluation is not None
        and execution_outcome.done_reason != "already_resolved"
    ):
        final_phase = "blocked"
    if (
        launch_context.session_kind == "repair"
        and execution_outcome.session_status in {"failed", "timeout"}
    ):
        final_phase = "completed"
    return final_phase


def _persist_claimed_session_completion(
    *,
    db: ConsumerDB,
    session_id: str,
    issue_ref: str,
    execution_outcome: SessionExecutionOutcome,
    final_phase: str,
) -> None:
    """Persist the final session record for a claimed execution outcome."""
    resolution_evaluation = execution_outcome.resolution_evaluation
    codex_result = execution_outcome.codex_result
    _complete_session(
        db,
        session_id,
        issue_ref,
        status=execution_outcome.session_status,
        failure_reason=execution_outcome.failure_reason,
        outcome_json=json.dumps(codex_result) if codex_result else None,
        pr_url=execution_outcome.pr_url,
        phase=final_phase,
        resolution_kind=(
            resolution_evaluation.resolution_kind
            if resolution_evaluation is not None
            else None
        ),
        verification_class=(
            resolution_evaluation.verification_class
            if resolution_evaluation is not None
            else None
        ),
        resolution_evidence_json=(
            json.dumps(resolution_evaluation.evidence, sort_keys=True)
            if resolution_evaluation is not None
            else None
        ),
        resolution_action=(
            resolution_evaluation.final_action
            if resolution_evaluation is not None
            else None
        ),
        done_reason=execution_outcome.done_reason,
    )


def _post_claimed_session_result_comment(
    *,
    issue_ref: str,
    session_id: str,
    codex_result: dict[str, Any] | None,
    cp_config: CriticalPathConfig,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Post the session result comment when Codex produced structured output."""
    if not codex_result:
        return
    try:
        _post_result_comment(
            issue_ref,
            codex_result,
            session_id,
            cp_config,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )
    except (GhQueryError, Exception) as err:
        logger.error("Result comment failed: %s", err)


def _maybe_escalate_claimed_session_failure(
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    issue_ref: str,
    effective_max_retries: int,
    session_status: str,
    codex_result: dict[str, Any] | None,
    cp_config: CriticalPathConfig,
    board_info_resolver: Callable | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Escalate terminal failed/timeout sessions once retry ceiling is reached."""
    if session_status not in {"failed", "timeout"}:
        return
    new_retries = db.count_retries(issue_ref)
    if new_retries < effective_max_retries:
        return
    try:
        escalation_reason = ""
        if codex_result:
            escalation_reason = codex_result.get("blocker_reason") or codex_result.get(
                "summary", ""
            )
        _escalate_to_claude(
            issue_ref,
            cp_config,
            config.project_owner,
            config.project_number,
            reason=escalation_reason or f"max retries ({effective_max_retries}) exceeded",
            board_info_resolver=board_info_resolver,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )
    except (GhQueryError, Exception) as err:
        logger.error("Escalation failed: %s", err)


def _execute_claimed_session(
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None,
    file_reader: Callable[[Path], str] | None,
    board_info_resolver: Callable | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> SessionExecutionOutcome:
    """Execute Codex for a claimed session and apply immediate board handoff."""
    candidate = launch_context.issue_ref
    session_id = claimed_context.session_id

    prompt = _assemble_codex_prompt(
        launch_context.issue_context,
        candidate,
        prepared.cp_config,
        launch_context.effective_consumer_config,
        launch_context.worktree_path,
        launch_context.branch_name,
        dependency_summary=launch_context.dependency_summary,
        workflow_definition=launch_context.workflow_definition,
        session_kind=launch_context.session_kind,
        repair_pr_url=launch_context.repair_pr_url,
        branch_reconcile_state=launch_context.branch_reconcile_state,
        branch_reconcile_error=launch_context.branch_reconcile_error,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = config.output_dir / f"{session_id}.json"
    exit_code = _run_codex_session(
        launch_context.worktree_path,
        prompt,
        config.schema_path,
        output_path,
        launch_context.effective_consumer_config.codex_timeout_seconds,
        heartbeat_fn=lambda: db.update_heartbeat(candidate),
        subprocess_runner=subprocess_runner,
    )

    codex_result = _parse_codex_result(output_path, file_reader=file_reader)

    session_status, failure_reason = _session_status_from_codex_result(
        exit_code,
        codex_result,
    )
    pr_outcome = _create_pr_for_execution_result(
        config=config,
        launch_context=launch_context,
        claimed_context=claimed_context,
        codex_result=codex_result,
        session_status=session_status,
        failure_reason=failure_reason,
        subprocess_runner=subprocess_runner,
        gh_runner=gh_runner,
    )

    should_transition_to_review = bool(pr_outcome.pr_url) and (
        launch_context.session_kind != "repair"
        or pr_outcome.session_status == "success"
    )

    immediate_review_summary = ReviewQueueDrainSummary()
    resolution_evaluation: ResolutionEvaluation | None = None
    done_reason: str | None = None
    effective_session_status = pr_outcome.session_status
    if should_transition_to_review:
        immediate_review_summary = _handoff_execution_to_review(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            session_id=session_id,
            pr_url=pr_outcome.pr_url or "",
            session_status=pr_outcome.session_status,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
    else:
        (
            effective_session_status,
            resolution_evaluation,
            done_reason,
        ) = _handle_non_review_execution_outcome(
            config=config,
            db=db,
            prepared=prepared,
            launch_context=launch_context,
            session_id=session_id,
            session_status=pr_outcome.session_status,
            codex_result=codex_result,
            has_commits=pr_outcome.has_commits,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_poster=comment_poster,
            subprocess_runner=subprocess_runner,
            gh_runner=gh_runner,
        )

    return SessionExecutionOutcome(
        session_status=effective_session_status,
        failure_reason=pr_outcome.failure_reason,
        pr_url=pr_outcome.pr_url,
        has_commits=pr_outcome.has_commits,
        codex_result=codex_result,
        should_transition_to_review=should_transition_to_review,
        immediate_review_summary=immediate_review_summary,
        resolution_evaluation=resolution_evaluation,
        done_reason=done_reason,
    )


def _finalize_claimed_session(
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext,
    claimed_context: ClaimedSessionContext,
    execution_outcome: SessionExecutionOutcome,
    board_info_resolver: Callable | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> CycleResult:
    """Persist final session state and return the cycle result."""
    cp_config = prepared.cp_config
    candidate = launch_context.issue_ref
    session_id = claimed_context.session_id
    effective_max_retries = claimed_context.effective_max_retries

    db.release_lease(candidate)
    final_phase = _final_phase_for_claimed_session(
        launch_context=launch_context,
        execution_outcome=execution_outcome,
    )
    _persist_claimed_session_completion(
        db=db,
        session_id=session_id,
        issue_ref=candidate,
        execution_outcome=execution_outcome,
        final_phase=final_phase,
    )
    _post_claimed_session_result_comment(
        issue_ref=candidate,
        session_id=session_id,
        codex_result=execution_outcome.codex_result,
        cp_config=cp_config,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )
    _maybe_escalate_claimed_session_failure(
        config=config,
        db=db,
        issue_ref=candidate,
        effective_max_retries=effective_max_retries,
        session_status=execution_outcome.session_status,
        codex_result=execution_outcome.codex_result,
        cp_config=cp_config,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )

    return CycleResult(
        action="claimed",
        issue_ref=candidate,
        session_id=session_id,
        reason=execution_outcome.session_status,
        pr_url=execution_outcome.pr_url,
    )


# ---------------------------------------------------------------------------
# Core: run_one_cycle
# ---------------------------------------------------------------------------


def run_one_cycle(
    config: ConsumerConfig,
    db: ConsumerDB,
    *,
    dry_run: bool = False,
    target_issue: str | None = None,
    prepared: PreparedCycleContext | None = None,
    launch_context: PreparedLaunchContext | None = None,
    slot_id_override: int | None = None,
    skip_control_plane: bool = False,
    # DI points
    gh_runner: Callable[..., str] | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    file_reader: Callable[[Path], str] | None = None,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
) -> CycleResult:
    """Execute one poll-claim-execute cycle.

    Returns a CycleResult describing what happened.
    """
    try:
        if skip_control_plane:
            if prepared is None:
                raise ValueError("prepared cycle context is required when skip_control_plane=True")
        else:
            prepared = _prepare_cycle(
                config,
                db,
                dry_run=dry_run,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                comment_checker=comment_checker,
                comment_poster=comment_poster,
                gh_runner=gh_runner,
            )
    except ConfigError as err:
        logger.error("Config error: %s", err)
        return CycleResult(action="error", reason=f"config-error:{err}")
    except WorkflowConfigError as err:
        logger.error("Workflow config error: %s", err)
        return CycleResult(action="error", reason=f"workflow-config:{err}")
    except GhQueryError as err:
        logger.error("Control-plane preflight failed: %s", err)
        _mark_degraded(db, f"control-plane:{gh_reason_code(err)}:{err}")
        return CycleResult(action="error", reason=f"control-plane:{err}")

    assert prepared is not None
    cp_config = prepared.cp_config
    auto_config = prepared.auto_config
    config.poll_interval_seconds = prepared.effective_interval

    suppression_state = _claim_suppression_state(db)
    if suppression_state is not None and launch_context is None:
        return CycleResult(
            action="idle",
            reason=f"claim-suppressed:{suppression_state['scope']}",
        )

    if slot_id_override is None:
        if db.active_lease_count() >= prepared.global_limit:
            return CycleResult(action="idle", reason="lease-cap")
        slot_id = _next_available_slot(db, prepared.global_limit)
        if slot_id is None:
            return CycleResult(action="idle", reason="lease-cap")
    else:
        slot_id = slot_id_override

    launch_context, cycle_result = _resolve_launch_context_for_cycle(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        target_issue=target_issue,
        dry_run=dry_run,
        status_resolver=status_resolver,
        subprocess_runner=subprocess_runner,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    if cycle_result is not None:
        return cycle_result

    assert launch_context is not None
    candidate = launch_context.issue_ref
    candidate_prefix = launch_context.repo_prefix

    claimed_context, cycle_result = _claim_launch_context(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        slot_id=slot_id,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )
    if cycle_result is not None:
        return cycle_result

    assert claimed_context is not None
    session_id = claimed_context.session_id
    effective_max_retries = claimed_context.effective_max_retries

    execution_outcome = _execute_claimed_session(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        claimed_context=claimed_context,
        subprocess_runner=subprocess_runner,
        file_reader=file_reader,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )

    return _finalize_claimed_session(
        config=config,
        db=db,
        prepared=prepared,
        launch_context=launch_context,
        claimed_context=claimed_context,
        execution_outcome=execution_outcome,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )


def _run_worker_cycle(
    config: ConsumerConfig,
    *,
    target_issue: str,
    slot_id: int,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext | None = None,
    dry_run: bool = False,
    di_kwargs: dict[str, Any] | None = None,
) -> CycleResult:
    """Execute one issue in an isolated worker DB connection."""
    worker_db = ConsumerDB(db_path=config.db_path)
    worker_config = replace(config)
    try:
        return run_one_cycle(
            worker_config,
            worker_db,
            dry_run=dry_run,
            target_issue=target_issue,
            prepared=prepared,
            launch_context=launch_context,
            slot_id_override=slot_id,
            skip_control_plane=True,
            **(di_kwargs or {}),
        )
    finally:
        worker_db.close()


def _next_available_slots(
    db: ConsumerDB,
    limit: int,
    *,
    reserved_slots: set[int] | None = None,
) -> list[int]:
    """Return deterministic lowest-available slot ids."""
    occupied = set(db.active_slot_ids())
    if reserved_slots:
        occupied.update(reserved_slots)
    return [slot_id for slot_id in range(1, limit + 1) if slot_id not in occupied]


def _run_multi_worker_daemon_loop(
    config: ConsumerConfig,
    db: ConsumerDB,
    *,
    dry_run: bool = False,
    sleep_fn: Callable[[float], None] | None = None,
    **di_kwargs: Any,
) -> None:
    """Run the daemon loop with multiple concurrent worker slots."""
    sleeper = sleep_fn or time.sleep
    active_tasks: dict[Future[CycleResult], ActiveWorkerTask] = {}
    with ThreadPoolExecutor(max_workers=max(1, config.global_concurrency)) as executor:
        while True:
            for future, task in list(active_tasks.items()):
                if not future.done():
                    continue
                del active_tasks[future]
                try:
                    result = future.result()
                    logger.info("Worker result [slot=%s issue=%s]: %s", task.slot_id, task.issue_ref, result)
                except Exception:
                    logger.exception(
                        "Unhandled worker failure [slot=%s issue=%s]",
                        task.slot_id,
                        task.issue_ref,
                    )

            if _drain_requested(config.drain_path):
                if not active_tasks and db.active_lease_count() == 0:
                    logger.info(
                        "Drain requested via %s; stopping after worker drain",
                        config.drain_path,
                    )
                    return
                sleeper(min(5.0, float(config.poll_interval_seconds)))
                continue

            try:
                prepared = _prepare_cycle(
                    config,
                    db,
                    dry_run=dry_run,
                    board_info_resolver=di_kwargs.get("board_info_resolver"),
                    board_mutator=di_kwargs.get("board_mutator"),
                    comment_checker=di_kwargs.get("comment_checker"),
                    comment_poster=di_kwargs.get("comment_poster"),
                    gh_runner=di_kwargs.get("gh_runner"),
                )
            except ConfigError:
                logger.exception("Config error during multi-worker cycle")
                sleeper(config.poll_interval_seconds)
                continue
            except WorkflowConfigError:
                logger.exception("Workflow config error during multi-worker cycle")
                sleeper(config.poll_interval_seconds)
                continue
            except GhQueryError as err:
                logger.error("Multi-worker preflight failed: %s", err)
                _mark_degraded(db, f"control-plane:{gh_reason_code(err)}:{err}")
                sleeper(config.poll_interval_seconds)
                continue

            reserved_slots = {task.slot_id for task in active_tasks.values()}
            active_issue_refs = {task.issue_ref for task in active_tasks.values()}
            active_issue_refs.update(worker.issue_ref for worker in db.active_workers())
            available_slots = _next_available_slots(
                db,
                prepared.global_limit,
                reserved_slots=reserved_slots,
            )
            suppression_state = _claim_suppression_state(db)
            if suppression_state is not None:
                until = _parse_iso8601_timestamp(suppression_state["until"])
                if until is None:
                    sleeper(config.poll_interval_seconds)
                else:
                    remaining = max(
                        1.0,
                        (until - datetime.now(timezone.utc)).total_seconds(),
                    )
                    sleeper(min(float(config.poll_interval_seconds), remaining))
                continue

            launched = 0
            hydration_budget = max(1, config.launch_hydration_concurrency)
            for slot_id in available_slots:
                if launched >= hydration_budget and not dry_run:
                    break
                try:
                    candidate = _select_candidate_for_cycle(
                        config,
                        db,
                        prepared,
                        status_resolver=di_kwargs.get("status_resolver"),
                        gh_runner=di_kwargs.get("gh_runner"),
                        excluded_issue_refs=active_issue_refs,
                    )
                except GhQueryError as err:
                    logger.error("Ready-item selection failed: %s", err)
                    _mark_degraded(db, f"selection-error:{gh_reason_code(err)}:{err}")
                    break
                if not candidate:
                    break

                active_issue_refs.add(candidate)
                launch_context: PreparedLaunchContext | None = None
                if not dry_run:
                    _record_metric(db, config, "candidate_selected", issue_ref=candidate)
                    try:
                        launch_context = _prepare_launch_candidate(
                            candidate,
                            config=config,
                            prepared=prepared,
                            db=db,
                            subprocess_runner=di_kwargs.get("subprocess_runner"),
                            status_resolver=di_kwargs.get("status_resolver"),
                            board_info_resolver=di_kwargs.get("board_info_resolver"),
                            board_mutator=di_kwargs.get("board_mutator"),
                            gh_runner=di_kwargs.get("gh_runner"),
                        )
                    except GhQueryError as err:
                        _record_metric(
                            db,
                            config,
                            "context_hydration_failed",
                            issue_ref=candidate,
                            payload={"reason": gh_reason_code(err), "detail": str(err)},
                        )
                        if not _maybe_activate_claim_suppression(
                            db,
                            config,
                            scope="hydration",
                            error=err,
                        ):
                            _mark_degraded(db, f"launch-prep:{gh_reason_code(err)}:{err}")
                        break
                    except WorkflowConfigError as err:
                        _block_prelaunch_issue(
                            candidate,
                            f"workflow-config:{err}",
                            config=config,
                            cp_config=prepared.cp_config,
                            db=db,
                            board_info_resolver=di_kwargs.get("board_info_resolver"),
                            board_mutator=di_kwargs.get("board_mutator"),
                            gh_runner=di_kwargs.get("gh_runner"),
                        )
                        _record_metric(
                            db,
                            config,
                            "worker_start_failed",
                            issue_ref=candidate,
                            payload={"reason": "workflow_config_error", "detail": str(err)},
                        )
                        continue
                    except WorktreePrepareError as err:
                        _record_metric(
                            db,
                            config,
                            "worker_start_failed",
                            issue_ref=candidate,
                            payload={"reason": err.reason_code, "detail": err.detail},
                        )
                        continue
                    except RuntimeError as err:
                        _block_prelaunch_issue(
                            candidate,
                            f"workflow-hook:{err}",
                            config=config,
                            cp_config=prepared.cp_config,
                            db=db,
                            board_info_resolver=di_kwargs.get("board_info_resolver"),
                            board_mutator=di_kwargs.get("board_mutator"),
                            gh_runner=di_kwargs.get("gh_runner"),
                        )
                        _record_metric(
                            db,
                            config,
                            "worker_start_failed",
                            issue_ref=candidate,
                            payload={"reason": "workflow_hook_error", "detail": str(err)},
                        )
                        continue
                future = executor.submit(
                    _run_worker_cycle,
                    replace(config),
                    target_issue=candidate,
                    slot_id=slot_id,
                    prepared=prepared,
                    launch_context=launch_context,
                    dry_run=dry_run,
                    di_kwargs=di_kwargs,
                )
                active_tasks[future] = ActiveWorkerTask(
                    issue_ref=candidate,
                    slot_id=slot_id,
                    launched_at=datetime.now(timezone.utc).isoformat(),
                )
                launched += 1

            sleeper(1.0 if active_tasks or launched else config.poll_interval_seconds)


# ---------------------------------------------------------------------------
# Daemon loop
# ---------------------------------------------------------------------------


def run_daemon_loop(
    config: ConsumerConfig,
    db: ConsumerDB,
    *,
    dry_run: bool = False,
    sleep_fn: Callable[[float], None] | None = None,
    **di_kwargs: Any,
) -> None:
    """Run continuous poll-claim-execute loop."""
    sleeper = sleep_fn or time.sleep
    try:
        auto_config = load_automation_config(config.automation_config_path)
    except ConfigError:
        auto_config = None
    _apply_automation_runtime(config, auto_config)
    try:
        _workflows, workflow_statuses, effective_interval = _current_main_workflows(
            config,
            persist_snapshot=False,
        )
        config.poll_interval_seconds = effective_interval
        repo_summary = ",".join(
            repo_prefix
            for repo_prefix, status in workflow_statuses.items()
            if status.available
        ) or ",".join(config.repo_prefixes)
    except Exception:
        repo_summary = ",".join(config.repo_prefixes)
    logger.info(
        "Starting consumer daemon (interval=%ds, executor=%s, repos=%s, concurrency=%s)",
        config.poll_interval_seconds,
        config.executor,
        repo_summary,
        config.global_concurrency,
    )
    try:
        auto_config = None
        try:
            auto_config = load_automation_config(config.automation_config_path)
        except ConfigError:
            auto_config = None
        _apply_automation_runtime(config, auto_config)
        recovered = _recover_interrupted_sessions(
            config,
            db,
            automation_config=auto_config,
            board_info_resolver=di_kwargs.get("board_info_resolver"),
            board_mutator=di_kwargs.get("board_mutator"),
            gh_runner=di_kwargs.get("gh_runner"),
        )
        if recovered:
            logger.info(
                "Recovered interrupted leases: %s",
                [lease.issue_ref for lease in recovered],
            )
    except Exception:
        logger.exception("Unhandled error recovering interrupted sessions")

    if config.multi_worker_enabled:
        _run_multi_worker_daemon_loop(
            config,
            db,
            dry_run=dry_run,
            sleep_fn=sleep_fn,
            **di_kwargs,
        )
        return

    while True:
        if _drain_requested(config.drain_path):
            logger.info(
                "Drain requested via %s; stopping before next claim",
                config.drain_path,
            )
            return
        try:
            result = run_one_cycle(config, db, dry_run=dry_run, **di_kwargs)
            logger.info("Cycle result: %s", result)
        except Exception:
            logger.exception("Unhandled error in cycle")

        sleeper(config.poll_interval_seconds)


# ---------------------------------------------------------------------------
# CLI: status
# ---------------------------------------------------------------------------


def _local_review_summary(db: ConsumerDB) -> dict[str, Any]:
    """Build review summary from local consumer state only."""
    review_refs = db.latest_review_issue_refs()
    return {
        "count": len(review_refs),
        "refs": review_refs,
        "source": "local",
    }


def _github_review_summary(
    config: ConsumerConfig,
    db: ConsumerDB,
) -> dict[str, Any]:
    """Build review summary from GitHub with local fallback."""
    try:
        cp_config = load_config(config.critical_paths_path)
        review_refs: list[str] = []
        for snapshot in _list_project_items_by_status(
            "Review",
            config.project_owner,
            config.project_number,
        ):
            issue_ref = _snapshot_to_issue_ref(snapshot, cp_config)
            if issue_ref is None:
                continue
            if parse_issue_ref(issue_ref).prefix not in config.repo_prefixes:
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
    db: ConsumerDB,
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
    db: ConsumerDB,
    *,
    hours: int,
    now: datetime,
) -> dict[str, Any]:
    """Return one rolling SLO/throughput window summary."""
    since = now - timedelta(hours=hours)
    counts = db.count_metric_events_since(since)
    occupied_slot_seconds = db.occupied_slot_seconds_since(since, now=now)
    hydration_total = counts.get("context_cache_hit", 0) + counts.get("context_cache_miss", 0)
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
            durable_starts / claim_attempted
            if claim_attempted
            else None
        ),
    }


def _ready_pressure_hours(
    events: list[MetricEvent],
    *,
    minimum_ready: int,
    since: datetime,
    now: datetime,
) -> float:
    """Approximate hours where the queue had at least minimum ready items."""
    if not events:
        return 0.0
    observations: list[tuple[datetime, int]] = []
    for event in events:
        created_at = _parse_iso8601_timestamp(event.created_at)
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
        next_started_at = observations[index + 1][0] if index + 1 < len(observations) else now
        window_end = min(next_started_at, now)
        if ready_count < minimum_ready or window_end <= window_start:
            continue
        total_seconds += (window_end - window_start).total_seconds()
    return total_seconds / 3600.0


def _augment_slo_window_payload(
    db: ConsumerDB,
    payload: dict[str, Any],
    *,
    hours: int,
    now: datetime,
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
    )
    ready_hours_ge_4 = _ready_pressure_hours(
        observations,
        minimum_ready=4,
        since=since,
        now=now,
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


def _collect_status_payload(
    config: ConsumerConfig,
    *,
    local_only: bool = False,
) -> dict[str, Any]:
    """Collect consumer status as a JSON-serializable payload."""
    try:
        auto_config = load_automation_config(config.automation_config_path)
    except ConfigError:
        auto_config = None
    _apply_automation_runtime(config, auto_config)

    main_workflows, workflow_statuses, effective_interval = _current_main_workflows(
        config,
        persist_snapshot=False,
    )
    persisted_snapshot = read_workflow_snapshot(config.workflow_state_path)
    last_reload_at = (
        persisted_snapshot.generated_at
        if persisted_snapshot is not None
        else None
    )

    db = ConsumerDB(db_path=config.db_path)
    status_now = datetime.now(timezone.utc)
    try:
        leases = db.active_lease_count()
        slots = sorted(db.active_slot_ids())
        workers = db.active_workers()
        sessions = db.recent_sessions(limit=10)
        control_state = db.control_state_snapshot()
        deferred_action_count = db.deferred_action_count()
        oldest_deferred_action_age_seconds = db.oldest_deferred_action_age_seconds()
        review_summary = (
            _local_review_summary(db)
            if local_only
            else _github_review_summary(config, db)
        )
        review_queue = _local_review_queue_summary(db, now=status_now)
        admission_summary = _load_admission_summary(control_state, auto_config)
        throughput_1h = _augment_slo_window_payload(
            db,
            _metric_window_payload(db, hours=1, now=status_now),
            hours=1,
            now=status_now,
        )
        throughput_24h = _augment_slo_window_payload(
            db,
            _metric_window_payload(db, hours=24, now=status_now),
            hours=24,
            now=status_now,
        )
    finally:
        db.close()

    degraded = control_state.get(CONTROL_KEY_DEGRADED) == "true"
    claim_suppressed_until = control_state.get(CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL)
    claim_suppressed_reason = control_state.get(CONTROL_KEY_CLAIM_SUPPRESSED_REASON)
    claim_suppressed_scope = control_state.get(CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE)
    last_rate_limit_at = control_state.get(CONTROL_KEY_LAST_RATE_LIMIT_AT)
    control_plane_health = _control_plane_health_summary(
        control_state,
        deferred_action_count=deferred_action_count,
        oldest_deferred_action_age_seconds=oldest_deferred_action_age_seconds,
        poll_interval_seconds=effective_interval,
    )
    lane_wip_limits: dict[str, dict[str, int]] = {}
    if auto_config is not None:
        for executor in auto_config.execution_authority_executors:
            executor_limits = auto_config.wip_limits.get(executor, {})
            lane_wip_limits[executor] = {
                repo_prefix: executor_limits.get(repo_prefix, 1)
                for repo_prefix in auto_config.execution_authority_repos
            }

    return {
        "active_leases": leases,
        "active_slots": slots,
        "workers": [
            {
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
            for worker in workers
        ],
        "repo_prefixes": list(config.repo_prefixes),
        "global_concurrency": config.global_concurrency,
        "lane_wip_limits": lane_wip_limits,
        "poll_interval_seconds": effective_interval,
        "drain_requested": _drain_requested(config.drain_path),
        "drain_path": str(config.drain_path),
        "workflow_state_path": str(config.workflow_state_path),
        "workflow_last_reload_at": last_reload_at,
        "degraded": degraded,
        "degraded_reason": control_state.get(CONTROL_KEY_DEGRADED_REASON),
        "claim_suppressed_until": claim_suppressed_until,
        "claim_suppressed_reason": claim_suppressed_reason,
        "claim_suppressed_scope": claim_suppressed_scope,
        "last_rate_limit_at": last_rate_limit_at,
        "last_successful_board_sync_at": control_state.get(
            CONTROL_KEY_LAST_SUCCESSFUL_BOARD_SYNC_AT
        ),
        "last_successful_github_mutation_at": control_state.get(
            CONTROL_KEY_LAST_SUCCESSFUL_GITHUB_MUTATION_AT
        ),
        "deferred_action_count": deferred_action_count,
        "oldest_deferred_action_age_seconds": oldest_deferred_action_age_seconds,
        "control_plane_health": control_plane_health,
        "throughput_metrics": {
            "baseline_status": "pending-soak",
            "windows": {"1h": throughput_1h, "24h": throughput_24h},
        },
        "reliability_metrics": {
            "durable_start_reliability_1h": throughput_1h["durable_start_reliability"],
            "durable_start_reliability_24h": throughput_24h["durable_start_reliability"],
            "startup_failures_1h": throughput_1h["startup_failures"],
            "startup_failures_24h": throughput_24h["startup_failures"],
        },
        "context_cache_metrics": {
            "hit_rate_1h": throughput_1h["cache_hit_rate"],
            "hit_rate_24h": throughput_24h["cache_hit_rate"],
            "hits_1h": throughput_1h["cache_hits"],
            "hits_24h": throughput_24h["cache_hits"],
            "misses_1h": throughput_1h["cache_misses"],
            "misses_24h": throughput_24h["cache_misses"],
        },
        "worktree_reuse_metrics": {
            "reused_1h": throughput_1h["worktree_reused"],
            "reused_24h": throughput_24h["worktree_reused"],
            "blocked_1h": throughput_1h["worktree_blocked"],
            "blocked_24h": throughput_24h["worktree_blocked"],
        },
        "review_summary": review_summary,
        "review_queue": review_queue,
        "admission": admission_summary,
        "repo_workflows": {
            repo_prefix: workflow_status_payload(status)
            for repo_prefix, status in workflow_statuses.items()
        },
        "recent_sessions": [
            {
                "id": s.id,
                "issue_ref": s.issue_ref,
                "status": s.status,
                "executor": s.executor,
                "slot_id": s.slot_id,
                "phase": s.phase,
                "session_kind": s.session_kind,
                "repair_pr_url": s.repair_pr_url,
                "branch_reconcile_state": s.branch_reconcile_state,
                "branch_reconcile_error": s.branch_reconcile_error,
                "started_at": s.started_at,
                "completed_at": s.completed_at,
                "pr_url": s.pr_url,
                "resolution_kind": s.resolution_kind,
                "verification_class": s.verification_class,
                "resolution_action": s.resolution_action,
                "done_reason": s.done_reason,
                **_session_retry_state(
                    s,
                    config=config,
                    workflows=main_workflows,
                    now=status_now,
                ),
            }
            for s in sessions
        ],
        "local_only": local_only,
    }


def _cmd_status(
    config: ConsumerConfig,
    *,
    as_json: bool = False,
    local_only: bool = False,
) -> int:
    """Show current consumer state."""
    data = _collect_status_payload(config, local_only=local_only)

    if as_json:
        print(json.dumps(data, indent=2))
    else:
        print(f"Active leases: {data['active_leases']}")
        print(f"Active slots: {data['active_slots']}")
        print(f"Deferred actions: {data['deferred_action_count']}")
        if data["oldest_deferred_action_age_seconds"] is not None:
            print(
                "Oldest deferred age (s): "
                f"{data['oldest_deferred_action_age_seconds']:.1f}"
            )
        print(
            "Control-plane health: "
            f"{data['control_plane_health']['health']} "
            f"({data['control_plane_health']['reason_code']})"
        )
        print(f"Poll interval: {data['poll_interval_seconds']}s")
        print(
            f"Degraded: {'yes' if data['degraded'] else 'no'}"
            + (
                f" ({data['degraded_reason']})"
                if data["degraded"] and data["degraded_reason"]
                else ""
            )
        )
        if data["claim_suppressed_until"]:
            print(
                "Claim suppression: "
                f"{data['claim_suppressed_scope']} until {data['claim_suppressed_until']}"
            )
            if data["claim_suppressed_reason"]:
                print(f"Claim suppression reason: {data['claim_suppressed_reason']}")
        if data["local_only"]:
            print("Review source: local-only")
        if data["last_successful_board_sync_at"]:
            print(
                "Last successful board sync: "
                f"{data['last_successful_board_sync_at']}"
            )
        if data["last_successful_github_mutation_at"]:
            print(
                "Last successful GitHub mutation: "
                f"{data['last_successful_github_mutation_at']}"
            )
        print(
            "Durable starts (1h/24h): "
            f"{data['throughput_metrics']['windows']['1h']['durable_starts']}/"
            f"{data['throughput_metrics']['windows']['24h']['durable_starts']}"
        )
        print(
            "Occupied slots/hour (1h/24h): "
            f"{data['throughput_metrics']['windows']['1h']['occupied_slots_per_hour']:.2f}/"
            f"{data['throughput_metrics']['windows']['24h']['occupied_slots_per_hour']:.2f}"
        )
        print(
            "Drain requested: "
            f"{'yes' if data['drain_requested'] else 'no'} "
            f"({data['drain_path']})"
        )
        print(f"Workflow snapshot: {data['workflow_state_path']}")
        if data["workflow_last_reload_at"]:
            print(f"Workflow last reload: {data['workflow_last_reload_at']}")
        print("Repo workflows:")
        for repo_prefix in sorted(data["repo_workflows"]):
            status = data["repo_workflows"][repo_prefix]
            detail = (
                "available"
                if status["available"]
                else f"disabled ({status['disabled_reason']})"
            )
            print(
                f"  {repo_prefix}: {detail} "
                f"[{status['source_kind']}] {status['source_path']}"
            )
        print(f"Review summary: {data['review_summary']}")
        print(f"Review queue: {data['review_queue']}")
        print(f"Recent sessions ({len(data['recent_sessions'])}):")
        for session in data["recent_sessions"]:
            pr = f" PR: {session['pr_url']}" if session["pr_url"] else ""
            slot = f" slot={session['slot_id']}" if session["slot_id"] is not None else ""
            phase = f" phase={session['phase']}" if session["phase"] else ""
            kind = f" kind={session['session_kind']}"
            reconcile = (
                f" reconcile={session['branch_reconcile_state']}"
                if session["branch_reconcile_state"]
                else ""
            )
            failure = (
                f" failure={session['failure_reason']}"
                if session["failure_reason"]
                else ""
            )
            retry = (
                f" retry={session['retry_count']}"
                if session["retry_count"]
                else ""
            )
            next_retry = (
                f" next_retry={session['next_retry_at']}"
                if session["next_retry_at"]
                else ""
            )
            resolution = (
                f" resolution={session['resolution_kind']}/{session['verification_class']}"
                if session["resolution_kind"]
                else ""
            )
            resolution_action = (
                f" action={session['resolution_action']}"
                if session["resolution_action"]
                else ""
            )
            done_reason = (
                f" done={session['done_reason']}"
                if session["done_reason"]
                else ""
            )
            print(
                f"  {session['id']}  {session['issue_ref']:>10}  "
                f"{session['status']:<8}  {session['executor']}{slot}{phase}"
                f"{kind}{reconcile}"
                f"{failure}{retry}{next_retry}{resolution}{resolution_action}{done_reason}{pr}"
            )

    return 0


def _cmd_report_slo(
    config: ConsumerConfig,
    *,
    as_json: bool = False,
    local_only: bool = False,
) -> int:
    """Report rolling reliability and throughput metrics."""
    data = _collect_status_payload(config, local_only=local_only)
    report = {
        "baseline_status": data["throughput_metrics"]["baseline_status"],
        "claim_suppressed_until": data["claim_suppressed_until"],
        "claim_suppressed_reason": data["claim_suppressed_reason"],
        "claim_suppressed_scope": data["claim_suppressed_scope"],
        "degraded": data["degraded"],
        "degraded_reason": data["degraded_reason"],
        "windows": data["throughput_metrics"]["windows"],
        "reliability_metrics": data["reliability_metrics"],
        "context_cache_metrics": data["context_cache_metrics"],
        "worktree_reuse_metrics": data["worktree_reuse_metrics"],
    }
    if as_json:
        print(json.dumps(report, indent=2))
        return 0

    print(f"Baseline status: {report['baseline_status']}")
    if report["claim_suppressed_until"]:
        print(
            "Claim suppression: "
            f"{report['claim_suppressed_scope']} until {report['claim_suppressed_until']}"
        )
        if report["claim_suppressed_reason"]:
            print(f"Claim suppression reason: {report['claim_suppressed_reason']}")
    print(
        "Degraded: "
        f"{'yes' if report['degraded'] else 'no'}"
        + (
            f" ({report['degraded_reason']})"
            if report["degraded"] and report["degraded_reason"]
            else ""
        )
    )
    for window_name in ("1h", "24h"):
        window = report["windows"][window_name]
        print(f"{window_name}:")
        print(
            "  durable_starts="
            f"{window['durable_starts']} startup_failures={window['startup_failures']} "
            f"reliability={window['durable_start_reliability']}"
        )
        print(
            "  occupied_slots_per_hour="
            f"{window['occupied_slots_per_hour']:.2f} "
            f"occupied_slots_per_ready_hour_ge_1={window['occupied_slots_per_ready_hour_ge_1']}"
        )
        print(
            "  ready_hours_ge_1="
            f"{window['ready_hours_ge_1']:.2f} "
            f"ready_hours_ge_4={window['ready_hours_ge_4']:.2f}"
        )
    return 0


def _create_status_http_server(
    config: ConsumerConfig,
    *,
    host: str = DEFAULT_STATUS_HOST,
    port: int = DEFAULT_STATUS_PORT,
) -> ThreadingHTTPServer:
    """Create a local HTTP server that exposes consumer status."""

    class StatusHandler(BaseHTTPRequestHandler):
        def _write_json(self, payload: dict[str, Any], *, status_code: int = 200) -> None:
            body = json.dumps(payload, indent=2).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            if self.path in {"/", "/status"}:
                self._write_json(_collect_status_payload(config, local_only=True))
                return
            if self.path == "/healthz":
                payload = _collect_status_payload(config, local_only=True)
                self._write_json(
                    {
                        "ok": True,
                        "health": payload["control_plane_health"]["health"],
                        "degraded": payload["degraded"],
                        "degraded_reason": payload["degraded_reason"],
                    }
                )
                return
            self._write_json({"error": "not_found", "path": self.path}, status_code=404)

        def log_message(self, format: str, *args: Any) -> None:
            logger.debug("status-http %s - %s", self.address_string(), format % args)

    return ThreadingHTTPServer((host, port), StatusHandler)


def _cmd_serve_status(
    config: ConsumerConfig,
    *,
    host: str = DEFAULT_STATUS_HOST,
    port: int = DEFAULT_STATUS_PORT,
) -> int:
    """Serve local-only consumer status over localhost HTTP."""
    server = _create_status_http_server(config, host=host, port=port)
    logger.info("Serving local consumer status on http://%s:%s", host, server.server_port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Status server stopped by user")
    finally:
        server.server_close()
    return 0


def _cmd_reconcile(config: ConsumerConfig, *, dry_run: bool = False) -> int:
    """Reconcile board truth against local consumer state."""
    db = ConsumerDB(db_path=config.db_path)
    try:
        cp_config = load_config(config.critical_paths_path)
        auto_config = load_automation_config(config.automation_config_path)
        _apply_automation_runtime(config, auto_config)
        session_store = SqliteSessionStore(db)
        result = _reconcile_board_truth(
            config,
            cp_config,
            auto_config,
            db,
            session_store=session_store,
            dry_run=dry_run,
        )
    finally:
        db.close()

    data = {
        "dry_run": dry_run,
        "moved_ready": list(result.moved_ready),
        "moved_in_progress": list(result.moved_in_progress),
        "moved_review": list(result.moved_review),
        "moved_blocked": list(result.moved_blocked),
    }
    print(json.dumps(data, indent=2))
    return 0


def _cmd_drain(config: ConsumerConfig) -> int:
    """Request a graceful drain at the next cycle boundary."""
    _request_drain(config.drain_path)
    print(
        json.dumps(
            {
                "drain_requested": True,
                "drain_path": str(config.drain_path),
            },
            indent=2,
        )
    )
    return 0


def _cmd_resume(config: ConsumerConfig) -> int:
    """Clear a pending graceful drain request."""
    cleared = _clear_drain(config.drain_path)
    print(
        json.dumps(
            {
                "drain_requested": False,
                "drain_path": str(config.drain_path),
                "cleared": cleared,
            },
            indent=2,
        )
    )
    return 0


# ---------------------------------------------------------------------------
# CLI parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Board consumer daemon — poll, claim, execute.",
    )
    parser.add_argument(
        "--file",
        default=DEFAULT_CONFIG_PATH,
        help="Path to critical-paths.json",
    )
    parser.add_argument(
        "--automation-config",
        default=DEFAULT_AUTOMATION_CONFIG_PATH,
        help="Path to board-automation-config.json",
    )
    parser.add_argument(
        "--project-owner",
        default="StartupAI-site",
    )
    parser.add_argument(
        "--project-number",
        type=int,
        default=1,
    )

    sub = parser.add_subparsers(dest="command")

    # run
    run_p = sub.add_parser("run", help="Run daemon loop")
    run_p.add_argument("--interval", type=int, default=180, help="Poll interval seconds")
    run_p.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    run_p.add_argument("--dry-run", action="store_true")
    run_p.add_argument("--verbose", action="store_true")

    # one-shot
    one_p = sub.add_parser("one-shot", help="Run one cycle")
    one_p.add_argument("--issue", default=None, help="Target issue ref (e.g. crew#84)")
    one_p.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    one_p.add_argument("--dry-run", action="store_true")
    one_p.add_argument("--verbose", action="store_true")

    # status
    stat_p = sub.add_parser("status", help="Show consumer state")
    stat_p.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    stat_p.add_argument("--json", action="store_true", dest="as_json")
    stat_p.add_argument(
        "--local-only",
        action="store_true",
        help="Skip GitHub queries and report local consumer state only",
    )

    report_p = sub.add_parser("report-slo", help="Show rolling reliability and throughput metrics")
    report_p.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    report_p.add_argument("--json", action="store_true", dest="as_json")
    report_p.add_argument(
        "--local-only",
        action="store_true",
        help="Skip GitHub queries and report local consumer state only",
    )

    serve_p = sub.add_parser(
        "serve-status",
        help="Serve local-only consumer status over localhost HTTP",
    )
    serve_p.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    serve_p.add_argument("--host", default=DEFAULT_STATUS_HOST)
    serve_p.add_argument("--port", type=int, default=DEFAULT_STATUS_PORT)

    # reconcile
    rec_p = sub.add_parser(
        "reconcile",
        help="Reconcile board In Progress truth against local consumer state",
    )
    rec_p.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    rec_p.add_argument("--dry-run", action="store_true")

    drain_p = sub.add_parser(
        "drain",
        help="Request a graceful drain before the next issue claim",
    )
    drain_p.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)

    resume_p = sub.add_parser(
        "resume",
        help="Clear a pending graceful drain request",
    )
    resume_p.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 3

    log_level = logging.DEBUG if getattr(args, "verbose", False) else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    db_path = getattr(args, "db_path", DEFAULT_DB_PATH)
    config = ConsumerConfig(
        critical_paths_path=Path(args.file),
        automation_config_path=Path(args.automation_config),
        project_owner=args.project_owner,
        project_number=args.project_number,
        db_path=db_path,
        poll_interval_seconds=getattr(args, "interval", 180),
    )
    try:
        auto_config = load_automation_config(config.automation_config_path)
    except ConfigError:
        auto_config = None
    _apply_automation_runtime(config, auto_config)

    if args.command == "status":
        return _cmd_status(
            config,
            as_json=getattr(args, "as_json", False),
            local_only=getattr(args, "local_only", False),
        )
    if args.command == "report-slo":
        return _cmd_report_slo(
            config,
            as_json=getattr(args, "as_json", False),
            local_only=getattr(args, "local_only", False),
        )
    if args.command == "serve-status":
        return _cmd_serve_status(
            config,
            host=getattr(args, "host", DEFAULT_STATUS_HOST),
            port=getattr(args, "port", DEFAULT_STATUS_PORT),
        )
    if args.command == "reconcile":
        return _cmd_reconcile(config, dry_run=getattr(args, "dry_run", False))
    if args.command == "drain":
        return _cmd_drain(config)
    if args.command == "resume":
        return _cmd_resume(config)

    db = ConsumerDB(db_path=config.db_path)

    if args.command == "one-shot":
        result = run_one_cycle(
            config,
            db,
            dry_run=args.dry_run,
            target_issue=getattr(args, "issue", None),
        )
        logger.info("Result: %s", result)
        db.close()
        if result.action == "idle":
            return 2
        if result.action == "error":
            return 4
        return 0

    if args.command == "run":
        try:
            run_daemon_loop(config, db, dry_run=args.dry_run)
        except KeyboardInterrupt:
            logger.info("Daemon stopped by user")
        finally:
            db.close()
        return 0

    return 3


if __name__ == "__main__":
    sys.exit(main())
