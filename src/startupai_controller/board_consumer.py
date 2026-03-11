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

from concurrent.futures import Future, ThreadPoolExecutor
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable


from startupai_controller.board_automation import (
    mark_issues_done,
    _set_blocked_with_reason,
    review_rescue,
    sync_review_state,
)
from startupai_controller import consumer_board_state_helpers as _board_state_helpers
import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
import startupai_controller.consumer_codex_runtime_wiring as _codex_runtime_wiring
from startupai_controller import consumer_deferred_action_helpers as _deferred_action_helpers
import startupai_controller.consumer_execution_outcome_wiring as _execution_outcome_wiring
from startupai_controller import consumer_execution_support_helpers as _execution_support_helpers
import startupai_controller.consumer_launch_support_wiring as _launch_support_wiring
from startupai_controller import consumer_resolution_helpers as _resolution_helpers
from startupai_controller import consumer_review_queue_helpers as _review_queue_helpers
import startupai_controller.consumer_selection_retry_wiring as _selection_retry_wiring
from startupai_controller.board_automation_config import (
    BoardAutomationConfig,
    load_automation_config,
)
from startupai_controller.board_graph import (
    _ready_snapshot_rank,
    _resolve_issue_coordinates,
    admission_watermarks,
)
from startupai_controller.consumer_config import (
    ConsumerConfig,
    DEFAULT_AUTOMATION_CONFIG_PATH,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DB_PATH,
    DEFAULT_DRAIN_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SCHEMA_PATH,
    DEFAULT_WORKFLOW_STATE_PATH,
)
from startupai_controller.consumer_types import (
    ActiveWorkerTask,
    ClaimedSessionContext,
    PrCreationOutcome,
    PreparedCycleContext,
    PreparedDueReviewProcessing,
    PreparedLaunchContext,
    PreparedReviewQueueBatch,
    ReviewGroupProcessingOutcome,
    ReviewQueueProcessingOutcome,
    SelectedLaunchCandidate,
    SessionExecutionOutcome,
    WorktreePrepareError,
)
from startupai_controller.consumer_context_helpers import (
    fetch_issue_context as _fetch_issue_context_helper,
    hydrate_issue_context as _hydrate_issue_context_helper,
    issue_context_cache_is_fresh as _issue_context_cache_is_fresh_helper,
    snapshot_for_issue as _snapshot_for_issue_helper,
)
import startupai_controller.consumer_comment_pr_wiring as _comment_pr_wiring
from startupai_controller.consumer_worktree_helpers import (
    list_repo_worktrees as _list_repo_worktrees_helper,
    worktree_is_clean as _worktree_is_clean_helper,
    worktree_ownership_is_safe as _worktree_ownership_is_safe_helper,
)
import startupai_controller.consumer_claim_helpers as _claim_helpers
import startupai_controller.consumer_claim_wiring as _claim_wiring
import startupai_controller.consumer_cycle_wiring as _cycle_wiring
import startupai_controller.consumer_operational_wiring as _operational_wiring
import startupai_controller.consumer_recovery_helpers as _recovery_helpers
import startupai_controller.consumer_review_queue_wiring as _review_queue_wiring
import startupai_controller.consumer_runtime_wiring as _runtime_wiring
import startupai_controller.consumer_session_completion_helpers as _session_completion_helpers
from startupai_controller.control_plane_runtime import (
    CONTROL_KEY_CLAIM_SUPPRESSED_REASON,
    CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE,
    CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL,
    CONTROL_KEY_DEGRADED,
    CONTROL_KEY_DEGRADED_REASON,
    CONTROL_KEY_LAST_ADMISSION_SUMMARY,
    CONTROL_KEY_LAST_RATE_LIMIT_AT,
    CONTROL_KEY_LAST_SUCCESSFUL_BOARD_SYNC_AT,
    CONTROL_KEY_LAST_SUCCESSFUL_GITHUB_MUTATION_AT,
    _apply_automation_runtime,
    _clear_degraded,
    _control_plane_health_summary,
    _current_main_workflows,
    _mark_degraded,
    _persist_admission_summary,
    _record_control_timestamp,
    _record_successful_board_sync,
    _record_successful_github_mutation,
)
from startupai_controller.control_plane_rescue import (
    _drain_review_queue,
    _replay_deferred_actions,
)
from startupai_controller.application.consumer.cycle import run_prepared_cycle
from startupai_controller.application.consumer.preflight import (
    PrepareCyclePhasesDeps,
    ReconciliationDeps,
    ReconciliationResult,
    execute_prepare_cycle_phases as _execute_prepare_cycle_phases_use_case,
    reconcile_board_truth as _reconcile_board_truth_use_case,
)
from startupai_controller.application.consumer.preflight_runtime import (
    CycleRuntimeContext as _AppCycleRuntimeContext,
    InitializeCycleRuntimeDeps,
    PhaseHelperDeps,
    PrepareCycleDeps,
    initialize_cycle_runtime as _initialize_cycle_runtime_use_case,
    run_deferred_replay_phase as _run_deferred_replay_phase_use_case,
    load_board_snapshot_phase as _load_board_snapshot_phase_use_case,
    run_executor_routing_phase as _run_executor_routing_phase_use_case,
    run_reconciliation_phase as _run_reconciliation_phase_use_case,
    run_review_queue_phase as _run_review_queue_phase_use_case,
    run_admission_phase as _run_admission_phase_use_case,
    prepare_cycle as _prepare_cycle_use_case,
)
from startupai_controller.application.consumer.reconciliation import (
    ReconciliationWiringDeps,
    wire_reconcile_board_truth as _wire_reconcile_board_truth_use_case,
)
from startupai_controller.application.consumer.recovery import (
    recover_interrupted_sessions as _recover_interrupted_sessions_use_case,
)
from startupai_controller.application.consumer.daemon import (
    log_completed_worker_results as _log_completed_worker_results_use_case,
    next_available_slots as _next_available_slots_use_case,
    run_worker_cycle as _run_worker_cycle_use_case,
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
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.issue_context import IssueContextPort
from startupai_controller.ports.ready_flow import ReadyFlowPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.ports.session_store import SessionStorePort
from startupai_controller.ports.worktrees import WorktreePort
from startupai_controller.runtime.wiring import (
    begin_runtime_request_stats,
    build_github_port_bundle,
    build_gh_runner_port,
    build_process_runner_port,
    build_ready_flow_port,
    build_session_store,
    build_worktree_port,
    clear_github_runtime_caches,
    end_runtime_request_stats,
    GitHubRuntimeMemo as CycleGitHubMemo,
    open_consumer_db,
    run_runtime_gh as _run_gh,
    runtime_gh_reason_code as gh_reason_code,
)
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
    LinkedIssue,
    CycleBoardSnapshot,
    ProjectItemSnapshot as _ProjectItemSnapshot,
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
    marker_for as _marker_for,
    parse_pr_url as _parse_pr_url,
)
from startupai_controller.domain.scheduling_policy import (
    snapshot_to_issue_ref as _snapshot_to_issue_ref,
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

if TYPE_CHECKING:
    from startupai_controller.runtime.wiring import (
        ConsumerDB,
        MetricEvent,
        RecoveredLease,
    )

logger = logging.getLogger("board-consumer")


def claim_ready_issue(
    config,
    project_owner: str,
    project_number: int,
    **kwargs,
):
    """Compatibility wrapper around the ready-flow port claim operation."""
    ready_flow_port = kwargs.pop("ready_flow_port", None) or build_ready_flow_port()
    return ready_flow_port.claim_ready_issue(
        config,
        project_owner,
        project_number,
        **kwargs,
    )

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_STATUS_HOST = "127.0.0.1"
DEFAULT_STATUS_PORT = 8765
# Review queue constants: re-exported from domain.review_queue_policy


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


# CycleRuntimeContext: re-exported from application.consumer.preflight_runtime
CycleRuntimeContext = _AppCycleRuntimeContext


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
    return _selection_retry_wiring.effective_retry_backoff(
        config,
        workflow,
        effective_retry_backoff_primitives=_effective_retry_backoff_primitives,
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
    return _selection_retry_wiring.retry_backoff_active(
        db,
        issue_ref,
        base_seconds=base_seconds,
        max_seconds=max_seconds,
        session_retry_due_at=_session_retry_due_at,
    )


def _session_retry_state(
    session: SessionInfo,
    *,
    config: ConsumerConfig,
    workflows: dict[str, WorkflowDefinition],
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return retry metadata for a session."""
    return _selection_retry_wiring.session_retry_state(
        session,
        config=config,
        workflows=workflows,
        parse_issue_ref=parse_issue_ref,
        effective_retry_backoff=_effective_retry_backoff,
        session_retry_due_at=_session_retry_due_at,
        retry_delay_seconds=_retry_delay_seconds,
        now=now,
    )


def _repo_root_for_issue_ref(config: ConsumerConfig, issue_ref: str) -> Path:
    """Return the canonical main-checkout root for an issue ref."""
    return _resolution_helpers.repo_root_for_issue_ref(
        config,
        issue_ref,
        parse_issue_ref=parse_issue_ref,
        config_error_type=ConfigError,
    )


def _verify_code_refs_on_main(
    repo_root: Path,
    code_refs: list[str],
) -> tuple[bool, list[str]]:
    """Verify that every referenced path exists on canonical main."""
    return _resolution_helpers.verify_code_refs_on_main(repo_root, code_refs)


def _run_validation_on_main(
    repo_root: Path,
    command: str,
    *,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> tuple[bool, int | None, str]:
    """Run the repo validation command against canonical main."""
    return _resolution_helpers.run_validation_on_main(
        repo_root,
        command,
        subprocess_runner=subprocess_runner,
    )


def _commit_reachable_from_origin_main(
    repo_root: Path,
    commit_sha: str,
    *,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> bool:
    """Return True when a commit is reachable from origin/main."""
    return _resolution_helpers.commit_reachable_from_origin_main(
        repo_root,
        commit_sha,
        subprocess_runner=subprocess_runner,
    )


def _pr_is_merged(
    pr_url: str,
    *,
    pr_port: PullRequestPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Return True when a PR URL points at a merged pull request."""
    return _resolution_helpers.pr_is_merged(
        pr_url,
        pr_port=pr_port,
        gh_runner=gh_runner,
        parse_pr_url=_parse_pr_url,
        run_gh=_run_gh,
    )


def _verify_resolution_payload(
    issue_ref: str,
    resolution: dict[str, Any] | None,
    *,
    config: ConsumerConfig,
    workflows: dict[str, WorkflowDefinition],
    pr_port: PullRequestPort | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ResolutionEvaluation:
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
        repo_root_for_issue_ref_fn=_repo_root_for_issue_ref,
        resolution_validation_command_fn=_resolution_validation_command,
        resolution_evidence_payload_fn=_resolution_evidence_payload,
        resolution_is_strong_fn=_resolution_is_strong,
        resolution_blocked_reason_fn=_resolution_blocked_reason,
    )


def _resolution_validation_command(
    issue_ref: str,
    normalized: dict[str, Any],
    *,
    config: ConsumerConfig,
    workflows: dict[str, WorkflowDefinition],
) -> str:
    """Resolve the validation command for a resolution verification run."""
    return _resolution_helpers.resolution_validation_command(
        issue_ref,
        normalized,
        config=config,
        workflows=workflows,
        parse_issue_ref=parse_issue_ref,
    )


def _resolution_evidence_payload(
    repo_root: Path,
    normalized: dict[str, Any],
    validation_command: str,
    *,
    pr_port: PullRequestPort | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
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
        verify_code_refs_on_main_fn=_verify_code_refs_on_main,
        commit_reachable_from_origin_main_fn=_commit_reachable_from_origin_main,
        pr_is_merged_fn=_pr_is_merged,
    )


def _resolution_is_strong(
    normalized: dict[str, Any],
    evidence: dict[str, Any],
) -> bool:
    """Return True when resolution evidence is strong enough to auto-close."""
    return _resolution_helpers.resolution_is_strong(
        normalized,
        evidence,
        resolution_allows_autoclose=resolution_allows_autoclose,
    )


def _resolution_blocked_reason(
    normalized: dict[str, Any],
    evidence: dict[str, Any],
) -> str:
    """Return the deterministic blocked reason for a non-strong resolution."""
    return _resolution_helpers.resolution_blocked_reason(
        normalized,
        evidence,
        resolution_allows_autoclose=resolution_allows_autoclose,
    )


def _queue_issue_comment(
    db: ConsumerDB,
    issue_ref: str,
    body: str,
) -> None:
    """Queue an issue comment for replay after GitHub recovery."""
    _resolution_helpers.queue_issue_comment(db, issue_ref, body)


def _queue_issue_close(
    db: ConsumerDB,
    issue_ref: str,
) -> None:
    """Queue an issue close mutation for replay."""
    _resolution_helpers.queue_issue_close(db, issue_ref)


def _set_issue_handoff_target(
    issue_ref: str,
    target: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set the board Handoff To field for an issue."""
    _resolution_helpers.set_issue_handoff_target(
        issue_ref,
        target,
        config,
        project_owner,
        project_number,
        board_port=board_port,
        gh_runner=gh_runner,
        build_github_port_bundle=build_github_port_bundle,
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
    return _resolution_helpers.apply_resolution_action(
        issue_ref,
        evaluation,
        session_id=session_id,
        db=db,
        config=config,
        critical_path_config=critical_path_config,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        build_resolution_comment=build_resolution_comment,
        mark_issues_done=mark_issues_done,
        record_successful_github_mutation=_record_successful_github_mutation,
        mark_degraded=_mark_degraded,
        gh_reason_code=gh_reason_code,
        queue_status_transition=_queue_status_transition,
        runtime_comment_poster=_runtime_comment_poster,
        runtime_issue_closer=_runtime_issue_closer,
        set_blocked_with_reason=_set_blocked_with_reason,
        set_issue_handoff_target_fn=_set_issue_handoff_target,
        linked_issue_type=LinkedIssue,
        gh_query_error_type=GhQueryError,
    )


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
    _execution_support_helpers.run_workspace_hooks(
        commands,
        worktree_path=worktree_path,
        issue_ref=issue_ref,
        branch_name=branch_name,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
        build_worktree_port=build_worktree_port,
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
    return _selection_retry_wiring.select_best_candidate(
        config,
        project_owner,
        project_number,
        executor=executor,
        this_repo_prefix=this_repo_prefix,
        repo_prefixes=repo_prefixes,
        status_resolver=status_resolver,
        ready_items=ready_items,
        github_memo=github_memo,
        gh_runner=gh_runner,
        issue_filter=issue_filter,
        build_github_port_bundle=build_github_port_bundle,
        parse_issue_ref=parse_issue_ref,
        config_error_type=ConfigError,
        snapshot_to_issue_ref=_snapshot_to_issue_ref,
        in_any_critical_path=in_any_critical_path,
        evaluate_ready_promotion=evaluate_ready_promotion,
        ready_snapshot_rank=_ready_snapshot_rank,
    )


def _list_project_items_by_status(
    status: str,
    project_owner: str,
    project_number: int,
    *,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[_ProjectItemSnapshot]:
    """Compatibility helper that reads board snapshots through ReviewStatePort."""
    return _selection_retry_wiring.list_project_items_by_status(
        status,
        project_owner,
        project_number,
        config=config,
        gh_runner=gh_runner,
        build_github_port_bundle=build_github_port_bundle,
    )


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
    return _fetch_issue_context_helper(
        owner,
        repo,
        number,
        build_github_port_bundle=build_github_port_bundle,
        issue_context_port=issue_context_port,
        gh_runner=gh_runner,
    )


def _snapshot_for_issue(
    board_snapshot: CycleBoardSnapshot,
    issue_ref: str,
    config: CriticalPathConfig,
) -> _ProjectItemSnapshot | None:
    """Return the thin board snapshot row for an issue ref."""
    return _snapshot_for_issue_helper(
        board_snapshot,
        issue_ref,
        config,
        snapshot_to_issue_ref=_snapshot_to_issue_ref,
    )


def _issue_context_cache_is_fresh(
    cached: Any,
    *,
    snapshot_updated_at: str,
    now: datetime,
) -> bool:
    """Return True when cached issue context is safe to reuse."""
    return _issue_context_cache_is_fresh_helper(
        cached,
        snapshot_updated_at=snapshot_updated_at,
        now=now,
        parse_iso8601_timestamp=_parse_iso8601_timestamp,
    )


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


def _list_repo_worktrees(
    repo_root: Path,
    *,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> list[tuple[str, str]]:
    """Return (worktree_path, branch_name) pairs for a repo root."""
    return _list_repo_worktrees_helper(
        repo_root,
        build_worktree_port=build_worktree_port,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )


def _worktree_is_clean(
    worktree_path: str,
    *,
    worktree_port: WorktreePort | None = None,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> bool:
    """Return True when a worktree has no local changes."""
    return _worktree_is_clean_helper(
        worktree_path,
        build_worktree_port=build_worktree_port,
        worktree_port=worktree_port,
        subprocess_runner=subprocess_runner,
    )


def _worktree_ownership_is_safe(
    store: SessionStorePort,
    issue_ref: str,
    worktree_path: str,
) -> bool:
    """Return True when a clean worktree is safe to adopt for an issue."""
    return _worktree_ownership_is_safe_helper(store, issue_ref, worktree_path)


_prepare_worktree = _launch_support_wiring.prepare_worktree


# ---------------------------------------------------------------------------
# Helper: create worktree
# ---------------------------------------------------------------------------


_create_worktree = _launch_support_wiring.create_worktree


_fast_forward_existing_worktree = _launch_support_wiring.fast_forward_existing_worktree


_git_command_detail = _launch_support_wiring.git_command_detail


_reconcile_repair_branch = _launch_support_wiring.reconcile_repair_branch


# ---------------------------------------------------------------------------
# Helper: assemble codex prompt
# ---------------------------------------------------------------------------


_assemble_codex_prompt = _codex_comment_wiring.assemble_codex_prompt


def _extract_acceptance_criteria(body: str) -> str:
    """Extract acceptance criteria section from issue body."""
    return _comment_pr_wiring.extract_acceptance_criteria(body)


# ---------------------------------------------------------------------------
# Helper: run codex session
# ---------------------------------------------------------------------------


_run_codex_session = _codex_comment_wiring.run_codex_session


def _resolve_cli_command(command: str) -> str:
    """Resolve a CLI binary without relying on interactive shell PATH setup."""
    return _codex_runtime_wiring.resolve_cli_command(command)


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


_parse_codex_result = _codex_comment_wiring.parse_codex_result


# ---------------------------------------------------------------------------
# Helper: create or update PR
# ---------------------------------------------------------------------------


_create_or_update_pr = _codex_comment_wiring.create_or_update_pr


_build_pr_body = _codex_comment_wiring.build_pr_body


_default_review_comment_checker = _codex_comment_wiring.default_review_comment_checker


_runtime_comment_poster = _codex_comment_wiring.runtime_comment_poster


_runtime_issue_closer = _codex_comment_wiring.runtime_issue_closer


_runtime_automerge_enabler = _codex_comment_wiring.runtime_automerge_enabler


_runtime_failed_check_rerun = _codex_comment_wiring.runtime_failed_check_rerun


_post_consumer_claim_comment = _codex_comment_wiring.post_consumer_claim_comment


# OpenPullRequestMatch: re-exported from domain.models


_list_open_pr_candidates = _codex_comment_wiring.list_open_pr_candidates


_classify_open_pr_candidates = _codex_comment_wiring.classify_open_pr_candidates


# ---------------------------------------------------------------------------
# Helper: post result comment
# ---------------------------------------------------------------------------


_post_result_comment = _codex_comment_wiring.post_result_comment


# ---------------------------------------------------------------------------
# Helper: post Codex verdict marker on the PR
# ---------------------------------------------------------------------------


_post_pr_codex_verdict = _codex_comment_wiring.post_pr_codex_verdict


# ---------------------------------------------------------------------------
# Helper: backfill missing Codex verdict markers for review sessions
# ---------------------------------------------------------------------------


_backfill_review_verdicts = _codex_comment_wiring.backfill_review_verdicts


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
    _review_queue_helpers.repark_unchanged_review_queue_entries(
        store,
        entries,
        now=now,
    )


def _review_queue_state_probe_candidates(
    entries: list[ReviewQueueEntry],
) -> list[ReviewQueueEntry]:
    """Return review entries whose PR state can be tracked cheaply via digest."""
    return _review_queue_helpers.review_queue_state_probe_candidates(entries)


def _partition_review_queue_entries_by_probe_change(
    entries: list[ReviewQueueEntry],
    *,
    pr_port: PullRequestPort,
) -> tuple[list[ReviewQueueEntry], list[ReviewQueueEntry]]:
    """Split queued review entries into changed vs unchanged probe state."""
    return _review_queue_helpers.partition_review_queue_entries_by_probe_change(
        entries,
        pr_port=pr_port,
    )


def _wakeup_changed_review_queue_entries(
    store: SessionStorePort,
    entries: list[ReviewQueueEntry],
    *,
    now: datetime,
    pr_port: PullRequestPort,
    dry_run: bool = False,
) -> tuple[str, ...]:
    """Promote parked review entries whose lightweight PR state has changed."""
    return _review_queue_helpers.wakeup_changed_review_queue_entries(
        store,
        entries,
        now=now,
        pr_port=pr_port,
        dry_run=dry_run,
    )


def _build_review_snapshots_for_queue_entries(
    *,
    queue_entries: list[ReviewQueueEntry],
    review_refs: set[str],
    pr_port: PullRequestPort,
    trusted_codex_actors: frozenset[str],
) -> dict[tuple[str, int], ReviewSnapshot]:
    """Build one review snapshot per unique PR for queued review entries."""
    return _review_queue_helpers.build_review_snapshots_for_queue_entries(
        queue_entries=queue_entries,
        review_refs=review_refs,
        pr_port=pr_port,
        trusted_codex_actors=trusted_codex_actors,
    )


_backfill_review_verdicts_from_snapshots = _codex_comment_wiring.backfill_review_verdicts_from_snapshots


_pre_backfill_verdicts_for_due_prs = _codex_comment_wiring.pre_backfill_verdicts_for_due_prs


def _review_scope_issue_refs(
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    board_snapshot: CycleBoardSnapshot,
) -> tuple[str, ...]:
    """Return governed review issue refs for the consumer executor."""
    refs: list[str] = []
    for snapshot in board_snapshot.items_with_status("Review"):
        issue_ref = _snapshot_to_issue_ref(
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
    return _review_queue_helpers.apply_review_queue_result(
        store,
        entry,
        result,
        now=now,
        retry_seconds=retry_seconds,
        last_state_digest=last_state_digest,
    )


def _apply_review_queue_partial_failure(
    store: SessionStorePort,
    entries: list[ReviewQueueEntry],
    *,
    config: ConsumerConfig,
    error: str | None,
    now: datetime | None = None,
) -> None:
    """Back off queued review entries after a partial-failure cycle."""
    _review_queue_helpers.apply_review_queue_partial_failure(
        store,
        entries,
        config=config,
        error=error,
        gh_reason_code=gh_reason_code,
        now=now,
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
        issue_ref = _snapshot_to_issue_ref(
            snapshot.issue_ref, critical_path_config.issue_prefixes
        )
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
    return _review_queue_wiring.prepare_review_queue_batch(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        board_snapshot=board_snapshot,
        pr_port=pr_port,
        now=now,
        dry_run=dry_run,
        deps=_build_review_queue_wiring_deps(),
        review_queue_helpers=_review_queue_helpers,
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
    return _review_queue_wiring.prepare_due_review_processing(
        store=store,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        deps=_build_review_queue_wiring_deps(),
        review_queue_helpers=_review_queue_helpers,
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
    return _review_queue_wiring.process_due_review_group(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=pr_port,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        snapshot=snapshot,
        updated_snapshot=updated_snapshot,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        deps=_build_review_queue_wiring_deps(),
        review_queue_helpers=_review_queue_helpers,
    )


ReviewRescueExecution = _review_queue_helpers.ReviewRescueExecution


def _build_review_queue_wiring_deps() -> _review_queue_wiring.ReviewQueueWiringDeps:
    """Build shell-facing review-queue wiring dependencies."""
    return _review_queue_wiring.ReviewQueueWiringDeps(
        prepared_batch_factory=PreparedReviewQueueBatch,
        summary_factory=ReviewQueueDrainSummary,
        prepared_due_processing_factory=PreparedDueReviewProcessing,
        review_group_outcome_factory=ReviewGroupProcessingOutcome,
        review_queue_processing_outcome_factory=ReviewQueueProcessingOutcome,
        post_pr_codex_verdict=_post_pr_codex_verdict,
        review_rescue_fn=review_rescue,
        escalate_to_claude=_escalate_to_claude,
        gh_reason_code=gh_reason_code,
        log_probe_warning=lambda err: logger.warning(
            "Review queue wakeup probe failed: %s",
            err,
        ),
        log_pre_backfill_warning=lambda issue_ref, err: logger.warning(
            "Pre-backfill verdict failed for %s: %s",
            issue_ref,
            err,
        ),
        log_backfill_warning=lambda issue_ref, session_id, err: logger.warning(
            "Review verdict backfill failed for %s (%s): %s",
            issue_ref,
            session_id,
            err,
        ),
    )


def _run_review_rescue_for_group(
    *,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    pr_port: PullRequestPort,
    pr_repo: str,
    pr_number: int,
    snapshot: ReviewSnapshot,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> ReviewRescueExecution:
    """Run rescue logic for one due review group."""
    return _review_queue_helpers.run_review_rescue_for_group(
        config=config,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=pr_port,
        pr_repo=pr_repo,
        pr_number=pr_number,
        snapshot=snapshot,
        dry_run=dry_run,
        gh_runner=gh_runner,
        review_rescue_fn=review_rescue,
    )


def _apply_review_queue_group_result(
    *,
    store: SessionStorePort,
    critical_path_config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    pr_port: PullRequestPort,
    pr_repo: str,
    pr_number: int,
    entries: tuple[ReviewQueueEntry, ...],
    result: ReviewRescueResult,
    now: datetime,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> tuple[str, ...]:
    """Persist one review-group result and return escalated issue refs."""
    return _review_queue_wiring.apply_review_queue_group_result(
        store=store,
        critical_path_config=critical_path_config,
        project_owner=project_owner,
        project_number=project_number,
        pr_port=pr_port,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        result=result,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        deps=_build_review_queue_wiring_deps(),
        review_queue_helpers=_review_queue_helpers,
    )


def _summarize_review_group_outcome(
    *,
    critical_path_config: CriticalPathConfig,
    store: SessionStorePort,
    project_owner: str,
    project_number: int,
    pr_repo: str,
    pr_number: int,
    entries: tuple[ReviewQueueEntry, ...],
    result: ReviewRescueResult,
    updated_snapshot: CycleBoardSnapshot,
    escalated: tuple[str, ...],
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> ReviewGroupProcessingOutcome:
    """Build the public outcome for one processed review group."""
    return _review_queue_wiring.summarize_review_group_outcome(
        critical_path_config=critical_path_config,
        store=store,
        project_owner=project_owner,
        project_number=project_number,
        pr_repo=pr_repo,
        pr_number=pr_number,
        entries=entries,
        result=result,
        updated_snapshot=updated_snapshot,
        escalated=escalated,
        dry_run=dry_run,
        gh_runner=gh_runner,
        deps=_build_review_queue_wiring_deps(),
        review_queue_helpers=_review_queue_helpers,
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
    return _review_queue_wiring.process_review_queue_due_groups(
        config=config,
        store=store,
        critical_path_config=critical_path_config,
        automation_config=automation_config,
        pr_port=pr_port,
        prepared_batch=prepared_batch,
        board_snapshot=board_snapshot,
        now=now,
        dry_run=dry_run,
        gh_runner=gh_runner,
        deps=_build_review_queue_wiring_deps(),
        review_queue_helpers=_review_queue_helpers,
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
    _review_queue_helpers.reconcile_review_queue_identity(
        store,
        review_refs,
        now=now,
    )


def _replay_deferred_action(
    *,
    action: DeferredAction,
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    pr_port: PullRequestPort | None,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Execute one deferred control-plane action."""
    _deferred_action_helpers.replay_deferred_action(
        action=action,
        config=config,
        critical_path_config=critical_path_config,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        set_blocked_with_reason=_set_blocked_with_reason,
        transition_issue_to_review=_transition_issue_to_review,
        transition_issue_to_in_progress=_transition_issue_to_in_progress,
        return_issue_to_ready=_return_issue_to_ready,
        post_pr_codex_verdict=_post_pr_codex_verdict,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        runtime_comment_poster=_runtime_comment_poster,
        runtime_issue_closer=_runtime_issue_closer,
        runtime_failed_check_rerun=_runtime_failed_check_rerun,
        runtime_automerge_enabler=_runtime_automerge_enabler,
    )


def _replay_deferred_status_action(
    *,
    payload: dict[str, Any],
    config: ConsumerConfig,
    critical_path_config: CriticalPathConfig,
    review_state_port: ReviewStatePort | None,
    board_port: BoardMutationPort | None,
    board_info_resolver: Callable[..., Any] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Replay a deferred issue status transition."""
    _deferred_action_helpers.replay_deferred_status_action(
        payload=payload,
        config=config,
        critical_path_config=critical_path_config,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        set_blocked_with_reason=_set_blocked_with_reason,
        transition_issue_to_review=_transition_issue_to_review,
        transition_issue_to_in_progress=_transition_issue_to_in_progress,
        return_issue_to_ready=_return_issue_to_ready,
    )


def _replay_deferred_verdict_marker(
    *,
    payload: dict[str, Any],
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Replay a deferred PR verdict marker post."""
    _deferred_action_helpers.replay_deferred_verdict_marker(
        payload=payload,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        post_pr_codex_verdict=_post_pr_codex_verdict,
    )


def _replay_deferred_issue_comment(
    *,
    payload: dict[str, Any],
    critical_path_config: CriticalPathConfig,
    board_port: BoardMutationPort | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Replay a deferred issue comment post."""
    _deferred_action_helpers.replay_deferred_issue_comment(
        payload=payload,
        critical_path_config=critical_path_config,
        board_port=board_port,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        runtime_comment_poster=_runtime_comment_poster,
    )


def _replay_deferred_issue_close(
    *,
    payload: dict[str, Any],
    critical_path_config: CriticalPathConfig,
    board_port: BoardMutationPort | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Replay a deferred issue close."""
    _deferred_action_helpers.replay_deferred_issue_close(
        payload=payload,
        critical_path_config=critical_path_config,
        board_port=board_port,
        gh_runner=gh_runner,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        runtime_issue_closer=_runtime_issue_closer,
    )


def _replay_deferred_check_rerun(
    *,
    payload: dict[str, Any],
    pr_port: PullRequestPort | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Replay a deferred failed-check rerun request."""
    _deferred_action_helpers.replay_deferred_check_rerun(
        payload=payload,
        pr_port=pr_port,
        gh_runner=gh_runner,
        runtime_failed_check_rerun=_runtime_failed_check_rerun,
    )


def _replay_deferred_automerge_enable(
    *,
    payload: dict[str, Any],
    pr_port: PullRequestPort | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Replay a deferred auto-merge enablement."""
    _deferred_action_helpers.replay_deferred_automerge_enable(
        payload=payload,
        pr_port=pr_port,
        gh_runner=gh_runner,
        runtime_automerge_enabler=_runtime_automerge_enabler,
    )


# ---------------------------------------------------------------------------
# Helper: advance board state after successful PR creation
# ---------------------------------------------------------------------------


def _transition_issue_to_review(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Move a successfully submitted issue from In Progress to Review."""
    _board_state_helpers.transition_issue_to_review(
        issue_ref,
        config,
        project_owner,
        project_number,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )


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
    _board_state_helpers.transition_issue_to_in_progress(
        issue_ref,
        config,
        project_owner,
        project_number,
        build_github_port_bundle=build_github_port_bundle,
        from_statuses=from_statuses,
        review_state_port=review_state_port,
        board_port=board_port,
        gh_runner=gh_runner,
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
    _board_state_helpers.return_issue_to_ready(
        issue_ref,
        config,
        project_owner,
        project_number,
        build_github_port_bundle=build_github_port_bundle,
        from_statuses=from_statuses,
        review_state_port=review_state_port,
        board_port=board_port,
        gh_runner=gh_runner,
    )


_build_reconciliation_wiring_deps = _operational_wiring.build_reconciliation_wiring_deps


_reconcile_active_repair_review_items = _operational_wiring.reconcile_active_repair_review_items


_reconcile_single_in_progress_item = _operational_wiring.reconcile_single_in_progress_item


_reconcile_stale_in_progress_items = _operational_wiring.reconcile_stale_in_progress_items


_recover_interrupted_sessions = _operational_wiring.recover_interrupted_sessions


def _build_init_cycle_runtime_deps() -> InitializeCycleRuntimeDeps:
    """Build the deps for cycle runtime initialization."""
    return InitializeCycleRuntimeDeps(
        build_session_store=build_session_store,
        load_config=load_config,
        load_automation_config=load_automation_config,
        apply_automation_runtime=_apply_automation_runtime,
        current_main_workflows=_current_main_workflows,
        build_github_port_bundle=build_github_port_bundle,
        build_ready_flow_port=build_ready_flow_port,
        cycle_github_memo_factory=CycleGitHubMemo,
        config_error_type=ConfigError,
        logger=logger,
    )


def _build_phase_helper_deps() -> PhaseHelperDeps:
    """Build the deps for preflight phase helpers."""
    return PhaseHelperDeps(
        replay_deferred_actions=_replay_deferred_actions,
        drain_review_queue=_drain_review_queue,
        reconcile_board_truth=_reconcile_board_truth,
        record_successful_github_mutation=_record_successful_github_mutation,
        record_successful_board_sync=_record_successful_board_sync,
        clear_degraded=_clear_degraded,
        mark_degraded=_mark_degraded,
        persist_admission_summary=_persist_admission_summary,
        logger=logger,
    )


def _initialize_cycle_runtime(
    config: ConsumerConfig,
    db: ConsumerDB,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> CycleRuntimeContext:
    """Build cycle-scoped runtime wiring and effective config."""
    return _initialize_cycle_runtime_use_case(
        config,
        db,
        deps=_build_init_cycle_runtime_deps(),
        gh_runner=gh_runner,
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
    return _run_deferred_replay_phase_use_case(
        config,
        db,
        runtime,
        deps=_build_phase_helper_deps(),
        timings_ms=timings_ms,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )


def _load_board_snapshot_phase(
    config: ConsumerConfig,
    runtime: CycleRuntimeContext,
    *,
    timings_ms: dict[str, int],
    gh_runner: Callable[..., str] | None,
) -> CycleBoardSnapshot:
    """Load the cycle board snapshot."""
    return _load_board_snapshot_phase_use_case(
        config,
        runtime,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
    )


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
    return _run_executor_routing_phase_use_case(
        config,
        db,
        runtime,
        deps=_build_phase_helper_deps(),
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
        dry_run=dry_run,
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
    return _run_reconciliation_phase_use_case(
        config,
        db,
        runtime,
        deps=_build_phase_helper_deps(),
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )


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
    return _run_review_queue_phase_use_case(
        config,
        db,
        runtime,
        deps=_build_phase_helper_deps(),
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )


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
    return _run_admission_phase_use_case(
        config,
        db,
        runtime,
        deps=_build_phase_helper_deps(),
        board_snapshot=board_snapshot,
        timings_ms=timings_ms,
        gh_runner=gh_runner,
        dry_run=dry_run,
    )


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
    result = _execute_prepare_cycle_phases_use_case(
        config,
        db,
        runtime=runtime,
        deps=PrepareCyclePhasesDeps(
            run_deferred_replay_phase=_run_deferred_replay_phase,
            load_board_snapshot_phase=_load_board_snapshot_phase,
            run_executor_routing_phase=_run_executor_routing_phase,
            run_reconciliation_phase=_run_reconciliation_phase,
            run_review_queue_phase=_run_review_queue_phase,
            run_admission_phase=_run_admission_phase,
        ),
        dry_run=dry_run,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )
    return (
        result.board_snapshot,
        result.review_queue_summary,
        result.admission_summary,
        result.timings_ms,
    )


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
    return _prepare_cycle_use_case(
        config,
        db,
        deps=PrepareCycleDeps(
            initialize_cycle_runtime_deps=_build_init_cycle_runtime_deps(),
            phase_helper_deps=_build_phase_helper_deps(),
            begin_runtime_request_stats=begin_runtime_request_stats,
            end_runtime_request_stats=end_runtime_request_stats,
            snapshot_to_issue_ref=_snapshot_to_issue_ref,
            parse_issue_ref=parse_issue_ref,
            record_metric=_record_metric,
            control_key_degraded=CONTROL_KEY_DEGRADED,
            prepare_cycle_phases_deps_factory=PrepareCyclePhasesDeps,
            execute_prepare_cycle_phases=_execute_prepare_cycle_phases_use_case,
        ),
        prepared_cycle_context_factory=PreparedCycleContext,
        dry_run=dry_run,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
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
    return _selection_retry_wiring.select_candidate_for_cycle(
        config,
        db,
        prepared,
        target_issue=target_issue,
        status_resolver=status_resolver,
        gh_runner=gh_runner,
        excluded_issue_refs=excluded_issue_refs,
        parse_issue_ref=parse_issue_ref,
        effective_retry_backoff=_effective_retry_backoff,
        retry_backoff_active=_retry_backoff_active,
        select_best_candidate=_select_best_candidate,
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
    _execution_support_helpers.escalate_to_claude(
        issue_ref,
        config,
        project_owner,
        project_number,
        reason,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        marker_for=_marker_for,
        resolve_issue_coordinates=_resolve_issue_coordinates,
        set_blocked_with_reason=_set_blocked_with_reason,
        set_issue_handoff_target=_set_issue_handoff_target,
        default_review_comment_checker=_default_review_comment_checker,
        runtime_comment_poster=_runtime_comment_poster,
        logger=logger,
    )


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
    return _execution_support_helpers.build_dependency_summary(
        issue_ref,
        config,
        project_owner,
        project_number,
        status_resolver=status_resolver,
        in_any_critical_path=in_any_critical_path,
    )


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
    return _execution_support_helpers.has_commits_on_branch(
        worktree_path,
        branch,
        subprocess_runner=subprocess_runner,
    )


def _next_available_slot(db: ConsumerDB, limit: int) -> int | None:
    """Return the next available execution slot id, or None if saturated."""
    occupied = db.active_slot_ids()
    for slot_id in range(1, limit + 1):
        if slot_id not in occupied:
            return slot_id
    return None


_reconcile_board_truth = _operational_wiring.reconcile_board_truth


_block_prelaunch_issue = _operational_wiring.block_prelaunch_issue


# ---------------------------------------------------------------------------
# _prepare_launch_candidate sub-functions: focused extraction from god-function
# ---------------------------------------------------------------------------


_setup_launch_worktree = _launch_support_wiring.setup_launch_worktree


_resolve_launch_runtime = _launch_support_wiring.resolve_launch_runtime


_resolve_launch_candidate_metadata = _launch_support_wiring.resolve_launch_candidate_metadata


_resolve_launch_issue_context = _launch_support_wiring.resolve_launch_issue_context


_run_launch_workspace_hooks = _launch_support_wiring.run_launch_workspace_hooks


_assemble_prepared_launch_context = _cycle_wiring.assemble_prepared_launch_context


_prepare_launch_candidate = _cycle_wiring.prepare_launch_candidate


_select_launch_candidate_for_cycle = _operational_wiring.select_launch_candidate_for_cycle


_prepare_selected_launch_candidate = _operational_wiring.prepare_selected_launch_candidate


_handle_selected_launch_query_error = _operational_wiring.handle_selected_launch_query_error


_handle_selected_launch_workflow_config_error = _operational_wiring.handle_selected_launch_workflow_config_error


_handle_selected_launch_worktree_error = _operational_wiring.handle_selected_launch_worktree_error


_handle_selected_launch_runtime_error = _operational_wiring.handle_selected_launch_runtime_error


_resolve_launch_context_for_cycle = _operational_wiring.resolve_launch_context_for_cycle


@dataclass(frozen=True)
class PendingClaimContext:
    """Session state prepared for board claim after local launch prep."""

    session_id: str
    effective_max_retries: int


_open_pending_claim_session = _operational_wiring.open_pending_claim_session


_enforce_claim_retry_ceiling = _operational_wiring.enforce_claim_retry_ceiling


_attempt_launch_context_claim = _operational_wiring.attempt_launch_context_claim


_claim_launch_ready_issue = _operational_wiring.claim_launch_ready_issue


_handle_launch_claim_api_failure = _operational_wiring.handle_launch_claim_api_failure


_handle_launch_claim_unexpected_failure = _operational_wiring.handle_launch_claim_unexpected_failure


_handle_launch_claim_rejection = _operational_wiring.handle_launch_claim_rejection


_mark_claimed_session_running = _operational_wiring.mark_claimed_session_running


_claim_launch_context = _operational_wiring.claim_launch_context


def _session_status_from_codex_result(
    exit_code: int,
    codex_result: dict[str, Any] | None,
) -> tuple[str, str | None]:
    """Map Codex exit/result into session status and failure reason."""
    return _session_completion_helpers.session_status_from_codex_result(
        exit_code,
        codex_result,
    )


_create_pr_for_execution_result = _operational_wiring.create_pr_for_execution_result


_handoff_execution_to_review = _operational_wiring.handoff_execution_to_review


_transition_claimed_session_to_review = _operational_wiring.transition_claimed_session_to_review


_post_claimed_session_verdict_marker = _operational_wiring.post_claimed_session_verdict_marker


_queue_claimed_session_for_review = _operational_wiring.queue_claimed_session_for_review


_run_immediate_review_handoff = _operational_wiring.run_immediate_review_handoff


_handle_non_review_execution_outcome = _operational_wiring.handle_non_review_execution_outcome


_final_phase_for_claimed_session = _operational_wiring.final_phase_for_claimed_session


_persist_claimed_session_completion = _operational_wiring.persist_claimed_session_completion


_post_claimed_session_result_comment = _operational_wiring.post_claimed_session_result_comment


_maybe_escalate_claimed_session_failure = _operational_wiring.maybe_escalate_claimed_session_failure


_execute_claimed_session = _operational_wiring.execute_claimed_session


_finalize_claimed_session = _operational_wiring.finalize_claimed_session


_prepared_cycle_deps = _operational_wiring.prepared_cycle_deps


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
    return _runtime_wiring.run_one_cycle(
        config=config,
        db=db,
        dry_run=dry_run,
        target_issue=target_issue,
        prepared=prepared,
        launch_context=launch_context,
        slot_id_override=slot_id_override,
        skip_control_plane=skip_control_plane,
        gh_runner=gh_runner,
        subprocess_runner=subprocess_runner,
        file_reader=file_reader,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        prepare_cycle=_prepare_cycle,
        config_error_type=ConfigError,
        workflow_config_error_type=WorkflowConfigError,
        gh_query_error_type=GhQueryError,
        mark_degraded=_mark_degraded,
        gh_reason_code=gh_reason_code,
        cycle_result_factory=CycleResult,
        build_gh_runner_port=build_gh_runner_port,
        build_process_runner_port=build_process_runner_port,
        run_prepared_cycle=run_prepared_cycle,
        prepared_cycle_deps=_prepared_cycle_deps(),
        logger=logger,
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
    return _run_worker_cycle_use_case(
        config,
        target_issue=target_issue,
        slot_id=slot_id,
        prepared=prepared,
        launch_context=launch_context,
        dry_run=dry_run,
        di_kwargs=di_kwargs,
        open_consumer_db=open_consumer_db,
        run_one_cycle=run_one_cycle,
    )


def _next_available_slots(
    db: ConsumerDB,
    limit: int,
    *,
    reserved_slots: set[int] | None = None,
) -> list[int]:
    """Return deterministic lowest-available slot ids."""
    return _next_available_slots_use_case(
        db,
        limit,
        reserved_slots=reserved_slots,
    )


def _log_completed_worker_results(
    active_tasks: dict[Future[CycleResult], ActiveWorkerTask],
) -> None:
    """Log and discard completed worker futures."""
    _log_completed_worker_results_use_case(active_tasks, logger=logger)


def _prepare_multi_worker_cycle(
    config: ConsumerConfig,
    db: ConsumerDB,
    *,
    dry_run: bool,
    sleeper: Callable[[float], None],
    di_kwargs: dict[str, Any],
) -> PreparedCycleContext | None:
    """Run one bounded preflight pass for the multi-worker daemon."""
    return _runtime_wiring.prepare_multi_worker_cycle(
        config,
        db,
        dry_run=dry_run,
        sleeper=sleeper,
        di_kwargs=di_kwargs,
        shell=sys.modules[__name__],
    )


def _multi_worker_dispatch_state(
    db: ConsumerDB,
    prepared: PreparedCycleContext,
    active_tasks: dict[Future[CycleResult], ActiveWorkerTask],
) -> tuple[list[int], set[str]]:
    """Compute currently available slots and active issue refs."""
    return _runtime_wiring.multi_worker_dispatch_state(
        db,
        prepared,
        active_tasks,
        shell=sys.modules[__name__],
    )


def _sleep_for_claim_suppression_if_needed(
    db: ConsumerDB,
    config: ConsumerConfig,
    *,
    sleeper: Callable[[float], None],
) -> bool:
    """Sleep until claim suppression clears, if active."""
    return _runtime_wiring.sleep_for_claim_suppression_if_needed(
        db,
        config,
        sleeper=sleeper,
        shell=sys.modules[__name__],
    )


def _prepare_multi_worker_launch_context(
    candidate: str,
    *,
    config: ConsumerConfig,
    db: ConsumerDB,
    prepared: PreparedCycleContext,
    dry_run: bool,
    di_kwargs: dict[str, Any],
) -> tuple[PreparedLaunchContext | None, bool]:
    """Prepare launch context for one candidate.

    Returns `(launch_context, stop_dispatch)`.
    """
    return _runtime_wiring.prepare_multi_worker_launch_context(
        candidate,
        config=config,
        db=db,
        prepared=prepared,
        dry_run=dry_run,
        di_kwargs=di_kwargs,
        shell=sys.modules[__name__],
    )


def _submit_multi_worker_task(
    executor: ThreadPoolExecutor,
    active_tasks: dict[Future[CycleResult], ActiveWorkerTask],
    *,
    config: ConsumerConfig,
    candidate: str,
    slot_id: int,
    prepared: PreparedCycleContext,
    launch_context: PreparedLaunchContext | None,
    dry_run: bool,
    di_kwargs: dict[str, Any],
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
        di_kwargs=di_kwargs,
        shell=sys.modules[__name__],
    )


def _dispatch_multi_worker_launches(
    executor: ThreadPoolExecutor,
    config: ConsumerConfig,
    db: ConsumerDB,
    *,
    prepared: PreparedCycleContext,
    available_slots: list[int],
    active_issue_refs: set[str],
    active_tasks: dict[Future[CycleResult], ActiveWorkerTask],
    dry_run: bool,
    di_kwargs: dict[str, Any],
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
        di_kwargs=di_kwargs,
        shell=sys.modules[__name__],
    )


def _run_multi_worker_daemon_loop(
    config: ConsumerConfig,
    db: ConsumerDB,
    *,
    dry_run: bool = False,
    sleep_fn: Callable[[float], None] | None = None,
    **di_kwargs: Any,
) -> None:
    """Run the daemon loop with multiple concurrent worker slots."""
    _runtime_wiring.run_multi_worker_daemon_loop(
        config,
        db,
        dry_run=dry_run,
        sleep_fn=sleep_fn,
        di_kwargs=di_kwargs,
        shell=sys.modules[__name__],
    )


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
    _runtime_wiring.run_daemon_loop(
        config,
        db,
        dry_run=dry_run,
        sleep_fn=sleep_fn,
        di_kwargs=di_kwargs,
        shell=sys.modules[__name__],
    )


# ---------------------------------------------------------------------------
# CLI: status
# ---------------------------------------------------------------------------


def _collect_status_payload(
    config: ConsumerConfig,
    *,
    local_only: bool = False,
) -> dict[str, Any]:
    """Collect consumer status as a JSON-serializable payload."""
    return _runtime_wiring.collect_status_payload(
        config,
        local_only=local_only,
        shell=sys.modules[__name__],
    )


from startupai_controller.board_consumer_cli import (
    _cmd_drain,
    _cmd_reconcile,
    _cmd_report_slo,
    _cmd_resume,
    _cmd_serve_status,
    _cmd_status,
    _create_status_http_server,
    build_parser,
    main,
)


if __name__ == "__main__":
    sys.exit(main())
