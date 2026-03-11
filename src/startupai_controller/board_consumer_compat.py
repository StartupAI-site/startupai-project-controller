#!/usr/bin/env python3
"""Compatibility surface for the board consumer daemon.

Originally this module was the consumer daemon entrypoint. It now preserves the
historical import/monkeypatch surface while the thin shell lives in
`board_consumer.py`.

Board consumer daemon — polls Ready items, claims for codex, executes.

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


from startupai_controller import consumer_board_state_helpers as _board_state_helpers
import startupai_controller.consumer_automation_bridge as _automation_bridge
import startupai_controller.consumer_codex_comment_wiring as _codex_comment_wiring
import startupai_controller.consumer_codex_runtime_wiring as _codex_runtime_wiring
from startupai_controller import consumer_deferred_action_helpers as _deferred_action_helpers
from startupai_controller import consumer_execution_support_helpers as _execution_support_helpers
import startupai_controller.consumer_launch_support_wiring as _launch_support_wiring
import startupai_controller.consumer_preflight_wiring as _preflight_wiring
from startupai_controller import consumer_resolution_helpers as _resolution_helpers
from startupai_controller import consumer_review_queue_helpers as _review_queue_helpers
import startupai_controller.consumer_selection_retry_wiring as _selection_retry_wiring
import startupai_controller.consumer_support_wiring as _support_wiring
from startupai_controller.board_automation_config import (
    load_automation_config,
)
from startupai_controller.board_graph import _resolve_issue_coordinates
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
    issue_context_cache_is_fresh as _issue_context_cache_is_fresh_helper,
    snapshot_for_issue as _snapshot_for_issue_helper,
)
import startupai_controller.consumer_comment_pr_wiring as _comment_pr_wiring
from startupai_controller.consumer_worktree_helpers import (
    list_repo_worktrees as _list_repo_worktrees_helper,
    worktree_is_clean as _worktree_is_clean_helper,
    worktree_ownership_is_safe as _worktree_ownership_is_safe_helper,
)
import startupai_controller.consumer_cycle_wiring as _cycle_wiring
import startupai_controller.consumer_operational_wiring as _operational_wiring
import startupai_controller.consumer_review_queue_wiring as _review_queue_wiring
import startupai_controller.consumer_runtime_wiring as _runtime_wiring
import startupai_controller.consumer_session_completion_helpers as _session_completion_helpers
from startupai_controller.control_plane_runtime import (
    CONTROL_KEY_CLAIM_SUPPRESSED_REASON,
    CONTROL_KEY_CLAIM_SUPPRESSED_SCOPE,
    CONTROL_KEY_CLAIM_SUPPRESSED_UNTIL,
    CONTROL_KEY_DEGRADED,
    CONTROL_KEY_DEGRADED_REASON,
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
from startupai_controller.application.consumer.preflight import ReconciliationResult
from startupai_controller.application.consumer.preflight_runtime import (
    CycleRuntimeContext as _AppCycleRuntimeContext,
)
from startupai_controller.application.consumer.recovery import (
    recover_interrupted_sessions as _recover_interrupted_sessions_use_case,
)
from startupai_controller.application.consumer.daemon import (
    log_completed_worker_results as _log_completed_worker_results_use_case,
    next_available_slots as _next_available_slots_use_case,
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


_DYNAMIC_EXPORTS: dict[str, Any] = {
    "CycleRuntimeContext": _AppCycleRuntimeContext,
    "mark_issues_done": (_automation_bridge, "mark_issues_done"),
    "_set_blocked_with_reason": (_automation_bridge, "set_blocked_with_reason"),
    "review_rescue": (_automation_bridge, "review_rescue"),
    "sync_review_state": (_automation_bridge, "sync_review_state"),
    "_complete_session": (_support_wiring, "complete_session"),
    "_verify_resolution_payload": (_support_wiring, "verify_resolution_payload"),
    "_resolution_evidence_payload": (_support_wiring, "resolution_evidence_payload"),
    "_apply_resolution_action": (_resolution_helpers, "apply_resolution_action_from_shell"),
    "_select_best_candidate": (_selection_retry_wiring, "select_best_candidate_from_shell"),
    "_hydrate_issue_context": (_support_wiring, "hydrate_issue_context"),
    "_prepare_worktree": (_launch_support_wiring, "prepare_worktree"),
    "_create_worktree": (_launch_support_wiring, "create_worktree"),
    "_git_command_detail": (_launch_support_wiring, "git_command_detail"),
    "_reconcile_repair_branch": (_launch_support_wiring, "reconcile_repair_branch"),
    "_assemble_codex_prompt": (_codex_comment_wiring, "assemble_codex_prompt"),
    "_run_codex_session": (_codex_comment_wiring, "run_codex_session"),
    "_parse_codex_result": (_codex_comment_wiring, "parse_codex_result"),
    "_create_or_update_pr": (_codex_comment_wiring, "create_or_update_pr"),
    "_build_pr_body": (_codex_comment_wiring, "build_pr_body"),
    "_default_review_comment_checker": (_codex_comment_wiring, "default_review_comment_checker"),
    "_post_consumer_claim_comment": (_codex_comment_wiring, "post_consumer_claim_comment"),
    "_list_open_pr_candidates": (_codex_comment_wiring, "list_open_pr_candidates"),
    "_classify_open_pr_candidates": (_codex_comment_wiring, "classify_open_pr_candidates"),
    "_post_result_comment": (_codex_comment_wiring, "post_result_comment"),
    "_post_pr_codex_verdict": (_codex_comment_wiring, "post_pr_codex_verdict"),
    "_backfill_review_verdicts": (_codex_comment_wiring, "backfill_review_verdicts"),
    "_backfill_review_verdicts_from_snapshots": (
        _codex_comment_wiring,
        "backfill_review_verdicts_from_snapshots",
    ),
    "_pre_backfill_verdicts_for_due_prs": (_codex_comment_wiring, "pre_backfill_verdicts_for_due_prs"),
    "_queue_review_item": (_review_queue_helpers, "queue_review_item"),
    "_apply_review_queue_result": (_review_queue_helpers, "apply_review_queue_result"),
    "ReviewRescueExecution": (_review_queue_helpers, "ReviewRescueExecution"),
    "_transition_issue_to_review": (_board_state_helpers, "transition_issue_to_review_from_shell"),
    "_transition_issue_to_in_progress": (
        _board_state_helpers,
        "transition_issue_to_in_progress_from_shell",
    ),
    "_return_issue_to_ready": (_board_state_helpers, "return_issue_to_ready_from_shell"),
    "_recover_interrupted_sessions": (_operational_wiring, "recover_interrupted_sessions"),
    "_build_init_cycle_runtime_deps": (_preflight_wiring, "build_init_cycle_runtime_deps"),
    "_build_phase_helper_deps": (_preflight_wiring, "build_phase_helper_deps"),
    "_run_deferred_replay_phase": (_preflight_wiring, "run_deferred_replay_phase"),
    "_load_board_snapshot_phase": (_preflight_wiring, "load_board_snapshot_phase"),
    "_run_executor_routing_phase": (_preflight_wiring, "run_executor_routing_phase"),
    "_run_reconciliation_phase": (_preflight_wiring, "run_reconciliation_phase"),
    "_run_review_queue_phase": (_preflight_wiring, "run_review_queue_phase"),
    "_run_admission_phase": (_preflight_wiring, "run_admission_phase"),
    "_execute_prepare_cycle_phases": (_preflight_wiring, "execute_prepare_cycle_phases"),
    "_prepare_cycle": (_preflight_wiring, "prepare_cycle"),
    "_select_candidate_for_cycle": (_selection_retry_wiring, "select_candidate_for_cycle_from_shell"),
    "_escalate_to_claude": (_board_state_helpers, "escalate_to_claude_from_shell"),
    "_reconcile_board_truth": (_operational_wiring, "reconcile_board_truth"),
    "_block_prelaunch_issue": (_operational_wiring, "block_prelaunch_issue"),
    "_setup_launch_worktree": (_launch_support_wiring, "setup_launch_worktree"),
    "_resolve_launch_runtime": (_launch_support_wiring, "resolve_launch_runtime"),
    "_resolve_launch_candidate_metadata": (_launch_support_wiring, "resolve_launch_candidate_metadata"),
    "_resolve_launch_issue_context": (_launch_support_wiring, "resolve_launch_issue_context"),
    "_run_launch_workspace_hooks": (_launch_support_wiring, "run_launch_workspace_hooks"),
    "_assemble_prepared_launch_context": (_cycle_wiring, "assemble_prepared_launch_context"),
    "_prepare_launch_candidate": (_cycle_wiring, "prepare_launch_candidate"),
    "_select_launch_candidate_for_cycle": (_operational_wiring, "select_launch_candidate_for_cycle"),
    "_prepare_selected_launch_candidate": (_operational_wiring, "prepare_selected_launch_candidate"),
    "_handle_selected_launch_query_error": (_operational_wiring, "handle_selected_launch_query_error"),
    "_handle_selected_launch_workflow_config_error": (
        _operational_wiring,
        "handle_selected_launch_workflow_config_error",
    ),
    "_handle_selected_launch_worktree_error": (
        _operational_wiring,
        "handle_selected_launch_worktree_error",
    ),
    "_handle_selected_launch_runtime_error": (
        _operational_wiring,
        "handle_selected_launch_runtime_error",
    ),
    "_resolve_launch_context_for_cycle": (_operational_wiring, "resolve_launch_context_for_cycle"),
    "_open_pending_claim_session": (_operational_wiring, "open_pending_claim_session"),
    "_enforce_claim_retry_ceiling": (_operational_wiring, "enforce_claim_retry_ceiling"),
    "_attempt_launch_context_claim": (_operational_wiring, "attempt_launch_context_claim"),
    "_claim_launch_ready_issue": (_operational_wiring, "claim_launch_ready_issue"),
    "_handle_launch_claim_api_failure": (_operational_wiring, "handle_launch_claim_api_failure"),
    "_handle_launch_claim_unexpected_failure": (
        _operational_wiring,
        "handle_launch_claim_unexpected_failure",
    ),
    "_handle_launch_claim_rejection": (_operational_wiring, "handle_launch_claim_rejection"),
    "_mark_claimed_session_running": (_operational_wiring, "mark_claimed_session_running"),
    "_claim_launch_context": (_operational_wiring, "claim_launch_context"),
    "_create_pr_for_execution_result": (_operational_wiring, "create_pr_for_execution_result"),
    "_handoff_execution_to_review": (_operational_wiring, "handoff_execution_to_review"),
    "_transition_claimed_session_to_review": (
        _operational_wiring,
        "transition_claimed_session_to_review",
    ),
    "_post_claimed_session_verdict_marker": (
        _operational_wiring,
        "post_claimed_session_verdict_marker",
    ),
    "_queue_claimed_session_for_review": (_operational_wiring, "queue_claimed_session_for_review"),
    "_run_immediate_review_handoff": (_operational_wiring, "run_immediate_review_handoff"),
    "_handle_non_review_execution_outcome": (
        _operational_wiring,
        "handle_non_review_execution_outcome",
    ),
    "_final_phase_for_claimed_session": (_operational_wiring, "final_phase_for_claimed_session"),
    "_persist_claimed_session_completion": (
        _operational_wiring,
        "persist_claimed_session_completion",
    ),
    "_post_claimed_session_result_comment": (
        _operational_wiring,
        "post_claimed_session_result_comment",
    ),
    "_maybe_escalate_claimed_session_failure": (
        _operational_wiring,
        "maybe_escalate_claimed_session_failure",
    ),
    "_execute_claimed_session": (_operational_wiring, "execute_claimed_session"),
    "_finalize_claimed_session": (_operational_wiring, "finalize_claimed_session"),
    "_prepared_cycle_deps": (_operational_wiring, "prepared_cycle_deps"),
    "run_one_cycle": (_runtime_wiring, "run_one_cycle_from_shell"),
    "_run_worker_cycle": (_support_wiring, "run_worker_cycle"),
    "_prepare_multi_worker_launch_context": (_support_wiring, "prepare_multi_worker_launch_context"),
    "_submit_multi_worker_task": (_support_wiring, "submit_multi_worker_task"),
    "_dispatch_multi_worker_launches": (_support_wiring, "dispatch_multi_worker_launches"),
}


def __getattr__(name: str) -> Any:
    """Resolve compatibility exports lazily from their owning modules."""
    try:
        target = _DYNAMIC_EXPORTS[name]
    except KeyError as exc:  # pragma: no cover - normal missing attribute behavior
        raise AttributeError(name) from exc
    if isinstance(target, tuple):
        module, attr = target
        return getattr(module, attr)
    return target


def __dir__() -> list[str]:
    """Expose dynamic compatibility exports to introspection."""
    return sorted(set(globals()) | set(_DYNAMIC_EXPORTS))


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


# CycleRuntimeContext and selected bridge exports are resolved via __getattr__.


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
    _support_wiring.activate_claim_suppression(
        db,
        config,
        scope=scope,
        error=error,
        now=now,
    )
    return True


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

# ---------------------------------------------------------------------------
# Helper: create worktree
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Helper: assemble codex prompt
# ---------------------------------------------------------------------------

def _extract_acceptance_criteria(body: str) -> str:
    """Extract acceptance criteria section from issue body."""
    return _comment_pr_wiring.extract_acceptance_criteria(body)


# ---------------------------------------------------------------------------
# Helper: run codex session
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Helper: create or update PR
# ---------------------------------------------------------------------------


_runtime_comment_poster = _codex_comment_wiring.runtime_comment_poster


_runtime_issue_closer = _codex_comment_wiring.runtime_issue_closer


_runtime_automerge_enabler = _codex_comment_wiring.runtime_automerge_enabler


_runtime_failed_check_rerun = _codex_comment_wiring.runtime_failed_check_rerun


# OpenPullRequestMatch: re-exported from domain.models


# ---------------------------------------------------------------------------
# Helper: post result comment
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Helper: post Codex verdict marker on the PR
# ---------------------------------------------------------------------------


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

# ---------------------------------------------------------------------------
# Blocked-streak escalation policy: imported from domain.review_queue_policy
# _blocker_class, _escalation_ceiling_for_blocker_class, ESCALATION_CEILING_*
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Helper: advance board state after successful PR creation
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# _prepare_launch_candidate sub-functions: focused extraction from god-function
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PendingClaimContext:
    """Session state prepared for board claim after local launch prep."""

    session_id: str
    effective_max_retries: int



def _session_status_from_codex_result(
    exit_code: int,
    codex_result: dict[str, Any] | None,
) -> tuple[str, str | None]:
    """Map Codex exit/result into session status and failure reason."""
    return _session_completion_helpers.session_status_from_codex_result(
        exit_code,
        codex_result,
    )

# ---------------------------------------------------------------------------
# Core: run_one_cycle
# ---------------------------------------------------------------------------


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
