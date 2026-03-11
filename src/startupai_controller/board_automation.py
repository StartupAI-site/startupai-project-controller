#!/usr/bin/env python3
"""Board automation orchestration script.

Eight subcommands for automated GitHub Project board state management:
- mark-done: PR merge -> mark linked issues Done
- auto-promote: Done issue -> promote eligible successors to Ready
- admit-backlog: Fill Ready from governed Backlog items
- propagate-blocker: Blocked issue -> post advisory comments on successors
- reconcile-handoffs: Retry/escalate stale cross-repo handoffs
- schedule-ready: Advisory or claim-mode Ready queue scheduling
- claim-ready: Explicit claim of one Ready issue into In Progress
- dispatch-agent: Record deterministic dispatch for claimed In Progress issues
- rebalance-wip: Rebalance stale/overflow In Progress lanes
- enforce-ready-dependencies: Block Ready issues with unmet predecessors
- audit-in-progress: Escalate stale In Progress issues lacking PR activity
- sync-review-state: Sync board state with PR/review/check events
- classify-parallelism: Snapshot parallel vs dependency-waiting Ready items
- codex-review-gate: Enforce Codex review verdict contract on PRs
- automerge-review: Auto-merge PRs that satisfy strict review + CI gates
- review-rescue: Reconcile one PR in Review by rerunning cancelled checks or enabling auto-merge
- review-rescue-all: Sweep governed repos for stuck Review PRs

Exit codes: 0 success, 2 blocked/no-op, 3 config error, 4 API error.
"""

from __future__ import annotations

import argparse
from dataclasses import field
from datetime import datetime, timedelta, timezone
import json
import os
import re
import sys
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from startupai_controller.ports.board_mutations import BoardMutationPort as _BoardMutationPort
    from startupai_controller.ports.issue_context import IssueContextPort as _IssueContextPort
    from startupai_controller.ports.pull_requests import PullRequestPort as _PullRequestPort
    from startupai_controller.ports.review_state import ReviewStatePort as _ReviewStatePort
else:
    _BoardMutationPort = None  # runtime: structural typing, no import needed
    _IssueContextPort = None  # runtime: structural typing, no import needed
    _PullRequestPort = None  # runtime: structural typing, no import needed
    _ReviewStatePort = None  # runtime: structural typing, no import needed


from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    ConfigError,
    GhQueryError,
    direct_predecessors,
    direct_successors,
    evaluate_ready_promotion,
    in_any_critical_path,
    parse_issue_ref,
)
from startupai_controller.board_graph import (
    _issue_sort_key,
    _admission_candidate_rank,
    _count_wip_by_executor,
    _count_wip_by_executor_lane,
    _ready_snapshot_rank,
    classify_parallelism_snapshot,
    find_unmet_ready_dependencies,
)
from startupai_controller.board_automation_config import (
    AdmissionConfig,
    BoardAutomationConfig,
    DEFAULT_AUTOMATION_CONFIG_PATH,
    DEFAULT_MISSING_EXECUTOR_BLOCK_CAP,
    DEFAULT_PROJECT_NUMBER,
    DEFAULT_PROJECT_OWNER,
    DEFAULT_REBALANCE_CYCLE_MINUTES,
    VALID_NON_LOCAL_PR_POLICIES,
    load_automation_config,
)
from startupai_controller.domain.automerge_policy import automerge_gate_decision
from startupai_controller.domain.rescue_policy import rescue_decision
from startupai_controller.domain.repair_policy import (
    MARKER_PREFIX,
    marker_for as _marker_for,
    parse_consumer_provenance as _parse_consumer_provenance,
    parse_pr_url as _parse_pr_url,
    repo_to_prefix_for_repo as _repo_to_prefix,
)
from startupai_controller.domain.resolution_policy import (
    parse_resolution_comment,
)
from startupai_controller.domain.scheduling_policy import (
    VALID_DISPATCH_TARGETS,
    VALID_EXECUTION_AUTHORITY_MODES,
    VALID_EXECUTORS,
    PROTECTED_QUEUE_ROUTING_STATUSES,
    priority_rank as _priority_rank,
    snapshot_to_issue_ref as _snapshot_to_issue_ref,
    wip_limit_for_lane as _domain_wip_limit_for_lane,
    protected_queue_executor_target as _domain_protected_queue_executor_target,
)
from startupai_controller.domain.models import (
    AdmissionCandidate,
    AdmissionDecision,
    AdmissionSkip,
    CycleBoardSnapshot,
    CheckObservation,
    ClaimReadyResult,
    DispatchResult,
    ExecutionPolicyDecision,
    ExecutorRoutingDecision,
    LinkedIssue,
    OpenPullRequest,
    PrGateStatus,
    ProjectItemSnapshot as _ProjectItemSnapshot,
    PromotionResult,
    RebalanceDecision,
    ReviewRescueResult,
    ReviewRescueSweep,
    ReviewSnapshot,
    SchedulingDecision,
    IssueSnapshot,
)
from startupai_controller.application.automation.ready_claim import (
    _set_blocked_with_reason as _app_set_blocked_with_reason,
    _transition_issue_status as _app_transition_issue_status,
    _wip_limit_for_lane as _app_wip_limit_for_lane,
    claim_ready_issue as _app_claim_ready_issue,
)
from startupai_controller.application.automation.ready_wiring import (
    promote_to_ready as _wiring_promote_to_ready,
    auto_promote_successors as _wiring_auto_promote_successors,
    admit_backlog_items as _wiring_admit_backlog_items,
    admission_summary_payload as _wiring_admission_summary_payload,
    build_admission_pipeline_deps as _wiring_build_admission_pipeline_deps,
    load_admission_source_items as _wiring_load_admission_source_items,
    partition_admission_source_items as _wiring_partition_admission_source_items,
    build_provisional_admission_candidates as _wiring_build_provisional_admission_candidates,
    evaluate_admission_candidates as _wiring_evaluate_admission_candidates,
    apply_admitted_backlog_candidates as _wiring_apply_admitted_backlog_candidates,
    enforce_ready_dependency_guard as _wiring_enforce_ready_dependency_guard,
    schedule_ready_items as _wiring_schedule_ready_items,
    post_claim_comment as _wiring_post_claim_comment,
    wire_schedule_ready_items as _wiring_wire_schedule_ready_items,
    wire_claim_ready_issue as _wiring_wire_claim_ready_issue,
)
from startupai_controller.application.automation.audit_in_progress import (
    audit_in_progress as _app_audit_in_progress,
    wire_audit_in_progress as _wiring_audit_in_progress,
)
from startupai_controller.application.automation.dispatch_agent import (
    dispatch_agent as _app_dispatch_agent,
    wire_dispatch_agent as _wiring_dispatch_agent,
)
from startupai_controller.application.automation.execution_policy import (
    enforce_execution_policy as _app_enforce_execution_policy,
    wire_enforce_execution_policy as _wiring_enforce_execution_policy,
)
from startupai_controller.application.automation.codex_gate import (
    codex_review_gate as _app_codex_review_gate,
)
from startupai_controller.application.automation.rebalance import (
    rebalance_wip as _app_rebalance_wip,
    wire_rebalance_wip as _wiring_rebalance_wip,
)
from startupai_controller.application.automation.review_rescue import (
    automerge_review as _app_automerge_review,
    review_rescue as _app_review_rescue,
    review_rescue_all as _app_review_rescue_all,
)
from startupai_controller.application.automation.review_sync import (
    sync_review_state as _app_sync_review_state,
)
from startupai_controller.application.automation.review_wiring import (
    has_copilot_review_signal as _wiring_has_copilot_review_signal,
    review_scope_refs as _wiring_review_scope_refs,
    configured_review_checks as _wiring_configured_review_checks,
    build_review_snapshot as _wiring_build_review_snapshot,
    sync_review_state as _wiring_sync_review_state,
    codex_review_gate as _wiring_codex_review_gate,
    review_rescue as _wiring_review_rescue,
    review_rescue_all as _wiring_review_rescue_all,
    automerge_review as _wiring_automerge_review,
)
from startupai_controller.application.automation.event_resolution import (
    resolve_issues_from_event as _app_resolve_issues_from_event,
    resolve_pr_to_issues as _app_resolve_pr_to_issues,
)
from startupai_controller.application.automation.blocker_propagation import (
    propagate_blocker as _app_propagate_blocker,
)
from startupai_controller.application.automation.handoff_reconciliation import (
    reconcile_handoffs as _app_reconcile_handoffs,
)
from startupai_controller.application.automation.admission_helpers import (
    AdmissionPipelineDeps as _AdmissionPipelineDeps,
)
from startupai_controller.application.automation.codex_fail_routing import (
    apply_codex_fail_routing as _app_apply_codex_fail_routing,
)
from startupai_controller.application.automation.executor_routing import (
    route_protected_queue_executors as _app_route_protected_queue_executors,
    protected_queue_executor_target as _app_protected_queue_executor_target,
)
from startupai_controller.application.automation.board_field_helpers import (
    set_handoff_target as _app_set_handoff_target,
)
from startupai_controller.application.automation.execution_policy import (
    ExecutionPolicyPrContext as _ExecutionPolicyPrContext,
    load_execution_policy_pr_context as _app_load_execution_policy_pr_context,
)
from startupai_controller.automation_board_state_helpers import (
    set_board_status as _helpers_set_board_status,
    set_status_if_changed as _helpers_set_status_if_changed,
    legacy_board_status_mutator as _helpers_legacy_board_status_mutator,
    set_blocked_with_reason as _helpers_set_blocked_with_reason,
    transition_issue_status as _helpers_transition_issue_status,
    mark_issues_done as _helpers_mark_issues_done,
)
from startupai_controller.runtime.wiring import (
    build_github_port_bundle,
    GitHubPortBundle,
    GitHubRuntimeMemo as CycleGitHubMemo,
)
from startupai_controller.automation_port_helpers import (
    BoardInfo as BoardInfo,
    _query_issue_board_info as _query_issue_board_info,
    _comment_exists as _comment_exists,
    list_issue_comment_bodies as list_issue_comment_bodies,
    _list_project_items_by_status as _list_project_items_by_status,
    _post_comment as _post_comment,
    _query_latest_marker_timestamp as _query_latest_marker_timestamp,
    _query_project_item_field as _query_project_item_field,
    _set_single_select_field as _set_single_select_field,
    _set_text_field as _set_text_field,
    _list_project_items as _list_project_items,
    _is_copilot_coding_agent_actor as _is_copilot_coding_agent_actor,
    _issue_ref_to_repo_parts as _issue_ref_to_repo_parts,
    _new_handoff_job_id as _new_handoff_job_id,
    _workflow_mutations_enabled as _workflow_mutations_enabled,
    _query_issue_updated_at,
    _query_open_pr_updated_at,
    _query_latest_wip_activity_timestamp,
    _is_pr_open,
    _query_issue_assignees,
    _set_issue_assignees,
    query_closing_issues,
    query_open_pull_requests,
    query_required_status_checks,
    query_latest_codex_verdict,
    _query_failed_check_runs,
    _query_pr_head_sha,
    close_issue,
    close_pull_request,
    memoized_query_issue_body,
    rerun_actions_run,
)

# BoardInfo, I/O helpers, and small utilities now live in automation_port_helpers.py
# and are re-exported via the import block above.
#
# Port factories remain here so that tests patching ``build_github_port_bundle``
# on this module continue to intercept the port-creation call path.


def _ensure_github_bundle(
    github_bundle: GitHubPortBundle | None,
    *,
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    github_memo: CycleGitHubMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> GitHubPortBundle:
    """Return the per-command/per-cycle GitHub bundle for runtime paths."""
    return github_bundle or build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        github_memo=github_memo,
        gh_runner=gh_runner,
    )


def _default_pr_port(
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> _PullRequestPort:
    """Construct a default PullRequestPort adapter from context params."""
    return build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        gh_runner=gh_runner,
    ).pull_requests


def _default_review_state_port(
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    gh_runner: Callable[..., str] | None = None,
) -> _ReviewStatePort:
    """Construct a default ReviewStatePort adapter from context params."""
    return build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        gh_runner=gh_runner,
    ).review_state


def _default_board_mutation_port(
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    gh_runner: Callable[..., str] | None = None,
) -> _BoardMutationPort:
    """Construct a default BoardMutationPort adapter from context params."""
    return build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        gh_runner=gh_runner,
    ).board_mutations


def _default_issue_context_port(
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    *,
    github_memo: CycleGitHubMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> _IssueContextPort:
    """Construct a default IssueContextPort adapter from context params."""
    return build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        github_memo=github_memo,
        gh_runner=gh_runner,
    ).issue_context


def promote_to_ready(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    dry_run: bool = False,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    controller_owned_resolver: Callable[[str], bool] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[int, str]:
    """Validate and promote an issue from Backlog/Blocked to Ready."""
    return _wiring_promote_to_ready(
        issue_ref,
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        controller_owned_resolver=controller_owned_resolver,
        gh_runner=gh_runner,
        query_issue_board_info_fn=_query_issue_board_info,
        default_board_mutation_port_fn=_default_board_mutation_port,
    )


def _set_board_status(
    project_id: str,
    item_id: str,
    status: str,
    *,
    board_port: _BoardMutationPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility helper that writes the Status field via BoardMutationPort."""
    _helpers_set_board_status(
        project_id,
        item_id,
        status,
        board_port=board_port,
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
        set_single_select_field_fn=_set_single_select_field,
    )


def _set_status_if_changed(
    issue_ref: str,
    from_statuses: set[str],
    to_status: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[bool, str]:
    """Legacy-compatible status transition helper for test seams."""
    return _helpers_set_status_if_changed(
        issue_ref,
        from_statuses,
        to_status,
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        query_issue_board_info_fn=_query_issue_board_info,
        set_board_status_fn=_set_board_status,
    )


def _set_blocked_with_reason(
    issue_ref: str,
    reason: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set Status=Blocked and Blocked Reason on a board item."""
    _helpers_set_blocked_with_reason(
        issue_ref,
        reason,
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        legacy_board_status_mutator_fn=_legacy_board_status_mutator,
        app_set_blocked_with_reason_fn=_app_set_blocked_with_reason,
    )


def _transition_issue_status(
    issue_ref: str,
    from_statuses: set[str],
    to_status: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[bool, str]:
    """Transition issue status through ports, with legacy fallback for tests."""
    return _helpers_transition_issue_status(
        issue_ref,
        from_statuses,
        to_status,
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        legacy_board_status_mutator_fn=_legacy_board_status_mutator,
        app_transition_issue_status_fn=_app_transition_issue_status,
    )


def _wip_limit_for_lane(
    automation_config: BoardAutomationConfig | None,
    executor: str,
    lane: str,
    fallback: int,
) -> int:
    """Resolve WIP limit for an executor/lane pair."""
    return _app_wip_limit_for_lane(automation_config, executor, lane, fallback)


def _legacy_board_status_mutator(
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> Callable[..., None]:
    """Adapt legacy project-item status helpers to the application boundary."""
    return _helpers_legacy_board_status_mutator(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
        set_board_status_fn=_set_board_status,
    )


# ---------------------------------------------------------------------------
# Subcommand: mark-done
# ---------------------------------------------------------------------------


def _has_copilot_review_signal(
    pr_repo: str,
    pr_number: int,
    *,
    pr_port: _PullRequestPort | None = None,
    config: CriticalPathConfig | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Return True when Copilot has submitted approved/commented review."""
    return _wiring_has_copilot_review_signal(
        pr_repo,
        pr_number,
        pr_port=pr_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        gh_runner=gh_runner,
        default_pr_port_fn=_default_pr_port,
    )


def _apply_codex_fail_routing(
    issue_ref: str,
    route: str,
    checklist: list[str],
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Route failed codex review back to In Progress with explicit handoff."""
    _app_apply_codex_fail_routing(
        issue_ref,
        route,
        checklist,
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        gh_runner=gh_runner,
        transition_issue_status_fn=_transition_issue_status,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        query_project_item_field_fn=_query_project_item_field,
        issue_ref_to_repo_parts_fn=_issue_ref_to_repo_parts,
        comment_exists_fn=_comment_exists,
    )


def mark_issues_done(
    issues: list[LinkedIssue],
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Mark linked issues as Done on the board. Returns list of refs marked Done."""
    return _helpers_mark_issues_done(
        issues,
        config,
        project_owner,
        project_number,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        transition_issue_status_fn=_transition_issue_status,
    )


# ---------------------------------------------------------------------------
# Subcommand: auto-promote
# ---------------------------------------------------------------------------


# PromotionResult — imported from domain.models (M5)


def auto_promote_successors(
    issue_ref: str,
    config: CriticalPathConfig,
    this_repo_prefix: str,
    project_owner: str,
    project_number: int,
    *,
    automation_config: BoardAutomationConfig | None = None,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> PromotionResult:
    """Promote eligible successors of a Done issue."""
    return _wiring_auto_promote_successors(
        issue_ref,
        config,
        this_repo_prefix,
        project_owner,
        project_number,
        automation_config=automation_config,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        promote_to_ready_fn=promote_to_ready,
        controller_owned_resolver_fn=lambda ref: _controller_owned_admission(
            ref,
            automation_config,
        ),
        comment_exists_fn=_comment_exists,
        issue_ref_to_repo_parts_fn=_issue_ref_to_repo_parts,
        new_handoff_job_id_fn=_new_handoff_job_id,
        default_board_mutation_port_fn=_default_board_mutation_port,
    )


# ---------------------------------------------------------------------------
# Subcommand: propagate-blocker
# ---------------------------------------------------------------------------


def propagate_blocker(
    issue_ref: str | None,
    config: CriticalPathConfig,
    this_repo_prefix: str | None,
    project_owner: str,
    project_number: int,
    *,
    sweep_blocked: bool = False,
    all_prefixes: bool = False,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Propagate blocker info to successors. Returns list of commented refs."""
    return _app_propagate_blocker(
        issue_ref,
        config,
        this_repo_prefix,
        project_owner,
        project_number,
        sweep_blocked=sweep_blocked,
        all_prefixes=all_prefixes,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        board_info_resolver=board_info_resolver,
        gh_runner=gh_runner,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        list_project_items_by_status_fn=_list_project_items_by_status,
        snapshot_to_issue_ref_fn=_snapshot_to_issue_ref,
        query_project_item_field_fn=_query_project_item_field,
        comment_exists_fn=_comment_exists,
        post_comment_fn=_post_comment,
        issue_ref_to_repo_parts_fn=_issue_ref_to_repo_parts,
    )


# ---------------------------------------------------------------------------
# Subcommand: reconcile-handoffs
# ---------------------------------------------------------------------------


def reconcile_handoffs(
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    ack_timeout_minutes: int = 30,
    max_retries: int = 1,
    dry_run: bool = False,
    github_bundle: GitHubPortBundle | None = None,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> dict[str, int]:
    """Reconcile handoff jobs. Returns {completed, retried, escalated, pending}."""
    return _app_reconcile_handoffs(
        config,
        project_owner,
        project_number,
        ack_timeout_minutes=ack_timeout_minutes,
        max_retries=max_retries,
        dry_run=dry_run,
        github_bundle=github_bundle,
        review_state_port=review_state_port,
        board_port=board_port,
        gh_runner=gh_runner,
        ensure_github_bundle_fn=_ensure_github_bundle,
        set_blocked_with_reason_fn=_set_blocked_with_reason,
    )


# ---------------------------------------------------------------------------
# Subcommand: schedule-ready / claim-ready
# ---------------------------------------------------------------------------


# SchedulingDecision, ClaimReadyResult, ExecutorRoutingDecision — imported from domain.models (M5)


def _protected_queue_executor_target(
    automation_config: BoardAutomationConfig | None,
) -> str | None:
    """Return the sole protected execution executor when routing is deterministic."""
    return _app_protected_queue_executor_target(automation_config)


def route_protected_queue_executors(
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    project_owner: str,
    project_number: int,
    *,
    statuses: tuple[str, ...] = PROTECTED_QUEUE_ROUTING_STATUSES,
    dry_run: bool = False,
    board_snapshot: CycleBoardSnapshot | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ExecutorRoutingDecision:
    """Normalize protected Backlog/Ready queue items onto the local executor lane."""
    return _app_route_protected_queue_executors(
        config,
        automation_config,
        project_owner,
        project_number,
        statuses=statuses,
        dry_run=dry_run,
        board_snapshot=board_snapshot,
        gh_runner=gh_runner,
        default_board_mutation_port_fn=_default_board_mutation_port,
        list_project_items_by_status_fn=_list_project_items_by_status,
        query_issue_board_info_fn=_query_issue_board_info,
        set_single_select_field_fn=_set_single_select_field,
    )


def _controller_owned_admission(
    issue_ref: str,
    automation_config: BoardAutomationConfig | None,
) -> bool:
    """Return True when protected Backlog -> Ready is controller-owned."""
    if automation_config is None:
        return False
    if not automation_config.admission.enabled:
        return False
    if automation_config.execution_authority_mode != "single_machine":
        return False
    try:
        prefix = parse_issue_ref(issue_ref).prefix
    except ConfigError:
        return False
    return prefix in automation_config.execution_authority_repos


def _set_handoff_target(
    issue_ref: str,
    target: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set the board Handoff To field for an issue."""
    _app_set_handoff_target(
        issue_ref,
        target,
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        gh_runner=gh_runner,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
    )


def _build_admission_pipeline_deps() -> _AdmissionPipelineDeps:
    """Build the wiring deps for the admission pipeline."""
    return _wiring_build_admission_pipeline_deps(
        query_open_pull_requests_fn=query_open_pull_requests,
        list_issue_comment_bodies_fn=list_issue_comment_bodies,
        memoized_query_issue_body_fn=memoized_query_issue_body,
        mark_issues_done_fn=mark_issues_done,
        close_issue_fn=close_issue,
        set_blocked_with_reason_fn=_set_blocked_with_reason,
        set_handoff_target_fn=_set_handoff_target,
        default_board_mutation_port_fn=_default_board_mutation_port,
        set_single_select_field_fn=_set_single_select_field,
        set_text_field_fn=_set_text_field,
        list_project_items_fn=_list_project_items,
    )


def admission_summary_payload(
    decision: AdmissionDecision,
    *,
    enabled: bool,
) -> dict[str, object]:
    """Convert an AdmissionDecision into a JSON-friendly payload."""
    return _wiring_admission_summary_payload(decision, enabled=enabled)


def _load_admission_source_items(
    automation_config: BoardAutomationConfig,
    *,
    review_state_port: _ReviewStatePort | None,
    board_snapshot: CycleBoardSnapshot | None,
    gh_runner: Callable[..., str] | None,
) -> list[_ProjectItemSnapshot]:
    """Load backlog/ready items needed for one admission pass."""
    return _wiring_load_admission_source_items(
        automation_config,
        deps=_build_admission_pipeline_deps(),
        review_state_port=review_state_port,
        board_snapshot=board_snapshot,
        gh_runner=gh_runner,
    )


def _partition_admission_source_items(
    items: list[_ProjectItemSnapshot],
    *,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    target_executor: str,
) -> tuple[int, list[_ProjectItemSnapshot]]:
    """Count governed ready items and collect governed backlog items."""
    return _wiring_partition_admission_source_items(
        items,
        config=config,
        automation_config=automation_config,
        target_executor=target_executor,
    )


def _build_provisional_admission_candidates(
    backlog_items: list[_ProjectItemSnapshot],
    *,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    dispatchable_repo_prefixes: tuple[str, ...],
    active_lease_issue_refs: tuple[str, ...],
) -> tuple[list[_ProjectItemSnapshot], list[AdmissionSkip]]:
    """Apply cheap exact admission filters to backlog items."""
    return _wiring_build_provisional_admission_candidates(
        backlog_items,
        config=config,
        automation_config=automation_config,
        dispatchable_repo_prefixes=dispatchable_repo_prefixes,
        active_lease_issue_refs=active_lease_issue_refs,
    )


def _evaluate_admission_candidates(
    provisional_candidates: list[_ProjectItemSnapshot],
    *,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    needed: int,
    dry_run: bool,
    memo: CycleGitHubMemo,
    skipped: list[AdmissionSkip],
    pr_port: _PullRequestPort | None,
    review_state_port: _ReviewStatePort | None,
    board_port: _BoardMutationPort | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[
    list[AdmissionCandidate],
    list[str],
    list[str],
    bool,
    str | None,
    bool,
]:
    """Run the expensive admission checks needed to choose candidates."""
    return _wiring_evaluate_admission_candidates(
        provisional_candidates,
        deps=_build_admission_pipeline_deps(),
        config=config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        needed=needed,
        dry_run=dry_run,
        memo=memo,
        skipped=skipped,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        gh_runner=gh_runner,
    )


def _apply_admitted_backlog_candidates(
    selected: list[AdmissionCandidate],
    *,
    executor: str,
    assignment_owner: str,
    board_port: _BoardMutationPort | None,
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> tuple[list[str], bool, str | None]:
    """Apply backlog-to-ready mutations for the selected candidates."""
    return _wiring_apply_admitted_backlog_candidates(
        selected,
        deps=_build_admission_pipeline_deps(),
        executor=executor,
        assignment_owner=assignment_owner,
        board_port=board_port,
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        dry_run=dry_run,
        gh_runner=gh_runner,
    )


def admit_backlog_items(
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None,
    project_owner: str,
    project_number: int,
    *,
    dispatchable_repo_prefixes: tuple[str, ...] | None = None,
    active_lease_issue_refs: tuple[str, ...] = (),
    dry_run: bool = False,
    board_snapshot: CycleBoardSnapshot | None = None,
    github_bundle: GitHubPortBundle | None = None,
    github_memo: CycleGitHubMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> AdmissionDecision:
    """Autonomously admit governed Backlog items into Ready."""
    return _wiring_admit_backlog_items(
        config,
        automation_config,
        project_owner,
        project_number,
        dispatchable_repo_prefixes=dispatchable_repo_prefixes,
        active_lease_issue_refs=active_lease_issue_refs,
        dry_run=dry_run,
        board_snapshot=board_snapshot,
        github_bundle=github_bundle,
        github_memo=github_memo,
        gh_runner=gh_runner,
        protected_queue_executor_target_fn=_protected_queue_executor_target,
        load_admission_source_items_fn=_load_admission_source_items,
        partition_admission_source_items_fn=_partition_admission_source_items,
        build_provisional_candidates_fn=_build_provisional_admission_candidates,
        evaluate_candidates_fn=_evaluate_admission_candidates,
        apply_candidates_fn=_apply_admitted_backlog_candidates,
        CycleGitHubMemo=CycleGitHubMemo,
    )


def _post_claim_comment(
    issue_ref: str,
    executor: str,
    config: CriticalPathConfig,
    *,
    board_port: _BoardMutationPort | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Post deterministic kickoff comment on successful claim."""
    _wiring_post_claim_comment(
        issue_ref,
        executor,
        config,
        board_port=board_port,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        comment_exists_fn=_comment_exists,
    )


def schedule_ready_items(
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    mode: str = "advisory",
    per_executor_wip_limit: int = 3,
    automation_config: BoardAutomationConfig | None = None,
    missing_executor_block_cap: int = DEFAULT_MISSING_EXECUTOR_BLOCK_CAP,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> SchedulingDecision:
    """Classify and optionally claim Ready issues. Returns SchedulingDecision."""
    return _wiring_wire_schedule_ready_items(
        config,
        project_owner,
        project_number,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        mode=mode,
        per_executor_wip_limit=per_executor_wip_limit,
        automation_config=automation_config,
        missing_executor_block_cap=missing_executor_block_cap,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        legacy_board_status_mutator_fn=_legacy_board_status_mutator,
    )


def claim_ready_issue(
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    executor: str,
    issue_ref: str | None = None,
    next_issue: bool = False,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    per_executor_wip_limit: int = 3,
    automation_config: BoardAutomationConfig | None = None,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ClaimReadyResult:
    """Claim one Ready issue for a specific executor."""
    return _wiring_wire_claim_ready_issue(
        config,
        project_owner,
        project_number,
        executor=executor,
        issue_ref=issue_ref,
        next_issue=next_issue,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        per_executor_wip_limit=per_executor_wip_limit,
        automation_config=automation_config,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        legacy_board_status_mutator_fn=_legacy_board_status_mutator,
    )


# ---------------------------------------------------------------------------
# Subcommand: enforce-ready-dependencies
# ---------------------------------------------------------------------------


def enforce_ready_dependency_guard(
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    dry_run: bool = False,
    status_resolver: Callable[..., str] | None = None,
    review_state_port: _ReviewStatePort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Block Ready issues with unmet predecessors. Returns corrected refs."""
    return _wiring_enforce_ready_dependency_guard(
        config,
        project_owner,
        project_number,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        dry_run=dry_run,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        review_state_port=review_state_port,
        default_review_state_port_fn=_default_review_state_port,
        find_unmet_dependencies_fn=find_unmet_ready_dependencies,
        set_blocked_with_reason_fn=_set_blocked_with_reason,
    )


# ---------------------------------------------------------------------------
# Subcommand: audit-in-progress
# ---------------------------------------------------------------------------


def audit_in_progress(
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    max_age_hours: int = 24,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Escalate stale In Progress issues with no linked PR."""
    return _wiring_audit_in_progress(
        config,
        project_owner,
        project_number,
        max_age_hours=max_age_hours,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        list_project_items_by_status_fn=_list_project_items_by_status,
        query_issue_board_info_fn=_query_issue_board_info,
        set_single_select_field_fn=_set_single_select_field,
        comment_exists_fn=_comment_exists,
        post_comment_fn=_post_comment,
        query_project_item_field_fn=_query_project_item_field,
        query_issue_updated_at_fn=_query_issue_updated_at,
        snapshot_to_issue_ref_fn=_snapshot_to_issue_ref,
    )


# ---------------------------------------------------------------------------
# Subcommand: dispatch-agent
# ---------------------------------------------------------------------------


def dispatch_agent(
    issue_refs: list[str],
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> DispatchResult:
    """Dispatch eligible In Progress issues according to dispatch target."""
    return _wiring_dispatch_agent(
        issue_refs,
        config,
        automation_config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        query_issue_board_info_fn=_query_issue_board_info,
        query_project_item_field_fn=_query_project_item_field,
        comment_exists_fn=_comment_exists,
        post_comment_fn=_post_comment,
    )


# ---------------------------------------------------------------------------
# Subcommand: rebalance-wip
# ---------------------------------------------------------------------------


def rebalance_wip(
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    this_repo_prefix: str | None = None,
    all_prefixes: bool = False,
    cycle_minutes: int = DEFAULT_REBALANCE_CYCLE_MINUTES,
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    status_resolver: Callable[..., str] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> RebalanceDecision:
    """Rebalance In Progress lanes with stale demotion and dependency blocking."""
    return _wiring_rebalance_wip(
        config,
        automation_config,
        project_owner,
        project_number,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        cycle_minutes=cycle_minutes,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        status_resolver=status_resolver,
        gh_runner=gh_runner,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        list_project_items_by_status_fn=_list_project_items_by_status,
        is_graph_member_fn=in_any_critical_path,
        ready_promotion_evaluator_fn=evaluate_ready_promotion,
        set_blocked_with_reason_fn=_set_blocked_with_reason,
        set_handoff_target_fn=_set_handoff_target,
        query_project_item_field_fn=_query_project_item_field,
        query_open_pr_updated_at_fn=_query_open_pr_updated_at,
        query_latest_wip_activity_timestamp_fn=_query_latest_wip_activity_timestamp,
        query_latest_marker_timestamp_fn=_query_latest_marker_timestamp,
        comment_exists_fn=_comment_exists,
        transition_issue_status_fn=_transition_issue_status,
        snapshot_to_issue_ref_fn=_snapshot_to_issue_ref,
    )


# ---------------------------------------------------------------------------
# Subcommand: sync-review-state
# ---------------------------------------------------------------------------


def resolve_issues_from_event(
    event_path: str,
    config: CriticalPathConfig,
    *,
    pr_port: _PullRequestPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[tuple[str, str, list[str] | None]]:
    """Parse GITHUB_EVENT_PATH -> list of (issue_ref, event_kind, failed_checks)."""
    return _app_resolve_issues_from_event(
        event_path,
        config,
        pr_port=pr_port,
        gh_runner=gh_runner,
        query_closing_issues_fn=query_closing_issues,
        query_failed_check_runs_fn=_query_failed_check_runs,
    )


def resolve_pr_to_issues(
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    *,
    pr_port: _PullRequestPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Resolve PR -> linked issue refs using closingIssuesReferences."""
    return _app_resolve_pr_to_issues(
        pr_repo,
        pr_number,
        config,
        pr_port=pr_port,
        gh_runner=gh_runner,
        query_closing_issues_fn=query_closing_issues,
    )


def sync_review_state(
    event_kind: str,
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    automation_config: BoardAutomationConfig | None = None,
    pr_state: str | None = None,
    review_state: str | None = None,
    checks_state: str | None = None,
    failed_checks: list[str] | None = None,
    dry_run: bool = False,
    github_bundle: GitHubPortBundle | None = None,
    pr_port: _PullRequestPort | None = None,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[int, str]:
    """Sync board state based on PR/review/check events."""
    return _wiring_sync_review_state(
        event_kind,
        issue_ref,
        config,
        project_owner,
        project_number,
        automation_config=automation_config,
        pr_state=pr_state,
        review_state=review_state,
        checks_state=checks_state,
        failed_checks=failed_checks,
        dry_run=dry_run,
        github_bundle=github_bundle,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        default_pr_port_fn=_default_pr_port,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        legacy_board_status_mutator_fn=_legacy_board_status_mutator,
    )


# ---------------------------------------------------------------------------
# Subcommand: codex-review-gate
# ---------------------------------------------------------------------------


def _review_scope_refs(
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Return linked issue refs currently in Review for a PR."""
    return _wiring_review_scope_refs(
        pr_repo,
        pr_number,
        config,
        project_owner,
        project_number,
        gh_runner=gh_runner,
        query_closing_issues_fn=query_closing_issues,
        query_issue_board_info_fn=_query_issue_board_info,
    )


def codex_review_gate(
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    apply_fail_routing: bool = True,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[int, str]:
    """Evaluate strict codex review verdict contract for a PR."""
    return _wiring_codex_review_gate(
        pr_repo,
        pr_number,
        config,
        automation_config,
        project_owner,
        project_number,
        dry_run=dry_run,
        apply_fail_routing=apply_fail_routing,
        gh_runner=gh_runner,
        query_closing_issues_fn=query_closing_issues,
        query_issue_board_info_fn=_query_issue_board_info,
        query_latest_codex_verdict_fn=query_latest_codex_verdict,
        apply_codex_fail_routing_fn=_apply_codex_fail_routing,
    )


# ---------------------------------------------------------------------------
# Review snapshot / rescue
# ---------------------------------------------------------------------------


# ReviewSnapshot, ReviewRescueResult, ReviewRescueSweep — imported from domain.models (M5)


def _configured_review_checks(
    pr_repo: str,
    automation_config: BoardAutomationConfig,
) -> tuple[str, ...]:
    """Return repo-specific review checks that should be reconciled."""
    return _wiring_configured_review_checks(pr_repo, automation_config)


def _build_review_snapshot(
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    pr_port: _PullRequestPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ReviewSnapshot:
    """Project PR review state into one explicit snapshot."""
    return _wiring_build_review_snapshot(
        pr_repo,
        pr_number,
        config,
        automation_config,
        project_owner,
        project_number,
        dry_run=dry_run,
        pr_port=pr_port,
        gh_runner=gh_runner,
        default_pr_port_fn=_default_pr_port,
        query_closing_issues_fn=query_closing_issues,
        query_issue_board_info_fn=_query_issue_board_info,
    )


def review_rescue(
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    snapshot: ReviewSnapshot | None = None,
    gh_runner: Callable[..., str] | None = None,
    pr_port: _PullRequestPort | None = None,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
) -> ReviewRescueResult:
    """Reconcile one PR in Review back toward self-healing merge flow."""
    return _wiring_review_rescue(
        pr_repo,
        pr_number,
        config,
        automation_config,
        project_owner,
        project_number,
        dry_run=dry_run,
        snapshot=snapshot,
        gh_runner=gh_runner,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        default_pr_port_fn=_default_pr_port,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        query_closing_issues_fn=query_closing_issues,
        query_issue_board_info_fn=_query_issue_board_info,
        automerge_review_fn=automerge_review,
    )

def review_rescue_all(
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
    pr_port: _PullRequestPort | None = None,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
) -> ReviewRescueSweep:
    """Run review rescue across all governed repos."""
    return _wiring_review_rescue_all(
        config,
        automation_config,
        project_owner,
        project_number,
        dry_run=dry_run,
        gh_runner=gh_runner,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        default_pr_port_fn=_default_pr_port,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        review_rescue_fn=review_rescue,
    )


# ---------------------------------------------------------------------------
# Subcommand: automerge-review
# ---------------------------------------------------------------------------


def automerge_review(
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    update_branch: bool = True,
    delete_branch: bool = True,
    snapshot: ReviewSnapshot | None = None,
    gh_runner: Callable[..., str] | None = None,
    pr_port: _PullRequestPort | None = None,
    review_state_port: _ReviewStatePort | None = None,
) -> tuple[int, str]:
    """Auto-merge PR when codex gate + required checks pass."""
    return _wiring_automerge_review(
        pr_repo,
        pr_number,
        config,
        automation_config,
        project_owner,
        project_number,
        dry_run=dry_run,
        update_branch=update_branch,
        delete_branch=delete_branch,
        snapshot=snapshot,
        gh_runner=gh_runner,
        pr_port=pr_port,
        review_state_port=review_state_port,
        default_pr_port_fn=_default_pr_port,
        default_review_state_port_fn=_default_review_state_port,
        codex_review_gate_fn=codex_review_gate,
    )



# ---------------------------------------------------------------------------
# Subcommand: enforce-execution-policy
# ---------------------------------------------------------------------------


# ExecutionPolicyDecision — imported from domain.models (M5)


def enforce_execution_policy(
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    *,
    allow_copilot_coding_agent: bool = False,
    dry_run: bool = False,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ExecutionPolicyDecision:
    """Enforce local execution authority for protected coding PRs."""
    return _wiring_enforce_execution_policy(
        pr_repo,
        pr_number,
        config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        allow_copilot_coding_agent=allow_copilot_coding_agent,
        dry_run=dry_run,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        default_pr_port_fn=_default_pr_port,
        is_copilot_actor_fn=_is_copilot_coding_agent_actor,
        query_project_item_field_fn=_query_project_item_field,
        set_status_if_changed_fn=_set_status_if_changed,
        query_issue_assignees_fn=_query_issue_assignees,
        set_issue_assignees_fn=_set_issue_assignees,
        comment_exists_fn=_comment_exists,
        post_comment_fn=_post_comment,
        close_pull_request_fn=close_pull_request,
        query_closing_issues_fn=query_closing_issues,
    )


# ---------------------------------------------------------------------------
# CLI Parser
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Subcommand handlers (implementations in automation_cli_handlers.py)
# ---------------------------------------------------------------------------

from startupai_controller.automation_cli_handlers import (  # noqa: E402
    _cmd_mark_done,
    _cmd_auto_promote,
    _cmd_admit_backlog,
    _cmd_propagate_blocker,
    _cmd_reconcile_handoffs,
    _cmd_schedule_ready,
    _cmd_claim_ready,
    _cmd_dispatch_agent,
    _cmd_rebalance_wip,
    _cmd_enforce_ready_deps,
    _cmd_audit_in_progress,
    _cmd_sync_review_state,
    _cmd_codex_review_gate,
    _cmd_automerge_review,
    _cmd_review_rescue,
    _cmd_review_rescue_all,
    _cmd_enforce_execution_policy,
    _cmd_classify_parallelism,
)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


from startupai_controller.board_automation_cli import build_parser, main


if __name__ == "__main__":
    raise SystemExit(main())
