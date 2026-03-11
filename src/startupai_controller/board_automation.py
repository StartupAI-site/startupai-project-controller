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
    _transition_issue_status as _app_transition_issue_status,
)
from startupai_controller.application.automation.ready_wiring import (
    enforce_ready_dependency_guard as _wiring_enforce_ready_dependency_guard,
    wire_schedule_ready_items as _wiring_wire_schedule_ready_items,
    wire_claim_ready_issue as _wiring_wire_claim_ready_issue,
)
from startupai_controller.application.automation.codex_gate import (
    codex_review_gate as _app_codex_review_gate,
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
from startupai_controller.application.automation.execution_policy import (
    ExecutionPolicyPrContext as _ExecutionPolicyPrContext,
    load_execution_policy_pr_context as _app_load_execution_policy_pr_context,
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
# State/admission/coordination wiring (implementations in automation_state_admission_wiring.py)
# ---------------------------------------------------------------------------

from startupai_controller.automation_state_admission_wiring import (  # noqa: E402
    _set_board_status,
    _set_status_if_changed,
    _set_blocked_with_reason,
    _legacy_board_status_mutator,
    mark_issues_done,
    _wip_limit_for_lane,
    _has_copilot_review_signal,
    promote_to_ready,
    _controller_owned_admission,
    auto_promote_successors,
    propagate_blocker,
    reconcile_handoffs,
    _protected_queue_executor_target,
    route_protected_queue_executors,
    _set_handoff_target,
    _build_admission_pipeline_deps,
    admission_summary_payload,
    _load_admission_source_items,
    _partition_admission_source_items,
    _build_provisional_admission_candidates,
    _evaluate_admission_candidates,
    _apply_admitted_backlog_candidates,
    admit_backlog_items,
    _post_claim_comment,
)


# ---------------------------------------------------------------------------
# Execution/advisory wiring (implementations in automation_execution_wiring.py)
# ---------------------------------------------------------------------------

from startupai_controller.automation_execution_wiring import (  # noqa: E402
    _transition_issue_status,
    _apply_codex_fail_routing,
    audit_in_progress,
    dispatch_agent,
    rebalance_wip,
    enforce_execution_policy,
)


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
