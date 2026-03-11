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

from datetime import datetime, timedelta, timezone
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
    DEFAULT_PROJECT_NUMBER,
    DEFAULT_PROJECT_OWNER,
    DEFAULT_REBALANCE_CYCLE_MINUTES,
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


# ---------------------------------------------------------------------------
# Ready/review wiring (implementations in automation_ready_review_wiring.py)
# ---------------------------------------------------------------------------

from startupai_controller.automation_ready_review_wiring import (  # noqa: E402
    _has_copilot_review_signal,
    schedule_ready_items,
    claim_ready_issue,
    enforce_ready_dependency_guard,
    resolve_issues_from_event,
    resolve_pr_to_issues,
    sync_review_state,
    _review_scope_refs,
    codex_review_gate,
    _configured_review_checks,
    _build_review_snapshot,
    review_rescue,
    review_rescue_all,
    automerge_review,
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
