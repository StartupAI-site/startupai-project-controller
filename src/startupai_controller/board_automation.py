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
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sys
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from startupai_controller.ports.pull_requests import PullRequestPort as _PullRequestPort
else:
    _PullRequestPort = None  # runtime: structural typing, no import needed


from startupai_controller.board_io import (  # transitional: non-adapted mechanism functions (ADR-002)
    COPILOT_CODING_AGENT_LOGINS,
    CycleBoardSnapshot,
    CycleGitHubMemo,
    _ProjectItemSnapshot,
    LinkedIssue,
    CodexReviewVerdict,
    PullRequestViewPayload,
    _run_gh,
    _parse_github_timestamp,
    _is_automation_login,
    _is_copilot_coding_agent_actor,
    _repo_to_prefix,
    _issue_ref_to_repo_parts,
    _snapshot_to_issue_ref,
    _marker_for,
    _comment_exists,
    _post_comment,
    _query_issue_comments,
    _comment_activity_timestamp,
    _query_latest_matching_comment_timestamp,
    _query_latest_non_automation_comment_timestamp,
    _query_latest_marker_timestamp,
    _query_project_item_field,
    _set_text_field,
    _query_single_select_field_option,
    _set_single_select_field,
    _set_status_if_changed,
    _list_project_items,
    _list_project_items_by_status,
    _query_issue_updated_at,
    _query_open_pr_updated_at,
    _query_latest_wip_activity_timestamp,
    _parse_pr_url,
    _is_pr_open,
    _query_issue_assignees,
    _set_issue_assignees,
    query_closing_issues,
    query_open_pull_requests,
    _parse_codex_verdict_from_text,
    build_pr_gate_status_from_payload,
    has_copilot_review_signal_from_payload,
    latest_codex_verdict_from_payload,
    query_required_status_checks,
    query_pull_request_view_payload,
    query_latest_codex_verdict,
    _query_failed_check_runs,
    _query_pr_head_sha,
    close_issue,
    close_pull_request,
    list_issue_comment_bodies,
    memoized_query_issue_body,
    rerun_actions_run,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    ConfigError,
    GhQueryError,
    direct_predecessors,
    direct_successors,
    evaluate_ready_promotion,
    in_any_critical_path,
    load_config,
    parse_issue_ref,
)
from startupai_controller.board_graph import (
    _issue_sort_key,
    _resolve_issue_coordinates,
    _admission_candidate_rank,
    _ready_snapshot_rank,
    _count_wip_by_executor,
    _count_wip_by_executor_lane,
    classify_parallelism_snapshot,
    find_unmet_ready_dependencies,
)
from startupai_controller.promote_ready import (
    BoardInfo,
    _query_issue_board_info,
    _query_status_field_option,
    _set_board_status,
    promote_to_ready,
)
from startupai_controller.domain.automerge_policy import automerge_gate_decision
from startupai_controller.domain.rescue_policy import rescue_decision
from startupai_controller.domain.repair_policy import (
    MARKER_PREFIX,
)
from startupai_controller.domain.resolution_policy import (
    parse_resolution_comment,
)
from startupai_controller.domain.scheduling_policy import (
    VALID_DISPATCH_TARGETS,
    VALID_EXECUTION_AUTHORITY_MODES,
    VALID_EXECUTORS,
    PROTECTED_QUEUE_ROUTING_STATUSES,
    admission_watermarks,
    has_structured_acceptance_criteria,
    priority_rank as _priority_rank,
    wip_limit_for_lane as _domain_wip_limit_for_lane,
    protected_queue_executor_target as _domain_protected_queue_executor_target,
)
from startupai_controller.domain.models import (
    AdmissionCandidate,
    AdmissionDecision,
    AdmissionSkip,
    CheckObservation,
    ClaimReadyResult,
    ExecutionPolicyDecision,
    ExecutorRoutingDecision,
    OpenPullRequest,
    PrGateStatus,
    PromotionResult,
    ReviewRescueResult,
    ReviewRescueSweep,
    ReviewSnapshot,
    SchedulingDecision,
)

# ---------------------------------------------------------------------------
# Port wiring helpers
# ---------------------------------------------------------------------------


def _default_pr_port(
    project_owner: str,
    project_number: int,
    gh_runner: Callable[..., str] | None = None,
) -> _PullRequestPort:
    """Construct a default PullRequestPort adapter from context params."""
    from startupai_controller.adapters.github_cli import GitHubCliAdapter

    return GitHubCliAdapter(
        project_owner=project_owner,
        project_number=project_number,
        gh_runner=gh_runner,
    )


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CONFIG_PATH = str(_REPO_ROOT / "config" / "critical-paths.json")
DEFAULT_AUTOMATION_CONFIG_PATH = str(
    _REPO_ROOT / "config" / "board-automation-config.json"
)
DEFAULT_PROJECT_OWNER = "StartupAI-site"
DEFAULT_PROJECT_NUMBER = 1
DEFAULT_MISSING_EXECUTOR_BLOCK_CAP = 5
DEFAULT_REBALANCE_CYCLE_MINUTES = 30
VALID_NON_LOCAL_PR_POLICIES = {"block", "requeue", "close"}


@dataclass(frozen=True)
class AdmissionConfig:
    """Runtime policy for autonomous Backlog -> Ready admission."""

    enabled: bool = False
    source_statuses: tuple[str, ...] = ("Backlog",)
    ready_floor_multiplier: int = 2
    ready_cap_multiplier: int = 3
    assignment_owner: str = "codex:local-consumer"
    max_batch_size: int = 6
    acceptance_headings: tuple[str, ...] = (
        "Acceptance Criteria",
        "Definition of Done",
    )


@dataclass(frozen=True)
class BoardAutomationConfig:
    """Runtime policy config for board dispatch/rebalance workflows."""

    wip_limits: dict[str, dict[str, int]]
    freshness_hours: int
    stale_confirmation_cycles: int
    trusted_codex_actors: set[str]
    trusted_local_authors: set[str] = field(default_factory=set)
    dispatch_target: str = "executor"
    canary_thresholds: dict[str, float] = field(default_factory=dict)
    execution_authority_mode: str = "board"
    execution_authority_repos: tuple[str, ...] = ("crew",)
    execution_authority_executors: tuple[str, ...] = ("codex",)
    global_concurrency: int = 1
    deprecated_workflow_mutations: dict[str, bool] = field(default_factory=dict)
    required_checks_by_repo: dict[str, tuple[str, ...]] = field(default_factory=dict)
    non_local_pr_policy: str = "requeue"
    deferred_replay_enabled: bool = True
    multi_worker_enabled: bool = False
    admission: AdmissionConfig = field(default_factory=AdmissionConfig)
    issue_context_cache_enabled: bool = True
    issue_context_cache_ttl_seconds: int = 900
    launch_hydration_concurrency: int = 1
    rate_limit_pause_enabled: bool = True
    rate_limit_cooldown_seconds: int = 300
    worktree_reuse_enabled: bool = True
    slo_metrics_enabled: bool = True


def load_automation_config(path: Path) -> BoardAutomationConfig:
    """Load board automation policy config from JSON."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ConfigError(
            f"Cannot read automation config '{path}': {error}"
        ) from error
    except json.JSONDecodeError as error:
        raise ConfigError(
            f"Invalid JSON in automation config '{path}': {error}"
        ) from error

    version = payload.get("version")
    if version not in {1, 2}:
        raise ConfigError(
            f"Unsupported automation config version '{version}'."
        )

    wip_limits_raw = payload.get("wip_limits")
    if not isinstance(wip_limits_raw, dict):
        raise ConfigError("automation config wip_limits must be an object")
    wip_limits: dict[str, dict[str, int]] = {}
    for executor, lanes in wip_limits_raw.items():
        if not isinstance(lanes, dict):
            raise ConfigError(
                f"automation config wip_limits.{executor} must be an object"
            )
        executor_key = str(executor).strip().lower()
        if not executor_key:
            continue
        normalized_lanes: dict[str, int] = {}
        for lane, value in lanes.items():
            lane_key = str(lane).strip().lower()
            if not lane_key:
                continue
            try:
                limit = int(value)
            except (TypeError, ValueError) as error:
                raise ConfigError(
                    f"automation config wip_limits.{executor_key}.{lane_key} "
                    "must be an integer"
                ) from error
            if limit < 1:
                raise ConfigError(
                    f"automation config wip_limits.{executor_key}.{lane_key} "
                    "must be >= 1"
                )
            normalized_lanes[lane_key] = limit
        wip_limits[executor_key] = normalized_lanes

    try:
        freshness_hours = int(payload.get("freshness_hours", 24))
    except (TypeError, ValueError) as error:
        raise ConfigError(
            "automation config freshness_hours must be an integer"
        ) from error
    if freshness_hours < 1:
        raise ConfigError("automation config freshness_hours must be >= 1")

    try:
        stale_confirmation_cycles = int(
            payload.get("stale_confirmation_cycles", 2)
        )
    except (TypeError, ValueError) as error:
        raise ConfigError(
            "automation config stale_confirmation_cycles must be an integer"
        ) from error
    if stale_confirmation_cycles < 1:
        raise ConfigError(
            "automation config stale_confirmation_cycles must be >= 1"
        )

    trusted_raw = payload.get("trusted_codex_actors", [])
    if not isinstance(trusted_raw, list):
        raise ConfigError(
            "automation config trusted_codex_actors must be a list"
        )
    trusted_codex_actors = {
        str(actor).strip().lower() for actor in trusted_raw if str(actor).strip()
    }
    trusted_local_authors_raw = payload.get(
        "trusted_local_authors", trusted_raw
    )
    if not isinstance(trusted_local_authors_raw, list):
        raise ConfigError(
            "automation config trusted_local_authors must be a list"
        )
    trusted_local_authors = {
        str(actor).strip().lower()
        for actor in trusted_local_authors_raw
        if str(actor).strip()
    }

    dispatch_raw = payload.get("dispatch") or {}
    if not isinstance(dispatch_raw, dict):
        raise ConfigError("automation config dispatch must be an object")
    dispatch_target = str(dispatch_raw.get("target", "executor")).strip().lower()
    if dispatch_target not in VALID_DISPATCH_TARGETS:
        raise ConfigError(
            "automation config dispatch.target must be one of: "
            f"{sorted(VALID_DISPATCH_TARGETS)}"
        )

    canary_raw = payload.get("canary_thresholds") or {}
    if not isinstance(canary_raw, dict):
        raise ConfigError(
            "automation config canary_thresholds must be an object"
        )
    canary_thresholds: dict[str, float] = {}
    for key, value in canary_raw.items():
        try:
            canary_thresholds[str(key)] = float(value)
        except (TypeError, ValueError):
            continue

    authority_raw = payload.get("execution_authority") or {}
    if authority_raw and not isinstance(authority_raw, dict):
        raise ConfigError("automation config execution_authority must be an object")
    authority_mode = str(
        authority_raw.get("mode", "board" if version == 1 else "single_machine")
    ).strip().lower()
    if authority_mode not in VALID_EXECUTION_AUTHORITY_MODES:
        raise ConfigError(
            "automation config execution_authority.mode must be one of: "
            f"{sorted(VALID_EXECUTION_AUTHORITY_MODES)}"
        )
    authority_repos_raw = authority_raw.get("repos", ["crew"])
    if not isinstance(authority_repos_raw, list) or not authority_repos_raw:
        raise ConfigError(
            "automation config execution_authority.repos must be a non-empty list"
        )
    authority_repos = tuple(
        sorted(
            {
                str(repo).strip().lower()
                for repo in authority_repos_raw
                if str(repo).strip()
            }
        )
    )
    if not authority_repos:
        raise ConfigError(
            "automation config execution_authority.repos must contain values"
        )
    authority_executors_raw = authority_raw.get("executors", ["codex"])
    if not isinstance(authority_executors_raw, list) or not authority_executors_raw:
        raise ConfigError(
            "automation config execution_authority.executors must be a non-empty list"
        )
    authority_executors = tuple(
        sorted(
            {
                str(executor).strip().lower()
                for executor in authority_executors_raw
                if str(executor).strip()
            }
        )
    )
    if not authority_executors:
        raise ConfigError(
            "automation config execution_authority.executors must contain values"
        )
    try:
        global_concurrency = int(
            authority_raw.get("global_concurrency", 1 if version == 1 else 2)
        )
    except (TypeError, ValueError) as error:
        raise ConfigError(
            "automation config execution_authority.global_concurrency must be an integer"
        ) from error
    if global_concurrency < 1:
        raise ConfigError(
            "automation config execution_authority.global_concurrency must be >= 1"
        )
    try:
        issue_context_cache_ttl_seconds = int(
            authority_raw.get("issue_context_cache_ttl_seconds", 900)
        )
        launch_hydration_concurrency = int(
            authority_raw.get("launch_hydration_concurrency", 1)
        )
        rate_limit_cooldown_seconds = int(
            authority_raw.get("rate_limit_cooldown_seconds", 300)
        )
    except (TypeError, ValueError) as error:
        raise ConfigError(
            "automation config execution_authority cache/hydration/cooldown "
            "controls must be integers"
        ) from error
    if issue_context_cache_ttl_seconds < 1:
        raise ConfigError(
            "automation config execution_authority.issue_context_cache_ttl_seconds "
            "must be >= 1"
        )
    if launch_hydration_concurrency < 1:
        raise ConfigError(
            "automation config execution_authority.launch_hydration_concurrency "
            "must be >= 1"
        )
    if rate_limit_cooldown_seconds < 1:
        raise ConfigError(
            "automation config execution_authority.rate_limit_cooldown_seconds "
            "must be >= 1"
        )
    issue_context_cache_enabled = bool(
        authority_raw.get("issue_context_cache_enabled", True)
    )
    rate_limit_pause_enabled = bool(
        authority_raw.get("rate_limit_pause_enabled", True)
    )
    worktree_reuse_enabled = bool(
        authority_raw.get("worktree_reuse_enabled", True)
    )
    slo_metrics_enabled = bool(
        authority_raw.get("slo_metrics_enabled", True)
    )

    deprecated_raw = payload.get("deprecated_workflow_mutations") or {}
    if deprecated_raw and not isinstance(deprecated_raw, dict):
        raise ConfigError(
            "automation config deprecated_workflow_mutations must be an object"
        )
    deprecated_workflow_mutations = {
        str(name).strip(): bool(enabled)
        for name, enabled in deprecated_raw.items()
        if str(name).strip()
    }

    checks_raw = payload.get("required_checks_by_repo") or {}
    if checks_raw and not isinstance(checks_raw, dict):
        raise ConfigError(
            "automation config required_checks_by_repo must be an object"
        )
    required_checks_by_repo: dict[str, tuple[str, ...]] = {}
    for repo_name, checks in checks_raw.items():
        if not isinstance(checks, list):
            raise ConfigError(
                "automation config required_checks_by_repo entries must be lists"
            )
        normalized_checks = tuple(
            check for check in [str(item).strip() for item in checks] if check
        )
        required_checks_by_repo[str(repo_name).strip().lower()] = normalized_checks

    non_local_pr_policy = str(
        payload.get("non_local_pr_policy", "block" if version == 2 else "requeue")
    ).strip().lower()
    if non_local_pr_policy not in VALID_NON_LOCAL_PR_POLICIES:
        raise ConfigError(
            "automation config non_local_pr_policy must be one of: "
            f"{sorted(VALID_NON_LOCAL_PR_POLICIES)}"
        )

    feature_flags_raw = payload.get("feature_flags") or {}
    if feature_flags_raw and not isinstance(feature_flags_raw, dict):
        raise ConfigError("automation config feature_flags must be an object")
    deferred_replay_enabled = bool(feature_flags_raw.get("deferred_replay", True))
    multi_worker_enabled = bool(feature_flags_raw.get("multi_worker", False))

    admission_raw = payload.get("admission") or {}
    if admission_raw and not isinstance(admission_raw, dict):
        raise ConfigError("automation config admission must be an object")
    admission_enabled = bool(admission_raw.get("enabled", False))
    source_statuses_raw = admission_raw.get("source_statuses", ["Backlog"])
    if not isinstance(source_statuses_raw, list) or not source_statuses_raw:
        raise ConfigError("automation config admission.source_statuses must be a non-empty list")
    source_statuses = tuple(
        status
        for status in [str(item).strip() for item in source_statuses_raw]
        if status
    )
    if not source_statuses:
        raise ConfigError("automation config admission.source_statuses must contain values")
    try:
        ready_floor_multiplier = int(admission_raw.get("ready_floor_multiplier", 2))
        ready_cap_multiplier = int(admission_raw.get("ready_cap_multiplier", 3))
        max_batch_size = int(admission_raw.get("max_batch_size", 6))
    except (TypeError, ValueError) as error:
        raise ConfigError(
            "automation config admission multipliers and max_batch_size must be integers"
        ) from error
    if ready_floor_multiplier < 1 or ready_cap_multiplier < 1 or max_batch_size < 1:
        raise ConfigError(
            "automation config admission multipliers and max_batch_size must be >= 1"
        )
    acceptance_headings_raw = admission_raw.get(
        "acceptance_headings",
        ["Acceptance Criteria", "Definition of Done"],
    )
    if not isinstance(acceptance_headings_raw, list) or not acceptance_headings_raw:
        raise ConfigError(
            "automation config admission.acceptance_headings must be a non-empty list"
        )
    acceptance_headings = tuple(
        heading
        for heading in [str(item).strip() for item in acceptance_headings_raw]
        if heading
    )
    if not acceptance_headings:
        raise ConfigError(
            "automation config admission.acceptance_headings must contain values"
        )
    assignment_owner = str(
        admission_raw.get("assignment_owner", "codex:local-consumer")
    ).strip()
    if not assignment_owner:
        raise ConfigError("automation config admission.assignment_owner must be non-empty")
    admission = AdmissionConfig(
        enabled=admission_enabled,
        source_statuses=source_statuses,
        ready_floor_multiplier=ready_floor_multiplier,
        ready_cap_multiplier=ready_cap_multiplier,
        assignment_owner=assignment_owner,
        max_batch_size=max_batch_size,
        acceptance_headings=acceptance_headings,
    )

    if authority_mode == "single_machine" and multi_worker_enabled:
        protected_repos = tuple(
            repo for repo in authority_repos if repo in {"app", "crew", "site"}
        )
        if "codex" in authority_executors:
            codex_limits = wip_limits.get("codex")
            if not isinstance(codex_limits, dict):
                raise ConfigError(
                    "automation config wip_limits.codex is required in "
                    "single-machine multi-worker mode"
                )
            for repo in protected_repos:
                lane_limit = codex_limits.get(repo)
                if lane_limit is None:
                    raise ConfigError(
                        f"automation config wip_limits.codex.{repo} is required "
                        "in single-machine multi-worker mode"
                    )
                if lane_limit < global_concurrency:
                    raise ConfigError(
                        f"automation config wip_limits.codex.{repo} "
                        f"wip limit {lane_limit} is below global concurrency "
                        f"{global_concurrency}"
                    )

    return BoardAutomationConfig(
        wip_limits=wip_limits,
        freshness_hours=freshness_hours,
        stale_confirmation_cycles=stale_confirmation_cycles,
        trusted_codex_actors=trusted_codex_actors,
        trusted_local_authors=trusted_local_authors,
        dispatch_target=dispatch_target,
        canary_thresholds=canary_thresholds,
        execution_authority_mode=authority_mode,
        execution_authority_repos=authority_repos,
        execution_authority_executors=authority_executors,
        global_concurrency=global_concurrency,
        deprecated_workflow_mutations=deprecated_workflow_mutations,
        required_checks_by_repo=required_checks_by_repo,
        non_local_pr_policy=non_local_pr_policy,
        deferred_replay_enabled=deferred_replay_enabled,
        multi_worker_enabled=multi_worker_enabled,
        admission=admission,
        issue_context_cache_enabled=issue_context_cache_enabled,
        issue_context_cache_ttl_seconds=issue_context_cache_ttl_seconds,
        launch_hydration_concurrency=launch_hydration_concurrency,
        rate_limit_pause_enabled=rate_limit_pause_enabled,
        rate_limit_cooldown_seconds=rate_limit_cooldown_seconds,
        worktree_reuse_enabled=worktree_reuse_enabled,
        slo_metrics_enabled=slo_metrics_enabled,
    )

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _new_handoff_job_id(issue_ref: str, target: str) -> str:
    """Generate a deterministic handoff job ID."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_ref = issue_ref.replace("#", "-")
    safe_target = target.replace("#", "-")
    return f"{safe_ref}-to-{safe_target}-{ts}"


def _workflow_mutations_enabled(
    automation_config: BoardAutomationConfig,
    workflow_name: str,
) -> bool:
    """Return whether a deprecated workflow is still allowed to mutate state."""
    if automation_config.execution_authority_mode != "single_machine":
        return True
    return automation_config.deprecated_workflow_mutations.get(workflow_name, False)


def _set_blocked_with_reason(
    issue_ref: str,
    reason: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set Status=Blocked and Blocked Reason on a board item."""
    resolve_info = board_info_resolver or _query_issue_board_info
    info = resolve_info(issue_ref, config, project_owner, project_number)

    if info.status == "NOT_ON_BOARD":
        raise GhQueryError(f"{issue_ref} is not on the project board.")

    mutate = board_mutator
    if mutate is None:
        field_id, option_id = _query_status_field_option(
            info.project_id,
            "Blocked",
            gh_runner=gh_runner,
        )
        _set_board_status(
            info.project_id,
            info.item_id,
            field_id,
            option_id,
            gh_runner=gh_runner,
        )
    else:
        mutate(info.project_id, info.item_id)

    # Set Blocked Reason text field
    _set_text_field(
        info.project_id,
        info.item_id,
        "Blocked Reason",
        reason,
        gh_runner=gh_runner,
    )


def _wip_limit_for_lane(
    automation_config: BoardAutomationConfig | None,
    executor: str,
    lane: str,
    fallback: int,
) -> int:
    """Resolve WIP limit for an executor/lane pair."""
    wip_limits = automation_config.wip_limits if automation_config else None
    return _domain_wip_limit_for_lane(wip_limits, executor, lane, fallback)


# ---------------------------------------------------------------------------
# Subcommand: mark-done
# ---------------------------------------------------------------------------


def _has_copilot_review_signal(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Return True when Copilot has submitted approved/commented review."""
    payload = query_pull_request_view_payload(
        pr_repo,
        pr_number,
        gh_runner=gh_runner,
    )
    return has_copilot_review_signal_from_payload(payload)


def _apply_codex_fail_routing(
    issue_ref: str,
    route: str,
    checklist: list[str],
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Route failed codex review back to In Progress with explicit handoff."""
    _changed, old_status = _set_status_if_changed(
        issue_ref,
        {"Review"},
        "In Progress",
        config,
        project_owner,
        project_number,
        board_info_resolver=board_info_resolver,
        board_mutator=(lambda *_: None) if dry_run else None,
        gh_runner=gh_runner,
    )

    if old_status not in {"Review", "In Progress"}:
        return

    if route == "executor":
        executor = _query_project_item_field(
            issue_ref,
            "Executor",
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        ).lower()
        handoff_target = executor if executor in VALID_EXECUTORS else "human"
    elif route in VALID_EXECUTORS:
        handoff_target = route
    else:
        handoff_target = "human"

    resolve_info = board_info_resolver or _query_issue_board_info
    info = resolve_info(issue_ref, config, project_owner, project_number)
    if info.status != "NOT_ON_BOARD" and not dry_run:
        _set_single_select_field(
            info.project_id,
            info.item_id,
            "Handoff To",
            handoff_target,
            gh_runner=gh_runner,
        )

    owner, repo, number = _issue_ref_to_repo_parts(issue_ref, config)
    marker = _marker_for("codex-review-fail", issue_ref)
    if _comment_exists(owner, repo, number, marker, gh_runner=gh_runner):
        return

    checklist_text = ""
    if checklist:
        checklist_text = "\n".join(f"- [ ] {item}" for item in checklist)
    else:
        checklist_text = "- [ ] Address Codex review findings"

    body = (
        f"{marker}\n"
        f"Codex review verdict: `fail`\n"
        f"Route: `{route}` (handoff: `{handoff_target}`)\n\n"
        "Required fixes:\n"
        f"{checklist_text}"
    )
    if not dry_run:
        _post_comment(owner, repo, number, body, gh_runner=gh_runner)


def mark_issues_done(
    issues: list[LinkedIssue],
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Mark linked issues as Done on the board. Returns list of refs marked Done."""
    resolve_info = board_info_resolver or _query_issue_board_info
    marked: list[str] = []

    for issue in issues:
        info = resolve_info(issue.ref, config, project_owner, project_number)

        if info.status in ("Done", "NOT_ON_BOARD"):
            continue

        mutate = board_mutator
        if mutate is None:
            field_id, option_id = _query_status_field_option(
                info.project_id,
                "Done",
                gh_runner=gh_runner,
            )
            _set_board_status(
                info.project_id,
                info.item_id,
                field_id,
                option_id,
                gh_runner=gh_runner,
            )
        else:
            mutate(info.project_id, info.item_id)

        marked.append(issue.ref)

    return marked


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
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
) -> PromotionResult:
    """Promote eligible successors of a Done issue."""
    result = PromotionResult()
    successors = direct_successors(config, issue_ref)

    if not successors:
        return result

    check_comment = comment_checker or _comment_exists
    post_comment = comment_poster or _post_comment

    for successor_ref in sorted(successors):
        parsed = parse_issue_ref(successor_ref)

        if parsed.prefix == this_repo_prefix:
            # Same-repo successor: attempt direct promotion
            code, output = promote_to_ready(
                issue_ref=successor_ref,
                config=config,
                project_owner=project_owner,
                project_number=project_number,
                dry_run=dry_run,
                status_resolver=status_resolver,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                controller_owned_resolver=lambda ref: _controller_owned_admission(
                    ref, automation_config
                ),
            )
            if code == 0:
                result.promoted.append(successor_ref)
            else:
                result.skipped.append((successor_ref, output))
        else:
            # Cross-repo successor: post bridge comment
            job_id = _new_handoff_job_id(issue_ref, successor_ref)
            marker = _marker_for("promote-bridge", successor_ref)

            succ_owner, succ_repo, succ_number = _issue_ref_to_repo_parts(
                successor_ref, config
            )

            if check_comment(
                succ_owner, succ_repo, succ_number, marker
            ):
                result.skipped.append(
                    (successor_ref, "Bridge comment already exists")
                )
                continue

            if not dry_run:
                handoff_marker = (
                    f"<!-- {MARKER_PREFIX}:handoff:job={job_id} -->"
                )
                body = (
                    f"{marker}\n"
                    f"{handoff_marker}\n"
                    f"**Auto-promote candidate**: `{successor_ref}` may be "
                    f"eligible for Ready now that `{issue_ref}` is Done.\n\n"
                    f"Run from the appropriate repo:\n"
                    f"```\nmake promote-ready ISSUE={successor_ref}\n```"
                )
                post_comment(succ_owner, succ_repo, succ_number, body)

            result.cross_repo_pending.append(successor_ref)
            result.handoff_jobs.append(job_id)

    return result


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
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Propagate blocker info to successors. Returns list of commented refs."""
    check_comment = comment_checker or _comment_exists
    post_comment = comment_poster or _post_comment
    commented: list[str] = []

    if sweep_blocked:
        # Sweep mode: scan all Blocked items
        blocked_items = _list_project_items_by_status(
            "Blocked", project_owner, project_number, gh_runner=gh_runner
        )
        for snapshot in blocked_items:
            ref = _snapshot_to_issue_ref(snapshot, config)
            if ref is None:
                continue

            if not all_prefixes and this_repo_prefix:
                parsed = parse_issue_ref(ref)
                if parsed.prefix != this_repo_prefix:
                    continue

            blocked_reason = _query_project_item_field(
                ref,
                "Blocked Reason",
                config,
                project_owner,
                project_number,
                gh_runner=gh_runner,
            )
            if not blocked_reason:
                continue

            new_comments = _propagate_single_blocker(
                ref,
                blocked_reason,
                config,
                check_comment=check_comment,
                post_comment=post_comment,
                dry_run=dry_run,
            )
            commented.extend(new_comments)
    else:
        # Single-issue mode
        if issue_ref is None:
            return commented

        blocked_reason = _query_project_item_field(
            issue_ref,
            "Blocked Reason",
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        )
        if not blocked_reason:
            return commented

        commented = _propagate_single_blocker(
            issue_ref,
            blocked_reason,
            config,
            check_comment=check_comment,
            post_comment=post_comment,
            dry_run=dry_run,
        )

    return commented


def _propagate_single_blocker(
    issue_ref: str,
    blocked_reason: str,
    config: CriticalPathConfig,
    *,
    check_comment: Callable[..., bool],
    post_comment: Callable[..., None],
    dry_run: bool = False,
) -> list[str]:
    """Post advisory comments on successors of a single blocked issue."""
    commented: list[str] = []
    successors = direct_successors(config, issue_ref)

    for successor_ref in sorted(successors):
        marker = _marker_for("blocker", issue_ref)
        succ_owner, succ_repo, succ_number = _issue_ref_to_repo_parts(
            successor_ref, config
        )

        if check_comment(succ_owner, succ_repo, succ_number, marker):
            continue

        if not dry_run:
            body = (
                f"{marker}\n"
                f"**Upstream blocker**: `{issue_ref}` is Blocked "
                f"(reason: {blocked_reason}).\n"
                f"This may affect `{successor_ref}`."
            )
            try:
                post_comment(succ_owner, succ_repo, succ_number, body)
            except Exception:
                # Cross-repo comment failure is non-fatal — log and continue
                import sys

                print(
                    f"WARNING: Failed posting blocker comment on "
                    f"{successor_ref}",
                    file=sys.stderr,
                )
                continue

        commented.append(successor_ref)

    return commented


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
    gh_runner: Callable[..., str] | None = None,
) -> dict[str, int]:
    """Reconcile handoff jobs. Returns {completed, retried, escalated, pending}."""
    now = datetime.now(timezone.utc)
    ack_timeout = timedelta(minutes=max(0, ack_timeout_minutes))
    counters = {
        "completed": 0,
        "retried": 0,
        "escalated": 0,
        "pending": 0,
    }

    # Scan all issue prefixes for handoff markers
    for prefix, repo_slug in config.issue_prefixes.items():
        owner, repo = repo_slug.split("/", maxsplit=1)

        # Search for open issues with handoff markers using the search API
        search_query = (
            f"repo:{repo_slug} is:issue is:open "
            f"in:comments \"{MARKER_PREFIX}:handoff:job=\""
        )
        try:
            output = _run_gh(
                [
                    "api",
                    "search/issues",
                    "-X",
                    "GET",
                    "-f",
                    f"q={search_query}",
                    "-f",
                    "per_page=100",
                ],
                gh_runner=gh_runner,
            )
        except GhQueryError:
            continue

        try:
            payload = json.loads(output)
        except json.JSONDecodeError:
            continue

        for item in payload.get("items", []):
            issue_number = item.get("number")
            if not issue_number:
                continue

            issue_ref = f"{prefix}#{issue_number}"

            # Check if this issue has been acknowledged (moved past Backlog)
            current_status = _query_project_item_field(
                issue_ref,
                "Status",
                config,
                project_owner,
                project_number,
                gh_runner=gh_runner,
            )

            ack_statuses = {"Ready", "In Progress", "Review", "Done"}
            if current_status in ack_statuses:
                counters["completed"] += 1
                continue

            try:
                comments = _query_issue_comments(
                    owner, repo, issue_number, gh_runner=gh_runner
                )
            except GhQueryError:
                counters["pending"] += 1
                continue

            retry_marker = f"{MARKER_PREFIX}:handoff-retry:{issue_ref}:"
            retry_count = 0
            for comment in comments:
                body = str(comment.get("body") or "")
                if retry_marker in body:
                    retry_count += 1

            latest_signal = _query_latest_handoff_signal_timestamp(
                owner,
                repo,
                issue_number,
                issue_ref,
                gh_runner=gh_runner,
            )
            if latest_signal is None or (now - latest_signal) < ack_timeout:
                counters["pending"] += 1
                continue

            if retry_count < max_retries:
                if not dry_run:
                    retry_id = (
                        f"{retry_marker}"
                        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
                    )
                    body = (
                        f"<!-- {retry_id} -->\n"
                        f"**Handoff retry**: `{issue_ref}` has an unacknowledged "
                        f"handoff. Retry #{retry_count + 1} of {max_retries}.\n\n"
                        f"Run:\n```\nmake promote-ready ISSUE={issue_ref}\n```"
                    )
                    _post_comment(
                        owner, repo, issue_number, body, gh_runner=gh_runner
                    )
                counters["retried"] += 1
            else:
                if not dry_run:
                    try:
                        _set_blocked_with_reason(
                            issue_ref,
                            "handoff-timeout:retries-exhausted",
                            config,
                            project_owner,
                            project_number,
                            gh_runner=gh_runner,
                        )
                    except GhQueryError:
                        pass
                counters["escalated"] += 1

    return counters


# ---------------------------------------------------------------------------
# Subcommand: schedule-ready / claim-ready
# ---------------------------------------------------------------------------


# SchedulingDecision, ClaimReadyResult, ExecutorRoutingDecision — imported from domain.models (M5)


def _protected_queue_executor_target(
    automation_config: BoardAutomationConfig | None,
) -> str | None:
    """Return the sole protected execution executor when routing is deterministic."""
    if automation_config is None:
        return None
    return _domain_protected_queue_executor_target(
        execution_authority_mode=automation_config.execution_authority_mode,
        execution_authority_executors=tuple(automation_config.execution_authority_executors),
    )


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
    decision = ExecutorRoutingDecision()
    target_executor = _protected_queue_executor_target(automation_config)
    if target_executor is None:
        decision.skipped.append(("*", "no-deterministic-executor-target"))
        return decision
    assert automation_config is not None

    for status in statuses:
        items = (
            board_snapshot.items_with_status(status)
            if board_snapshot is not None
            else _list_project_items_by_status(
                status,
                project_owner,
                project_number,
                gh_runner=gh_runner,
            )
        )
        for snapshot in items:
            issue_ref = _snapshot_to_issue_ref(snapshot, config)
            if issue_ref is None:
                decision.skipped.append((snapshot.issue_ref, "unknown-repo-prefix"))
                continue

            repo_prefix = parse_issue_ref(issue_ref).prefix
            if repo_prefix not in automation_config.execution_authority_repos:
                decision.skipped.append((issue_ref, "repo-not-governed"))
                continue

            current_executor = snapshot.executor.strip().lower()
            if current_executor == target_executor:
                decision.unchanged.append(issue_ref)
                continue

            project_id = snapshot.project_id.strip()
            item_id = snapshot.item_id.strip()
            if not project_id or not item_id:
                info = _query_issue_board_info(
                    issue_ref,
                    config,
                    project_owner,
                    project_number,
                )
                if info.status == "NOT_ON_BOARD":
                    decision.skipped.append((issue_ref, "not-on-board"))
                    continue
                project_id = info.project_id
                item_id = info.item_id

            if not dry_run:
                _set_single_select_field(
                    project_id,
                    item_id,
                    "Executor",
                    target_executor,
                    gh_runner=gh_runner,
                )
            decision.routed.append(issue_ref)

    return decision


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


def _deterministic_issue_branch_pattern(issue_ref: str) -> re.Pattern[str]:
    """Return the canonical issue branch naming pattern."""
    parsed = parse_issue_ref(issue_ref)
    return re.compile(rf"^feat/{parsed.number}-[a-z0-9-]+$")


def _parse_consumer_provenance(text: str) -> dict[str, str] | None:
    """Parse the consumer provenance marker from text."""
    match = re.search(
        rf"<!--\s*{re.escape(MARKER_PREFIX)}:consumer:"
        r"session=(?P<session>[^\s]+)\s+"
        r"issue=(?P<issue>[^\s]+)\s+"
        r"repo=(?P<repo>[^\s]+)\s+"
        r"branch=(?P<branch>[^\s]+)\s+"
        r"executor=(?P<executor>[^\s]+)\s*-->",
        text,
    )
    if not match:
        return None
    return {
        "session_id": match.group("session"),
        "issue_ref": match.group("issue"),
        "repo_prefix": match.group("repo"),
        "branch_name": match.group("branch"),
        "executor": match.group("executor"),
    }


def _extract_closing_issue_numbers(body: str) -> set[int]:
    """Return issue numbers referenced by common auto-close keywords."""
    matches = re.findall(
        r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s+#(\d+)\b",
        body,
        flags=re.IGNORECASE,
    )
    results: set[int] = set()
    for match in matches:
        try:
            results.add(int(match))
        except ValueError:
            continue
    return results


def _query_open_prs_by_prefix(
    config: CriticalPathConfig,
    repo_prefixes: tuple[str, ...],
    *,
    github_memo: CycleGitHubMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> dict[str, list[OpenPullRequest]]:
    """Return open PRs keyed by governed repo prefix."""
    result: dict[str, list[OpenPullRequest]] = {}
    for repo_prefix in repo_prefixes:
        repo_slug = config.issue_prefixes.get(repo_prefix)
        if not repo_slug:
            result[repo_prefix] = []
            continue
        if github_memo is not None:
            cached = github_memo.open_pull_requests.get(repo_slug)
            if cached is None:
                cached = query_open_pull_requests(
                    repo_slug,
                    gh_runner=gh_runner,
                )
                github_memo.open_pull_requests[repo_slug] = list(cached)
            result[repo_prefix] = list(cached)
        else:
            result[repo_prefix] = query_open_pull_requests(
                repo_slug,
                gh_runner=gh_runner,
            )
    return result


def _classify_admission_pr_state(
    issue_ref: str,
    issue_number: int,
    candidates: list[OpenPullRequest],
    automation_config: BoardAutomationConfig,
) -> str:
    """Classify PR state for backlog admission without extra per-issue queries."""
    linked_candidates = [
        pr for pr in candidates if issue_number in _extract_closing_issue_numbers(pr.body)
    ]
    if not linked_candidates:
        return "none"

    branch_pattern = _deterministic_issue_branch_pattern(issue_ref)
    local_count = 0
    ambiguous = False
    for candidate in linked_candidates:
        provenance = _parse_consumer_provenance(candidate.body)
        is_local = (
            provenance is not None
            and provenance.get("issue_ref") == issue_ref
            and provenance.get("executor") == "codex"
            and candidate.author in automation_config.trusted_local_authors
            and bool(branch_pattern.match(candidate.head_ref_name))
        )
        if is_local:
            local_count += 1
        else:
            ambiguous = True

    if local_count == 1 and not ambiguous:
        return "existing_local_pr"
    if local_count > 1 or (local_count and ambiguous):
        return "ambiguous_pr"
    return "non_local_pr"


def _latest_resolution_signal(
    issue_ref: str,
    owner: str,
    repo: str,
    number: int,
    *,
    github_memo: CycleGitHubMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> dict[str, object] | None:
    """Return the latest machine-readable resolution signal, if any."""
    if github_memo is not None:
        key = (owner, repo, number)
        cached = github_memo.issue_comment_bodies.get(key)
        if cached is None:
            cached = list_issue_comment_bodies(
                owner,
                repo,
                number,
                gh_runner=gh_runner,
            )
            github_memo.issue_comment_bodies[key] = list(cached)
        comment_bodies = list(cached)
    else:
        comment_bodies = list_issue_comment_bodies(
            owner,
            repo,
            number,
            gh_runner=gh_runner,
        )

    for body in reversed(comment_bodies):
        parsed = parse_resolution_comment(body)
        if parsed is None or parsed.issue_ref != issue_ref:
            continue
        payload = parsed.payload
        resolution_kind = str(payload.get("resolution_kind") or "").strip()
        verification_class = str(payload.get("verification_class") or "").strip()
        final_action = str(payload.get("final_action") or "").strip()
        summary = str(payload.get("summary") or "").strip()
        evidence = payload.get("evidence")
        if not isinstance(evidence, dict):
            evidence = {}
        return {
            "resolution_kind": resolution_kind,
            "verification_class": verification_class,
            "final_action": final_action,
            "summary": summary,
            "evidence": evidence,
        }
    return None


def _set_handoff_target(
    issue_ref: str,
    target: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set the board Handoff To field for an issue."""
    info = _query_issue_board_info(issue_ref, config, project_owner, project_number)
    if info.status == "NOT_ON_BOARD":
        return
    _set_single_select_field(
        info.project_id,
        info.item_id,
        "Handoff To",
        target,
        gh_runner=gh_runner,
    )


def _apply_prior_resolution_signal(
    issue_ref: str,
    signal: dict[str, object],
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Apply a prior machine-verified resolution signal before admission."""
    owner, repo, number = _resolve_issue_coordinates(issue_ref, config)
    verification_class = str(signal.get("verification_class") or "")
    final_action = str(signal.get("final_action") or "")
    resolution_kind = str(signal.get("resolution_kind") or "")
    blocked_reason = f"resolution-review-required:{resolution_kind or 'prior-signal'}"

    if verification_class == "strong" and final_action == "closed_as_already_resolved":
        if not dry_run:
            mark_issues_done(
                [LinkedIssue(owner=owner, repo=repo, number=number, ref=issue_ref)],
                config,
                project_owner,
                project_number,
                gh_runner=gh_runner,
            )
            close_issue(owner, repo, number, gh_runner=gh_runner)
        return "resolved"

    if not dry_run:
        _set_blocked_with_reason(
            issue_ref,
            blocked_reason,
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        )
        _set_handoff_target(
            issue_ref,
            "claude",
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        )
    return "blocked"


def _admit_backlog_item(
    candidate: AdmissionCandidate,
    *,
    executor: str,
    assignment_owner: str,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Mutate one backlog card into a controller-owned Ready card."""
    _set_single_select_field(
        candidate.project_id,
        candidate.item_id,
        "Executor",
        executor,
        gh_runner=gh_runner,
    )
    _set_text_field(
        candidate.project_id,
        candidate.item_id,
        "Owner",
        assignment_owner,
        gh_runner=gh_runner,
    )
    _set_single_select_field(
        candidate.project_id,
        candidate.item_id,
        "Handoff To",
        "none",
        gh_runner=gh_runner,
    )
    _set_text_field(
        candidate.project_id,
        candidate.item_id,
        "Blocked Reason",
        "",
        gh_runner=gh_runner,
    )
    _set_single_select_field(
        candidate.project_id,
        candidate.item_id,
        "Status",
        "Ready",
        gh_runner=gh_runner,
    )


def admission_summary_payload(
    decision: AdmissionDecision,
    *,
    enabled: bool,
) -> dict[str, object]:
    """Convert an AdmissionDecision into a JSON-friendly payload."""
    return {
        "enabled": enabled,
        "ready_count": decision.ready_count,
        "ready_floor": decision.ready_floor,
        "ready_cap": decision.ready_cap,
        "needed": decision.needed,
        "scanned_backlog": decision.scanned_backlog,
        "eligible_count": decision.eligible_count,
        "admitted": list(decision.admitted),
        "resolved": list(decision.resolved),
        "blocked": list(decision.blocked),
        "skip_reason_counts": decision.skip_reason_counts,
        "top_candidates": [
            {
                "issue_ref": candidate.issue_ref,
                "priority": candidate.priority,
                "title": candidate.title,
                "graph_member": candidate.is_graph_member,
            }
            for candidate in decision.eligible[:10]
        ],
        "top_skipped": [
            {
                "issue_ref": skip.issue_ref,
                "reason": skip.reason_code,
            }
            for skip in decision.skipped[:10]
        ],
        "partial_failure": decision.partial_failure,
        "error": decision.error,
        "deep_evaluation_performed": decision.deep_evaluation_performed,
        "deep_evaluation_truncated": decision.deep_evaluation_truncated,
        "controller_owned_admission_rejections": decision.skip_reason_counts.get(
            "controller_owned_admission", 0
        ),
    }


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
    github_memo: CycleGitHubMemo | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> AdmissionDecision:
    """Autonomously admit governed Backlog items into Ready."""
    if automation_config is None:
        return AdmissionDecision(
            ready_count=0,
            ready_floor=0,
            ready_cap=0,
            needed=0,
            scanned_backlog=0,
        )

    target_executor = _protected_queue_executor_target(automation_config)
    floor, cap = admission_watermarks(
        automation_config.global_concurrency,
        floor_multiplier=automation_config.admission.ready_floor_multiplier,
        cap_multiplier=automation_config.admission.ready_cap_multiplier,
    )
    if (
        not automation_config.admission.enabled
        or target_executor is None
    ):
        return AdmissionDecision(
            ready_count=0,
            ready_floor=floor,
            ready_cap=cap,
            needed=0,
            scanned_backlog=0,
        )

    statuses = set(automation_config.admission.source_statuses)
    statuses.add("Ready")
    if board_snapshot is None:
        items = _list_project_items(
            project_owner,
            project_number,
            statuses=statuses,
            gh_runner=gh_runner,
        )
    else:
        items = [
            item
            for item in board_snapshot.items
            if item.status in statuses
        ]
    ready_count = 0
    backlog_items: list[_ProjectItemSnapshot] = []
    dispatchable = set(dispatchable_repo_prefixes or automation_config.execution_authority_repos)
    active_leases = set(active_lease_issue_refs)

    for item in items:
        issue_ref = _snapshot_to_issue_ref(item, config)
        if issue_ref is None:
            continue
        repo_prefix = parse_issue_ref(issue_ref).prefix
        if repo_prefix not in automation_config.execution_authority_repos:
            continue
        if item.status == "Ready" and item.executor.strip().lower() == target_executor:
            ready_count += 1
        elif item.status in automation_config.admission.source_statuses:
            backlog_items.append(item)

    needed = min(
        max(0, floor - ready_count),
        max(0, cap - ready_count),
        automation_config.admission.max_batch_size,
    )

    provisional_candidates: list[_ProjectItemSnapshot] = []
    skipped: list[AdmissionSkip] = []

    for item in backlog_items:
        issue_ref = _snapshot_to_issue_ref(item, config)
        if issue_ref is None:
            skipped.append(AdmissionSkip(item.issue_ref, "unknown_repo_prefix"))
            continue
        repo_prefix = parse_issue_ref(issue_ref).prefix
        if repo_prefix not in automation_config.execution_authority_repos:
            skipped.append(AdmissionSkip(issue_ref, "not_governed"))
            continue
        if item.status != "Backlog":
            skipped.append(AdmissionSkip(issue_ref, "status_not_backlog"))
            continue
        if repo_prefix not in dispatchable:
            skipped.append(AdmissionSkip(issue_ref, "repo_dispatch_disabled"))
            continue
        if issue_ref in active_leases:
            skipped.append(AdmissionSkip(issue_ref, "already_active_locally"))
            continue
        if not item.priority.strip():
            skipped.append(AdmissionSkip(issue_ref, "missing_priority"))
            continue
        if _priority_rank(item.priority)[0] == 99:
            skipped.append(AdmissionSkip(issue_ref, "invalid_priority"))
            continue
        if not item.sprint.strip():
            skipped.append(AdmissionSkip(issue_ref, "missing_sprint"))
            continue
        if not item.agent.strip():
            skipped.append(AdmissionSkip(issue_ref, "missing_agent"))
            continue
        provisional_candidates.append(item)

    provisional_candidates.sort(
        key=lambda item: _admission_candidate_rank(
            _snapshot_to_issue_ref(item, config) or item.issue_ref,
            priority=item.priority,
            is_graph_member=in_any_critical_path(
                config,
                _snapshot_to_issue_ref(item, config) or item.issue_ref,
            ),
        )
    )

    if needed <= 0:
        return AdmissionDecision(
            ready_count=ready_count,
            ready_floor=floor,
            ready_cap=cap,
            needed=needed,
            scanned_backlog=len(backlog_items),
            skipped=tuple(skipped),
            deep_evaluation_performed=False,
            deep_evaluation_truncated=False,
        )

    memo = github_memo or CycleGitHubMemo()
    eligible: list[AdmissionCandidate] = []
    resolved: list[str] = []
    blocked: list[str] = []
    partial_failure = False
    error: str | None = None
    deep_evaluation_truncated = False
    for item in provisional_candidates:
        if len(eligible) >= needed:
            deep_evaluation_truncated = True
            break
        issue_ref = _snapshot_to_issue_ref(item, config)
        assert issue_ref is not None
        repo_prefix = parse_issue_ref(issue_ref).prefix
        owner, repo, number = _resolve_issue_coordinates(issue_ref, config)
        body = item.body or memoized_query_issue_body(
            memo,
            owner,
            repo,
            number,
            gh_runner=gh_runner,
        )
        if not has_structured_acceptance_criteria(
            body,
            automation_config.admission.acceptance_headings,
        ):
            skipped.append(AdmissionSkip(issue_ref, "missing_acceptance_criteria"))
            continue

        pr_state = _classify_admission_pr_state(
            issue_ref,
            item.issue_number,
            _query_open_prs_by_prefix(
                config,
                (repo_prefix,),
                github_memo=memo,
                gh_runner=gh_runner,
            ).get(repo_prefix, []),
            automation_config,
        )
        if pr_state != "none":
            skipped.append(AdmissionSkip(issue_ref, pr_state))
            continue
        is_graph_member = in_any_critical_path(config, issue_ref)
        if is_graph_member:
            is_ready = memo.dependency_ready.get(issue_ref)
            if is_ready is None:
                val_code, _ = evaluate_ready_promotion(
                    issue_ref=issue_ref,
                    config=config,
                    project_owner=project_owner,
                    project_number=project_number,
                    require_in_graph=True,
                )
                is_ready = val_code == 0
                memo.dependency_ready[issue_ref] = is_ready
            if not is_ready:
                skipped.append(AdmissionSkip(issue_ref, "dependency_unmet"))
                continue
        prior_resolution = _latest_resolution_signal(
            issue_ref,
            owner,
            repo,
            number,
            github_memo=memo,
            gh_runner=gh_runner,
        )
        if prior_resolution is not None:
            try:
                signal_action = _apply_prior_resolution_signal(
                    issue_ref,
                    prior_resolution,
                    config,
                    project_owner,
                    project_number,
                    dry_run=dry_run,
                    gh_runner=gh_runner,
                )
            except GhQueryError as exc:
                partial_failure = True
                error = str(exc)
                break
            if signal_action == "resolved":
                resolved.append(issue_ref)
            else:
                blocked.append(issue_ref)
            skipped.append(AdmissionSkip(issue_ref, "prior_resolution_signal"))
            continue
        eligible.append(
            AdmissionCandidate(
                issue_ref=issue_ref,
                repo_prefix=repo_prefix,
                item_id=item.item_id,
                project_id=item.project_id,
                priority=item.priority,
                title=item.title,
                is_graph_member=is_graph_member,
            )
        )

    selected = eligible[:needed]
    admitted: list[str] = []
    if not dry_run:
        for candidate in selected:
            try:
                _admit_backlog_item(
                    candidate,
                    executor=target_executor,
                    assignment_owner=automation_config.admission.assignment_owner,
                    gh_runner=gh_runner,
                )
            except GhQueryError as exc:
                partial_failure = True
                error = str(exc)
                break
            admitted.append(candidate.issue_ref)

    return AdmissionDecision(
        ready_count=ready_count,
        ready_floor=floor,
        ready_cap=cap,
        needed=needed,
        scanned_backlog=len(backlog_items),
        eligible=tuple(eligible),
        admitted=tuple(admitted),
        skipped=tuple(skipped),
        resolved=tuple(resolved),
        blocked=tuple(blocked),
        partial_failure=partial_failure,
        error=error,
        deep_evaluation_performed=True,
        deep_evaluation_truncated=deep_evaluation_truncated,
    )


def _query_latest_handoff_signal_timestamp(
    owner: str,
    repo: str,
    number: int,
    issue_ref: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return the latest handoff-start or handoff-retry timestamp."""
    return _query_latest_matching_comment_timestamp(
        owner,
        repo,
        number,
        (
            f"{MARKER_PREFIX}:handoff:job=",
            f"{MARKER_PREFIX}:handoff-retry:{issue_ref}:",
        ),
        gh_runner=gh_runner,
    )


def _post_claim_comment(
    issue_ref: str,
    executor: str,
    config: CriticalPathConfig,
    *,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Post deterministic kickoff comment on successful claim."""
    marker = _marker_for("claim-ready", issue_ref)
    owner, repo, number = _resolve_issue_coordinates(issue_ref, config)

    checker = comment_checker or _comment_exists
    if checker(owner, repo, number, marker, gh_runner=gh_runner):
        return

    poster = comment_poster or _post_comment
    executor_label = f"Executor: `{executor}`" if executor else ""
    body = (
        f"{marker}\n"
        f"**Claimed for execution** by `{executor}`.\n\n"
        "Board transition: `Ready -> In Progress`.\n"
        f"{executor_label}".strip()
    )
    poster(owner, repo, number, body, gh_runner=gh_runner)


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
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> SchedulingDecision:
    """Classify and optionally claim Ready issues. Returns SchedulingDecision.

    All Ready items in selected scope are considered. Dependency gating is
    applied only to issues that are members of a critical-path graph.
    """
    if mode not in {"advisory", "claim"}:
        raise ConfigError(
            f"Invalid schedule mode '{mode}'. Use advisory or claim."
        )

    decision = SchedulingDecision()

    # Get current WIP counts (global or per-lane policy)
    wip_counts = _count_wip_by_executor(
        project_owner, project_number, gh_runner=gh_runner
    )
    lane_wip_counts: dict[tuple[str, str], int] = {}
    if automation_config is not None:
        lane_wip_counts = _count_wip_by_executor_lane(
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        )

    # List Ready items
    ready_items = _list_project_items_by_status(
        "Ready", project_owner, project_number, gh_runner=gh_runner
    )
    ready_items = sorted(
        ready_items,
        key=lambda snapshot: _ready_snapshot_rank(snapshot, config),
    )

    for snapshot in ready_items:
        ref = _snapshot_to_issue_ref(snapshot, config)
        if ref is None:
            continue

        # Filter by prefix if not all_prefixes
        if not all_prefixes and this_repo_prefix:
            parsed = parse_issue_ref(ref)
            if parsed.prefix != this_repo_prefix:
                continue

        is_graph_member = in_any_critical_path(config, ref)

        # Check executor field
        executor = snapshot.executor.strip().lower()
        if executor not in VALID_EXECUTORS:
            reason = (
                "missing-executor"
                if not executor
                else f"invalid-executor:{executor}"
            )
            if len(decision.blocked_missing_executor) < missing_executor_block_cap:
                if not dry_run:
                    try:
                        _set_blocked_with_reason(
                            ref,
                            reason,
                            config,
                            project_owner,
                            project_number,
                            board_info_resolver=board_info_resolver,
                            board_mutator=board_mutator,
                            gh_runner=gh_runner,
                        )
                    except GhQueryError:
                        pass
                decision.blocked_missing_executor.append(ref)
            else:
                decision.skipped_missing_executor.append(ref)
            continue

        if not is_graph_member:
            decision.skipped_non_graph.append(ref)
        else:
            # Check predecessors for graph-member issues only
            val_code, _val_output = evaluate_ready_promotion(
                issue_ref=ref,
                config=config,
                project_owner=project_owner,
                project_number=project_number,
                status_resolver=status_resolver,
                require_in_graph=True,
            )

            if val_code != 0:
                # Unmet predecessors -> block
                preds = direct_predecessors(config, ref)
                pred_list = ",".join(sorted(preds))
                reason = f"dependency-unmet:{pred_list}"

                if not dry_run:
                    try:
                        _set_blocked_with_reason(
                            ref,
                            reason,
                            config,
                            project_owner,
                            project_number,
                            board_info_resolver=board_info_resolver,
                            board_mutator=board_mutator,
                            gh_runner=gh_runner,
                        )
                    except GhQueryError:
                        pass

                decision.blocked_invalid_ready.append(ref)
                continue

        # Check WIP limit
        lane = parse_issue_ref(ref).prefix
        if automation_config is not None:
            current_wip = lane_wip_counts.get((executor, lane), 0)
            lane_limit = _wip_limit_for_lane(
                automation_config,
                executor,
                lane,
                fallback=per_executor_wip_limit,
            )
        else:
            current_wip = wip_counts.get(executor, 0)
            lane_limit = per_executor_wip_limit

        if current_wip >= lane_limit:
            decision.deferred_wip.append(ref)
            continue

        if mode == "claim":
            if not dry_run:
                changed, _old = _set_status_if_changed(
                    ref,
                    {"Ready"},
                    "In Progress",
                    config,
                    project_owner,
                    project_number,
                    board_info_resolver=board_info_resolver,
                    board_mutator=board_mutator,
                    gh_runner=gh_runner,
                )
                if not changed:
                    continue

            decision.claimed.append(ref)
        else:
            decision.claimable.append(ref)

        # Reserve a virtual WIP slot so advisory output mirrors claim behavior.
        if automation_config is not None:
            lane_wip_counts[(executor, lane)] = current_wip + 1
        else:
            wip_counts[executor] = current_wip + 1

    return decision


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
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ClaimReadyResult:
    """Claim one Ready issue for a specific executor."""
    norm_executor = executor.strip().lower()
    if norm_executor not in VALID_EXECUTORS:
        return ClaimReadyResult(reason=f"invalid-executor:{executor}")

    ready_items = _list_project_items_by_status(
        "Ready", project_owner, project_number, gh_runner=gh_runner
    )
    wip_counts = _count_wip_by_executor(
        project_owner, project_number, gh_runner=gh_runner
    )
    lane_wip_counts: dict[tuple[str, str], int] = {}
    if automation_config is not None:
        lane_wip_counts = _count_wip_by_executor_lane(
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        )

    ready_by_ref: dict[str, _ProjectItemSnapshot] = {}
    for snapshot in ready_items:
        ref = _snapshot_to_issue_ref(snapshot, config)
        if ref is None:
            continue
        if not all_prefixes and this_repo_prefix:
            parsed = parse_issue_ref(ref)
            if parsed.prefix != this_repo_prefix:
                continue
        ready_by_ref[ref] = snapshot

    candidates: list[str] = []
    if issue_ref:
        candidates = [issue_ref]
    elif next_issue:
        candidates = sorted(
            [
                ref
                for ref, snapshot in ready_by_ref.items()
                if snapshot.executor.strip().lower() == norm_executor
            ],
            key=_issue_sort_key,
        )
    else:
        return ClaimReadyResult(reason="missing-target")

    if not candidates:
        return ClaimReadyResult(reason="no-ready-for-executor")

    for ref in candidates:
        snapshot = ready_by_ref.get(ref)
        if snapshot is None:
            if issue_ref:
                return ClaimReadyResult(reason="issue-not-ready")
            continue

        item_executor = snapshot.executor.strip().lower()
        if item_executor != norm_executor:
            if issue_ref:
                return ClaimReadyResult(
                    reason=f"executor-mismatch:{item_executor or 'unset'}"
                )
            continue

        if in_any_critical_path(config, ref):
            val_code, _val_output = evaluate_ready_promotion(
                issue_ref=ref,
                config=config,
                project_owner=project_owner,
                project_number=project_number,
                status_resolver=status_resolver,
                require_in_graph=True,
            )
            if val_code != 0:
                if issue_ref:
                    return ClaimReadyResult(reason="dependency-unmet")
                continue

        lane = parse_issue_ref(ref).prefix
        if automation_config is not None:
            current_wip = lane_wip_counts.get((norm_executor, lane), 0)
            lane_limit = _wip_limit_for_lane(
                automation_config,
                norm_executor,
                lane,
                fallback=per_executor_wip_limit,
            )
        else:
            current_wip = wip_counts.get(norm_executor, 0)
            lane_limit = per_executor_wip_limit

        if current_wip >= lane_limit:
            return ClaimReadyResult(reason="wip-limit")

        if not dry_run:
            changed, old_status = _set_status_if_changed(
                ref,
                {"Ready"},
                "In Progress",
                config,
                project_owner,
                project_number,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                gh_runner=gh_runner,
            )
            if not changed:
                return ClaimReadyResult(reason=f"status-not-ready:{old_status}")

            try:
                _post_claim_comment(
                    ref,
                    norm_executor,
                    config,
                    comment_checker=comment_checker,
                    comment_poster=comment_poster,
                    gh_runner=gh_runner,
                )
            except GhQueryError:
                # Comment delivery failure should not roll back claim mutation.
                pass

        return ClaimReadyResult(claimed=ref)

    return ClaimReadyResult(reason="no-eligible-ready")


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
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Block Ready issues with unmet predecessors. Returns corrected refs."""
    unmet = find_unmet_ready_dependencies(
        config,
        project_owner,
        project_number,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        status_resolver=status_resolver,
        gh_runner=gh_runner,
    )
    for ref, reason in unmet:
        if not dry_run:
            try:
                _set_blocked_with_reason(
                    ref,
                    reason,
                    config,
                    project_owner,
                    project_number,
                    board_info_resolver=board_info_resolver,
                    board_mutator=board_mutator,
                    gh_runner=gh_runner,
                )
            except GhQueryError:
                pass
    return [ref for ref, _ in unmet]


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
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Escalate stale In Progress issues with no linked PR."""
    now = datetime.now(timezone.utc)
    stale_refs: list[str] = []

    in_progress_items = _list_project_items_by_status(
        "In Progress", project_owner, project_number, gh_runner=gh_runner
    )
    checker = comment_checker or _comment_exists
    poster = comment_poster or _post_comment
    resolve_info = board_info_resolver or _query_issue_board_info

    for snapshot in in_progress_items:
        ref = _snapshot_to_issue_ref(snapshot, config)
        if ref is None:
            continue
        if not all_prefixes and this_repo_prefix:
            parsed = parse_issue_ref(ref)
            if parsed.prefix != this_repo_prefix:
                continue

        pr_field = _query_project_item_field(
            ref,
            "PR",
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        ).strip()
        if pr_field and pr_field.lower() not in {"n/a", "none"}:
            continue

        owner, repo, number = _resolve_issue_coordinates(ref, config)
        updated_at = _query_issue_updated_at(
            owner, repo, number, gh_runner=gh_runner
        )
        age_hours = (now - updated_at).total_seconds() / 3600
        if age_hours < max_age_hours:
            continue

        stale_refs.append(ref)
        if dry_run:
            continue

        marker = _marker_for("stale-in-progress", ref)
        info = resolve_info(ref, config, project_owner, project_number)
        if info.status == "NOT_ON_BOARD":
            continue

        # Escalation signal for orchestrator reassignment
        try:
            _set_single_select_field(
                info.project_id,
                info.item_id,
                "Handoff To",
                "claude",
                gh_runner=gh_runner,
            )
        except GhQueryError:
            pass

        if not checker(owner, repo, number, marker, gh_runner=gh_runner):
            body = (
                f"{marker}\n"
                f"Stale `In Progress` for ~{int(age_hours)}h with no linked PR. "
                "Escalating handoff to `claude` (board field `Handoff To` updated)."
            )
            try:
                poster(owner, repo, number, body, gh_runner=gh_runner)
            except GhQueryError:
                pass

    return stale_refs


# ---------------------------------------------------------------------------
# Subcommand: dispatch-agent
# ---------------------------------------------------------------------------


@dataclass
class DispatchResult:
    dispatched: list[str] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)


def dispatch_agent(
    issue_refs: list[str],
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> DispatchResult:
    """Dispatch eligible In Progress issues according to dispatch target."""
    result = DispatchResult()

    target = automation_config.dispatch_target

    for issue_ref in issue_refs:
        owner, repo, number = _resolve_issue_coordinates(issue_ref, config)
        info = _query_issue_board_info(
            issue_ref,
            config,
            project_owner,
            project_number,
        )
        if info.status != "In Progress":
            result.skipped.append((issue_ref, f"status={info.status}"))
            continue

        executor = _query_project_item_field(
            issue_ref,
            "Executor",
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        ).strip().lower()
        if executor not in VALID_EXECUTORS:
            result.skipped.append((issue_ref, f"executor={executor or 'unset'}"))
            continue

        marker = _marker_for("dispatch-agent", issue_ref)
        if _comment_exists(owner, repo, number, marker, gh_runner=gh_runner):
            result.skipped.append((issue_ref, "already-dispatched"))
            continue

        if dry_run:
            result.dispatched.append(issue_ref)
            continue

        if target == "executor":
            body = (
                f"{marker}\n"
                f"Dispatch acknowledged for `Executor={executor}` lane.\n"
                "Execution is handled by the assigned local agent lane."
            )
            try:
                _post_comment(owner, repo, number, body, gh_runner=gh_runner)
                result.dispatched.append(issue_ref)
            except GhQueryError as error:
                reason_code = "comment-api-error"
                result.failed.append((issue_ref, f"{reason_code}:{error}"))
            continue

        result.failed.append((issue_ref, f"unsupported-dispatch-target:{target}"))

    return result


# ---------------------------------------------------------------------------
# Subcommand: rebalance-wip
# ---------------------------------------------------------------------------


@dataclass
class RebalanceDecision:
    kept: list[str] = field(default_factory=list)
    moved_ready: list[str] = field(default_factory=list)
    moved_blocked: list[str] = field(default_factory=list)
    marked_stale: list[str] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class _WipCandidate:
    ref: str
    owner: str
    repo: str
    number: int
    lane: str
    executor: str
    activity_at: datetime
    has_open_pr: bool


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
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    status_resolver: Callable[..., str] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> RebalanceDecision:
    """Rebalance In Progress lanes with stale demotion and dependency blocking."""
    now = datetime.now(timezone.utc)
    freshness_cutoff = now - timedelta(hours=automation_config.freshness_hours)
    confirm_delay = timedelta(
        minutes=cycle_minutes * max(1, automation_config.stale_confirmation_cycles - 1)
    )
    decision = RebalanceDecision()

    in_progress = _list_project_items_by_status(
        "In Progress",
        project_owner,
        project_number,
        gh_runner=gh_runner,
    )
    lane_buckets: dict[tuple[str, str], list[_WipCandidate]] = {}

    for snapshot in in_progress:
        ref = _snapshot_to_issue_ref(snapshot, config)
        if ref is None:
            continue
        parsed = parse_issue_ref(ref)
        if not all_prefixes and this_repo_prefix and parsed.prefix != this_repo_prefix:
            continue

        owner, repo, number = _resolve_issue_coordinates(ref, config)
        executor = snapshot.executor.strip().lower()
        if executor not in VALID_EXECUTORS:
            decision.skipped.append((ref, "invalid-executor"))
            continue

        if in_any_critical_path(config, ref):
            code, _msg = evaluate_ready_promotion(
                issue_ref=ref,
                config=config,
                project_owner=project_owner,
                project_number=project_number,
                status_resolver=status_resolver,
                require_in_graph=True,
            )
            if code != 0:
                preds = ",".join(sorted(direct_predecessors(config, ref)))
                reason = f"dependency-unmet:{preds}"
                if not dry_run:
                    _set_blocked_with_reason(
                        ref,
                        reason,
                        config,
                        project_owner,
                        project_number,
                        board_info_resolver=board_info_resolver,
                        board_mutator=board_mutator,
                        gh_runner=gh_runner,
                    )
                    info = _query_issue_board_info(
                        ref, config, project_owner, project_number
                    )
                    if info.status != "NOT_ON_BOARD":
                        try:
                            _set_single_select_field(
                                info.project_id,
                                info.item_id,
                                "Handoff To",
                                "claude",
                                gh_runner=gh_runner,
                            )
                        except GhQueryError:
                            pass
                decision.moved_blocked.append(ref)
                continue

        pr_field = _query_project_item_field(
            ref,
            "PR",
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        )
        has_open_pr = False
        parsed_pr = _parse_pr_url(pr_field)
        if parsed_pr is not None:
            pr_owner, pr_repo, pr_number = parsed_pr
            has_open_pr = (
                _query_open_pr_updated_at(
                    pr_owner, pr_repo, pr_number, gh_runner=gh_runner
                )
                is not None
            )
        activity_at = _query_latest_wip_activity_timestamp(
            ref,
            owner,
            repo,
            number,
            pr_field,
            gh_runner=gh_runner,
        )

        is_fresh = activity_at is not None and activity_at >= freshness_cutoff
        if not has_open_pr and not is_fresh:
            stale_prefix = f"<!-- {MARKER_PREFIX}:stale-candidate:{ref}"
            stale_ts = _query_latest_marker_timestamp(
                owner,
                repo,
                number,
                stale_prefix,
                gh_runner=gh_runner,
            )
            if (
                stale_ts is None
                or (activity_at is not None and activity_at > stale_ts)
                or (now - stale_ts) < confirm_delay
            ):
                decision.marked_stale.append(ref)
                if not dry_run and stale_ts is None:
                    stale_marker = (
                        f"<!-- {MARKER_PREFIX}:stale-candidate:{ref}:"
                        f"{now.strftime('%Y-%m-%dT%H:%M:%SZ')} -->"
                    )
                    if not _comment_exists(
                        owner, repo, number, stale_marker, gh_runner=gh_runner
                    ):
                        _post_comment(
                            owner,
                            repo,
                            number,
                            f"{stale_marker}\nStale candidate detected; will demote on next cycle if still inactive.",
                            gh_runner=gh_runner,
                        )
                continue

            # Confirmed stale across at least one full cycle window.
            if not dry_run:
                _set_status_if_changed(
                    ref,
                    {"In Progress"},
                    "Ready",
                    config,
                    project_owner,
                    project_number,
                    board_info_resolver=board_info_resolver,
                    board_mutator=board_mutator,
                    gh_runner=gh_runner,
                )
                demote_marker = _marker_for("stale-demote", ref)
                if not _comment_exists(
                    owner, repo, number, demote_marker, gh_runner=gh_runner
                ):
                    _post_comment(
                        owner,
                        repo,
                        number,
                        (
                            f"{demote_marker}\n"
                            "Moved back to `Ready` after consecutive stale cycles "
                            "with no PR and no fresh activity."
                        ),
                        gh_runner=gh_runner,
                    )
            decision.moved_ready.append(ref)
            continue

        key = (executor, parsed.prefix)
        lane_buckets.setdefault(key, []).append(
            _WipCandidate(
                ref=ref,
                owner=owner,
                repo=repo,
                number=number,
                lane=parsed.prefix,
                executor=executor,
                activity_at=activity_at or datetime.min.replace(
                    tzinfo=timezone.utc
                ),
                has_open_pr=has_open_pr,
            )
        )

    for (executor, lane), candidates in lane_buckets.items():
        limit = _wip_limit_for_lane(
            automation_config, executor, lane, fallback=3
        )
        ranked = sorted(
            candidates,
            key=lambda item: (
                0 if item.has_open_pr else 1,
                -int(item.activity_at.timestamp()),
                _issue_sort_key(item.ref),
            ),
        )
        keep = ranked[:limit]
        overflow = ranked[limit:]
        decision.kept.extend(item.ref for item in keep)
        for item in overflow:
            if not dry_run:
                _set_status_if_changed(
                    item.ref,
                    {"In Progress"},
                    "Ready",
                    config,
                    project_owner,
                    project_number,
                    board_info_resolver=board_info_resolver,
                    board_mutator=board_mutator,
                    gh_runner=gh_runner,
                )
                marker = _marker_for("wip-rebalance", item.ref)
                if not _comment_exists(
                    item.owner, item.repo, item.number, marker, gh_runner=gh_runner
                ):
                    _post_comment(
                        item.owner,
                        item.repo,
                        item.number,
                        f"{marker}\nMoved back to `Ready` by lane WIP rebalance (limit={limit}).",
                        gh_runner=gh_runner,
                    )
            decision.moved_ready.append(item.ref)

    return decision


# ---------------------------------------------------------------------------
# Subcommand: sync-review-state
# ---------------------------------------------------------------------------


def resolve_issues_from_event(
    event_path: str,
    config: CriticalPathConfig,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[tuple[str, str, list[str] | None]]:
    """Parse GITHUB_EVENT_PATH -> list of (issue_ref, event_kind, failed_checks).

    failed_checks is populated for check_suite failure events (names of failed
    check runs queried from the API). None for all other event types.
    """
    try:
        event_data = json.loads(Path(event_path).read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as error:
        raise ConfigError(
            f"Failed reading event file {event_path}: {error}"
        ) from error

    results: list[tuple[str, str, list[str] | None]] = []

    # Determine event type from structure
    if "pull_request" in event_data:
        pr = event_data["pull_request"]
        pr_number = pr.get("number")
        pr_repo = pr.get("base", {}).get("repo", {}).get("full_name", "")
        merged = pr.get("merged", False)
        action = event_data.get("action", "")

        if not pr_number or not pr_repo:
            return results

        if "/" not in pr_repo:
            return results
        owner, repo = pr_repo.split("/", maxsplit=1)

        linked = query_closing_issues(
            owner, repo, pr_number, config, gh_runner=gh_runner
        )

        if action in ("opened", "reopened", "synchronize"):
            event_kind = "pr_open"
        elif action == "ready_for_review":
            event_kind = "pr_ready_for_review"
        elif action == "closed" and merged:
            event_kind = "pr_close_merged"
        elif action == "closed" and not merged:
            return results  # Closed without merge, no state change
        else:
            return results

        for issue in linked:
            results.append((issue.ref, event_kind, None))

    elif "review" in event_data:
        review = event_data["review"]
        review_state = review.get("state", "")
        pr = event_data.get("pull_request", {})
        pr_number = pr.get("number")
        pr_repo = pr.get("base", {}).get("repo", {}).get("full_name", "")

        if not pr_number or not pr_repo:
            return results

        if "/" not in pr_repo:
            return results
        owner, repo = pr_repo.split("/", maxsplit=1)

        linked = query_closing_issues(
            owner, repo, pr_number, config, gh_runner=gh_runner
        )

        if review_state == "changes_requested":
            for issue in linked:
                results.append((issue.ref, "changes_requested", None))
        elif review_state in {"approved", "commented"}:
            for issue in linked:
                results.append((issue.ref, "review_submitted", None))

    elif "check_suite" in event_data:
        check_suite = event_data["check_suite"]
        conclusion = check_suite.get("conclusion", "")
        head_sha = check_suite.get("head_sha", "")
        pull_requests = check_suite.get("pull_requests", [])

        for pr_info in pull_requests:
            pr_number = pr_info.get("number")
            pr_repo_full = (
                pr_info.get("base", {}).get("repo", {}).get("full_name", "")
            )

            if not pr_number or not pr_repo_full:
                continue

            if "/" not in pr_repo_full:
                continue
            owner, repo = pr_repo_full.split("/", maxsplit=1)

            linked = query_closing_issues(
                owner, repo, pr_number, config, gh_runner=gh_runner
            )

            if conclusion == "failure":
                event_kind = "checks_failed"
            elif conclusion == "success":
                event_kind = "checks_passed"
            else:
                continue

            # For failure events, query the actual failed check run names
            failed_names: list[str] | None = None
            if event_kind == "checks_failed" and head_sha:
                failed_names = _query_failed_check_runs(
                    owner, repo, head_sha, gh_runner=gh_runner
                )

            for issue in linked:
                results.append((issue.ref, event_kind, failed_names))

    return results


def resolve_pr_to_issues(
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Resolve PR -> linked issue refs using closingIssuesReferences."""
    if "/" not in pr_repo:
        raise ConfigError(f"pr_repo must be 'owner/repo', got '{pr_repo}'.")
    owner, repo = pr_repo.split("/", maxsplit=1)
    linked = query_closing_issues(
        owner, repo, pr_number, config, gh_runner=gh_runner
    )
    return [issue.ref for issue in linked]


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
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[int, str]:
    """Sync board state based on PR/review/check events.

    State machine:
    1. pr_open -> no board change (waiting for review signal)
    2. pr_ready_for_review -> In Progress -> Review
    3. review_submitted (approved/commented) -> In Progress -> Review
    4. changes_requested -> Review -> In Progress
    5. checks_failed (required checks only) -> Review -> In Progress
    6. pr_close_merged + checks_passed -> {Review, In Progress} -> Done

    For checks events: read required checks from branch protection API.
    If branch protection API fails: exit 4, no mutation.
    """
    noop_mutator = lambda *a: None  # noqa: E731
    governed_single_machine_issue = (
        automation_config is not None
        and automation_config.execution_authority_mode == "single_machine"
        and parse_issue_ref(issue_ref).prefix
        in automation_config.execution_authority_repos
    )

    if event_kind == "pr_open":
        # PR opening does NOT change board status — waiting for a real
        # review signal (review_submitted) before moving to Review.
        return 2, (
            f"{issue_ref}: no board change on PR open — "
            "waiting for review signal"
        )

    elif event_kind == "pr_ready_for_review":
        changed, old = _set_status_if_changed(
            issue_ref,
            {"In Progress"},
            "Review",
            config,
            project_owner,
            project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=noop_mutator if dry_run else board_mutator,
            gh_runner=gh_runner,
        )
        if changed:
            return 0, f"{issue_ref}: {old} -> Review (PR ready for review)"
        return 2, f"{issue_ref}: no change (Status={old})"

    elif event_kind == "review_submitted":
        changed, old = _set_status_if_changed(
            issue_ref,
            {"In Progress"},
            "Review",
            config,
            project_owner,
            project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=noop_mutator if dry_run else board_mutator,
            gh_runner=gh_runner,
        )
        if changed:
            return 0, f"{issue_ref}: {old} -> Review (review submitted)"
        return 2, f"{issue_ref}: no change (Status={old})"

    elif event_kind == "changes_requested":
        if governed_single_machine_issue:
            return 2, (
                f"{issue_ref}: governed local repair transition deferred to consumer"
            )
        changed, old = _set_status_if_changed(
            issue_ref,
            {"Review"},
            "In Progress",
            config,
            project_owner,
            project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=noop_mutator if dry_run else board_mutator,
            gh_runner=gh_runner,
        )
        if changed:
            return 0, f"{issue_ref}: {old} -> In Progress (changes requested)"
        return 2, f"{issue_ref}: no change (Status={old})"

    elif event_kind == "checks_failed":
        if governed_single_machine_issue:
            return 2, (
                f"{issue_ref}: governed local repair transition deferred to consumer"
            )
        # Only transition if a *required* check failed.
        # Read required checks from branch protection API.
        owner, repo, _number = _issue_ref_to_repo_parts(issue_ref, config)
        try:
            bp_output = _run_gh(
                [
                    "api",
                    f"repos/{owner}/{repo}/branches/main/protection/required_status_checks",
                ],
                gh_runner=gh_runner,
            )
        except GhQueryError as error:
            return 4, (
                f"Cannot read branch protection for {owner}/{repo}: {error}"
            )

        # Parse required check names from branch protection response
        try:
            bp_data = json.loads(bp_output)
        except json.JSONDecodeError:
            return 4, (
                f"Cannot parse branch protection response for {owner}/{repo}"
            )

        required_contexts: set[str] = set()
        # GitHub API returns contexts in "contexts" array and/or "checks" array
        for ctx in bp_data.get("contexts", []):
            if isinstance(ctx, str):
                required_contexts.add(ctx)
        for check in bp_data.get("checks", []):
            if isinstance(check, dict) and check.get("context"):
                required_contexts.add(check["context"])

        # Filter: only transition if at least one failed check is required
        actual_failed = set(failed_checks) if failed_checks else set()
        if required_contexts and actual_failed:
            required_failures = actual_failed & required_contexts
            if not required_failures:
                return 2, (
                    f"{issue_ref}: check failures are non-required, "
                    f"no board change (failed: {sorted(actual_failed)})"
                )
        elif required_contexts and not actual_failed:
            # Cannot determine which checks failed — refuse to transition
            # to avoid board regression from non-required failures.
            return 2, (
                f"{issue_ref}: checks_failed but no failed check names "
                f"provided; cannot filter by required checks "
                f"({sorted(required_contexts)}) — no board change"
            )

        changed, old = _set_status_if_changed(
            issue_ref,
            {"Review"},
            "In Progress",
            config,
            project_owner,
            project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=noop_mutator if dry_run else board_mutator,
            gh_runner=gh_runner,
        )
        if changed:
            return 0, f"{issue_ref}: {old} -> In Progress (required check failed)"
        return 2, f"{issue_ref}: no change (Status={old})"

    elif event_kind == "pr_close_merged":
        # Merged PR with passing checks -> Done
        if checks_state and checks_state != "passed":
            return 2, (
                f"{issue_ref}: PR merged but checks_state={checks_state}"
            )

        changed, old = _set_status_if_changed(
            issue_ref,
            {"Review", "In Progress"},
            "Done",
            config,
            project_owner,
            project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=noop_mutator if dry_run else board_mutator,
            gh_runner=gh_runner,
        )
        if changed:
            return 0, f"{issue_ref}: {old} -> Done (PR merged)"
        return 2, f"{issue_ref}: no change (Status={old})"

    elif event_kind == "checks_passed":
        # Checks passed alone doesn't change state unless combined with merge
        return 2, (
            f"{issue_ref}: checks_passed (no state change without merge)"
        )

    else:
        return 2, f"{issue_ref}: unknown event_kind '{event_kind}'"


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
    linked = query_closing_issues(
        *pr_repo.split("/", maxsplit=1),
        pr_number,
        config,
        gh_runner=gh_runner,
    )
    review_refs: list[str] = []
    for issue in linked:
        info = _query_issue_board_info(
            issue.ref, config, project_owner, project_number
        )
        if info.status == "Review":
            review_refs.append(issue.ref)
    return review_refs


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
    """Evaluate strict codex review verdict contract for a PR.

    Returns:
      0 -> explicit codex pass verdict found.
      2 -> missing verdict or fail verdict.
      3/4 -> configuration/API errors.
    """
    linked = query_closing_issues(
        *pr_repo.split("/", maxsplit=1),
        pr_number,
        config,
        gh_runner=gh_runner,
    )
    if not linked:
        return 0, (
            f"{pr_repo}#{pr_number}: codex gate not applicable "
            "(no linked issues)"
        )

    review_refs = _review_scope_refs(
        pr_repo,
        pr_number,
        config,
        project_owner,
        project_number,
        gh_runner=gh_runner,
    )
    if not review_refs:
        refs = [issue.ref for issue in linked]
        return 0, (
            f"{pr_repo}#{pr_number}: codex gate not applicable "
            f"(linked issues not in Review: {refs})"
        )

    verdict = query_latest_codex_verdict(
        pr_repo,
        pr_number,
        trusted_actors=automation_config.trusted_codex_actors,
        gh_runner=gh_runner,
    )
    if verdict is None:
        return 2, (
            f"{pr_repo}#{pr_number}: missing codex verdict marker "
            "(codex-review: pass|fail from trusted actor)"
        )

    if verdict.decision == "pass":
        return 0, (
            f"{pr_repo}#{pr_number}: codex-review=pass "
            f"(source={verdict.source}, actor={verdict.actor})"
        )

    if apply_fail_routing:
        for issue_ref in review_refs:
            _apply_codex_fail_routing(
                issue_ref=issue_ref,
                route=verdict.route,
                checklist=verdict.checklist,
                config=config,
                project_owner=project_owner,
                project_number=project_number,
                dry_run=dry_run,
                gh_runner=gh_runner,
            )

    return 2, (
        f"{pr_repo}#{pr_number}: codex-review=fail "
        f"(route={verdict.route}, source={verdict.source}, actor={verdict.actor})"
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
    from startupai_controller.domain.rescue_policy import configured_review_checks
    return configured_review_checks(pr_repo, automation_config.required_checks_by_repo)


def _build_review_snapshot(
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
) -> ReviewSnapshot:
    """Project PR review state into one explicit snapshot."""
    review_refs = tuple(
        _review_scope_refs(
            pr_repo,
            pr_number,
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        )
    )
    pr_payload = query_pull_request_view_payload(
        pr_repo,
        pr_number,
        gh_runner=gh_runner,
    )
    required_checks = query_required_status_checks(
        pr_repo,
        pr_payload.base_ref_name or "main",
        gh_runner=gh_runner,
    )
    return _build_review_snapshot_from_payload(
        pr_repo=pr_repo,
        pr_number=pr_number,
        review_refs=review_refs,
        pr_payload=pr_payload,
        automation_config=automation_config,
        required_checks=required_checks,
    )


def _codex_gate_from_payload(
    pr_repo: str,
    pr_number: int,
    *,
    review_refs: tuple[str, ...],
    verdict: CodexReviewVerdict | None,
) -> tuple[int, str]:
    """Project codex gate status from one preloaded payload."""
    if not review_refs:
        return 0, (
            f"{pr_repo}#{pr_number}: codex gate not applicable "
            "(linked issues not in Review)"
        )
    if verdict is None:
        return 2, (
            f"{pr_repo}#{pr_number}: missing codex verdict marker "
            "(codex-review: pass|fail from trusted actor)"
        )
    if verdict.decision == "pass":
        return 0, (
            f"{pr_repo}#{pr_number}: codex-review=pass "
            f"(source={verdict.source}, actor={verdict.actor})"
        )
    return 2, (
        f"{pr_repo}#{pr_number}: codex-review=fail "
        f"(route={verdict.route}, source={verdict.source}, actor={verdict.actor})"
    )


def _build_review_snapshot_from_payload(
    *,
    pr_repo: str,
    pr_number: int,
    review_refs: tuple[str, ...],
    pr_payload: PullRequestViewPayload,
    automation_config: BoardAutomationConfig,
    required_checks: set[str],
) -> ReviewSnapshot:
    """Project review state from one preloaded PR payload."""
    copilot_review_present = has_copilot_review_signal_from_payload(pr_payload)
    verdict = latest_codex_verdict_from_payload(
        pr_payload,
        trusted_actors=automation_config.trusted_codex_actors,
    )
    codex_gate_code, codex_gate_message = _codex_gate_from_payload(
        pr_repo,
        pr_number,
        review_refs=review_refs,
        verdict=verdict,
    )
    gate_status = build_pr_gate_status_from_payload(
        pr_payload,
        required=required_checks,
    )

    rescue_checks = tuple(sorted(gate_status.required))
    rescue_passed: set[str] = set()
    rescue_pending: set[str] = set()
    rescue_failed: set[str] = set()
    rescue_cancelled: set[str] = set()
    rescue_missing: set[str] = set()
    for name in rescue_checks:
        observation = gate_status.checks.get(name)
        if observation is None:
            rescue_missing.add(name)
            continue
        if observation.result == "pass":
            rescue_passed.add(name)
        elif observation.result == "cancelled":
            rescue_cancelled.add(name)
        elif observation.result == "fail":
            rescue_failed.add(name)
        else:
            rescue_pending.add(name)

    return ReviewSnapshot(
        pr_repo=pr_repo,
        pr_number=pr_number,
        review_refs=review_refs,
        pr_author=pr_payload.author,
        pr_body=pr_payload.body,
        pr_comment_bodies=tuple(
            str(comment.get("body") or "") for comment in pr_payload.comments
        ),
        copilot_review_present=copilot_review_present,
        codex_verdict=verdict,
        codex_gate_code=codex_gate_code,
        codex_gate_message=codex_gate_message,
        gate_status=gate_status,
        rescue_checks=rescue_checks,
        rescue_passed=rescue_passed,
        rescue_pending=rescue_pending,
        rescue_failed=rescue_failed,
        rescue_cancelled=rescue_cancelled,
        rescue_missing=rescue_missing,
    )


def _rerun_check_observation(
    pr_repo: str,
    observation: CheckObservation,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
    pr_port: _PullRequestPort | None = None,
) -> bool:
    """Rerun a GitHub Actions-backed check when possible."""
    if observation.run_id is None:
        return False
    if dry_run:
        return True
    if pr_port is not None:
        return pr_port.rerun_failed_check(pr_repo, observation.name, observation.run_id)
    rerun_actions_run(pr_repo, observation.run_id, gh_runner=gh_runner)
    return True


def _requeue_local_review_failures(
    pr_repo: str,
    pr_number: int,
    review_refs: tuple[str, ...],
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    pr_author: str | None = None,
    pr_body: str | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, ...]:
    """Return linked review refs to Ready when a local PR needs another coding pass."""
    actor = (pr_author or "").strip().lower()
    body = pr_body or ""
    if not actor and not body:
        pr_output = _run_gh(
            [
                "pr",
                "view",
                str(pr_number),
                "--repo",
                pr_repo,
                "--json",
                "author,body",
            ],
            gh_runner=gh_runner,
        )
        try:
            pr_data = json.loads(pr_output)
        except json.JSONDecodeError as error:
            raise GhQueryError(
                f"Failed parsing PR payload for {pr_repo}#{pr_number}."
            ) from error
        actor = ((pr_data.get("author") or {}).get("login") or "").strip().lower()
        body = str(pr_data.get("body") or "")
    if actor not in automation_config.trusted_local_authors:
        return ()

    provenance = _parse_consumer_provenance(body)
    if provenance is None:
        return ()

    issue_ref = provenance.get("issue_ref", "").strip()
    executor = provenance.get("executor", "").strip().lower()
    if executor not in automation_config.execution_authority_executors:
        return ()
    if issue_ref not in review_refs:
        return ()

    noop_mutator = lambda *a: None  # noqa: E731
    changed, _old_status = _set_status_if_changed(
        issue_ref,
        {"Review"},
        "Ready",
        config,
        project_owner,
        project_number,
        board_mutator=noop_mutator if dry_run else None,
        gh_runner=gh_runner,
    )
    return (issue_ref,) if changed or dry_run else ()


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
) -> ReviewRescueResult:
    """Reconcile one PR in Review back toward self-healing merge flow."""
    if pr_port is None:
        pr_port = _default_pr_port(project_owner, project_number, gh_runner)

    if snapshot is None:
        snapshot = _build_review_snapshot(
            pr_repo=pr_repo,
            pr_number=pr_number,
            config=config,
            automation_config=automation_config,
            project_owner=project_owner,
            project_number=project_number,
            dry_run=dry_run,
            gh_runner=gh_runner,
        )
    # Phase 1: Handle cancelled checks via port
    rerun_checks: list[str] = []
    if snapshot.rescue_cancelled:
        for check_name in sorted(snapshot.rescue_cancelled):
            observation = snapshot.gate_status.checks.get(check_name)
            if observation is None:
                continue
            if _rerun_check_observation(
                pr_repo, observation, dry_run=dry_run, pr_port=pr_port
            ):
                rerun_checks.append(check_name)
        if rerun_checks:
            return ReviewRescueResult(
                pr_repo=pr_repo,
                pr_number=pr_number,
                rerun_checks=tuple(rerun_checks),
            )

    # Phase 2: Domain policy decision (cancelled checks already handled above)
    action, reason = rescue_decision(
        review_refs=tuple(snapshot.review_refs),
        has_cancelled_checks=False,
        pr_state=snapshot.gate_status.state,
        is_draft=snapshot.gate_status.is_draft,
        mergeable=snapshot.gate_status.mergeable,
        copilot_review_present=snapshot.copilot_review_present,
        codex_gate_code=snapshot.codex_gate_code,
        codex_gate_message=snapshot.codex_gate_message,
        required_failed=snapshot.gate_status.failed,
        required_pending=snapshot.gate_status.pending,
        rescue_failed=snapshot.rescue_failed,
        rescue_pending=snapshot.rescue_pending,
        rescue_missing=snapshot.rescue_missing,
        auto_merge_enabled=snapshot.gate_status.auto_merge_enabled,
    )

    # Phase 3: Execute the decision via ports
    if action == "skipped":
        return ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            skipped_reason=reason,
        )

    if action == "blocked":
        return ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            blocked_reason=reason,
        )

    if action in ("requeue_conflicting", "requeue_failed"):
        requeued_refs = _requeue_local_review_failures(
            pr_repo=pr_repo,
            pr_number=pr_number,
            review_refs=snapshot.review_refs,
            config=config,
            automation_config=automation_config,
            project_owner=project_owner,
            project_number=project_number,
            dry_run=dry_run,
            pr_author=snapshot.pr_author,
            pr_body=snapshot.pr_body,
            gh_runner=gh_runner,
        )
        if requeued_refs:
            return ReviewRescueResult(
                pr_repo=pr_repo,
                pr_number=pr_number,
                requeued_refs=requeued_refs,
            )
        return ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            blocked_reason=reason,
        )

    if action == "enable_automerge":
        code, _msg = automerge_review(
            pr_repo=pr_repo,
            pr_number=pr_number,
            config=config,
            automation_config=automation_config,
            project_owner=project_owner,
            project_number=project_number,
            dry_run=dry_run,
            snapshot=snapshot,
            gh_runner=gh_runner,
            pr_port=pr_port,
        )
        return ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            auto_merge_enabled=code == 0,
            blocked_reason=None if code == 0 else "automerge-not-enabled",
        )

    # Unreachable — all rescue_decision actions handled above
    return ReviewRescueResult(
        pr_repo=pr_repo,
        pr_number=pr_number,
        blocked_reason=reason,
    )


def _execution_authority_repo_slugs(
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
) -> tuple[str, ...]:
    """Return full repo slugs governed by execution authority."""
    slugs = {
        repo_slug
        for prefix, repo_slug in config.issue_prefixes.items()
        if prefix in automation_config.execution_authority_repos
    }
    return tuple(sorted(slugs))


def review_rescue_all(
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
    pr_port: _PullRequestPort | None = None,
) -> ReviewRescueSweep:
    """Run review rescue across all governed repos."""
    if pr_port is None:
        pr_port = _default_pr_port(project_owner, project_number, gh_runner)
    repos = _execution_authority_repo_slugs(config, automation_config)
    rerun: list[str] = []
    auto_merge_enabled: list[str] = []
    requeued: list[str] = []
    blocked: list[str] = []
    skipped: list[str] = []
    scanned_prs = 0

    for pr_repo in repos:
        for pr in query_open_pull_requests(pr_repo, gh_runner=gh_runner):
            scanned_prs += 1
            result = review_rescue(
                pr_repo=pr_repo,
                pr_number=pr.number,
                config=config,
                automation_config=automation_config,
                project_owner=project_owner,
                project_number=project_number,
                dry_run=dry_run,
                gh_runner=gh_runner,
                pr_port=pr_port,
            )
            ref = f"{pr_repo}#{pr.number}"
            if result.rerun_checks:
                rerun.append(f"{ref}:{','.join(result.rerun_checks)}")
            elif result.auto_merge_enabled:
                auto_merge_enabled.append(ref)
            elif result.requeued_refs:
                requeued.extend(result.requeued_refs)
            elif result.blocked_reason:
                blocked.append(f"{ref}:{result.blocked_reason}")
            elif result.skipped_reason:
                skipped.append(f"{ref}:{result.skipped_reason}")

    return ReviewRescueSweep(
        scanned_repos=repos,
        scanned_prs=scanned_prs,
        rerun=tuple(rerun),
        auto_merge_enabled=tuple(auto_merge_enabled),
        requeued=tuple(requeued),
        blocked=tuple(blocked),
        skipped=tuple(skipped),
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
) -> tuple[int, str]:
    """Auto-merge PR when codex gate + required checks pass."""
    if pr_port is None:
        pr_port = _default_pr_port(project_owner, project_number, gh_runner)

    if snapshot is not None:
        review_refs = list(snapshot.review_refs)
        copilot_review_present = snapshot.copilot_review_present
        gate_code = snapshot.codex_gate_code
        gate_msg = snapshot.codex_gate_message
        status = snapshot.gate_status
    else:
        linked = query_closing_issues(
            *pr_repo.split("/", maxsplit=1),
            pr_number,
            config,
            gh_runner=gh_runner,
        )
        review_refs = []
        for issue in linked:
            info = _query_issue_board_info(
                issue.ref, config, project_owner, project_number
            )
            if info.status == "Review":
                review_refs.append(issue.ref)
        if not review_refs:
            return 2, (
                f"{pr_repo}#{pr_number}: not in board Review scope; "
                "automerge controller no-op"
            )
        copilot_review_present = _has_copilot_review_signal(
            pr_repo,
            pr_number,
            gh_runner=gh_runner,
        )
        gate_code, gate_msg = codex_review_gate(
            pr_repo=pr_repo,
            pr_number=pr_number,
            config=config,
            automation_config=automation_config,
            project_owner=project_owner,
            project_number=project_number,
            dry_run=dry_run,
            apply_fail_routing=True,
            gh_runner=gh_runner,
        )
        status = pr_port.get_gate_status(pr_repo, pr_number)

    # Domain policy decision
    code, msg, action = automerge_gate_decision(
        pr_repo=pr_repo,
        pr_number=pr_number,
        review_refs=tuple(review_refs),
        copilot_review_present=copilot_review_present,
        codex_gate_code=gate_code,
        codex_gate_message=gate_msg,
        pr_state=status.state,
        is_draft=status.is_draft,
        auto_merge_enabled=status.auto_merge_enabled,
        required_failed=status.failed,
        required_pending=status.pending,
        mergeable=status.mergeable,
        merge_state_status=status.merge_state_status,
    )

    if action in ("no_op", "already_enabled"):
        return code, msg

    # Execute branch update via port
    if action == "update_branch_then_enable" and update_branch:
        if dry_run:
            return code, msg
        pr_port.update_branch(pr_repo, pr_number)

    # Post-update mergeable guard (preserves original fallthrough behavior)
    if status.mergeable not in {"MERGEABLE", "UNKNOWN"}:
        return 2, (
            f"{pr_repo}#{pr_number}: mergeable={status.mergeable}, "
            "cannot auto-merge"
        )

    if dry_run:
        return 0, (
            f"{pr_repo}#{pr_number}: would enable auto-merge "
            "(squash, strict gates)"
        )

    # Execute automerge via port
    merge_status = pr_port.enable_automerge(
        pr_repo,
        pr_number,
        delete_branch=delete_branch,
    )
    if merge_status == "confirmed":
        return 0, f"{pr_repo}#{pr_number}: auto-merge enabled (verified)"
    # merge_status == "pending"
    return 2, f"{pr_repo}#{pr_number}: auto-merge pending verification"



# ---------------------------------------------------------------------------
# Subcommand: enforce-execution-policy
# ---------------------------------------------------------------------------


# ExecutionPolicyDecision — imported from domain.models (M5)


def _parse_consumer_provenance(body: str) -> dict[str, str] | None:
    """Parse consumer provenance marker from PR body."""
    match = re.search(
        rf"<!--\s*{re.escape(MARKER_PREFIX)}:consumer:"
        r"session=(?P<session>[^\s]+)\s+"
        r"issue=(?P<issue>[^\s]+)\s+"
        r"repo=(?P<repo>[^\s]+)\s+"
        r"branch=(?P<branch>[^\s]+)\s+"
        r"executor=(?P<executor>[^\s]+)\s*-->",
        body or "",
    )
    if not match:
        return None
    return {
        "session_id": match.group("session"),
        "issue_ref": match.group("issue"),
        "repo_prefix": match.group("repo"),
        "branch_name": match.group("branch"),
        "executor": match.group("executor"),
    }


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
    if "/" not in pr_repo:
        raise ConfigError(f"pr_repo must be owner/repo, got '{pr_repo}'.")

    decision = ExecutionPolicyDecision()
    owner, repo = pr_repo.split("/", maxsplit=1)

    pr_output = _run_gh(
        [
            "pr",
            "view",
            str(pr_number),
            "--repo",
            pr_repo,
            "--json",
            "author,state,url,body",
        ],
        gh_runner=gh_runner,
    )
    try:
        pr_data = json.loads(pr_output)
    except json.JSONDecodeError as error:
        raise GhQueryError(
            f"Failed parsing PR payload for {pr_repo}#{pr_number}."
        ) from error

    actor = ((pr_data.get("author") or {}).get("login") or "").strip().lower()
    state = str(pr_data.get("state") or "").strip().upper()
    body = str(pr_data.get("body") or "")
    provenance = _parse_consumer_provenance(body)
    legacy_copilot_only_mode = automation_config is None

    if allow_copilot_coding_agent:
        decision.skipped_reason = "policy-disabled"
        return decision
    if automation_config is None:
        if not _is_copilot_coding_agent_actor(actor):
            decision.skipped_reason = f"actor={actor or 'unknown'}"
            return decision
        automation_config = BoardAutomationConfig(
            wip_limits={},
            freshness_hours=24,
            stale_confirmation_cycles=2,
            trusted_codex_actors=set(),
            trusted_local_authors=set(),
            execution_authority_mode="single_machine",
            execution_authority_repos=tuple(sorted(set(config.issue_prefixes))),
            execution_authority_executors=("codex",),
            global_concurrency=1,
            non_local_pr_policy="close",
        )

    noop_mutator = lambda *a: None  # noqa: E731
    linked = query_closing_issues(
        owner, repo, pr_number, config, gh_runner=gh_runner
    )
    protected_linked: list[LinkedIssue] = []
    for issue in linked:
        prefix = parse_issue_ref(issue.ref).prefix
        if prefix not in automation_config.execution_authority_repos:
            continue
        if legacy_copilot_only_mode:
            protected_linked.append(issue)
            continue
        executor = _query_project_item_field(
            issue.ref,
            "Executor",
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        ).strip().lower()
        if executor in automation_config.execution_authority_executors:
            protected_linked.append(issue)

    if not protected_linked:
        decision.skipped_reason = "no-protected-linked-issues"
        return decision

    valid_provenance_issue = provenance.get("issue_ref") if provenance else None
    if (
        provenance is not None
        and actor in automation_config.trusted_local_authors
        and any(issue.ref == valid_provenance_issue for issue in protected_linked)
    ):
        decision.skipped_reason = "valid-local-pr"
        return decision

    policy = automation_config.non_local_pr_policy
    for issue in linked:
        if issue not in protected_linked:
            continue
        changed, old_status = _set_status_if_changed(
            issue.ref,
            {"In Progress", "Review", "Ready"},
            "Blocked" if policy == "block" else "Ready",
            config,
            project_owner,
            project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=noop_mutator if dry_run else board_mutator,
            gh_runner=gh_runner,
        )
        if changed and policy == "block":
            decision.blocked.append(issue.ref)
        elif changed:
            decision.requeued.append(issue.ref)

        assignees = _query_issue_assignees(
            issue.owner, issue.repo, issue.number, gh_runner=gh_runner
        )
        filtered_assignees = [
            login for login in assignees if not _is_copilot_coding_agent_actor(login)
        ]
        if filtered_assignees != assignees:
            if not dry_run:
                _set_issue_assignees(
                    issue.owner,
                    issue.repo,
                    issue.number,
                    filtered_assignees,
                    gh_runner=gh_runner,
                )
            decision.copilot_unassigned.append(issue.ref)

        marker = _marker_for("execution-policy", issue.ref)
        if not _comment_exists(
            issue.owner, issue.repo, issue.number, marker, gh_runner=gh_runner
        ):
            policy_message = (
                "moved this issue to `Blocked`"
                if policy == "block"
                else "re-queued this issue to `Ready`"
            )
            body = (
                f"{marker}\n"
                "Execution policy found a non-local coding PR without valid "
                f"consumer provenance and {policy_message} "
                f"(previous status: `{old_status}`).\n"
                f"PR: {pr_data.get('url')}\n"
                "Consumer provenance is required for protected coding lanes."
            )
            if not dry_run:
                _post_comment(
                    issue.owner,
                    issue.repo,
                    issue.number,
                    body,
                    gh_runner=gh_runner,
                )

    decision.enforced_pr = True
    pr_marker = (
        f"<!-- {MARKER_PREFIX}:execution-policy-pr:{pr_repo}#{pr_number} -->"
    )
    if not _comment_exists(owner, repo, pr_number, pr_marker, gh_runner=gh_runner):
        action_text = (
            "The linked issue has been moved to `Blocked` for operator review."
            if policy == "block"
            else "The linked issue has been re-queued for local execution."
        )
        pr_body = (
            f"{pr_marker}\n"
            "Execution policy rejected this coding PR because it lacks valid local "
            "consumer provenance for a protected execution lane.\n"
            f"{action_text}"
        )
        if not dry_run:
            _post_comment(owner, repo, pr_number, pr_body, gh_runner=gh_runner)

    if state == "OPEN" and policy == "close":
        decision.pr_closed = True
        if not dry_run:
            close_pull_request(
                pr_repo,
                pr_number,
                comment=(
                    "Closed by execution policy: Copilot coding-agent PRs are "
                    "disabled in the strict execution lane."
                ),
                gh_runner=gh_runner,
            )

    return decision


# ---------------------------------------------------------------------------
# CLI Parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Board automation orchestration for GitHub Project boards.",
    )
    parser.add_argument(
        "--file",
        default=DEFAULT_CONFIG_PATH,
        help="Path to critical-paths JSON file",
    )
    parser.add_argument(
        "--automation-config",
        default=DEFAULT_AUTOMATION_CONFIG_PATH,
        help="Path to board-automation policy JSON file",
    )
    parser.add_argument(
        "--project-owner",
        default=DEFAULT_PROJECT_OWNER,
        help="GitHub org/user that owns the Project board",
    )
    parser.add_argument(
        "--project-number",
        type=int,
        default=DEFAULT_PROJECT_NUMBER,
        help="GitHub Project number",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Report what would happen without mutating the board (global)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Subcommand")

    def _add_dry_run_argument(subparser: argparse.ArgumentParser) -> None:
        """Support --dry-run after subcommand as well as global form."""
        subparser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Report what would happen without mutating the board",
        )

    # mark-done
    p_done = subparsers.add_parser(
        "mark-done",
        help="PR merge -> mark linked issues Done",
    )
    p_done.add_argument(
        "--pr-repo",
        required=True,
        help="PR repository (owner/repo)",
    )
    p_done.add_argument(
        "--pr-number",
        type=int,
        required=True,
        help="PR number",
    )
    _add_dry_run_argument(p_done)
    p_done.set_defaults(func=_cmd_mark_done)

    # auto-promote
    p_promote = subparsers.add_parser(
        "auto-promote",
        help="Done issue -> promote eligible successors to Ready",
    )
    p_promote.add_argument(
        "--issue",
        required=True,
        help="Issue ref of the Done issue, e.g. crew#88",
    )
    p_promote.add_argument(
        "--this-repo-prefix",
        required=True,
        help="Prefix for the current repo, e.g. crew",
    )
    _add_dry_run_argument(p_promote)
    p_promote.set_defaults(func=_cmd_auto_promote)

    # admit-backlog
    p_admit = subparsers.add_parser(
        "admit-backlog",
        help="Autonomously fill Ready from governed Backlog items",
    )
    p_admit.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Emit machine-readable JSON output",
    )
    p_admit.add_argument(
        "--issue",
        help="Optional issue ref filter for targeted diagnosis",
    )
    p_admit.add_argument(
        "--limit",
        type=int,
        help="Optional cap for candidate/skip lists in output",
    )
    _add_dry_run_argument(p_admit)
    p_admit.set_defaults(func=_cmd_admit_backlog)

    # propagate-blocker
    p_blocker = subparsers.add_parser(
        "propagate-blocker",
        help="Blocked issue -> post advisory comments on successors",
    )
    blocker_group = p_blocker.add_mutually_exclusive_group(required=True)
    blocker_group.add_argument(
        "--issue",
        help="Issue ref of the Blocked issue",
    )
    blocker_group.add_argument(
        "--sweep-blocked",
        action="store_true",
        default=False,
        help="Sweep all Blocked items",
    )
    p_blocker.add_argument(
        "--this-repo-prefix",
        help="Prefix for the current repo (required in single-issue mode)",
    )
    p_blocker.add_argument(
        "--all-prefixes",
        action="store_true",
        default=False,
        help="Include all repo prefixes in sweep",
    )
    _add_dry_run_argument(p_blocker)
    p_blocker.set_defaults(func=_cmd_propagate_blocker)

    # reconcile-handoffs
    p_handoffs = subparsers.add_parser(
        "reconcile-handoffs",
        help="Retry/escalate stale cross-repo handoffs",
    )
    p_handoffs.add_argument(
        "--ack-timeout-minutes",
        type=int,
        default=30,
        help="Minutes before a handoff is considered stale",
    )
    p_handoffs.add_argument(
        "--max-retries",
        type=int,
        default=1,
        help="Maximum retry attempts before escalation",
    )
    _add_dry_run_argument(p_handoffs)
    p_handoffs.set_defaults(func=_cmd_reconcile_handoffs)

    # schedule-ready
    p_schedule = subparsers.add_parser(
        "schedule-ready",
        help="Classify Ready issues; optional claim mode",
    )
    schedule_scope = p_schedule.add_mutually_exclusive_group(required=True)
    schedule_scope.add_argument(
        "--this-repo-prefix",
        help="Prefix for the current repo",
    )
    schedule_scope.add_argument(
        "--all-prefixes",
        action="store_true",
        default=False,
        help="Include all repo prefixes",
    )
    p_schedule.add_argument(
        "--mode",
        choices=["advisory", "claim"],
        default="advisory",
        help="advisory (default) or claim (mutating)",
    )
    p_schedule.add_argument(
        "--per-executor-wip-limit",
        type=int,
        default=3,
        help="Maximum In Progress items per executor",
    )
    p_schedule.add_argument(
        "--missing-executor-block-cap",
        type=int,
        default=DEFAULT_MISSING_EXECUTOR_BLOCK_CAP,
        help="Max Ready items to auto-block per run for missing/invalid executor",
    )
    _add_dry_run_argument(p_schedule)
    p_schedule.set_defaults(func=_cmd_schedule_ready)

    # claim-ready
    p_claim = subparsers.add_parser(
        "claim-ready",
        help="Explicitly claim one Ready issue into In Progress",
    )
    p_claim.add_argument(
        "--executor",
        required=True,
        choices=sorted(VALID_EXECUTORS),
        help="Executor claiming work",
    )
    claim_target = p_claim.add_mutually_exclusive_group(required=True)
    claim_target.add_argument(
        "--next",
        action="store_true",
        default=False,
        help="Claim next eligible Ready item for executor",
    )
    claim_target.add_argument(
        "--issue",
        help="Claim a specific issue ref (e.g. crew#88)",
    )
    claim_scope = p_claim.add_mutually_exclusive_group(required=False)
    claim_scope.add_argument(
        "--this-repo-prefix",
        help="Limit --next candidate selection to one prefix",
    )
    claim_scope.add_argument(
        "--all-prefixes",
        action="store_true",
        default=False,
        help="Consider all prefixes for --next",
    )
    p_claim.add_argument(
        "--per-executor-wip-limit",
        type=int,
        default=3,
        help="Maximum In Progress items per executor",
    )
    _add_dry_run_argument(p_claim)
    p_claim.set_defaults(func=_cmd_claim_ready)

    # dispatch-agent
    p_dispatch = subparsers.add_parser(
        "dispatch-agent",
        help="Dispatch In Progress issues per configured dispatch target",
    )
    p_dispatch.add_argument(
        "--issue",
        action="append",
        required=True,
        help="Issue ref to dispatch (repeatable)",
    )
    _add_dry_run_argument(p_dispatch)
    p_dispatch.set_defaults(func=_cmd_dispatch_agent)

    # rebalance-wip
    p_rebalance = subparsers.add_parser(
        "rebalance-wip",
        help="Rebalance In Progress lanes with stale demotion and dependency blocking",
    )
    rebalance_scope = p_rebalance.add_mutually_exclusive_group(required=True)
    rebalance_scope.add_argument(
        "--this-repo-prefix",
        help="Prefix for the current repo",
    )
    rebalance_scope.add_argument(
        "--all-prefixes",
        action="store_true",
        default=False,
        help="Include all repo prefixes",
    )
    p_rebalance.add_argument(
        "--cycle-minutes",
        type=int,
        default=DEFAULT_REBALANCE_CYCLE_MINUTES,
        help="Cadence used for stale-cycle confirmation windows",
    )
    _add_dry_run_argument(p_rebalance)
    p_rebalance.set_defaults(func=_cmd_rebalance_wip)

    # enforce-ready-dependencies
    p_enforce = subparsers.add_parser(
        "enforce-ready-dependencies",
        help="Block Ready issues with unmet predecessors",
    )
    enforce_scope = p_enforce.add_mutually_exclusive_group(required=True)
    enforce_scope.add_argument(
        "--this-repo-prefix",
        help="Prefix for the current repo",
    )
    enforce_scope.add_argument(
        "--all-prefixes",
        action="store_true",
        default=False,
        help="Include all repo prefixes",
    )
    _add_dry_run_argument(p_enforce)
    p_enforce.set_defaults(func=_cmd_enforce_ready_deps)

    # audit-in-progress
    p_audit = subparsers.add_parser(
        "audit-in-progress",
        help="Escalate stale In Progress issues with no PR activity",
    )
    audit_scope = p_audit.add_mutually_exclusive_group(required=True)
    audit_scope.add_argument(
        "--this-repo-prefix",
        help="Prefix for the current repo",
    )
    audit_scope.add_argument(
        "--all-prefixes",
        action="store_true",
        default=False,
        help="Include all repo prefixes",
    )
    p_audit.add_argument(
        "--max-age-hours",
        type=int,
        default=24,
        help="Staleness threshold in hours",
    )
    _add_dry_run_argument(p_audit)
    p_audit.set_defaults(func=_cmd_audit_in_progress)

    # sync-review-state
    p_sync = subparsers.add_parser(
        "sync-review-state",
        help="Sync board state with PR/review/check events",
    )
    sync_source = p_sync.add_mutually_exclusive_group(required=True)
    sync_source.add_argument(
        "--from-github-event",
        action="store_true",
        default=False,
        help="Read event from $GITHUB_EVENT_PATH env var",
    )
    sync_source.add_argument(
        "--event-kind",
        choices=[
            "pr_open",
            "pr_ready_for_review",
            "pr_close_merged",
            "changes_requested",
            "checks_failed",
            "checks_passed",
            "review_submitted",
        ],
        help="Event kind to process",
    )
    p_sync.add_argument(
        "--issue",
        help="Issue ref (required with --event-kind unless --resolve-pr is used)",
    )
    p_sync.add_argument(
        "--resolve-pr",
        nargs=2,
        metavar=("PR_REPO", "PR_NUMBER"),
        help="Resolve PR to linked issues: owner/repo number",
    )
    p_sync.add_argument(
        "--checks-state",
        help="Override checks state (passed/failed)",
    )
    p_sync.add_argument(
        "--failed-checks",
        nargs="*",
        default=None,
        help="Names of failed checks (used to filter by required checks)",
    )
    _add_dry_run_argument(p_sync)
    p_sync.set_defaults(func=_cmd_sync_review_state)

    # codex-review-gate
    p_codex_gate = subparsers.add_parser(
        "codex-review-gate",
        help="Enforce codex-review verdict contract on PRs",
    )
    p_codex_gate.add_argument(
        "--pr-repo",
        required=True,
        help="PR repository (owner/repo)",
    )
    p_codex_gate.add_argument(
        "--pr-number",
        type=int,
        required=True,
        help="PR number",
    )
    p_codex_gate.add_argument(
        "--no-fail-routing",
        action="store_true",
        default=False,
        help="Do not route failed verdicts back to In Progress",
    )
    _add_dry_run_argument(p_codex_gate)
    p_codex_gate.set_defaults(func=_cmd_codex_review_gate)

    # automerge-review
    p_automerge = subparsers.add_parser(
        "automerge-review",
        help="Auto-merge PR when codex + required checks are satisfied",
    )
    p_automerge.add_argument(
        "--pr-repo",
        required=True,
        help="PR repository (owner/repo)",
    )
    p_automerge.add_argument(
        "--pr-number",
        type=int,
        required=True,
        help="PR number",
    )
    p_automerge.add_argument(
        "--no-update-branch",
        action="store_true",
        default=False,
        help="Skip branch update when merge state is BEHIND",
    )
    p_automerge.add_argument(
        "--no-delete-branch",
        action="store_true",
        default=False,
        help="Do not delete source branch after merge",
    )
    _add_dry_run_argument(p_automerge)
    p_automerge.set_defaults(func=_cmd_automerge_review)

    # review-rescue
    p_review_rescue = subparsers.add_parser(
        "review-rescue",
        help="Reconcile one PR in Review by rerunning cancelled checks or enabling auto-merge",
    )
    p_review_rescue.add_argument(
        "--pr-repo",
        required=True,
        help="PR repository (owner/repo)",
    )
    p_review_rescue.add_argument(
        "--pr-number",
        type=int,
        required=True,
        help="PR number",
    )
    _add_dry_run_argument(p_review_rescue)
    p_review_rescue.set_defaults(func=_cmd_review_rescue)

    # review-rescue-all
    p_review_rescue_all = subparsers.add_parser(
        "review-rescue-all",
        help="Scan governed repos and reconcile PRs in Review",
    )
    p_review_rescue_all.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Emit machine-readable JSON output",
    )
    _add_dry_run_argument(p_review_rescue_all)
    p_review_rescue_all.set_defaults(func=_cmd_review_rescue_all)

    # enforce-execution-policy
    p_policy = subparsers.add_parser(
        "enforce-execution-policy",
        help="Close Copilot coding-agent PRs and re-queue linked issues",
    )
    p_policy.add_argument(
        "--pr-repo",
        required=True,
        help="PR repository (owner/repo)",
    )
    p_policy.add_argument(
        "--pr-number",
        type=int,
        required=True,
        help="PR number",
    )
    p_policy.add_argument(
        "--allow-copilot-coding-agent",
        action="store_true",
        default=False,
        help="Bypass strict execution-lane policy",
    )
    _add_dry_run_argument(p_policy)
    p_policy.set_defaults(func=_cmd_enforce_execution_policy)

    # classify-parallelism
    p_classify = subparsers.add_parser(
        "classify-parallelism",
        help="Snapshot parallel vs dependency-waiting Ready items",
    )
    _add_dry_run_argument(p_classify)
    p_classify.set_defaults(func=_cmd_classify_parallelism)

    return parser


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------


def _cmd_mark_done(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for mark-done subcommand."""
    if "/" not in args.pr_repo:
        print(
            f"CONFIG ERROR: --pr-repo must be 'owner/repo', got '{args.pr_repo}'",
            file=sys.stderr,
        )
        return 3

    pr_owner, pr_repo = args.pr_repo.split("/", maxsplit=1)

    issues = query_closing_issues(
        pr_owner, pr_repo, args.pr_number, config
    )

    if not issues:
        print("No linked issues found for this PR.")
        return 0

    if args.dry_run:
        refs = [i.ref for i in issues]
        print(f"Would mark Done: {refs}")
        return 0

    marked = mark_issues_done(
        issues, config, args.project_owner, args.project_number
    )

    print(f'DONE_ISSUES={json.dumps(marked)}')

    if not marked:
        print("No issues needed status change.")
        return 0

    return 0


def _cmd_auto_promote(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for auto-promote subcommand."""
    automation_config = load_automation_config(Path(args.automation_config))
    result = auto_promote_successors(
        issue_ref=args.issue,
        config=config,
        this_repo_prefix=args.this_repo_prefix,
        project_owner=args.project_owner,
        project_number=args.project_number,
        automation_config=automation_config,
        dry_run=args.dry_run,
    )

    if result.promoted:
        print(f"Promoted: {result.promoted}")
    if result.skipped:
        for ref, reason in result.skipped:
            print(f"Skipped {ref}: {reason}")
    if result.cross_repo_pending:
        print(f"Cross-repo pending: {result.cross_repo_pending}")
    if result.handoff_jobs:
        print(f"Handoff jobs: {result.handoff_jobs}")

    if not result.promoted and not result.cross_repo_pending:
        return 0

    return 0


def _cmd_admit_backlog(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for autonomous backlog admission."""
    automation_config = load_automation_config(Path(args.automation_config))
    decision = admit_backlog_items(
        config,
        automation_config,
        args.project_owner,
        args.project_number,
        dry_run=args.dry_run,
    )
    payload = admission_summary_payload(
        decision,
        enabled=automation_config.admission.enabled,
    )
    if args.issue:
        payload["top_candidates"] = [
            item
            for item in payload["top_candidates"]
            if item["issue_ref"] == args.issue
        ]
        payload["top_skipped"] = [
            item
            for item in payload["top_skipped"]
            if item["issue_ref"] == args.issue
        ]
    if args.limit is not None:
        payload["top_candidates"] = payload["top_candidates"][: args.limit]
        payload["top_skipped"] = payload["top_skipped"][: args.limit]

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if not decision.partial_failure else 4

    print(
        "Admission ready count: "
        f"{payload['ready_count']} / floor {payload['ready_floor']} (cap {payload['ready_cap']})"
    )
    print(f"Admission needed: {payload['needed']}")
    print(f"Admission admitted: {payload['admitted']}")
    if payload["top_candidates"]:
        print(f"Top candidates: {payload['top_candidates']}")
    if payload["top_skipped"]:
        print(f"Top skipped: {payload['top_skipped']}")
    if payload["error"]:
        print(f"Admission error: {payload['error']}")
    return 0 if not decision.partial_failure else 4


def _cmd_propagate_blocker(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for propagate-blocker subcommand."""
    commented = propagate_blocker(
        issue_ref=getattr(args, "issue", None),
        config=config,
        this_repo_prefix=getattr(args, "this_repo_prefix", None),
        project_owner=args.project_owner,
        project_number=args.project_number,
        sweep_blocked=args.sweep_blocked,
        all_prefixes=args.all_prefixes,
        dry_run=args.dry_run,
    )

    if commented:
        print(f"Commented on: {commented}")
        return 0

    print("No new advisory comments posted.")
    return 0


def _cmd_reconcile_handoffs(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for reconcile-handoffs subcommand."""
    counters = reconcile_handoffs(
        config=config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        ack_timeout_minutes=args.ack_timeout_minutes,
        max_retries=args.max_retries,
        dry_run=args.dry_run,
    )

    print(json.dumps(counters, indent=2))

    if counters["escalated"] > 0:
        return 2  # Escalations indicate unresolved handoffs
    return 0


def _cmd_schedule_ready(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for schedule-ready subcommand."""
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    effective_mode = args.mode
    effective_dry_run = args.dry_run
    if not _workflow_mutations_enabled(automation_config, "ready-scheduler"):
        effective_mode = "advisory"
        effective_dry_run = True

    decision = schedule_ready_items(
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        this_repo_prefix=getattr(args, "this_repo_prefix", None),
        all_prefixes=getattr(args, "all_prefixes", False),
        mode=effective_mode,
        per_executor_wip_limit=args.per_executor_wip_limit,
        missing_executor_block_cap=args.missing_executor_block_cap,
        dry_run=effective_dry_run,
    )

    if decision.claimable:
        print(f"Claimable (advisory): {decision.claimable}")
    if decision.claimed:
        print(f"Claimed: {decision.claimed}")
    print(f"CLAIMED_ISSUES_JSON={json.dumps(sorted(decision.claimed))}")
    if decision.deferred_dependency:
        print(f"Deferred (dependency): {decision.deferred_dependency}")
    if decision.deferred_wip:
        print(f"Deferred (WIP limit): {decision.deferred_wip}")
    if decision.blocked_invalid_ready:
        print(f"Blocked (invalid Ready): {decision.blocked_invalid_ready}")
    if decision.blocked_missing_executor:
        print(
            "Blocked (missing/invalid executor): "
            f"{decision.blocked_missing_executor}"
        )
    if decision.skipped_non_graph:
        print(
            "Considered (non-graph, dependency check skipped): "
            f"{decision.skipped_non_graph}"
        )
    if decision.skipped_missing_executor:
        print(
            "Skipped (missing executor over cap): "
            f"{decision.skipped_missing_executor}"
        )

    if not decision.claimed and not decision.claimable:
        print("No claimable Ready items this run.")
        return 0
    return 0


def _cmd_claim_ready(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for claim-ready subcommand."""
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    result = claim_ready_issue(
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        executor=args.executor,
        issue_ref=getattr(args, "issue", None),
        next_issue=getattr(args, "next", False),
        this_repo_prefix=getattr(args, "this_repo_prefix", None),
        all_prefixes=getattr(args, "all_prefixes", False),
        per_executor_wip_limit=args.per_executor_wip_limit,
        dry_run=args.dry_run,
    )

    if result.claimed:
        if args.dry_run:
            print(f"Would claim: {result.claimed}")
        else:
            print(f"Claimed: {result.claimed}")
        return 0

    print(f"Claim rejected: {result.reason}")
    return 2


def _cmd_dispatch_agent(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for dispatch-agent subcommand."""
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    if not _workflow_mutations_enabled(automation_config, "ready-scheduler"):
        print("Dispatch skipped: local consumer owns execution claims in single_machine mode.")
        return 0

    result = dispatch_agent(
        issue_refs=args.issue,
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        dry_run=args.dry_run,
    )
    if result.dispatched:
        print(f"Dispatched: {sorted(result.dispatched)}")
    if result.skipped:
        for ref, reason in result.skipped:
            print(f"Skipped {ref}: {reason}")
    if result.failed:
        for ref, reason in result.failed:
            print(f"Failed {ref}: {reason}", file=sys.stderr)
        return 4
    return 0


def _cmd_rebalance_wip(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for rebalance-wip subcommand."""
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    effective_dry_run = args.dry_run
    if not _workflow_mutations_enabled(automation_config, "wip-rebalance"):
        effective_dry_run = True

    decision = rebalance_wip(
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        this_repo_prefix=getattr(args, "this_repo_prefix", None),
        all_prefixes=getattr(args, "all_prefixes", False),
        cycle_minutes=args.cycle_minutes,
        dry_run=effective_dry_run,
    )
    if decision.kept:
        print(f"Kept: {sorted(decision.kept)}")
    if decision.marked_stale:
        print(f"Marked stale: {sorted(decision.marked_stale)}")
    if decision.moved_ready:
        print(f"Moved to Ready: {sorted(decision.moved_ready)}")
    if decision.moved_blocked:
        print(f"Moved to Blocked: {sorted(decision.moved_blocked)}")
    if decision.skipped:
        for ref, reason in decision.skipped:
            print(f"Skipped {ref}: {reason}")
    return 0


def _cmd_enforce_ready_deps(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for enforce-ready-dependencies subcommand."""
    corrected = enforce_ready_dependency_guard(
        config=config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        this_repo_prefix=getattr(args, "this_repo_prefix", None),
        all_prefixes=getattr(args, "all_prefixes", False),
        dry_run=args.dry_run,
    )

    if corrected:
        print(f"Corrected: {corrected}")
        return 0

    print("No Ready items have unmet predecessors.")
    return 0


def _cmd_audit_in_progress(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for audit-in-progress subcommand."""
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    effective_dry_run = args.dry_run
    if not _workflow_mutations_enabled(automation_config, "stale-work-guard"):
        effective_dry_run = True

    stale = audit_in_progress(
        config=config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        max_age_hours=args.max_age_hours,
        this_repo_prefix=getattr(args, "this_repo_prefix", None),
        all_prefixes=getattr(args, "all_prefixes", False),
        dry_run=effective_dry_run,
    )

    if stale:
        print(f"Escalated stale In Progress: {stale}")
        return 0

    print("No stale In Progress items found.")
    return 0


def _cmd_sync_review_state(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for sync-review-state subcommand."""
    try:
        automation_config = load_automation_config(
            Path(getattr(args, "automation_config", DEFAULT_AUTOMATION_CONFIG_PATH))
        )
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    if args.from_github_event:
        # Read event path from environment variable
        event_path = os.environ.get("GITHUB_EVENT_PATH", "")
        if not event_path:
            print(
                "CONFIG ERROR: $GITHUB_EVENT_PATH not set",
                file=sys.stderr,
            )
            return 3

        # Parse event file and process all linked issues
        pairs = resolve_issues_from_event(event_path, config)

        if not pairs:
            print("No actionable issue/event pairs found in event.")
            return 0  # No-op success — benign events are not errors

        cli_failed_checks = getattr(args, "failed_checks", None)
        fatal_code = 0
        for issue_ref, event_kind, event_failed_checks in pairs:
            # CLI --failed-checks overrides; else use event-derived names
            effective_failed = cli_failed_checks or event_failed_checks
            code, msg = sync_review_state(
                event_kind=event_kind,
                issue_ref=issue_ref,
                config=config,
                project_owner=args.project_owner,
                project_number=args.project_number,
                automation_config=automation_config,
                checks_state=args.checks_state,
                failed_checks=effective_failed,
                dry_run=args.dry_run,
            )
            print(msg)
            if code in (3, 4) and code > fatal_code:
                fatal_code = code
        return fatal_code

    # Manual mode with --event-kind
    event_kind = args.event_kind

    if args.resolve_pr:
        pr_repo = args.resolve_pr[0]
        try:
            pr_number = int(args.resolve_pr[1])
        except ValueError:
            print(
                "CONFIG ERROR: PR number must be integer, "
                f"got '{args.resolve_pr[1]}'",
                file=sys.stderr,
            )
            return 3

        issue_refs = resolve_pr_to_issues(pr_repo, pr_number, config)
        if not issue_refs:
            print("No linked issues found for this PR.")
            return 0  # No-op success
    elif args.issue:
        issue_refs = [args.issue]
    else:
        print(
            "CONFIG ERROR: --event-kind requires --issue or --resolve-pr",
            file=sys.stderr,
        )
        return 3

    failed_checks = getattr(args, "failed_checks", None)

    # For bridge path: resolve failed check names from PR when not provided
    if event_kind == "checks_failed" and failed_checks is None and args.resolve_pr:
        pr_owner, pr_repo_name = pr_repo.split("/", maxsplit=1)
        head_sha = _query_pr_head_sha(
            pr_owner, pr_repo_name, pr_number
        )
        if head_sha:
            failed_checks = _query_failed_check_runs(
                pr_owner, pr_repo_name, head_sha
            )

    fatal_code = 0
    for issue_ref in issue_refs:
        code, msg = sync_review_state(
            event_kind=event_kind,
            issue_ref=issue_ref,
            config=config,
            project_owner=args.project_owner,
            project_number=args.project_number,
            automation_config=automation_config,
            checks_state=args.checks_state,
            failed_checks=failed_checks,
            dry_run=args.dry_run,
        )
        print(msg)
        if code in (3, 4) and code > fatal_code:
            fatal_code = code
    return fatal_code


def _cmd_codex_review_gate(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for codex-review-gate subcommand."""
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    if "/" not in args.pr_repo:
        print(
            f"CONFIG ERROR: --pr-repo must be 'owner/repo', got '{args.pr_repo}'",
            file=sys.stderr,
        )
        return 3

    code, msg = codex_review_gate(
        pr_repo=args.pr_repo,
        pr_number=args.pr_number,
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        dry_run=args.dry_run,
        apply_fail_routing=not args.no_fail_routing,
    )
    gate_value = "pass"
    if code != 0:
        if "codex-review=fail" in msg:
            gate_value = "fail"
        elif "missing codex verdict" in msg:
            gate_value = "missing"
        else:
            gate_value = "blocked"
    print(f"CODEX_GATE={gate_value}")
    print(msg)
    return code


def _cmd_automerge_review(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for automerge-review subcommand."""
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    if "/" not in args.pr_repo:
        print(
            f"CONFIG ERROR: --pr-repo must be 'owner/repo', got '{args.pr_repo}'",
            file=sys.stderr,
        )
        return 3

    code, msg = automerge_review(
        pr_repo=args.pr_repo,
        pr_number=args.pr_number,
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        dry_run=args.dry_run,
        update_branch=not args.no_update_branch,
        delete_branch=not args.no_delete_branch,
    )
    print(msg)
    return code


def _cmd_review_rescue(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for review-rescue subcommand."""
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    if "/" not in args.pr_repo:
        print(
            f"CONFIG ERROR: --pr-repo must be 'owner/repo', got '{args.pr_repo}'",
            file=sys.stderr,
        )
        return 3

    result = review_rescue(
        pr_repo=args.pr_repo,
        pr_number=args.pr_number,
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        dry_run=args.dry_run,
    )
    ref = f"{args.pr_repo}#{args.pr_number}"
    if result.rerun_checks:
        print(f"{ref}: reran review checks {list(result.rerun_checks)}")
        return 0
    if result.auto_merge_enabled:
        print(f"{ref}: auto-merge enabled")
        return 0
    if result.requeued_refs:
        print(f"{ref}: re-queued for repair {list(result.requeued_refs)}")
        return 0
    if result.skipped_reason is not None:
        print(f"{ref}: {result.skipped_reason}")
        return 0
    print(f"{ref}: {result.blocked_reason}")
    return 0


def _cmd_review_rescue_all(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for review-rescue-all subcommand."""
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    sweep = review_rescue_all(
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        dry_run=args.dry_run,
    )
    if args.json:
        print(
            json.dumps(
                {
                    "scanned_repos": list(sweep.scanned_repos),
                    "scanned_prs": sweep.scanned_prs,
                    "rerun": list(sweep.rerun),
                    "auto_merge_enabled": list(sweep.auto_merge_enabled),
                    "requeued": list(sweep.requeued),
                    "blocked": list(sweep.blocked),
                    "skipped": list(sweep.skipped),
                },
                indent=2,
            )
        )
        return 0
    print(f"Review rescue repos: {list(sweep.scanned_repos)}")
    print(f"Review rescue PRs scanned: {sweep.scanned_prs}")
    if sweep.rerun:
        print(f"Rerun checks: {list(sweep.rerun)}")
    if sweep.auto_merge_enabled:
        print(f"Auto-merge enabled: {list(sweep.auto_merge_enabled)}")
    if sweep.requeued:
        print(f"Re-queued for repair: {list(sweep.requeued)}")
    if sweep.blocked:
        print(f"Blocked: {list(sweep.blocked)}")
    if sweep.skipped:
        print(f"Skipped: {list(sweep.skipped)}")
    return 0


def _cmd_enforce_execution_policy(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for enforce-execution-policy subcommand."""
    try:
        automation_config = load_automation_config(Path(args.automation_config))
    except ConfigError as error:
        print(f"CONFIG_ERROR: {error}", file=sys.stderr)
        return 3

    if "/" not in args.pr_repo:
        print(
            f"CONFIG ERROR: --pr-repo must be 'owner/repo', got '{args.pr_repo}'",
            file=sys.stderr,
        )
        return 3

    decision = enforce_execution_policy(
        pr_repo=args.pr_repo,
        pr_number=args.pr_number,
        config=config,
        automation_config=automation_config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        allow_copilot_coding_agent=args.allow_copilot_coding_agent,
        dry_run=args.dry_run,
    )

    if decision.skipped_reason is not None:
        print(f"Execution policy no-op: {decision.skipped_reason}")
        return 0

    if decision.requeued:
        print(f"Re-queued: {sorted(decision.requeued)}")
    if decision.blocked:
        print(f"Blocked: {sorted(decision.blocked)}")
    if decision.copilot_unassigned:
        print(
            "Removed Copilot assignee: "
            f"{sorted(decision.copilot_unassigned)}"
        )
    if decision.pr_closed:
        print(f"Closed PR: {args.pr_repo}#{args.pr_number}")
    else:
        print(f"Execution policy applied: {args.pr_repo}#{args.pr_number}")

    # Policy intervention is a controlled block, not a workflow error.
    return 2


def _cmd_classify_parallelism(
    args: argparse.Namespace, config: CriticalPathConfig
) -> int:
    """Handler for classify-parallelism subcommand."""
    snapshot = classify_parallelism_snapshot(
        config=config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        dry_run=args.dry_run,
    )

    print(f'PARALLEL_READY={json.dumps(sorted(snapshot["parallel"]))}')
    print(
        "WAITING_DEPENDENCY="
        f'{json.dumps(sorted(snapshot["waiting_on_dependency"]))}'
    )
    print(f'BLOCKED_POLICY={json.dumps(sorted(snapshot["blocked_policy"]))}')

    if snapshot["non_graph"]:
        print(f'NON_GRAPH={json.dumps(sorted(snapshot["non_graph"]))}')

    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 3

    try:
        config = load_config(Path(args.file))
        return args.func(args, config)
    except ConfigError as error:
        print(f"CONFIG ERROR: {error}", file=sys.stderr)
        return 3
    except GhQueryError as error:
        print(f"GH QUERY ERROR: {error}", file=sys.stderr)
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
