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
    load_config,
    parse_issue_ref,
)
from startupai_controller.board_graph import (
    _issue_sort_key,
    _resolve_issue_coordinates,
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
    DEFAULT_CONFIG_PATH,
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
    admission_watermarks,
    has_structured_acceptance_criteria,
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
    _post_claim_comment as _app_post_claim_comment,
    _set_blocked_with_reason as _app_set_blocked_with_reason,
    _transition_issue_status as _app_transition_issue_status,
    _wip_limit_for_lane as _app_wip_limit_for_lane,
    claim_ready_issue as _app_claim_ready_issue,
    schedule_ready_items as _app_schedule_ready_items,
)
from startupai_controller.application.automation.auto_promote import (
    auto_promote_successors as _app_auto_promote_successors,
)
from startupai_controller.application.automation.admit_backlog import (
    admit_backlog_items as _app_admit_backlog_items,
)
from startupai_controller.application.automation.ready_dependencies import (
    enforce_ready_dependency_guard as _app_enforce_ready_dependency_guard,
)
from startupai_controller.application.automation.audit_in_progress import (
    audit_in_progress as _app_audit_in_progress,
)
from startupai_controller.application.automation.dispatch_agent import (
    dispatch_agent as _app_dispatch_agent,
)
from startupai_controller.application.automation.execution_policy import (
    enforce_execution_policy as _app_enforce_execution_policy,
)
from startupai_controller.application.automation.codex_gate import (
    codex_review_gate as _app_codex_review_gate,
)
from startupai_controller.application.automation.rebalance import (
    rebalance_wip as _app_rebalance_wip,
)
from startupai_controller.application.automation.review_rescue import (
    automerge_review as _app_automerge_review,
    review_rescue as _app_review_rescue,
    review_rescue_all as _app_review_rescue_all,
)
from startupai_controller.application.automation.review_sync import (
    sync_review_state as _app_sync_review_state,
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
    admission_summary_payload as _app_admission_summary_payload,
    load_admission_source_items as _app_load_admission_source_items,
    partition_admission_source_items as _app_partition_admission_source_items,
    build_provisional_admission_candidates as _app_build_provisional_admission_candidates,
    evaluate_admission_candidates as _app_evaluate_admission_candidates,
    apply_admitted_backlog_candidates as _app_apply_admitted_backlog_candidates,
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
from startupai_controller.runtime.wiring import (
    build_github_port_bundle,
    GitHubPortBundle,
    GitHubRuntimeMemo as CycleGitHubMemo,
)

# ---------------------------------------------------------------------------
# Port wiring helpers
# ---------------------------------------------------------------------------


PROMOTABLE_STATUSES = frozenset({"Backlog", "Blocked"})


@dataclass(frozen=True)
class BoardInfo:
    """Minimal board identity/status needed for local compatibility helpers."""

    status: str
    item_id: str
    project_id: str


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


def _query_issue_board_info(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    review_state_port: _ReviewStatePort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> BoardInfo:
    """Compatibility helper that resolves board item info through ReviewStatePort."""
    port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    snapshot = next(
        (item for item in port.build_board_snapshot().items if item.issue_ref == issue_ref),
        None,
    )
    if snapshot is None:
        return BoardInfo(status="NOT_ON_BOARD", item_id="", project_id="")
    return BoardInfo(
        status=snapshot.status or "UNKNOWN",
        item_id=snapshot.item_id,
        project_id=snapshot.project_id,
    )


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
    parse_issue_ref(issue_ref)
    if (
        not dry_run
        and controller_owned_resolver is not None
        and controller_owned_resolver(issue_ref)
    ):
        return 2, (
            "REJECTED: controller_owned_admission\n"
            f"{issue_ref} is governed by the local admission controller."
        )

    resolve_info = board_info_resolver or _query_issue_board_info
    info = resolve_info(issue_ref, config, project_owner, project_number)

    if info.status not in PROMOTABLE_STATUSES:
        return 2, (
            f"Current status: {info.status}\n"
            f"REJECTED: {issue_ref} has Status={info.status}; "
            "must be Backlog or Blocked."
        )

    val_code, val_output = evaluate_ready_promotion(
        issue_ref=issue_ref,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        status_resolver=status_resolver,
        require_in_graph=True,
    )
    if val_code != 0:
        lines = [f"Current status: {info.status}"]
        if val_output:
            lines.append(val_output)
        return val_code, "\n".join(lines)

    if dry_run:
        return 0, (
            f"Current status: {info.status}\n"
            "Validator: PASS (all predecessors Done)\n"
            f"Transition: {info.status} -> Ready (would promote)"
        )

    if board_mutator is not None:
        board_mutator(info.project_id, info.item_id)
    else:
        _default_board_mutation_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        ).set_issue_status(issue_ref, "Ready")

    return 0, (
        f"Current status: {info.status}\n"
        "Validator: PASS (all predecessors Done)\n"
        f"Transition: {info.status} -> Ready (promoted)"
    )


def _comment_exists(
    owner: str,
    repo: str,
    number: int,
    marker: str,
    *,
    review_state_port: _ReviewStatePort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Compatibility helper that checks marker presence through ReviewStatePort."""
    port = review_state_port or build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        gh_runner=gh_runner,
    ).review_state
    return port.comment_exists(f"{owner}/{repo}", number, marker)


def list_issue_comment_bodies(
    owner: str,
    repo: str,
    number: int,
    *,
    review_state_port: _ReviewStatePort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, ...]:
    """Compatibility helper that loads issue comment bodies through ReviewStatePort."""
    port = review_state_port or build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        gh_runner=gh_runner,
    ).review_state
    return port.list_issue_comment_bodies(f"{owner}/{repo}", number)


def _list_project_items_by_status(
    status: str,
    project_owner: str,
    project_number: int,
    *,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[_ProjectItemSnapshot]:
    """Compatibility helper that reads board status groups through ReviewStatePort."""
    port = build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        gh_runner=gh_runner,
    ).review_state
    return list(port.build_board_snapshot().items_with_status(status))


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


def _parse_github_timestamp(raw: str | None) -> datetime | None:
    """Parse one GitHub timestamp string into an aware datetime."""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_copilot_coding_agent_actor(login: str) -> bool:
    """Return whether a login belongs to the Copilot coding agent."""
    normalized = login.strip().lower()
    if not normalized:
        return False
    return normalized in {
        "app/copilot-swe-agent",
        "copilot-swe-agent[bot]",
        "copilot",
    }


def _issue_ref_to_repo_parts(
    issue_ref: str,
    config: CriticalPathConfig,
) -> tuple[str, str, int]:
    """Resolve one issue ref into owner/repo/number coordinates."""
    owner, repo, number = _resolve_issue_coordinates(issue_ref, config)
    return owner, repo, number


def _post_comment(
    owner: str,
    repo: str,
    number: int,
    body: str,
    *,
    board_port: _BoardMutationPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Post one issue/PR comment through the board-mutation port."""
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    board_port.post_issue_comment(f"{owner}/{repo}", number, body)


def _query_latest_marker_timestamp(
    owner: str,
    repo: str,
    number: int,
    marker: str,
    *,
    review_state_port: _ReviewStatePort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return the latest timestamp for comments containing one marker."""
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    return review_state_port.latest_matching_comment_timestamp(
        f"{owner}/{repo}",
        number,
        (marker,),
    )


def _query_project_item_field(
    issue_ref: str,
    field_name: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    review_state_port: _ReviewStatePort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Read one project field value through ReviewStatePort."""
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return review_state_port.project_field_value(issue_ref, field_name)


def _set_single_select_field(
    project_id: str,
    item_id: str,
    field_name: str,
    option_name: str,
    *,
    board_port: _BoardMutationPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility helper that writes one single-select project field via BoardMutationPort."""
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    board_port.set_project_single_select(project_id, item_id, field_name, option_name)


def _set_text_field(
    project_id: str,
    item_id: str,
    field_name: str,
    value: str,
    *,
    board_port: _BoardMutationPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility helper that writes one text project field via BoardMutationPort."""
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    board_port.set_project_text_field(project_id, item_id, field_name, value)


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
    if board_port is None and gh_runner is not None:
        from startupai_controller.promote_ready import (
            _query_status_field_option as _legacy_query_status_field_option,
            _set_board_status as _legacy_set_board_status,
        )

        field_id, option_id = _legacy_query_status_field_option(
            project_id,
            status,
            gh_runner=gh_runner,
        )
        _legacy_set_board_status(
            project_id,
            item_id,
            field_id,
            option_id,
            gh_runner=gh_runner,
        )
        return

    _set_single_select_field(
        project_id,
        item_id,
        "Status",
        status,
        board_port=board_port,
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
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
    resolver = board_info_resolver or _query_issue_board_info
    info = resolver(issue_ref, config, project_owner, project_number)
    current_status = info.status
    if current_status not in from_statuses:
        return False, current_status
    if not dry_run:
        if board_mutator is not None:
            board_mutator(info.project_id, info.item_id, to_status)
        elif board_port is not None:
            board_port.set_issue_status(issue_ref, to_status)
        else:
            _set_board_status(
                info.project_id,
                info.item_id,
                to_status,
                project_owner=project_owner,
                project_number=project_number,
                config=config,
                gh_runner=gh_runner,
            )
    return True, current_status


def _list_project_items(
    project_owner: str,
    project_number: int,
    *,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
    board_snapshot: CycleBoardSnapshot | None = None,
) -> list[_ProjectItemSnapshot]:
    """Return the full board snapshot items through ReviewStatePort."""
    if board_snapshot is not None:
        return list(board_snapshot.items)
    review_state_port = _default_review_state_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    return list(review_state_port.build_board_snapshot().items)


def _query_issue_updated_at(
    owner: str,
    repo: str,
    number: int,
    *,
    review_state_port: _ReviewStatePort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return one issue updated timestamp through ReviewStatePort."""
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    return review_state_port.issue_updated_at(f"{owner}/{repo}", number)


def _query_open_pr_updated_at(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return one open PR updated timestamp through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return _parse_github_timestamp(
        pr_port.pull_request_updated_at(f"{owner}/{repo}", pr_number)
    )


def _query_latest_wip_activity_timestamp(
    issue_ref: str,
    owner: str,
    repo: str,
    number: int,
    pr_field: str,
    *,
    review_state_port: _ReviewStatePort | None = None,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return the latest meaningful activity timestamp for one WIP issue."""
    issue_updated = _query_issue_updated_at(
        owner,
        repo,
        number,
        review_state_port=review_state_port,
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
    )
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    latest_comment = review_state_port.latest_non_automation_comment_timestamp(
        f"{owner}/{repo}",
        number,
    )
    latest_pr = None
    parsed_pr = _parse_pr_url(pr_field)
    if parsed_pr is not None:
        pr_owner, pr_repo, pr_number = parsed_pr
        latest_pr = _query_open_pr_updated_at(
            pr_owner,
            pr_repo,
            pr_number,
            pr_port=pr_port,
            project_owner=project_owner,
            project_number=project_number,
            config=config,
            gh_runner=gh_runner,
        )
    values = [ts for ts in (issue_updated, latest_comment, latest_pr) if ts is not None]
    return max(values) if values else None


def _is_pr_open(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Return whether the PR is currently open."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return pr_port.is_pull_request_open(f"{owner}/{repo}", pr_number)


def _query_issue_assignees(
    owner: str,
    repo: str,
    number: int,
    *,
    review_state_port: _ReviewStatePort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, ...]:
    """Return issue assignees through ReviewStatePort."""
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    return review_state_port.issue_assignees(f"{owner}/{repo}", number)


def _set_issue_assignees(
    owner: str,
    repo: str,
    number: int,
    assignees: tuple[str, ...] | list[str],
    *,
    board_port: _BoardMutationPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set issue assignees through BoardMutationPort."""
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    board_port.set_issue_assignees(f"{owner}/{repo}", number, tuple(assignees))


def query_closing_issues(
    owner: str,
    repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    gh_runner: Callable[..., str] | None = None,
) -> list[LinkedIssue]:
    """Resolve linked issues for one PR through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    linked: list[LinkedIssue] = []
    for issue_ref in pr_port.linked_issue_refs(f"{owner}/{repo}", pr_number):
        issue_owner, issue_repo, issue_number = _issue_ref_to_repo_parts(issue_ref, config)
        linked.append(
            LinkedIssue(
                owner=issue_owner,
                repo=issue_repo,
                number=issue_number,
                ref=issue_ref,
            )
        )
    return linked


def query_open_pull_requests(
    repo_prefix: str,
    config: CriticalPathConfig,
    *,
    github_memo: CycleGitHubMemo | None = None,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    gh_runner: Callable[..., str] | None = None,
) -> list[OpenPullRequest]:
    """List open pull requests for one repo prefix through PullRequestPort."""
    repo_slug = config.issue_prefixes[repo_prefix]
    pr_port = pr_port or build_github_port_bundle(
        project_owner,
        project_number,
        config=config,
        github_memo=github_memo,
        gh_runner=gh_runner,
    ).pull_requests
    return pr_port.list_open_prs(repo_slug)


def query_required_status_checks(
    pr_repo: str,
    base_ref_name: str = "main",
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> set[str]:
    """Return required status checks through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return pr_port.required_status_checks(pr_repo, base_ref_name)


def query_latest_codex_verdict(
    pr_repo: str,
    pr_number: int,
    *,
    trusted_actors: set[str],
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> object | None:
    """Return the latest trusted codex verdict through ReviewSnapshot reads."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    snapshots = pr_port.review_snapshots(
        {(pr_repo, pr_number): ()},
        trusted_codex_actors=frozenset(trusted_actors),
    )
    snapshot = snapshots.get((pr_repo, pr_number))
    return None if snapshot is None else snapshot.codex_verdict


def _query_failed_check_runs(
    owner: str,
    repo: str,
    head_sha: str,
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str] | None:
    """Return failed check runs through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    result = pr_port.failed_check_runs(f"{owner}/{repo}", head_sha)
    return None if result is None else list(result)


def _query_pr_head_sha(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> str | None:
    """Return the PR head SHA through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return pr_port.pull_request_head_sha(f"{owner}/{repo}", pr_number)


def close_issue(
    owner: str,
    repo: str,
    number: int,
    *,
    board_port: _BoardMutationPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Close an issue through BoardMutationPort."""
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        gh_runner=gh_runner,
    )
    board_port.close_issue(f"{owner}/{repo}", number)


def close_pull_request(
    pr_repo: str,
    pr_number: int,
    *,
    comment: str | None = None,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Close a pull request through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    pr_port.close_pull_request(pr_repo, pr_number, comment=comment)


def memoized_query_issue_body(
    memo: CycleGitHubMemo,
    owner: str,
    repo: str,
    number: int,
    *,
    issue_context_port: _IssueContextPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Return one issue body through IssueContextPort with cycle-local memoization."""
    key = (owner, repo, number)
    cached = memo.issue_bodies.get(key)
    if cached is not None:
        return cached
    issue_context_port = issue_context_port or _default_issue_context_port(
        project_owner,
        project_number,
        config or load_config(Path(DEFAULT_CONFIG_PATH)),
        github_memo=memo,
        gh_runner=gh_runner,
    )
    body = issue_context_port.get_issue_context(owner, repo, number).body
    memo.issue_bodies[key] = body
    return body


def rerun_actions_run(
    pr_repo: str,
    run_id: int,
    *,
    pr_port: _PullRequestPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    config: CriticalPathConfig | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Re-run one failed check run through PullRequestPort."""
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    pr_port.rerun_failed_check(pr_repo, "", run_id)


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
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set Status=Blocked and Blocked Reason on a board item."""
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    status_mutator = board_mutator
    if status_mutator is None and board_info_resolver is not None:
        status_mutator = _legacy_board_status_mutator(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    _app_set_blocked_with_reason(
        issue_ref,
        reason,
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=status_mutator,
        gh_runner=gh_runner,
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
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    status_mutator = board_mutator
    if status_mutator is None and board_info_resolver is not None:
        status_mutator = _legacy_board_status_mutator(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    return _app_transition_issue_status(
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
        board_mutator=status_mutator,
        gh_runner=gh_runner,
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

    def mutate(project_id: str, item_id: str, status: str = "Blocked") -> None:
        _set_board_status(
            project_id,
            item_id,
            status,
            project_owner=project_owner,
            project_number=project_number,
            config=config,
            gh_runner=gh_runner,
        )

    return mutate


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
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return pr_port.has_copilot_review_signal(
        pr_repo,
        pr_number,
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
    marked: list[str] = []

    for issue in issues:
        changed, old_status = _transition_issue_status(
            issue.ref,
            {"Review", "In Progress", "Blocked", "Ready", "Backlog"},
            "Done",
            config,
            project_owner,
            project_number,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        if changed:
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
    check_comment = comment_checker or _comment_exists

    def _post_bridge_comment(
        owner: str,
        repo: str,
        number: int,
        body: str,
        *,
        gh_runner: Callable[..., str] | None = None,
    ) -> None:
        if comment_poster is not None:
            comment_poster(owner, repo, number, body)
            return
        target_board_port = board_port or _default_board_mutation_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
        target_board_port.post_issue_comment(f"{owner}/{repo}", number, body)

    return _app_auto_promote_successors(
        issue_ref=issue_ref,
        config=config,
        this_repo_prefix=this_repo_prefix,
        project_owner=project_owner,
        project_number=project_number,
        automation_config=automation_config,
        dry_run=dry_run,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        promote_to_ready=promote_to_ready,
        controller_owned_resolver=lambda ref: _controller_owned_admission(
            ref,
            automation_config,
        ),
        comment_exists=check_comment,
        post_cross_repo_comment=_post_bridge_comment,
        resolve_issue_parts=_issue_ref_to_repo_parts,
        new_handoff_job_id=_new_handoff_job_id,
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
    return _AdmissionPipelineDeps(
        query_open_pull_requests=query_open_pull_requests,
        list_issue_comment_bodies=list_issue_comment_bodies,
        memoized_query_issue_body=memoized_query_issue_body,
        mark_issues_done=mark_issues_done,
        close_issue=close_issue,
        set_blocked_with_reason=_set_blocked_with_reason,
        set_handoff_target=_set_handoff_target,
        default_board_mutation_port=_default_board_mutation_port,
        set_single_select_field=_set_single_select_field,
        set_text_field=_set_text_field,
        list_project_items=_list_project_items,
    )


def admission_summary_payload(
    decision: AdmissionDecision,
    *,
    enabled: bool,
) -> dict[str, object]:
    """Convert an AdmissionDecision into a JSON-friendly payload."""
    return _app_admission_summary_payload(decision, enabled=enabled)


def _load_admission_source_items(
    automation_config: BoardAutomationConfig,
    *,
    review_state_port: _ReviewStatePort | None,
    board_snapshot: CycleBoardSnapshot | None,
    gh_runner: Callable[..., str] | None,
) -> list[_ProjectItemSnapshot]:
    """Load backlog/ready items needed for one admission pass."""
    return _app_load_admission_source_items(
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
    return _app_partition_admission_source_items(
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
    return _app_build_provisional_admission_candidates(
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
    return _app_evaluate_admission_candidates(
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
    return _app_apply_admitted_backlog_candidates(
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
    if github_bundle is not None:
        review_state_port = github_bundle.review_state
        pr_port = github_bundle.pull_requests
        board_port = github_bundle.board_mutations
        memo = github_bundle.github_memo
    else:
        review_state_port = None
        pr_port = None
        board_port = None
        memo = github_memo or CycleGitHubMemo()

    return _app_admit_backlog_items(
        config=config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        dispatchable_repo_prefixes=dispatchable_repo_prefixes,
        active_lease_issue_refs=active_lease_issue_refs,
        dry_run=dry_run,
        board_snapshot=board_snapshot,
        review_state_port=review_state_port,
        pr_port=pr_port,
        board_port=board_port,
        memo=memo,
        gh_runner=gh_runner,
        protected_queue_executor_target=_protected_queue_executor_target,
        load_admission_source_items=_load_admission_source_items,
        partition_admission_source_items=_partition_admission_source_items,
        build_provisional_candidates=_build_provisional_admission_candidates,
        evaluate_candidates=_evaluate_admission_candidates,
        apply_candidates=_apply_admitted_backlog_candidates,
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
    review_state_port = _default_review_state_port(
        DEFAULT_PROJECT_OWNER,
        DEFAULT_PROJECT_NUMBER,
        config,
        gh_runner=gh_runner,
    )
    board_port = board_port or _default_board_mutation_port(
        DEFAULT_PROJECT_OWNER,
        DEFAULT_PROJECT_NUMBER,
        config,
        gh_runner=gh_runner,
    )
    _app_post_claim_comment(
        issue_ref,
        executor,
        config,
        review_state_port=review_state_port,
        board_port=board_port,
        comment_checker=comment_checker or _comment_exists,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
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
    """Classify and optionally claim Ready issues. Returns SchedulingDecision.

    All Ready items in selected scope are considered. Dependency gating is
    applied only to issues that are members of a critical-path graph.
    """
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    status_mutator = board_mutator
    if status_mutator is None and board_info_resolver is not None:
        status_mutator = _legacy_board_status_mutator(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    return _app_schedule_ready_items(
        config,
        project_owner,
        project_number,
        review_state_port=review_state_port,
        board_port=board_port,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        mode=mode,
        per_executor_wip_limit=per_executor_wip_limit,
        automation_config=automation_config,
        missing_executor_block_cap=missing_executor_block_cap,
        dry_run=dry_run,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=status_mutator,
        gh_runner=gh_runner,
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
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    status_mutator = board_mutator
    if status_mutator is None and board_info_resolver is not None:
        status_mutator = _legacy_board_status_mutator(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    return _app_claim_ready_issue(
        config,
        project_owner,
        project_number,
        review_state_port=review_state_port,
        board_port=board_port,
        executor=executor,
        issue_ref=issue_ref,
        next_issue=next_issue,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        per_executor_wip_limit=per_executor_wip_limit,
        automation_config=automation_config,
        dry_run=dry_run,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=status_mutator,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
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
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return _app_enforce_ready_dependency_guard(
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        ready_items=review_state_port.list_issues_by_status("Ready"),
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        dry_run=dry_run,
        status_resolver=status_resolver,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        find_unmet_dependencies=find_unmet_ready_dependencies,
        set_blocked_with_reason=_set_blocked_with_reason,
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
    use_ports = (
        board_info_resolver is None
    ) or review_state_port is not None or board_port is not None
    if use_ports:
        review_state_port = review_state_port or _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
        board_port = board_port or _default_board_mutation_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
        in_progress_items = review_state_port.list_issues_by_status("In Progress")
    else:
        in_progress_items = _list_project_items_by_status(
            "In Progress", project_owner, project_number, gh_runner=gh_runner
        )
    checker = comment_checker or _comment_exists
    resolve_info = board_info_resolver or _query_issue_board_info
    info_cache: dict[str, BoardInfo] = {}

    def _resolve_status(ref: str) -> str:
        if use_ports:
            return review_state_port.get_issue_status(ref)
        info = info_cache.get(ref)
        if info is None:
            info = resolve_info(ref, config, project_owner, project_number)
            info_cache[ref] = info
        return info.status

    def _set_handoff(ref: str) -> None:
        info = info_cache.get(ref)
        if info is None:
            info = resolve_info(ref, config, project_owner, project_number)
            info_cache[ref] = info
        _set_single_select_field(
            info.project_id,
            info.item_id,
            "Handoff To",
            "claude",
            project_owner=project_owner,
            project_number=project_number,
            config=config,
            gh_runner=gh_runner,
        )

    def _post_audit_comment(
        owner: str,
        repo: str,
        number: int,
        body: str,
        *,
        gh_runner: Callable[..., str] | None = None,
    ) -> None:
        if comment_poster is not None:
            comment_poster(owner, repo, number, body, gh_runner=gh_runner)
            return
        _post_comment(
            owner,
            repo,
            number,
            body,
            board_port=board_port,
            project_owner=project_owner,
            project_number=project_number,
            config=config,
            gh_runner=gh_runner,
        )

    return _app_audit_in_progress(
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        in_progress_items=in_progress_items,
        use_ports=use_ports,
        review_state_port=review_state_port,
        board_port=board_port,
        max_age_hours=max_age_hours,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        dry_run=dry_run,
        gh_runner=gh_runner,
        resolve_issue_status=_resolve_status,
        query_project_item_field=_query_project_item_field,
        query_issue_updated_at=_query_issue_updated_at,
        comment_exists=checker,
        post_comment=_post_audit_comment,
        set_handoff_target=_set_handoff,
        snapshot_to_issue_ref=_snapshot_to_issue_ref,
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
    target = automation_config.dispatch_target
    use_ports = (board_info_resolver is None) or review_state_port is not None or board_port is not None
    if use_ports:
        review_state_port = review_state_port or _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
        board_port = board_port or _default_board_mutation_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    resolve_info = board_info_resolver or _query_issue_board_info

    def _resolve_status(issue_ref: str) -> str | None:
        if use_ports:
            return review_state_port.get_issue_status(issue_ref)
        return resolve_info(
            issue_ref,
            config,
            project_owner,
            project_number,
        ).status

    def _resolve_executor(issue_ref: str) -> str:
        if use_ports:
            return review_state_port.get_issue_fields(issue_ref).executor
        return _query_project_item_field(
            issue_ref,
            "Executor",
            config,
            project_owner,
            project_number,
            gh_runner=gh_runner,
        )

    def _post_dispatch_comment(
        owner: str,
        repo: str,
        number: int,
        body: str,
        *,
        gh_runner: Callable[..., str] | None = None,
    ) -> None:
        if use_ports:
            board_port.post_issue_comment(f"{owner}/{repo}", number, body)
            return
        _post_comment(
            owner,
            repo,
            number,
            body,
            project_owner=project_owner,
            project_number=project_number,
            config=config,
            gh_runner=gh_runner,
        )

    return _app_dispatch_agent(
        issue_refs=issue_refs,
        config=config,
        dispatch_target=target,
        dry_run=dry_run,
        gh_runner=gh_runner,
        resolve_issue_status=_resolve_status,
        resolve_executor=_resolve_executor,
        comment_exists=_comment_exists,
        post_comment=_post_dispatch_comment,
    )


# ---------------------------------------------------------------------------
# Subcommand: rebalance-wip
# ---------------------------------------------------------------------------


def _load_rebalance_in_progress_items(
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    review_state_port: _ReviewStatePort | None,
    board_port: _BoardMutationPort | None,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[bool, _ReviewStatePort | None, _BoardMutationPort | None, list[IssueSnapshot]]:
    """Load the current In Progress set for WIP rebalance."""
    use_ports = (
        board_info_resolver is None and board_mutator is None
    ) or review_state_port is not None or board_port is not None
    if use_ports:
        review_state_port = review_state_port or _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
        board_port = board_port or _default_board_mutation_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
        in_progress = review_state_port.list_issues_by_status("In Progress")
    else:
        in_progress = _list_project_items_by_status(
            "In Progress",
            project_owner,
            project_number,
            gh_runner=gh_runner,
        )
    return use_ports, review_state_port, board_port, in_progress


def _ensure_rebalance_board_port(
    board_port: _BoardMutationPort | None,
    *,
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    gh_runner: Callable[..., str] | None,
) -> _BoardMutationPort:
    """Materialize a board mutation port when the rebalance flow needs one."""
    return board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )


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
    use_ports, review_state_port, board_port, in_progress = (
        _load_rebalance_in_progress_items(
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
    )
    return _app_rebalance_wip(
        config=config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        in_progress_items=in_progress,
        use_ports=use_ports,
        review_state_port=review_state_port,
        board_port=board_port,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        cycle_minutes=cycle_minutes,
        dry_run=dry_run,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        status_resolver=status_resolver,
        gh_runner=gh_runner,
        is_graph_member=in_any_critical_path,
        ready_promotion_evaluator=evaluate_ready_promotion,
        set_blocked_with_reason=_set_blocked_with_reason,
        set_handoff_target=_set_handoff_target,
        query_project_item_field=_query_project_item_field,
        query_open_pr_updated_at=_query_open_pr_updated_at,
        query_latest_wip_activity_timestamp=_query_latest_wip_activity_timestamp,
        query_latest_marker_timestamp=_query_latest_marker_timestamp,
        comment_exists=_comment_exists,
        ensure_board_port=lambda current_board_port: _ensure_rebalance_board_port(
            current_board_port,
            project_owner=project_owner,
            project_number=project_number,
            config=config,
            gh_runner=gh_runner,
        ),
        transition_issue_status=_transition_issue_status,
        snapshot_to_issue_ref=_snapshot_to_issue_ref,
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
    if github_bundle is not None:
        pr_port = pr_port or github_bundle.pull_requests
        review_state_port = review_state_port or github_bundle.review_state
        board_port = board_port or github_bundle.board_mutations
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    status_mutator = board_mutator
    if status_mutator is None and board_info_resolver is not None:
        status_mutator = _legacy_board_status_mutator(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )

    return _app_sync_review_state(
        event_kind=event_kind,
        issue_ref=issue_ref,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        automation_config=automation_config,
        pr_state=pr_state,
        review_state=review_state,
        checks_state=checks_state,
        failed_checks=failed_checks,
        dry_run=dry_run,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=status_mutator,
        gh_runner=gh_runner,
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
    review_refs = _review_scope_refs(
        pr_repo,
        pr_number,
        config,
        project_owner,
        project_number,
        gh_runner=gh_runner,
    )
    verdict = query_latest_codex_verdict(
        pr_repo,
        pr_number,
        trusted_actors=automation_config.trusted_codex_actors,
        gh_runner=gh_runner,
    )

    def _fail_router(**kwargs) -> None:
        _apply_codex_fail_routing(
            issue_ref=kwargs["issue_ref"],
            route=kwargs["route"],
            checklist=kwargs["checklist"],
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            dry_run=kwargs["dry_run"],
            gh_runner=gh_runner,
        )

    return _app_codex_review_gate(
        pr_repo=pr_repo,
        pr_number=pr_number,
        linked_refs=[issue.ref for issue in linked],
        review_refs=review_refs,
        verdict=verdict,
        dry_run=dry_run,
        apply_fail_routing=apply_fail_routing,
        fail_router=_fail_router,
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
    pr_port: _PullRequestPort | None = None,
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
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return pr_port.review_snapshots(
        {(pr_repo, pr_number): review_refs},
        trusted_codex_actors=frozenset(automation_config.trusted_codex_actors),
    )[(pr_repo, pr_number)]


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
    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner,
    )
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config=config,
        gh_runner=gh_runner,
    )
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config,
        gh_runner,
    )
    snapshot = snapshot or _build_review_snapshot(
        pr_repo=pr_repo,
        pr_number=pr_number,
        config=config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        dry_run=dry_run,
        pr_port=pr_port,
        gh_runner=gh_runner,
    )

    def _automerge_runner(**kwargs) -> tuple[int, str]:
        return automerge_review(
            pr_repo=kwargs["pr_repo"],
            pr_number=kwargs["pr_number"],
            config=config,
            automation_config=automation_config,
            project_owner=project_owner,
            project_number=project_number,
            dry_run=kwargs["dry_run"],
            snapshot=kwargs["snapshot"],
            gh_runner=gh_runner,
            pr_port=kwargs["pr_port"],
            review_state_port=kwargs["review_state_port"],
        )

    return _app_review_rescue(
        pr_repo=pr_repo,
        pr_number=pr_number,
        snapshot=snapshot,
        automation_config=automation_config,
        dry_run=dry_run,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        automerge_runner=_automerge_runner,
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
    pr_port = pr_port or _default_pr_port(project_owner, project_number, config, gh_runner)
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner,
    )
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config,
        gh_runner,
    )

    def _review_rescue_runner(**kwargs) -> ReviewRescueResult:
        return review_rescue(
            pr_repo=kwargs["pr_repo"],
            pr_number=kwargs["pr_number"],
            config=config,
            automation_config=automation_config,
            project_owner=project_owner,
            project_number=project_number,
            dry_run=kwargs["dry_run"],
            gh_runner=gh_runner,
            pr_port=kwargs["pr_port"],
            review_state_port=kwargs["review_state_port"],
            board_port=kwargs["board_port"],
        )

    return _app_review_rescue_all(
        config=config,
        automation_config=automation_config,
        dry_run=dry_run,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        review_rescue_runner=_review_rescue_runner,
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
    pr_port = pr_port or _default_pr_port(project_owner, project_number, config, gh_runner)
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner,
    )

    def _codex_gate_evaluator(**kwargs) -> tuple[int, str]:
        return codex_review_gate(
            pr_repo=kwargs["pr_repo"],
            pr_number=kwargs["pr_number"],
            config=config,
            automation_config=automation_config,
            project_owner=project_owner,
            project_number=project_number,
            dry_run=kwargs["dry_run"],
            apply_fail_routing=True,
            gh_runner=gh_runner,
        )

    return _app_automerge_review(
        pr_repo=pr_repo,
        pr_number=pr_number,
        automation_config=automation_config,
        dry_run=dry_run,
        update_branch=update_branch,
        delete_branch=delete_branch,
        snapshot=snapshot,
        pr_port=pr_port,
        review_state_port=review_state_port,
        codex_gate_evaluator=_codex_gate_evaluator,
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
    if "/" not in pr_repo:
        raise ConfigError(f"pr_repo must be owner/repo, got '{pr_repo}'.")

    pr_context = _app_load_execution_policy_pr_context(
        pr_repo=pr_repo,
        pr_number=pr_number,
        project_owner=project_owner,
        project_number=project_number,
        gh_runner=gh_runner,
        default_pr_port_fn=_default_pr_port,
    )
    actor = pr_context.actor
    state = pr_context.state
    provenance = pr_context.provenance
    owner, repo = pr_repo.split("/", maxsplit=1)
    return _app_enforce_execution_policy(
        pr_repo=pr_repo,
        pr_number=pr_number,
        actor=actor,
        state=state,
        pr_url=pr_context.url,
        provenance=provenance,
        config=config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        allow_copilot_coding_agent=allow_copilot_coding_agent,
        dry_run=dry_run,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
        is_copilot_actor=_is_copilot_coding_agent_actor,
        query_issue_executor=_query_project_item_field,
        set_status_if_changed=_set_status_if_changed,
        query_issue_assignees=_query_issue_assignees,
        set_issue_assignees=_set_issue_assignees,
        comment_exists=_comment_exists,
        post_comment=_post_comment,
        close_pull_request=close_pull_request,
        load_linked_issues=lambda: query_closing_issues(
            owner,
            repo,
            pr_number,
            config,
            gh_runner=gh_runner,
        ),
    )


# ---------------------------------------------------------------------------
# CLI Parser
# ---------------------------------------------------------------------------


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
    github_bundle = build_github_port_bundle(
        args.project_owner,
        args.project_number,
        config=config,
    )
    decision = admit_backlog_items(
        config,
        automation_config,
        args.project_owner,
        args.project_number,
        github_bundle=github_bundle,
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
    github_bundle = build_github_port_bundle(
        args.project_owner,
        args.project_number,
        config=config,
    )
    counters = reconcile_handoffs(
        config=config,
        project_owner=args.project_owner,
        project_number=args.project_number,
        ack_timeout_minutes=args.ack_timeout_minutes,
        max_retries=args.max_retries,
        dry_run=args.dry_run,
        github_bundle=github_bundle,
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

    github_bundle = build_github_port_bundle(
        args.project_owner,
        args.project_number,
        config=config,
    )

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
        pairs = resolve_issues_from_event(
            event_path,
            config,
            pr_port=github_bundle.pull_requests,
        )

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
                github_bundle=github_bundle,
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

        issue_refs = resolve_pr_to_issues(
            pr_repo,
            pr_number,
            config,
            pr_port=github_bundle.pull_requests,
        )
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
            pr_owner,
            pr_repo_name,
            pr_number,
            pr_port=github_bundle.pull_requests,
        )
        if head_sha:
            failed_checks = _query_failed_check_runs(
                pr_owner,
                pr_repo_name,
                head_sha,
                pr_port=github_bundle.pull_requests,
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
            github_bundle=github_bundle,
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
    review_state_port = _default_review_state_port(
        args.project_owner,
        args.project_number,
        config,
    )
    snapshot = classify_parallelism_snapshot(
        config=config,
        ready_items=review_state_port.list_issues_by_status("Ready"),
        blocked_items=review_state_port.list_issues_by_status("Blocked"),
        project_owner=args.project_owner,
        project_number=args.project_number,
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


from startupai_controller.board_automation_cli import build_parser, main


if __name__ == "__main__":
    raise SystemExit(main())
