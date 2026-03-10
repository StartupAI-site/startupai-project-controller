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
    from startupai_controller.ports.pull_requests import PullRequestPort as _PullRequestPort
    from startupai_controller.ports.review_state import ReviewStatePort as _ReviewStatePort
else:
    _BoardMutationPort = None  # runtime: structural typing, no import needed
    _PullRequestPort = None  # runtime: structural typing, no import needed
    _ReviewStatePort = None  # runtime: structural typing, no import needed


from startupai_controller.adapters.github_cli import (  # canonical adapter surface (ADR-002)
    COPILOT_CODING_AGENT_LOGINS,
    CycleBoardSnapshot,
    CycleGitHubMemo,
    _ProjectItemSnapshot,
    LinkedIssue,
    CodexReviewVerdict,
    PullRequestViewPayload,
    _parse_github_timestamp,
    _is_automation_login,
    _is_copilot_coding_agent_actor,
    _repo_to_prefix,
    _issue_ref_to_repo_parts,
    _snapshot_to_issue_ref,
    _marker_for,
    _comment_exists,
    _post_comment,
    _comment_activity_timestamp,
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
from startupai_controller.adapters.github_transport import _run_gh
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
from startupai_controller.promote_ready import (
    BoardInfo,
    _query_issue_board_info,
    _query_status_field_option,
    _set_board_status,
    promote_to_ready,
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
    IssueSnapshot,
)
from startupai_controller.runtime.wiring import build_github_port_bundle

# ---------------------------------------------------------------------------
# Port wiring helpers
# ---------------------------------------------------------------------------


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
    if (
        board_info_resolver is None
        and board_mutator is None
    ) or review_state_port is not None or board_port is not None:
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
        current_status = review_state_port.get_issue_status(issue_ref)
        if current_status in {None, "NOT_ON_BOARD"}:
            raise GhQueryError(f"{issue_ref} is not on the project board.")
        if dry_run:
            return
        if current_status != "Blocked":
            board_port.set_issue_status(issue_ref, "Blocked")
        board_port.set_issue_field(issue_ref, "Blocked Reason", reason)
        return

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
    if (
        board_info_resolver is None
        and board_mutator is None
    ) or review_state_port is not None or board_port is not None:
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
        old_status = review_state_port.get_issue_status(issue_ref) or "NOT_ON_BOARD"
        if old_status not in from_statuses:
            return False, old_status
        if not dry_run:
            board_port.set_issue_status(issue_ref, to_status)
        return True, old_status

    return _set_status_if_changed(
        issue_ref,
        from_statuses,
        to_status,
        config,
        project_owner,
        project_number,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
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
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Route failed codex review back to In Progress with explicit handoff."""
    _changed, old_status = _transition_issue_status(
        issue_ref,
        {"Review"},
        "In Progress",
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        gh_runner=gh_runner,
    )

    if old_status not in {"Review", "In Progress"}:
        return

    if route == "executor":
        if review_state_port is not None or board_info_resolver is None:
            review_state_port = review_state_port or _default_review_state_port(
                project_owner,
                project_number,
                config,
                gh_runner=gh_runner,
            )
            executor = review_state_port.get_issue_fields(issue_ref).executor.lower()
        else:
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

    if not dry_run:
        board_port = board_port or _default_board_mutation_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
        board_port.set_issue_field(issue_ref, "Handoff To", handoff_target)

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
        board_port = board_port or _default_board_mutation_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
        board_port.post_issue_comment(f"{owner}/{repo}", number, body)


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
                if comment_poster is not None:
                    post_comment(succ_owner, succ_repo, succ_number, body)
                else:
                    board_port = board_port or _default_board_mutation_port(
                        project_owner,
                        project_number,
                        config,
                        gh_runner=gh_runner,
                    )
                    board_port.post_issue_comment(
                        f"{succ_owner}/{succ_repo}",
                        succ_number,
                        body,
                    )

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
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Propagate blocker info to successors. Returns list of commented refs."""
    check_comment = comment_checker or _comment_exists
    post_comment = comment_poster or _post_comment
    commented: list[str] = []

    use_ports = (board_info_resolver is None) or review_state_port is not None or board_port is not None
    if use_ports:
        review_state_port = review_state_port or _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
        if comment_poster is None:
            board_port = board_port or _default_board_mutation_port(
                project_owner,
                project_number,
                config,
                gh_runner=gh_runner,
            )

    if sweep_blocked:
        # Sweep mode: scan all Blocked items
        if use_ports:
            blocked_items = review_state_port.list_issues_by_status("Blocked")
        else:
            blocked_items = _list_project_items_by_status(
                "Blocked", project_owner, project_number, gh_runner=gh_runner
            )
        for snapshot in blocked_items:
            if use_ports:
                ref = snapshot.issue_ref
            else:
                ref = _snapshot_to_issue_ref(snapshot, config)
                if ref is None:
                    continue

            if not all_prefixes and this_repo_prefix:
                parsed = parse_issue_ref(ref)
                if parsed.prefix != this_repo_prefix:
                    continue

            if use_ports:
                blocked_reason = review_state_port.get_issue_fields(
                    ref
                ).blocked_reason
            else:
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
                board_port=board_port,
                project_owner=project_owner,
                project_number=project_number,
                gh_runner=gh_runner,
                dry_run=dry_run,
            )
            commented.extend(new_comments)
    else:
        # Single-issue mode
        if issue_ref is None:
            return commented

        if use_ports:
            blocked_reason = review_state_port.get_issue_fields(
                issue_ref
            ).blocked_reason
        else:
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
            board_port=board_port,
            project_owner=project_owner,
            project_number=project_number,
            gh_runner=gh_runner,
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
    board_port: _BoardMutationPort | None = None,
    project_owner: str = DEFAULT_PROJECT_OWNER,
    project_number: int = DEFAULT_PROJECT_NUMBER,
    gh_runner: Callable[..., str] | None = None,
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
                if board_port is not None:
                    board_port.post_issue_comment(
                        f"{succ_owner}/{succ_repo}",
                        succ_number,
                        body,
                    )
                else:
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
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
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

    # Scan all issue prefixes for handoff markers
    for prefix, repo_slug in config.issue_prefixes.items():
        try:
            issue_numbers = review_state_port.search_open_issue_numbers_with_comment_marker(
                repo_slug,
                f"{MARKER_PREFIX}:handoff:job=",
            )
        except GhQueryError:
            continue

        for issue_number in issue_numbers:
            issue_ref = f"{prefix}#{issue_number}"

            # Check if this issue has been acknowledged (moved past Backlog)
            current_status = review_state_port.get_issue_status(issue_ref)

            ack_statuses = {"Ready", "In Progress", "Review", "Done"}
            if current_status in ack_statuses:
                counters["completed"] += 1
                continue

            try:
                comments = review_state_port.list_issue_comment_bodies(
                    repo_slug,
                    issue_number,
                )
            except GhQueryError:
                counters["pending"] += 1
                continue

            retry_marker = f"{MARKER_PREFIX}:handoff-retry:{issue_ref}:"
            retry_count = 0
            for body in comments:
                if retry_marker in body:
                    retry_count += 1

            latest_signal = review_state_port.latest_matching_comment_timestamp(
                repo_slug,
                issue_number,
                (
                    f"{MARKER_PREFIX}:handoff:job=",
                    f"{MARKER_PREFIX}:handoff-retry:{issue_ref}:",
                ),
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
                    board_port.post_issue_comment(repo_slug, issue_number, body)
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
                            review_state_port=review_state_port,
                            board_port=board_port,
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
    dry_run: bool = False,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set the board Handoff To field for an issue."""
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    current_status = review_state_port.get_issue_status(issue_ref)
    if current_status in {None, "NOT_ON_BOARD"}:
        return
    if dry_run:
        return
    board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    board_port.set_issue_field(issue_ref, "Handoff To", target)


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


def _load_admission_source_items(
    automation_config: BoardAutomationConfig,
    *,
    project_owner: str,
    project_number: int,
    board_snapshot: CycleBoardSnapshot | None,
    gh_runner: Callable[..., str] | None,
) -> list[_ProjectItemSnapshot]:
    """Load backlog/ready items needed for one admission pass."""
    statuses = set(automation_config.admission.source_statuses)
    statuses.add("Ready")
    if board_snapshot is None:
        return _list_project_items(
            project_owner,
            project_number,
            statuses=statuses,
            gh_runner=gh_runner,
        )
    return [item for item in board_snapshot.items if item.status in statuses]


def _partition_admission_source_items(
    items: list[_ProjectItemSnapshot],
    *,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    target_executor: str,
) -> tuple[int, list[_ProjectItemSnapshot]]:
    """Count governed ready items and collect governed backlog items."""
    ready_count = 0
    backlog_items: list[_ProjectItemSnapshot] = []
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
    return ready_count, backlog_items


def _build_provisional_admission_candidates(
    backlog_items: list[_ProjectItemSnapshot],
    *,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    dispatchable_repo_prefixes: tuple[str, ...],
    active_lease_issue_refs: tuple[str, ...],
) -> tuple[list[_ProjectItemSnapshot], list[AdmissionSkip]]:
    """Apply cheap exact admission filters to backlog items."""
    dispatchable = set(dispatchable_repo_prefixes)
    active_leases = set(active_lease_issue_refs)
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
    return provisional_candidates, skipped


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

    return (
        eligible,
        resolved,
        blocked,
        partial_failure,
        error,
        deep_evaluation_truncated,
    )


def _apply_admitted_backlog_candidates(
    selected: list[AdmissionCandidate],
    *,
    executor: str,
    assignment_owner: str,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> tuple[list[str], bool, str | None]:
    """Apply backlog-to-ready mutations for the selected candidates."""
    admitted: list[str] = []
    partial_failure = False
    error: str | None = None
    if dry_run:
        return admitted, partial_failure, error
    for candidate in selected:
        try:
            _admit_backlog_item(
                candidate,
                executor=executor,
                assignment_owner=assignment_owner,
                gh_runner=gh_runner,
            )
        except GhQueryError as exc:
            partial_failure = True
            error = str(exc)
            break
        admitted.append(candidate.issue_ref)
    return admitted, partial_failure, error


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

    items = _load_admission_source_items(
        automation_config,
        project_owner=project_owner,
        project_number=project_number,
        board_snapshot=board_snapshot,
        gh_runner=gh_runner,
    )
    ready_count, backlog_items = _partition_admission_source_items(
        items,
        config=config,
        automation_config=automation_config,
        target_executor=target_executor,
    )

    needed = min(
        max(0, floor - ready_count),
        max(0, cap - ready_count),
        automation_config.admission.max_batch_size,
    )

    provisional_candidates, skipped = _build_provisional_admission_candidates(
        backlog_items,
        config=config,
        automation_config=automation_config,
        dispatchable_repo_prefixes=dispatchable_repo_prefixes
        or automation_config.execution_authority_repos,
        active_lease_issue_refs=active_lease_issue_refs,
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
    (
        eligible,
        resolved,
        blocked,
        partial_failure,
        error,
        deep_evaluation_truncated,
    ) = _evaluate_admission_candidates(
        provisional_candidates,
        config=config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        needed=needed,
        dry_run=dry_run,
        memo=memo,
        skipped=skipped,
        gh_runner=gh_runner,
    )
    selected = eligible[:needed]
    admitted, mutation_partial_failure, mutation_error = _apply_admitted_backlog_candidates(
        selected,
        executor=target_executor,
        assignment_owner=automation_config.admission.assignment_owner,
        dry_run=dry_run,
        gh_runner=gh_runner,
    )
    if mutation_partial_failure:
        partial_failure = True
        error = mutation_error

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
    marker = _marker_for("claim-ready", issue_ref)
    owner, repo, number = _resolve_issue_coordinates(issue_ref, config)

    checker = comment_checker or _comment_exists
    if checker(owner, repo, number, marker, gh_runner=gh_runner):
        return

    executor_label = f"Executor: `{executor}`" if executor else ""
    body = (
        f"{marker}\n"
        f"**Claimed for execution** by `{executor}`.\n\n"
        "Board transition: `Ready -> In Progress`.\n"
        f"{executor_label}".strip()
    )
    if comment_poster is not None:
        comment_poster(owner, repo, number, body, gh_runner=gh_runner)
        return
    board_port = board_port or _default_board_mutation_port(
        DEFAULT_PROJECT_OWNER,
        DEFAULT_PROJECT_NUMBER,
        config,
        gh_runner=gh_runner,
    )
    board_port.post_issue_comment(f"{owner}/{repo}", number, body)


def _load_schedule_ready_state(
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    automation_config: BoardAutomationConfig | None,
    review_state_port: _ReviewStatePort | None,
    board_port: _BoardMutationPort | None,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[
    bool,
    list[IssueSnapshot],
    dict[str, int],
    dict[tuple[str, str], int],
]:
    """Load Ready items and WIP counts for one scheduling pass."""
    use_ports = (
        board_info_resolver is None
        and board_mutator is None
    ) or review_state_port is not None or board_port is not None
    review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    wip_items = review_state_port.list_issues_by_status("In Progress")
    wip_counts = _count_wip_by_executor(wip_items)
    lane_wip_counts: dict[tuple[str, str], int] = {}
    if automation_config is not None:
        lane_wip_counts = _count_wip_by_executor_lane(config, wip_items)
    ready_items = review_state_port.list_issues_by_status("Ready")
    ready_items = sorted(
        ready_items,
        key=lambda snapshot: _ready_snapshot_rank(snapshot, config),
    )
    return use_ports, ready_items, wip_counts, lane_wip_counts


def _record_missing_executor_ready_item(
    ref: str,
    reason: str,
    *,
    decision: SchedulingDecision,
    missing_executor_block_cap: int,
    dry_run: bool,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Block or skip one Ready item with a missing/invalid executor."""
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
        return
    decision.skipped_missing_executor.append(ref)


def _process_schedule_ready_snapshot(
    snapshot: object,
    *,
    use_ports: bool,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    this_repo_prefix: str | None,
    all_prefixes: bool,
    mode: str,
    per_executor_wip_limit: int,
    automation_config: BoardAutomationConfig | None,
    missing_executor_block_cap: int,
    dry_run: bool,
    decision: SchedulingDecision,
    review_state_port: _ReviewStatePort | None,
    board_port: _BoardMutationPort | None,
    status_resolver: Callable[..., str] | None,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    wip_counts: dict[str, int],
    lane_wip_counts: dict[tuple[str, str], int],
) -> None:
    """Apply scheduling policy to one Ready snapshot."""
    ref = getattr(snapshot, "issue_ref", None)
    if ref is None:
        ref = _snapshot_to_issue_ref(snapshot, config)
        if ref is None:
            return

    if not all_prefixes and this_repo_prefix:
        parsed = parse_issue_ref(ref)
        if parsed.prefix != this_repo_prefix:
            return

    is_graph_member = in_any_critical_path(config, ref)
    executor = snapshot.executor.strip().lower()
    if executor not in VALID_EXECUTORS:
        reason = "missing-executor" if not executor else f"invalid-executor:{executor}"
        _record_missing_executor_ready_item(
            ref,
            reason,
            decision=decision,
            missing_executor_block_cap=missing_executor_block_cap,
            dry_run=dry_run,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        return

    if not is_graph_member:
        decision.skipped_non_graph.append(ref)
    else:
        val_code, _val_output = evaluate_ready_promotion(
            issue_ref=ref,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            status_resolver=status_resolver,
            require_in_graph=True,
        )
        if val_code != 0:
            preds = direct_predecessors(config, ref)
            reason = f"dependency-unmet:{','.join(sorted(preds))}"
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
            return

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
        return

    if mode == "claim":
        changed, _old = _transition_issue_status(
            ref,
            {"Ready"},
            "In Progress",
            config,
            project_owner,
            project_number,
            dry_run=dry_run,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        if not changed:
            return
        decision.claimed.append(ref)
    else:
        decision.claimable.append(ref)

    if automation_config is not None:
        lane_wip_counts[(executor, lane)] = current_wip + 1
    else:
        wip_counts[executor] = current_wip + 1


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
    if mode not in {"advisory", "claim"}:
        raise ConfigError(
            f"Invalid schedule mode '{mode}'. Use advisory or claim."
        )

    decision = SchedulingDecision()
    (
        use_ports,
        ready_items,
        wip_counts,
        lane_wip_counts,
    ) = _load_schedule_ready_state(
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        automation_config=automation_config,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    if use_ports:
        review_state_port = review_state_port or _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )

    for snapshot in ready_items:
        _process_schedule_ready_snapshot(
            snapshot,
            use_ports=use_ports,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            this_repo_prefix=this_repo_prefix,
            all_prefixes=all_prefixes,
            mode=mode,
            per_executor_wip_limit=per_executor_wip_limit,
            automation_config=automation_config,
            missing_executor_block_cap=missing_executor_block_cap,
            dry_run=dry_run,
            decision=decision,
            review_state_port=review_state_port,
            board_port=board_port,
            status_resolver=status_resolver,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
            wip_counts=wip_counts,
            lane_wip_counts=lane_wip_counts,
        )

    return decision


def _load_claim_ready_state(
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    automation_config: BoardAutomationConfig | None,
    this_repo_prefix: str | None,
    all_prefixes: bool,
    review_state_port: _ReviewStatePort | None,
    board_port: _BoardMutationPort | None,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[
    bool,
    dict[str, object],
    dict[str, int],
    dict[tuple[str, str], int],
]:
    """Load Ready items and current WIP state for one claim attempt."""
    use_ports = (
        board_info_resolver is None
        and board_mutator is None
    ) or review_state_port is not None or board_port is not None
    state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    ready_items = state_port.list_issues_by_status("Ready")
    wip_items = state_port.list_issues_by_status("In Progress")
    wip_counts = _count_wip_by_executor(wip_items)
    lane_wip_counts = {}
    if automation_config is not None:
        lane_wip_counts = _count_wip_by_executor_lane(
            config,
            wip_items,
        )

    ready_by_ref: dict[str, object] = {}
    for snapshot in ready_items:
        ref = snapshot.issue_ref
        if not all_prefixes and this_repo_prefix:
            parsed = parse_issue_ref(ref)
            if parsed.prefix != this_repo_prefix:
                continue
        ready_by_ref[ref] = snapshot
    return use_ports, ready_by_ref, wip_counts, lane_wip_counts


def _claim_ready_candidates(
    ready_by_ref: dict[str, object],
    *,
    norm_executor: str,
    issue_ref: str | None,
    next_issue: bool,
) -> tuple[list[str], ClaimReadyResult | None]:
    """Resolve the candidate issue refs for one claim request."""
    if issue_ref:
        return [issue_ref], None
    if not next_issue:
        return [], ClaimReadyResult(reason="missing-target")
    candidates = sorted(
        [
            ref
            for ref, snapshot in ready_by_ref.items()
            if snapshot.executor.strip().lower() == norm_executor
        ],
        key=_issue_sort_key,
    )
    if not candidates:
        return [], ClaimReadyResult(reason="no-ready-for-executor")
    return candidates, None


def _attempt_claim_ready_candidate(
    ref: str,
    snapshot: object,
    *,
    issue_ref: str | None,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    status_resolver: Callable[..., str] | None,
    per_executor_wip_limit: int,
    automation_config: BoardAutomationConfig | None,
    dry_run: bool,
    review_state_port: _ReviewStatePort | None,
    board_port: _BoardMutationPort | None,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    comment_checker: Callable[..., bool] | None,
    comment_poster: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
    norm_executor: str,
    lane_wip_counts: dict[tuple[str, str], int],
    wip_counts: dict[str, int],
) -> ClaimReadyResult | None:
    """Attempt to claim one Ready candidate for the executor."""
    item_executor = snapshot.executor.strip().lower()
    if item_executor != norm_executor:
        if issue_ref:
            return ClaimReadyResult(
                reason=f"executor-mismatch:{item_executor or 'unset'}"
            )
        return None

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
            return None

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

    changed, old_status = _transition_issue_status(
        ref,
        {"Ready"},
        "In Progress",
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    if not changed:
        return ClaimReadyResult(reason=f"status-not-ready:{old_status}")

    if not dry_run:
        try:
            _post_claim_comment(
                ref,
                norm_executor,
                config,
                board_port=board_port,
                comment_checker=comment_checker,
                comment_poster=comment_poster,
                gh_runner=gh_runner,
            )
        except GhQueryError:
            pass

    return ClaimReadyResult(claimed=ref)


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
    norm_executor = executor.strip().lower()
    if norm_executor not in VALID_EXECUTORS:
        return ClaimReadyResult(reason=f"invalid-executor:{executor}")

    use_ports, ready_by_ref, wip_counts, lane_wip_counts = _load_claim_ready_state(
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        automation_config=automation_config,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    if use_ports:
        review_state_port = review_state_port or _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )

    candidates, early_result = _claim_ready_candidates(
        ready_by_ref,
        norm_executor=norm_executor,
        issue_ref=issue_ref,
        next_issue=next_issue,
    )
    if early_result is not None:
        return early_result

    for ref in candidates:
        snapshot = ready_by_ref.get(ref)
        if snapshot is None:
            if issue_ref:
                return ClaimReadyResult(reason="issue-not-ready")
            continue

        result = _attempt_claim_ready_candidate(
            ref,
            snapshot,
            issue_ref=issue_ref,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            status_resolver=status_resolver,
            per_executor_wip_limit=per_executor_wip_limit,
            automation_config=automation_config,
            dry_run=dry_run,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
            norm_executor=norm_executor,
            lane_wip_counts=lane_wip_counts,
            wip_counts=wip_counts,
        )
        if result is None:
            continue
        return result

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
    unmet = find_unmet_ready_dependencies(
        config=config,
        ready_items=review_state_port.list_issues_by_status("Ready"),
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        status_resolver=status_resolver,
        project_owner=project_owner,
        project_number=project_number,
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
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Escalate stale In Progress issues with no linked PR."""
    now = datetime.now(timezone.utc)
    stale_refs: list[str] = []

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

    for snapshot in in_progress_items:
        if use_ports:
            ref = snapshot.issue_ref
        else:
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
        if use_ports:
            current_status = review_state_port.get_issue_status(ref)
            if current_status in {None, "NOT_ON_BOARD"}:
                continue
            try:
                board_port.set_issue_field(ref, "Handoff To", "claude")
            except GhQueryError:
                pass
        else:
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
                if comment_poster is not None:
                    comment_poster(owner, repo, number, body, gh_runner=gh_runner)
                else:
                    board_port = board_port or _default_board_mutation_port(
                        project_owner,
                        project_number,
                        config,
                        gh_runner=gh_runner,
                    )
                    board_port.post_issue_comment(f"{owner}/{repo}", number, body)
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
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> DispatchResult:
    """Dispatch eligible In Progress issues according to dispatch target."""
    result = DispatchResult()

    target = automation_config.dispatch_target
    use_ports = (board_info_resolver is None) or review_state_port is not None or board_port is not None

    for issue_ref in issue_refs:
        owner, repo, number = _resolve_issue_coordinates(issue_ref, config)
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
            status = review_state_port.get_issue_status(issue_ref)
        else:
            info = _query_issue_board_info(
                issue_ref,
                config,
                project_owner,
                project_number,
            )
            status = info.status
        if status != "In Progress":
            result.skipped.append((issue_ref, f"status={status or 'unknown'}"))
            continue

        if use_ports:
            executor = review_state_port.get_issue_fields(issue_ref).executor.strip().lower()
        else:
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
                board_port = board_port or _default_board_mutation_port(
                    project_owner,
                    project_number,
                    config,
                    gh_runner=gh_runner,
                )
                board_port.post_issue_comment(f"{owner}/{repo}", number, body)
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


def _handle_dependency_blocked_rebalance_item(
    ref: str,
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    dry_run: bool,
    decision: RebalanceDecision,
    review_state_port: _ReviewStatePort | None,
    board_port: _BoardMutationPort | None,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> _BoardMutationPort | None:
    """Route a dependency-blocked WIP item to Blocked with claude handoff."""
    preds = ",".join(sorted(direct_predecessors(config, ref)))
    reason = f"dependency-unmet:{preds}"
    if not dry_run:
        _set_blocked_with_reason(
            ref,
            reason,
            config,
            project_owner,
            project_number,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        _set_handoff_target(
            ref,
            "claude",
            config,
            project_owner,
            project_number,
            review_state_port=review_state_port,
            board_port=board_port,
            gh_runner=gh_runner,
        )
    decision.moved_blocked.append(ref)
    return board_port


def _handle_stale_rebalance_item(
    ref: str,
    *,
    owner: str,
    repo: str,
    number: int,
    now: datetime,
    activity_at: datetime | None,
    confirm_delay: timedelta,
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    dry_run: bool,
    decision: RebalanceDecision,
    review_state_port: _ReviewStatePort | None,
    board_port: _BoardMutationPort | None,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[bool, _BoardMutationPort | None]:
    """Mark a stale item or demote it back to Ready after confirmation."""
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
            if not _comment_exists(owner, repo, number, stale_marker, gh_runner=gh_runner):
                board_port = _ensure_rebalance_board_port(
                    board_port,
                    project_owner=project_owner,
                    project_number=project_number,
                    config=config,
                    gh_runner=gh_runner,
                )
                board_port.post_issue_comment(
                    f"{owner}/{repo}",
                    number,
                    (
                        f"{stale_marker}\n"
                        "Stale candidate detected; will demote on next cycle if still inactive."
                    ),
                )
        return True, board_port

    changed, _old = _transition_issue_status(
        ref,
        {"In Progress"},
        "Ready",
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    if changed and not dry_run:
        demote_marker = _marker_for("stale-demote", ref)
        if not _comment_exists(owner, repo, number, demote_marker, gh_runner=gh_runner):
            board_port = _ensure_rebalance_board_port(
                board_port,
                project_owner=project_owner,
                project_number=project_number,
                config=config,
                gh_runner=gh_runner,
            )
            board_port.post_issue_comment(
                f"{owner}/{repo}",
                number,
                (
                    f"{demote_marker}\n"
                    "Moved back to `Ready` after consecutive stale cycles "
                    "with no PR and no fresh activity."
                ),
            )
    decision.moved_ready.append(ref)
    return True, board_port


def _evaluate_rebalance_snapshot(
    snapshot: IssueSnapshot,
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    this_repo_prefix: str | None,
    all_prefixes: bool,
    freshness_cutoff: datetime,
    confirm_delay: timedelta,
    now: datetime,
    dry_run: bool,
    decision: RebalanceDecision,
    use_ports: bool,
    review_state_port: _ReviewStatePort | None,
    board_port: _BoardMutationPort | None,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    status_resolver: Callable[..., str] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[_WipCandidate | None, _BoardMutationPort | None]:
    """Evaluate one in-progress item for dependency, staleness, and lane placement."""
    if use_ports:
        ref = snapshot.issue_ref
    else:
        ref = _snapshot_to_issue_ref(snapshot, config)
        if ref is None:
            return None, board_port
    parsed = parse_issue_ref(ref)
    if not all_prefixes and this_repo_prefix and parsed.prefix != this_repo_prefix:
        return None, board_port

    owner, repo, number = _resolve_issue_coordinates(ref, config)
    executor = snapshot.executor.strip().lower()
    if executor not in VALID_EXECUTORS:
        decision.skipped.append((ref, "invalid-executor"))
        return None, board_port

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
            board_port = _handle_dependency_blocked_rebalance_item(
                ref,
                config=config,
                project_owner=project_owner,
                project_number=project_number,
                dry_run=dry_run,
                decision=decision,
                review_state_port=review_state_port,
                board_port=board_port,
                board_info_resolver=board_info_resolver,
                board_mutator=board_mutator,
                gh_runner=gh_runner,
            )
            return None, board_port

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
            _query_open_pr_updated_at(pr_owner, pr_repo, pr_number, gh_runner=gh_runner)
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
        handled, board_port = _handle_stale_rebalance_item(
            ref,
            owner=owner,
            repo=repo,
            number=number,
            now=now,
            activity_at=activity_at,
            confirm_delay=confirm_delay,
            project_owner=project_owner,
            project_number=project_number,
            config=config,
            dry_run=dry_run,
            decision=decision,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        if handled:
            return None, board_port

    return (
        _WipCandidate(
            ref=ref,
            owner=owner,
            repo=repo,
            number=number,
            lane=parsed.prefix,
            executor=executor,
            activity_at=activity_at or datetime.min.replace(tzinfo=timezone.utc),
            has_open_pr=has_open_pr,
        ),
        board_port,
    )


def _apply_rebalance_lane_overflow(
    candidates: list[_WipCandidate],
    *,
    executor: str,
    lane: str,
    automation_config: BoardAutomationConfig,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    dry_run: bool,
    decision: RebalanceDecision,
    review_state_port: _ReviewStatePort | None,
    board_port: _BoardMutationPort | None,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> _BoardMutationPort | None:
    """Keep the freshest lane items and demote overflow back to Ready."""
    limit = _wip_limit_for_lane(automation_config, executor, lane, fallback=3)
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
        changed, _old = _transition_issue_status(
            item.ref,
            {"In Progress"},
            "Ready",
            config,
            project_owner,
            project_number,
            dry_run=dry_run,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )
        if changed and not dry_run:
            marker = _marker_for("wip-rebalance", item.ref)
            if not _comment_exists(
                item.owner, item.repo, item.number, marker, gh_runner=gh_runner
            ):
                board_port = _ensure_rebalance_board_port(
                    board_port,
                    project_owner=project_owner,
                    project_number=project_number,
                    config=config,
                    gh_runner=gh_runner,
                )
                board_port.post_issue_comment(
                    f"{item.owner}/{item.repo}",
                    item.number,
                    f"{marker}\nMoved back to `Ready` by lane WIP rebalance (limit={limit}).",
                )
        decision.moved_ready.append(item.ref)
    return board_port


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
    now = datetime.now(timezone.utc)
    freshness_cutoff = now - timedelta(hours=automation_config.freshness_hours)
    confirm_delay = timedelta(
        minutes=cycle_minutes * max(1, automation_config.stale_confirmation_cycles - 1)
    )
    decision = RebalanceDecision()

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
    lane_buckets: dict[tuple[str, str], list[_WipCandidate]] = {}

    for snapshot in in_progress:
        candidate, board_port = _evaluate_rebalance_snapshot(
            snapshot,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            this_repo_prefix=this_repo_prefix,
            all_prefixes=all_prefixes,
            freshness_cutoff=freshness_cutoff,
            confirm_delay=confirm_delay,
            now=now,
            dry_run=dry_run,
            decision=decision,
            use_ports=use_ports,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            status_resolver=status_resolver,
            gh_runner=gh_runner,
        )
        if candidate is not None:
            lane_buckets.setdefault((candidate.executor, candidate.lane), []).append(
                candidate
            )

    for (executor, lane), candidates in lane_buckets.items():
        board_port = _apply_rebalance_lane_overflow(
            candidates,
            executor=executor,
            lane=lane,
            automation_config=automation_config,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            dry_run=dry_run,
            decision=decision,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )

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


def _sync_review_transition(
    issue_ref: str,
    from_statuses: set[str],
    target_status: str,
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
) -> tuple[int, str]:
    """Apply one review-state transition and format the result."""
    changed, old = _transition_issue_status(
        issue_ref,
        from_statuses,
        target_status,
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    if changed:
        return 0, f"{issue_ref}: {old} -> {target_status} ({reason})"
    return 2, f"{issue_ref}: no change (Status={old})"


def _required_review_check_contexts(
    issue_ref: str,
    config: CriticalPathConfig,
    *,
    project_owner: str,
    project_number: int,
    pr_port: _PullRequestPort | None,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> set[str] | tuple[int, str]:
    """Return required status-check contexts for the issue repo."""
    owner, repo, _number = _issue_ref_to_repo_parts(issue_ref, config)
    if pr_port is None and (
        board_info_resolver is not None or board_mutator is not None
    ):
        try:
            bp_output = _run_gh(
                [
                    "api",
                    f"repos/{owner}/{repo}/branches/main/protection/required_status_checks",
                ],
                gh_runner=gh_runner,
            )
        except GhQueryError as error:
            return 4, f"Cannot read branch protection for {owner}/{repo}: {error}"
        try:
            bp_data = json.loads(bp_output)
        except json.JSONDecodeError:
            return 4, f"Cannot parse branch protection response for {owner}/{repo}"

        required_contexts: set[str] = set()
        for ctx in bp_data.get("contexts", []):
            if isinstance(ctx, str):
                required_contexts.add(ctx)
        for check in bp_data.get("checks", []):
            if isinstance(check, dict) and check.get("context"):
                required_contexts.add(check["context"])
        return required_contexts

    pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    try:
        return pr_port.required_status_checks(f"{owner}/{repo}", "main")
    except GhQueryError as error:
        return 4, f"Cannot read branch protection for {owner}/{repo}: {error}"


def _handle_checks_failed_sync_event(
    issue_ref: str,
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    governed_single_machine_issue: bool,
    failed_checks: list[str] | None,
    dry_run: bool,
    pr_port: _PullRequestPort | None,
    review_state_port: _ReviewStatePort | None,
    board_port: _BoardMutationPort | None,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> tuple[int, str]:
    """Handle checks_failed state sync for one issue."""
    if governed_single_machine_issue:
        return 2, f"{issue_ref}: governed local repair transition deferred to consumer"

    required_contexts = _required_review_check_contexts(
        issue_ref,
        config,
        project_owner=project_owner,
        project_number=project_number,
        pr_port=pr_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    if isinstance(required_contexts, tuple):
        return required_contexts

    actual_failed = set(failed_checks) if failed_checks else set()
    if required_contexts and actual_failed:
        required_failures = actual_failed & required_contexts
        if not required_failures:
            return 2, (
                f"{issue_ref}: check failures are non-required, "
                f"no board change (failed: {sorted(actual_failed)})"
            )
    elif required_contexts and not actual_failed:
        return 2, (
            f"{issue_ref}: checks_failed but no failed check names "
            f"provided; cannot filter by required checks "
            f"({sorted(required_contexts)}) — no board change"
        )

    return _sync_review_transition(
        issue_ref,
        {"Review"},
        "In Progress",
        "required check failed",
        config,
        project_owner,
        project_number,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
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

    if event_kind == "pr_ready_for_review":
        return _sync_review_transition(
            issue_ref,
            {"In Progress"},
            "Review",
            "PR ready for review",
            config,
            project_owner,
            project_number,
            dry_run=dry_run,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )

    if event_kind == "review_submitted":
        return _sync_review_transition(
            issue_ref,
            {"In Progress"},
            "Review",
            "review submitted",
            config,
            project_owner,
            project_number,
            dry_run=dry_run,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )

    if event_kind == "changes_requested":
        if governed_single_machine_issue:
            return 2, f"{issue_ref}: governed local repair transition deferred to consumer"
        return _sync_review_transition(
            issue_ref,
            {"Review"},
            "In Progress",
            "changes requested",
            config,
            project_owner,
            project_number,
            dry_run=dry_run,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )

    if event_kind == "checks_failed":
        return _handle_checks_failed_sync_event(
            issue_ref,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            governed_single_machine_issue=governed_single_machine_issue,
            failed_checks=failed_checks,
            dry_run=dry_run,
            pr_port=pr_port,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )

    if event_kind == "pr_close_merged":
        # Merged PR with passing checks -> Done
        if checks_state and checks_state != "passed":
            return 2, f"{issue_ref}: PR merged but checks_state={checks_state}"

        return _sync_review_transition(
            issue_ref,
            {"Review", "In Progress"},
            "Done",
            "PR merged",
            config,
            project_owner,
            project_number,
            dry_run=dry_run,
            review_state_port=review_state_port,
            board_port=board_port,
            board_info_resolver=board_info_resolver,
            board_mutator=board_mutator,
            gh_runner=gh_runner,
        )

    if event_kind == "checks_passed":
        # Checks passed alone doesn't change state unless combined with merge
        return 2, f"{issue_ref}: checks_passed (no state change without merge)"

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


def _set_issue_status_if_matches(
    issue_ref: str,
    from_statuses: set[str],
    to_status: str,
    *,
    review_state_port: _ReviewStatePort,
    board_port: _BoardMutationPort,
    dry_run: bool = False,
) -> tuple[bool, str | None]:
    """Set one issue status through ports when the current status matches."""
    current_status = review_state_port.get_issue_status(issue_ref)
    if current_status not in from_statuses:
        return False, current_status
    if dry_run:
        return True, current_status
    if current_status != to_status:
        board_port.set_issue_status(issue_ref, to_status)
    return True, current_status


def _requeue_local_review_failures(
    pr_repo: str,
    pr_number: int,
    review_refs: tuple[str, ...],
    automation_config: BoardAutomationConfig,
    *,
    dry_run: bool = False,
    pr_author: str | None = None,
    pr_body: str | None = None,
    pr_port: _PullRequestPort,
    review_state_port: _ReviewStatePort,
    board_port: _BoardMutationPort,
) -> tuple[str, ...]:
    """Return linked review refs to Ready when a local PR needs another coding pass."""
    actor = (pr_author or "").strip().lower()
    body = pr_body or ""
    if not actor and not body:
        pr_view = pr_port.get_pull_request(pr_repo, pr_number)
        if pr_view is None:
            return ()
        actor = (pr_view.author or "").strip().lower()
        body = pr_view.body or ""
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

    changed, _old_status = _set_issue_status_if_matches(
        issue_ref,
        {"Review"},
        "Ready",
        review_state_port=review_state_port,
        board_port=board_port,
        dry_run=dry_run,
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
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
) -> ReviewRescueResult:
    """Reconcile one PR in Review back toward self-healing merge flow."""
    pr_port, review_state_port, board_port = _resolve_review_rescue_ports(
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
    )

    snapshot = _resolve_review_rescue_snapshot(
        snapshot=snapshot,
        pr_repo=pr_repo,
        pr_number=pr_number,
        config=config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        dry_run=dry_run,
        gh_runner=gh_runner,
    )

    rerun_result = _rerun_cancelled_review_checks(
        pr_repo=pr_repo,
        pr_number=pr_number,
        snapshot=snapshot,
        dry_run=dry_run,
        pr_port=pr_port,
    )
    if rerun_result is not None:
        return rerun_result

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
    return _apply_review_rescue_decision(
        pr_repo=pr_repo,
        pr_number=pr_number,
        snapshot=snapshot,
        action=action,
        reason=reason,
        config=config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        dry_run=dry_run,
        gh_runner=gh_runner,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
    )


def _resolve_review_rescue_ports(
    *,
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    gh_runner: Callable[..., str] | None,
    pr_port: _PullRequestPort | None,
    review_state_port: _ReviewStatePort | None,
    board_port: _BoardMutationPort | None,
) -> tuple[_PullRequestPort, _ReviewStatePort, _BoardMutationPort]:
    """Resolve the effective ports for the review rescue use case."""
    effective_pr_port = pr_port or _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner,
    )
    effective_review_state_port = review_state_port or _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner,
    )
    effective_board_port = board_port or _default_board_mutation_port(
        project_owner,
        project_number,
        config,
        gh_runner,
    )
    return effective_pr_port, effective_review_state_port, effective_board_port


def _resolve_review_rescue_snapshot(
    *,
    snapshot: ReviewSnapshot | None,
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> ReviewSnapshot:
    """Load the review snapshot when the caller did not provide one."""
    if snapshot is not None:
        return snapshot
    return _build_review_snapshot(
        pr_repo=pr_repo,
        pr_number=pr_number,
        config=config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        dry_run=dry_run,
        gh_runner=gh_runner,
    )


def _rerun_cancelled_review_checks(
    *,
    pr_repo: str,
    pr_number: int,
    snapshot: ReviewSnapshot,
    dry_run: bool,
    pr_port: _PullRequestPort,
) -> ReviewRescueResult | None:
    """Handle the cancelled-check rerun phase before domain policy."""
    if not snapshot.rescue_cancelled:
        return None
    rerun_checks: list[str] = []
    for check_name in sorted(snapshot.rescue_cancelled):
        observation = snapshot.gate_status.checks.get(check_name)
        if observation is None:
            continue
        if _rerun_check_observation(
            pr_repo,
            observation,
            dry_run=dry_run,
            pr_port=pr_port,
        ):
            rerun_checks.append(check_name)
    if not rerun_checks:
        return None
    return ReviewRescueResult(
        pr_repo=pr_repo,
        pr_number=pr_number,
        rerun_checks=tuple(rerun_checks),
    )


def _apply_review_rescue_decision(
    *,
    pr_repo: str,
    pr_number: int,
    snapshot: ReviewSnapshot,
    action: str,
    reason: str | None,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
    pr_port: _PullRequestPort,
    review_state_port: _ReviewStatePort,
    board_port: _BoardMutationPort,
) -> ReviewRescueResult:
    """Apply the domain rescue decision through the port boundary."""
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
            automation_config=automation_config,
            dry_run=dry_run,
            pr_author=snapshot.pr_author,
            pr_body=snapshot.pr_body,
            pr_port=pr_port,
            review_state_port=review_state_port,
            board_port=board_port,
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
            review_state_port=review_state_port,
        )
        return ReviewRescueResult(
            pr_repo=pr_repo,
            pr_number=pr_number,
            auto_merge_enabled=code == 0,
            blocked_reason=None if code == 0 else "automerge-not-enabled",
        )
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
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
) -> ReviewRescueSweep:
    """Run review rescue across all governed repos."""
    if pr_port is None:
        pr_port = _default_pr_port(project_owner, project_number, config, gh_runner)
    if review_state_port is None:
        review_state_port = _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner,
        )
    if board_port is None:
        board_port = _default_board_mutation_port(
            project_owner,
            project_number,
            config,
            gh_runner,
        )
    repos = _execution_authority_repo_slugs(config, automation_config)
    rerun: list[str] = []
    auto_merge_enabled: list[str] = []
    requeued: list[str] = []
    blocked: list[str] = []
    skipped: list[str] = []
    scanned_prs = 0

    for pr_repo in repos:
        for pr in pr_port.list_open_prs(pr_repo):
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
                review_state_port=review_state_port,
                board_port=board_port,
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
    review_state_port: _ReviewStatePort | None = None,
) -> tuple[int, str]:
    """Auto-merge PR when codex gate + required checks pass."""
    if pr_port is None:
        pr_port = _default_pr_port(project_owner, project_number, config, gh_runner)
    if review_state_port is None:
        review_state_port = _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner,
        )

    if snapshot is not None:
        review_refs = list(snapshot.review_refs)
        copilot_review_present = snapshot.copilot_review_present
        gate_code = snapshot.codex_gate_code
        gate_msg = snapshot.codex_gate_message
        status = snapshot.gate_status
    else:
        review_refs = []
        for issue_ref in pr_port.linked_issue_refs(pr_repo, pr_number):
            if review_state_port.get_issue_status(issue_ref) == "Review":
                review_refs.append(issue_ref)
        if not review_refs:
            return 2, (
                f"{pr_repo}#{pr_number}: not in board Review scope; "
                "automerge controller no-op"
            )
        copilot_review_present = pr_port.has_copilot_review_signal(
            pr_repo,
            pr_number,
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
    pr_context = _load_execution_policy_pr_context(
        pr_repo=pr_repo,
        pr_number=pr_number,
        gh_runner=gh_runner,
    )
    actor = pr_context.actor
    state = pr_context.state
    provenance = pr_context.provenance
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
    linked = query_closing_issues(owner, repo, pr_number, config, gh_runner=gh_runner)
    protected_linked = _select_execution_policy_issues(
        linked=linked,
        automation_config=automation_config,
        legacy_copilot_only_mode=legacy_copilot_only_mode,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        gh_runner=gh_runner,
    )

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
        _apply_execution_policy_to_issue(
            issue=issue,
            policy=policy,
            decision=decision,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            dry_run=dry_run,
            pr_url=pr_context.url,
            board_info_resolver=board_info_resolver,
            board_mutator=noop_mutator if dry_run else board_mutator,
            gh_runner=gh_runner,
        )

    decision.enforced_pr = True
    _post_execution_policy_pr_comment(
        pr_repo=pr_repo,
        pr_number=pr_number,
        owner=owner,
        repo=repo,
        policy=policy,
        dry_run=dry_run,
        gh_runner=gh_runner,
    )

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


@dataclass(frozen=True)
class _ExecutionPolicyPrContext:
    actor: str
    state: str
    url: str
    provenance: dict[str, str] | None


def _load_execution_policy_pr_context(
    *,
    pr_repo: str,
    pr_number: int,
    gh_runner: Callable[..., str] | None,
) -> _ExecutionPolicyPrContext:
    """Load the PR context needed for execution policy decisions."""
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
    url = str(pr_data.get("url") or "").strip()
    body = str(pr_data.get("body") or "")
    return _ExecutionPolicyPrContext(
        actor=actor,
        state=state,
        url=url,
        provenance=_parse_consumer_provenance(body),
    )


def _select_execution_policy_issues(
    *,
    linked: Sequence[LinkedIssue],
    automation_config: BoardAutomationConfig,
    legacy_copilot_only_mode: bool,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    gh_runner: Callable[..., str] | None,
) -> list[LinkedIssue]:
    """Filter linked issues to the protected execution-policy scope."""
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
    return protected_linked


def _apply_execution_policy_to_issue(
    *,
    issue: LinkedIssue,
    policy: str,
    decision: ExecutionPolicyDecision,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    dry_run: bool,
    pr_url: str,
    board_info_resolver: Callable[..., BoardInfo] | None,
    board_mutator: Callable[..., None] | None,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Apply the execution policy to one protected linked issue."""
    changed, old_status = _set_status_if_changed(
        issue.ref,
        {"In Progress", "Review", "Ready"},
        "Blocked" if policy == "block" else "Ready",
        config,
        project_owner,
        project_number,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
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
    if _comment_exists(issue.owner, issue.repo, issue.number, marker, gh_runner=gh_runner):
        return
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
        f"PR: {pr_url}\n"
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


def _post_execution_policy_pr_comment(
    *,
    pr_repo: str,
    pr_number: int,
    owner: str,
    repo: str,
    policy: str,
    dry_run: bool,
    gh_runner: Callable[..., str] | None,
) -> None:
    """Post the top-level execution policy PR comment if absent."""
    pr_marker = f"<!-- {MARKER_PREFIX}:execution-policy-pr:{pr_repo}#{pr_number} -->"
    if _comment_exists(owner, repo, pr_number, pr_marker, gh_runner=gh_runner):
        return
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
