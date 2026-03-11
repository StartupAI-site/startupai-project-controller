"""Execution/advisory shell wiring extracted from board_automation.py.

Each function injects concrete shell-level dependencies (port factories,
query helpers, field helpers) into the application-layer wiring functions.
A lazy ``_core()`` import avoids circular dependency since
``board_automation`` re-exports these names.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from startupai_controller.ports.board_mutations import BoardMutationPort as _BoardMutationPort
    from startupai_controller.ports.pull_requests import PullRequestPort as _PullRequestPort
    from startupai_controller.ports.review_state import ReviewStatePort as _ReviewStatePort
else:
    _BoardMutationPort = None
    _PullRequestPort = None
    _ReviewStatePort = None

from startupai_controller.board_automation_config import (
    BoardAutomationConfig,
    DEFAULT_PROJECT_NUMBER,
    DEFAULT_PROJECT_OWNER,
    DEFAULT_REBALANCE_CYCLE_MINUTES,
)
from startupai_controller.domain.models import (
    DispatchResult,
    ExecutionPolicyDecision,
    RebalanceDecision,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
)

from startupai_controller.application.automation.audit_in_progress import (
    wire_audit_in_progress as _wiring_audit_in_progress,
)
from startupai_controller.application.automation.dispatch_agent import (
    wire_dispatch_agent as _wiring_dispatch_agent,
)
from startupai_controller.application.automation.execution_policy import (
    wire_enforce_execution_policy as _wiring_enforce_execution_policy,
)
from startupai_controller.application.automation.rebalance import (
    wire_rebalance_wip as _wiring_rebalance_wip,
)
from startupai_controller.application.automation.codex_fail_routing import (
    apply_codex_fail_routing as _app_apply_codex_fail_routing,
)
from startupai_controller.automation_board_state_helpers import (
    transition_issue_status as _helpers_transition_issue_status,
)


# ---------------------------------------------------------------------------
# Lazy import to break circular dependency with board_automation
# ---------------------------------------------------------------------------


def _core():
    from startupai_controller import board_automation as core

    return core


# ---------------------------------------------------------------------------
# Shared board-transition seam wrapper
# ---------------------------------------------------------------------------


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
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[bool, str]:
    """Transition issue status through ports, with legacy fallback for tests."""
    core = _core()
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
        default_review_state_port_fn=core._default_review_state_port,
        default_board_mutation_port_fn=core._default_board_mutation_port,
        legacy_board_status_mutator_fn=core._legacy_board_status_mutator,
        app_transition_issue_status_fn=core._app_transition_issue_status,
    )


# ---------------------------------------------------------------------------
# Codex fail routing
# ---------------------------------------------------------------------------


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
    board_info_resolver: Callable | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Route failed codex review back to In Progress with explicit handoff."""
    core = _core()
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
        transition_issue_status_fn=core._transition_issue_status,
        default_review_state_port_fn=core._default_review_state_port,
        default_board_mutation_port_fn=core._default_board_mutation_port,
        query_project_item_field_fn=core._query_project_item_field,
        issue_ref_to_repo_parts_fn=core._issue_ref_to_repo_parts,
        comment_exists_fn=core._comment_exists,
    )


# ---------------------------------------------------------------------------
# Audit In Progress
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
    board_info_resolver: Callable | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Escalate stale In Progress issues with no linked PR."""
    core = _core()
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
        default_review_state_port_fn=core._default_review_state_port,
        default_board_mutation_port_fn=core._default_board_mutation_port,
        list_project_items_by_status_fn=core._list_project_items_by_status,
        query_issue_board_info_fn=core._query_issue_board_info,
        set_single_select_field_fn=core._set_single_select_field,
        comment_exists_fn=core._comment_exists,
        post_comment_fn=core._post_comment,
        query_project_item_field_fn=core._query_project_item_field,
        query_issue_updated_at_fn=core._query_issue_updated_at,
        snapshot_to_issue_ref_fn=core._snapshot_to_issue_ref,
    )


# ---------------------------------------------------------------------------
# Dispatch Agent
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
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> DispatchResult:
    """Dispatch eligible In Progress issues according to dispatch target."""
    core = _core()
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
        default_review_state_port_fn=core._default_review_state_port,
        default_board_mutation_port_fn=core._default_board_mutation_port,
        query_issue_board_info_fn=core._query_issue_board_info,
        query_project_item_field_fn=core._query_project_item_field,
        comment_exists_fn=core._comment_exists,
        post_comment_fn=core._post_comment,
    )


# ---------------------------------------------------------------------------
# Rebalance WIP
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
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    status_resolver: Callable[..., str] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> RebalanceDecision:
    """Rebalance In Progress lanes with stale demotion and dependency blocking."""
    core = _core()
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
        default_review_state_port_fn=core._default_review_state_port,
        default_board_mutation_port_fn=core._default_board_mutation_port,
        list_project_items_by_status_fn=core._list_project_items_by_status,
        is_graph_member_fn=core.in_any_critical_path,
        ready_promotion_evaluator_fn=core.evaluate_ready_promotion,
        set_blocked_with_reason_fn=core._set_blocked_with_reason,
        set_handoff_target_fn=core._set_handoff_target,
        query_project_item_field_fn=core._query_project_item_field,
        query_open_pr_updated_at_fn=core._query_open_pr_updated_at,
        query_latest_wip_activity_timestamp_fn=core._query_latest_wip_activity_timestamp,
        query_latest_marker_timestamp_fn=core._query_latest_marker_timestamp,
        comment_exists_fn=core._comment_exists,
        transition_issue_status_fn=core._transition_issue_status,
        snapshot_to_issue_ref_fn=core._snapshot_to_issue_ref,
    )


# ---------------------------------------------------------------------------
# Enforce Execution Policy
# ---------------------------------------------------------------------------


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
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ExecutionPolicyDecision:
    """Enforce local execution authority for protected coding PRs."""
    core = _core()
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
        default_pr_port_fn=core._default_pr_port,
        is_copilot_actor_fn=core._is_copilot_coding_agent_actor,
        query_project_item_field_fn=core._query_project_item_field,
        set_status_if_changed_fn=core._set_status_if_changed,
        query_issue_assignees_fn=core._query_issue_assignees,
        set_issue_assignees_fn=core._set_issue_assignees,
        comment_exists_fn=core._comment_exists,
        post_comment_fn=core._post_comment,
        close_pull_request_fn=core.close_pull_request,
        query_closing_issues_fn=core.query_closing_issues,
    )
