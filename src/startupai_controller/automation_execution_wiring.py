"""Execution/advisory shell wiring extracted from board_automation.py.

Each function injects concrete shell-level dependencies (port factories,
query helpers, field helpers) into the application-layer wiring functions.
A lazy ``_core()`` import avoids circular dependency since
``board_automation`` re-exports these names.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from startupai_controller.ports.board_mutations import (
        BoardMutationPort as _BoardMutationPort,
    )
    from startupai_controller.ports.pull_requests import (
        PullRequestPort as _PullRequestPort,
    )
    from startupai_controller.ports.review_state import (
        ReviewStatePort as _ReviewStatePort,
    )
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
    audit_in_progress as _app_audit_in_progress,
)
from startupai_controller.application.automation.dispatch_agent import (
    dispatch_agent as _app_dispatch_agent,
)
from startupai_controller.application.automation.execution_policy import (
    enforce_execution_policy as _app_enforce_execution_policy,
    load_execution_policy_pr_context as _load_execution_policy_pr_context,
)
from startupai_controller.application.automation.rebalance import (
    rebalance_wip as _app_rebalance_wip,
)
from startupai_controller.application.automation.codex_fail_routing import (
    apply_codex_fail_routing as _app_apply_codex_fail_routing,
)
from startupai_controller.automation_compat_ports import (
    wrap_board_port,
    wrap_review_state_port,
)
from startupai_controller.automation_board_state_helpers import (
    transition_issue_status as _helpers_transition_issue_status,
)
from startupai_controller.automation_port_helpers import (
    _default_board_mutation_port,
    _default_pr_port,
    _default_review_state_port,
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
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
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
    if review_state_port is None:
        review_state_port = _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    if board_port is None:
        board_port = _default_board_mutation_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
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
        issue_ref_to_repo_parts_fn=core._issue_ref_to_repo_parts,
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
    pr_port: _PullRequestPort | None = None,
    board_info_resolver: Callable | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Escalate stale In Progress issues with no linked PR."""
    core = _core()
    del board_info_resolver, comment_checker, comment_poster
    if review_state_port is None:
        review_state_port = _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    if board_port is None:
        board_port = _default_board_mutation_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    if pr_port is None:
        pr_port = _default_pr_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    return _app_audit_in_progress(
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        max_age_hours=max_age_hours,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        pr_port=pr_port,
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
    del board_info_resolver, board_mutator
    if review_state_port is None:
        review_state_port = _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    if board_port is None:
        board_port = _default_board_mutation_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    return _app_dispatch_agent(
        issue_refs=issue_refs,
        config=config,
        dispatch_target=automation_config.dispatch_target,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
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
    pr_port: _PullRequestPort | None = None,
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    status_resolver: Callable[..., str] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> RebalanceDecision:
    """Rebalance In Progress lanes with stale demotion and dependency blocking."""
    core = _core()
    if review_state_port is None:
        review_state_port = _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    if board_port is None:
        board_port = _default_board_mutation_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    if pr_port is None:
        pr_port = _default_pr_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )

    in_progress = review_state_port.list_issues_by_status("In Progress")
    review_state_port = wrap_review_state_port(
        review_state_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        status_resolver=status_resolver,
        comment_exists_fn=core._comment_exists,
        gh_runner=gh_runner,
    )
    board_port = wrap_board_port(
        board_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    return _app_rebalance_wip(
        config=config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        in_progress_items=in_progress,
        review_state_port=review_state_port,
        board_port=board_port,
        pr_port=pr_port,
        is_graph_member=core.in_any_critical_path,
        ready_promotion_evaluator=core.evaluate_ready_promotion,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        cycle_minutes=cycle_minutes,
        dry_run=dry_run,
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
    pr_port: _PullRequestPort | None = None,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> ExecutionPolicyDecision:
    """Enforce local execution authority for protected coding PRs."""
    core = _core()
    del board_info_resolver, board_mutator
    if pr_port is None:
        pr_port = _default_pr_port(
            project_owner,
            project_number,
            config=config,
            gh_runner=gh_runner,
        )
    if review_state_port is None:
        review_state_port = _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    if board_port is None:
        board_port = _default_board_mutation_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    pr_context = _load_execution_policy_pr_context(
        pr_repo=pr_repo,
        pr_number=pr_number,
        pr_port=pr_port,
    )
    return _app_enforce_execution_policy(
        pr_repo=pr_repo,
        pr_number=pr_number,
        actor=pr_context.actor,
        state=pr_context.state,
        pr_url=pr_context.url,
        provenance=pr_context.provenance,
        config=config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        allow_copilot_coding_agent=allow_copilot_coding_agent,
        dry_run=dry_run,
        pr_port=pr_port,
        review_state_port=review_state_port,
        board_port=board_port,
        is_copilot_actor=core._is_copilot_coding_agent_actor,
    )
