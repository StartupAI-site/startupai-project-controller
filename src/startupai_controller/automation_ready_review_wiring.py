"""Ready/review shell wiring extracted from board_automation.py.

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
    DEFAULT_MISSING_EXECUTOR_BLOCK_CAP,
    DEFAULT_PROJECT_NUMBER,
    DEFAULT_PROJECT_OWNER,
)
from startupai_controller.domain.models import (
    ClaimReadyResult,
    ReviewRescueResult,
    ReviewRescueSweep,
    ReviewSnapshot,
    SchedulingDecision,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
)
from startupai_controller.runtime.wiring import (
    GitHubPortBundle,
)

from startupai_controller.automation_port_helpers import (
    BoardInfo,
    _default_board_mutation_port,
    _default_pr_port,
    _default_review_state_port,
)
from startupai_controller.board_graph import (
    find_unmet_ready_dependencies,
)
from startupai_controller.application.automation.ready_wiring import (
    wire_schedule_ready_items as _wiring_wire_schedule_ready_items,
    wire_claim_ready_issue as _wiring_wire_claim_ready_issue,
    enforce_ready_dependency_guard as _wiring_enforce_ready_dependency_guard,
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

# ---------------------------------------------------------------------------
# Lazy import to break circular dependency with board_automation
# ---------------------------------------------------------------------------


def _core():
    from startupai_controller import board_automation as core

    return core


# ---------------------------------------------------------------------------
# Ready-lane wiring
# ---------------------------------------------------------------------------


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
    core = _core()
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
        legacy_board_status_mutator_fn=core._legacy_board_status_mutator,
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
    core = _core()
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
        legacy_board_status_mutator_fn=core._legacy_board_status_mutator,
    )


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
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Block Ready issues with unmet predecessors. Returns corrected refs."""
    core = _core()
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
        board_port=board_port,
        default_review_state_port_fn=_default_review_state_port,
        default_board_mutation_port_fn=_default_board_mutation_port,
        find_unmet_dependencies_fn=find_unmet_ready_dependencies,
    )


# ---------------------------------------------------------------------------
# Event resolution
# ---------------------------------------------------------------------------


def resolve_issues_from_event(
    event_path: str,
    config: CriticalPathConfig,
    *,
    pr_port: _PullRequestPort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[tuple[str, str, list[str] | None]]:
    """Parse GITHUB_EVENT_PATH -> list of (issue_ref, event_kind, failed_checks)."""
    core = _core()
    if pr_port is None:
        pr_port = _default_pr_port(
            DEFAULT_PROJECT_OWNER,
            DEFAULT_PROJECT_NUMBER,
            config,
            gh_runner=gh_runner,
        )
    return _app_resolve_issues_from_event(
        event_path,
        config,
        pr_port=pr_port,
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
    core = _core()
    if pr_port is None:
        pr_port = _default_pr_port(
            DEFAULT_PROJECT_OWNER,
            DEFAULT_PROJECT_NUMBER,
            config,
            gh_runner=gh_runner,
        )
    return _app_resolve_pr_to_issues(
        pr_repo,
        pr_number,
        config,
        pr_port=pr_port,
    )


# ---------------------------------------------------------------------------
# Review-lane wiring
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
    core = _core()
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
    core = _core()
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
        legacy_board_status_mutator_fn=core._legacy_board_status_mutator,
    )


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
    core = _core()
    pr_port = _default_pr_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    review_state_port = _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return _wiring_review_scope_refs(
        pr_repo,
        pr_number,
        pr_port=pr_port,
        review_state_port=review_state_port,
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
    pr_port: _PullRequestPort | None = None,
    review_state_port: _ReviewStatePort | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[int, str]:
    """Evaluate strict codex review verdict contract for a PR."""
    core = _core()
    if pr_port is None:
        pr_port = _default_pr_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    if review_state_port is None:
        review_state_port = _default_review_state_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    return _wiring_codex_review_gate(
        pr_repo,
        pr_number,
        config,
        automation_config,
        project_owner,
        project_number,
        dry_run=dry_run,
        apply_fail_routing=apply_fail_routing,
        pr_port=pr_port,
        review_state_port=review_state_port,
        apply_codex_fail_routing_fn=core._apply_codex_fail_routing,
    )


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
    core = _core()
    if pr_port is None:
        pr_port = _default_pr_port(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    review_state_port = _default_review_state_port(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    return _wiring_build_review_snapshot(
        pr_repo,
        pr_number,
        automation_config,
        pr_port=pr_port,
        review_state_port=review_state_port,
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
    core = _core()
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
        automerge_review_fn=core.automerge_review,
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
    core = _core()
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
        review_rescue_fn=core.review_rescue,
    )


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
    core = _core()
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
        codex_review_gate_fn=core.codex_review_gate,
    )
