"""State/admission/coordination shell wiring extracted from board_automation.py.

Each function injects concrete shell-level dependencies (port factories,
query helpers, field helpers) into the application-layer functions.
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
)
from startupai_controller.domain.models import (
    AdmissionCandidate,
    AdmissionDecision,
    AdmissionSkip,
    CycleBoardSnapshot,
    ExecutorRoutingDecision,
    LinkedIssue,
    ProjectItemSnapshot as _ProjectItemSnapshot,
    PromotionResult,
)
from startupai_controller.domain.scheduling_policy import (
    PROTECTED_QUEUE_ROUTING_STATUSES,
    controller_owned_admission as _domain_controller_owned_admission,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
)
from startupai_controller.runtime.wiring import (
    GitHubPortBundle,
    GitHubRuntimeMemo as CycleGitHubMemo,
)
from startupai_controller.application.automation.ready_claim import (
    _set_blocked_with_reason as _app_set_blocked_with_reason,
    _wip_limit_for_lane as _app_wip_limit_for_lane,
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
    post_claim_comment as _wiring_post_claim_comment,
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
from startupai_controller.application.automation.executor_routing import (
    route_protected_queue_executors as _app_route_protected_queue_executors,
    protected_queue_executor_target as _app_protected_queue_executor_target,
)
from startupai_controller.application.automation.board_field_helpers import (
    set_handoff_target as _app_set_handoff_target,
)
from startupai_controller.application.automation.review_wiring import (
    has_copilot_review_signal as _wiring_has_copilot_review_signal,
)
from startupai_controller.automation_board_state_helpers import (
    set_board_status as _helpers_set_board_status,
    set_status_if_changed as _helpers_set_status_if_changed,
    legacy_board_status_mutator as _helpers_legacy_board_status_mutator,
    set_blocked_with_reason as _helpers_set_blocked_with_reason,
    mark_issues_done as _helpers_mark_issues_done,
)


# ---------------------------------------------------------------------------
# Lazy import to break circular dependency with board_automation
# ---------------------------------------------------------------------------


def _core():
    from startupai_controller import board_automation as core

    return core


# ---------------------------------------------------------------------------
# Board-state mutation wrappers
# ---------------------------------------------------------------------------


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
    core = _core()
    _helpers_set_board_status(
        project_id,
        item_id,
        status,
        board_port=board_port,
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
        set_single_select_field_fn=core._set_single_select_field,
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
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[bool, str]:
    """Legacy-compatible status transition helper for test seams."""
    core = _core()
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
        query_issue_board_info_fn=core._query_issue_board_info,
        set_board_status_fn=core._set_board_status,
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
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set Status=Blocked and Blocked Reason on a board item."""
    core = _core()
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
        default_review_state_port_fn=core._default_review_state_port,
        default_board_mutation_port_fn=core._default_board_mutation_port,
        legacy_board_status_mutator_fn=core._legacy_board_status_mutator,
        app_set_blocked_with_reason_fn=_app_set_blocked_with_reason,
    )


def _legacy_board_status_mutator(
    project_owner: str,
    project_number: int,
    config: CriticalPathConfig,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> Callable[..., None]:
    """Adapt legacy project-item status helpers to the application boundary."""
    core = _core()
    return _helpers_legacy_board_status_mutator(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
        set_board_status_fn=core._set_board_status,
    )


def mark_issues_done(
    issues: list[LinkedIssue],
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    review_state_port: _ReviewStatePort | None = None,
    board_port: _BoardMutationPort | None = None,
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Mark linked issues as Done on the board. Returns list of refs marked Done."""
    core = _core()
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
        transition_issue_status_fn=core._transition_issue_status,
    )


def _wip_limit_for_lane(
    automation_config: BoardAutomationConfig | None,
    executor: str,
    lane: str,
    fallback: int,
) -> int:
    """Resolve WIP limit for an executor/lane pair."""
    return _app_wip_limit_for_lane(automation_config, executor, lane, fallback)


# ---------------------------------------------------------------------------
# Copilot review signal
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
        default_pr_port_fn=core._default_pr_port,
    )


# ---------------------------------------------------------------------------
# Promotion / auto-promote
# ---------------------------------------------------------------------------


def promote_to_ready(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    dry_run: bool = False,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    controller_owned_resolver: Callable[[str], bool] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[int, str]:
    """Validate and promote an issue from Backlog/Blocked to Ready."""
    core = _core()
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
        query_issue_board_info_fn=core._query_issue_board_info,
        default_board_mutation_port_fn=core._default_board_mutation_port,
    )


def _controller_owned_admission(
    issue_ref: str,
    automation_config: BoardAutomationConfig | None,
) -> bool:
    """Return True when protected Backlog -> Ready is controller-owned."""
    if automation_config is None:
        return False
    return _domain_controller_owned_admission(
        issue_ref,
        admission_enabled=automation_config.admission.enabled,
        execution_authority_mode=automation_config.execution_authority_mode,
        execution_authority_repos=automation_config.execution_authority_repos,
    )


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
    board_info_resolver: Callable | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> PromotionResult:
    """Promote eligible successors of a Done issue."""
    core = _core()
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
        promote_to_ready_fn=core.promote_to_ready,
        controller_owned_resolver_fn=lambda ref: core._controller_owned_admission(
            ref,
            automation_config,
        ),
        comment_exists_fn=core._comment_exists,
        issue_ref_to_repo_parts_fn=core._issue_ref_to_repo_parts,
        new_handoff_job_id_fn=core._new_handoff_job_id,
        default_board_mutation_port_fn=core._default_board_mutation_port,
    )


# ---------------------------------------------------------------------------
# Propagate blocker / reconcile handoffs
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
    board_info_resolver: Callable | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Propagate blocker info to successors. Returns list of commented refs."""
    core = _core()
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
        default_review_state_port_fn=core._default_review_state_port,
        default_board_mutation_port_fn=core._default_board_mutation_port,
        list_project_items_by_status_fn=core._list_project_items_by_status,
        snapshot_to_issue_ref_fn=core._snapshot_to_issue_ref,
        query_project_item_field_fn=core._query_project_item_field,
        comment_exists_fn=core._comment_exists,
        post_comment_fn=core._post_comment,
        issue_ref_to_repo_parts_fn=core._issue_ref_to_repo_parts,
    )


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
    core = _core()
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
        ensure_github_bundle_fn=core._ensure_github_bundle,
        set_blocked_with_reason_fn=core._set_blocked_with_reason,
    )


# ---------------------------------------------------------------------------
# Executor routing / handoff target
# ---------------------------------------------------------------------------


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
    core = _core()
    return _app_route_protected_queue_executors(
        config,
        automation_config,
        project_owner,
        project_number,
        statuses=statuses,
        dry_run=dry_run,
        board_snapshot=board_snapshot,
        gh_runner=gh_runner,
        default_board_mutation_port_fn=core._default_board_mutation_port,
        list_project_items_by_status_fn=core._list_project_items_by_status,
        query_issue_board_info_fn=core._query_issue_board_info,
        set_single_select_field_fn=core._set_single_select_field,
    )


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
    core = _core()
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
        default_review_state_port_fn=core._default_review_state_port,
        default_board_mutation_port_fn=core._default_board_mutation_port,
    )


# ---------------------------------------------------------------------------
# Admission pipeline
# ---------------------------------------------------------------------------


def _build_admission_pipeline_deps() -> _AdmissionPipelineDeps:
    """Build the wiring deps for the admission pipeline."""
    core = _core()
    return _wiring_build_admission_pipeline_deps(
        query_open_pull_requests_fn=core.query_open_pull_requests,
        list_issue_comment_bodies_fn=core.list_issue_comment_bodies,
        memoized_query_issue_body_fn=core.memoized_query_issue_body,
        mark_issues_done_fn=core.mark_issues_done,
        close_issue_fn=core.close_issue,
        set_blocked_with_reason_fn=core._set_blocked_with_reason,
        set_handoff_target_fn=core._set_handoff_target,
        default_board_mutation_port_fn=core._default_board_mutation_port,
        set_single_select_field_fn=core._set_single_select_field,
        set_text_field_fn=core._set_text_field,
        list_project_items_fn=core._list_project_items,
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
    core = _core()
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
        protected_queue_executor_target_fn=core._protected_queue_executor_target,
        load_admission_source_items_fn=core._load_admission_source_items,
        partition_admission_source_items_fn=core._partition_admission_source_items,
        build_provisional_candidates_fn=core._build_provisional_admission_candidates,
        evaluate_candidates_fn=core._evaluate_admission_candidates,
        apply_candidates_fn=core._apply_admitted_backlog_candidates,
        CycleGitHubMemo=CycleGitHubMemo,
    )


# ---------------------------------------------------------------------------
# Post-claim comment
# ---------------------------------------------------------------------------


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
    core = _core()
    _wiring_post_claim_comment(
        issue_ref,
        executor,
        config,
        board_port=board_port,
        comment_checker=comment_checker,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
        default_review_state_port_fn=core._default_review_state_port,
        default_board_mutation_port_fn=core._default_board_mutation_port,
        comment_exists_fn=core._comment_exists,
    )
