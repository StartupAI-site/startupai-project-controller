"""Ready/admission port wiring — assemble ports and delegate to use-case modules.

This module contains the port-resolution and closure-building logic that was
previously inline in board_automation.py for the ready/admission family:
promote-to-ready, auto-promote, admit-backlog, schedule-ready, claim-ready,
enforce-ready-dependencies, and admission pipeline helpers.

Each public function here has the same signature as the corresponding
board_automation.py wrapper, but receives port-factory callables as injected
parameters instead of importing concrete factories directly.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Callable

from startupai_controller.board_automation_config import (
    BoardAutomationConfig,
    DEFAULT_MISSING_EXECUTOR_BLOCK_CAP,
    DEFAULT_PROJECT_NUMBER,
    DEFAULT_PROJECT_OWNER,
)
from startupai_controller.domain.models import (
    AdmissionCandidate,
    AdmissionDecision,
    AdmissionSkip,
    CycleBoardSnapshot,
    ClaimReadyResult,
    ProjectItemSnapshot,
    PromotionResult,
    SchedulingDecision,
    IssueSnapshot,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    evaluate_ready_promotion,
    parse_issue_ref,
)

from startupai_controller.application.automation.ready_claim import (
    BoardInfo,
    _post_claim_comment as _app_post_claim_comment,
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
from startupai_controller.application.automation.admission_helpers import (
    AdmissionPipelineDeps,
    admission_summary_payload as _app_admission_summary_payload,
    load_admission_source_items as _app_load_admission_source_items,
    partition_admission_source_items as _app_partition_admission_source_items,
    build_provisional_admission_candidates as _app_build_provisional_admission_candidates,
    evaluate_admission_candidates as _app_evaluate_admission_candidates,
    apply_admitted_backlog_candidates as _app_apply_admitted_backlog_candidates,
)


PROMOTABLE_STATUSES = frozenset({"Backlog", "Blocked"})


class _DelegatingPort:
    """Delegate unknown attributes to an underlying port implementation."""

    def __init__(self, base) -> None:
        self._base = base

    def __getattr__(self, name: str):
        return getattr(self._base, name)


def _split_repo_slug(repo: str) -> tuple[str, str]:
    owner, repo_name = repo.split("/", 1)
    return owner, repo_name


def _wrap_review_state_port(
    review_state_port,
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    comment_exists_fn: Callable[..., bool] | None = None,
    gh_runner: Callable[..., str] | None = None,
):
    """Overlay legacy status/comment seams onto a typed review-state port."""
    if review_state_port is None or (
        status_resolver is None
        and board_info_resolver is None
        and comment_exists_fn is None
    ):
        return review_state_port

    class _CompatReviewStatePort(_DelegatingPort):
        def get_issue_status(self, issue_ref: str):
            if board_info_resolver is not None:
                return board_info_resolver(
                    issue_ref,
                    config,
                    project_owner,
                    project_number,
                ).status
            if status_resolver is not None:
                return status_resolver(
                    issue_ref,
                    config,
                    project_owner,
                    project_number,
                )
            return self._base.get_issue_status(issue_ref)

        def comment_exists(self, repo: str, issue_number: int, marker: str) -> bool:
            if comment_exists_fn is not None:
                owner, repo_name = _split_repo_slug(repo)
                return comment_exists_fn(
                    owner,
                    repo_name,
                    issue_number,
                    marker,
                    gh_runner=gh_runner,
                )
            return self._base.comment_exists(repo, issue_number, marker)

    return _CompatReviewStatePort(review_state_port)


def _wrap_board_port(
    board_port,
    *,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
):
    """Overlay legacy board mutation/comment seams onto a typed board port."""
    if board_port is None or (board_mutator is None and comment_poster is None):
        return board_port

    class _CompatBoardMutationPort(_DelegatingPort):
        def set_issue_status(self, issue_ref: str, status: str) -> None:
            if board_mutator is not None and board_info_resolver is not None:
                info = board_info_resolver(
                    issue_ref,
                    config,
                    project_owner,
                    project_number,
                )
                board_mutator(info.project_id, info.item_id, status)
                return
            self._base.set_issue_status(issue_ref, status)

        def post_issue_comment(self, repo: str, issue_number: int, body: str) -> None:
            if comment_poster is not None:
                owner, repo_name = _split_repo_slug(repo)
                comment_poster(
                    owner,
                    repo_name,
                    issue_number,
                    body,
                    gh_runner=gh_runner,
                )
                return
            self._base.post_issue_comment(repo, issue_number, body)

    return _CompatBoardMutationPort(board_port)


# ---------------------------------------------------------------------------
# promote_to_ready
# ---------------------------------------------------------------------------


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
    *,
    query_issue_board_info_fn: Callable[..., BoardInfo] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
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

    resolve_info = board_info_resolver or query_issue_board_info_fn
    if resolve_info is None:
        raise ValueError("board_info_resolver or query_issue_board_info_fn required")
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
    elif default_board_mutation_port_fn is not None:
        default_board_mutation_port_fn(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        ).set_issue_status(issue_ref, "Ready")
    else:
        raise ValueError("board_mutator or default_board_mutation_port_fn required")

    return 0, (
        f"Current status: {info.status}\n"
        "Validator: PASS (all predecessors Done)\n"
        f"Transition: {info.status} -> Ready (promoted)"
    )


# ---------------------------------------------------------------------------
# auto_promote_successors
# ---------------------------------------------------------------------------


def auto_promote_successors(
    issue_ref: str,
    config: CriticalPathConfig,
    this_repo_prefix: str,
    project_owner: str,
    project_number: int,
    *,
    automation_config: BoardAutomationConfig | None = None,
    dry_run: bool = False,
    review_state_port=None,
    board_port=None,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    # Injected wiring callables
    promote_to_ready_fn: Callable[..., tuple[int, str]],
    controller_owned_resolver_fn: Callable[[str], bool],
    comment_exists_fn: Callable[..., bool],
    issue_ref_to_repo_parts_fn: Callable[..., tuple[str, str, int]],
    new_handoff_job_id_fn: Callable[[str, str], str],
    default_board_mutation_port_fn: Callable[..., object] | None = None,
) -> PromotionResult:
    """Promote eligible successors of a Done issue."""
    comment_checker_fn = comment_checker or comment_exists_fn
    if review_state_port is None:
        if comment_checker_fn is not None:
            review_state_port = SimpleNamespace(
                comment_exists=lambda repo, issue_number, marker: comment_checker_fn(
                    *_split_repo_slug(repo),
                    issue_number,
                    marker,
                    gh_runner=gh_runner,
                )
            )
    else:
        review_state_port = _wrap_review_state_port(
            review_state_port,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            comment_exists_fn=comment_checker_fn,
            gh_runner=gh_runner,
        )

    if board_port is None:
        if default_board_mutation_port_fn is not None:
            board_port = default_board_mutation_port_fn(
                project_owner,
                project_number,
                config,
                gh_runner=gh_runner,
            )
        elif comment_poster is not None:
            board_port = SimpleNamespace(
                post_issue_comment=lambda repo, issue_number, body: comment_poster(
                    *_split_repo_slug(repo),
                    issue_number,
                    body,
                    gh_runner=gh_runner,
                )
            )
    if board_port is not None:
        board_port = _wrap_board_port(
            board_port,
            config=config,
            project_owner=project_owner,
            project_number=project_number,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )

    def _promote_to_ready(issue_ref: str, **kwargs):
        kwargs.pop("review_state_port", None)
        kwargs.pop("board_port", None)
        kwargs["controller_owned_resolver"] = controller_owned_resolver_fn
        if status_resolver is not None:
            kwargs["status_resolver"] = status_resolver
        if board_info_resolver is not None:
            kwargs["board_info_resolver"] = board_info_resolver
        if board_mutator is not None:
            kwargs["board_mutator"] = board_mutator
        if gh_runner is not None:
            kwargs["gh_runner"] = gh_runner
        return promote_to_ready_fn(
            issue_ref=issue_ref,
            **kwargs,
        )

    return _app_auto_promote_successors(
        issue_ref=issue_ref,
        config=config,
        this_repo_prefix=this_repo_prefix,
        project_owner=project_owner,
        project_number=project_number,
        automation_config=automation_config,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        promote_to_ready=_promote_to_ready,
        controller_owned_resolver=controller_owned_resolver_fn,
        resolve_issue_parts=issue_ref_to_repo_parts_fn,
        new_handoff_job_id=new_handoff_job_id_fn,
    )


# ---------------------------------------------------------------------------
# Admission pipeline
# ---------------------------------------------------------------------------


def build_admission_pipeline_deps(
    *,
    review_state_port,
    pr_port,
    board_port,
    issue_context_port,
) -> AdmissionPipelineDeps:
    """Assemble the wiring deps for the admission pipeline."""
    return AdmissionPipelineDeps(
        review_state_port=review_state_port,
        pr_port=pr_port,
        board_port=board_port,
        issue_context_port=issue_context_port,
    )


def admission_summary_payload(
    decision: AdmissionDecision,
    *,
    enabled: bool,
) -> dict[str, object]:
    """Convert an AdmissionDecision into a JSON-friendly payload."""
    return _app_admission_summary_payload(decision, enabled=enabled)


def load_admission_source_items(
    automation_config: BoardAutomationConfig,
    *,
    deps: AdmissionPipelineDeps,
    review_state_port=None,
    board_snapshot: CycleBoardSnapshot | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[ProjectItemSnapshot]:
    """Load backlog/ready items needed for one admission pass."""
    del review_state_port, gh_runner
    return _app_load_admission_source_items(
        automation_config,
        deps=deps,
        board_snapshot=board_snapshot,
    )


def partition_admission_source_items(
    items: list[ProjectItemSnapshot],
    *,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    target_executor: str,
) -> tuple[int, list[ProjectItemSnapshot]]:
    """Count governed ready items and collect governed backlog items."""
    return _app_partition_admission_source_items(
        items,
        config=config,
        automation_config=automation_config,
        target_executor=target_executor,
    )


def build_provisional_admission_candidates(
    backlog_items: list[ProjectItemSnapshot],
    *,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    dispatchable_repo_prefixes: tuple[str, ...],
    active_lease_issue_refs: tuple[str, ...],
) -> tuple[list[ProjectItemSnapshot], list[AdmissionSkip]]:
    """Apply cheap exact admission filters to backlog items."""
    return _app_build_provisional_admission_candidates(
        backlog_items,
        config=config,
        automation_config=automation_config,
        dispatchable_repo_prefixes=dispatchable_repo_prefixes,
        active_lease_issue_refs=active_lease_issue_refs,
    )


def evaluate_admission_candidates(
    provisional_candidates: list[ProjectItemSnapshot],
    *,
    deps: AdmissionPipelineDeps,
    config: CriticalPathConfig,
    automation_config: BoardAutomationConfig,
    project_owner: str,
    project_number: int,
    needed: int,
    dry_run: bool,
    memo,
    skipped: list[AdmissionSkip],
    pr_port=None,
    review_state_port=None,
    board_port=None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[
    list[AdmissionCandidate],
    list[str],
    list[str],
    bool,
    str | None,
    bool,
]:
    """Run the expensive admission checks needed to choose candidates."""
    del pr_port, review_state_port, board_port, gh_runner
    return _app_evaluate_admission_candidates(
        provisional_candidates,
        deps=deps,
        config=config,
        automation_config=automation_config,
        project_owner=project_owner,
        project_number=project_number,
        needed=needed,
        dry_run=dry_run,
        memo=memo,
        skipped=skipped,
    )


def apply_admitted_backlog_candidates(
    selected: list[AdmissionCandidate],
    *,
    deps: AdmissionPipelineDeps,
    executor: str,
    assignment_owner: str,
    board_port=None,
    project_owner: str | None = None,
    project_number: int | None = None,
    config: CriticalPathConfig | None = None,
    dry_run: bool,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[list[str], bool, str | None]:
    """Apply backlog-to-ready mutations for the selected candidates."""
    del board_port, project_owner, project_number, config, gh_runner
    return _app_apply_admitted_backlog_candidates(
        selected,
        deps=deps,
        executor=executor,
        assignment_owner=assignment_owner,
        dry_run=dry_run,
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
    github_bundle=None,
    github_memo=None,
    gh_runner: Callable[..., str] | None = None,
    # Injected wiring callables
    protected_queue_executor_target_fn: Callable,
    load_admission_source_items_fn: Callable,
    partition_admission_source_items_fn: Callable,
    build_provisional_candidates_fn: Callable,
    evaluate_candidates_fn: Callable,
    apply_candidates_fn: Callable,
    CycleGitHubMemo: type | None = None,
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
        memo = github_memo
        if memo is None and CycleGitHubMemo is not None:
            memo = CycleGitHubMemo()

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
        protected_queue_executor_target=protected_queue_executor_target_fn,
        load_admission_source_items=load_admission_source_items_fn,
        partition_admission_source_items=partition_admission_source_items_fn,
        build_provisional_candidates=build_provisional_candidates_fn,
        evaluate_candidates=evaluate_candidates_fn,
        apply_candidates=apply_candidates_fn,
    )


# ---------------------------------------------------------------------------
# enforce_ready_dependency_guard
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
    board_info_resolver=None,
    board_mutator=None,
    gh_runner: Callable[..., str] | None = None,
    # Injected wiring callables
    review_state_port=None,
    board_port=None,
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    find_unmet_dependencies_fn: Callable[..., list[tuple[str, str]]],
) -> list[str]:
    """Block Ready issues with unmet predecessors. Returns corrected refs."""
    if review_state_port is None and default_review_state_port_fn is not None:
        review_state_port = default_review_state_port_fn(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    ready_items: list[IssueSnapshot] = []
    if review_state_port is not None:
        ready_items = review_state_port.list_issues_by_status("Ready")
    review_state_port = _wrap_review_state_port(
        review_state_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        gh_runner=gh_runner,
    )
    if board_port is None and default_board_mutation_port_fn is not None:
        board_port = default_board_mutation_port_fn(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    board_port = _wrap_board_port(
        board_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        board_info_resolver=board_info_resolver,
        board_mutator=board_mutator,
        gh_runner=gh_runner,
    )
    return _app_enforce_ready_dependency_guard(
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        ready_items=ready_items,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        dry_run=dry_run,
        review_state_port=review_state_port,
        board_port=board_port,
        find_unmet_dependencies=find_unmet_dependencies_fn,
    )


# ---------------------------------------------------------------------------
# schedule_ready_items
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
    review_state_port=None,
    dependency_review_state_port=None,
    board_port=None,
) -> SchedulingDecision:
    """Classify and optionally claim Ready issues.

    Port resolution and legacy-mutator adaptation are handled by
    the board_automation.py shell wrapper that calls this function.
    """
    return _app_schedule_ready_items(
        config,
        project_owner,
        project_number,
        review_state_port=review_state_port,
        dependency_review_state_port=dependency_review_state_port,
        board_port=board_port,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        mode=mode,
        per_executor_wip_limit=per_executor_wip_limit,
        automation_config=automation_config,
        missing_executor_block_cap=missing_executor_block_cap,
        dry_run=dry_run,
    )


# ---------------------------------------------------------------------------
# _post_claim_comment
# ---------------------------------------------------------------------------


def post_claim_comment(
    issue_ref: str,
    executor: str,
    config: CriticalPathConfig,
    *,
    review_state_port=None,
    board_port=None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    # Injected wiring callables
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    comment_exists_fn: Callable[..., bool] | None = None,
) -> None:
    """Post deterministic kickoff comment on successful claim."""
    if review_state_port is None and default_review_state_port_fn is not None:
        review_state_port = default_review_state_port_fn(
            DEFAULT_PROJECT_OWNER,
            DEFAULT_PROJECT_NUMBER,
            config,
            gh_runner=gh_runner,
        )
    if board_port is None and default_board_mutation_port_fn is not None:
        board_port = default_board_mutation_port_fn(
            DEFAULT_PROJECT_OWNER,
            DEFAULT_PROJECT_NUMBER,
            config,
            gh_runner=gh_runner,
        )
    review_state_port = _wrap_review_state_port(
        review_state_port,
        config=config,
        project_owner=DEFAULT_PROJECT_OWNER,
        project_number=DEFAULT_PROJECT_NUMBER,
        comment_exists_fn=comment_checker or comment_exists_fn,
        gh_runner=gh_runner,
    )
    board_port = _wrap_board_port(
        board_port,
        config=config,
        project_owner=DEFAULT_PROJECT_OWNER,
        project_number=DEFAULT_PROJECT_NUMBER,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )
    _app_post_claim_comment(
        issue_ref,
        executor,
        config,
        review_state_port=review_state_port,
        board_port=board_port,
    )


# ---------------------------------------------------------------------------
# wire_schedule_ready_items (port materialisation + legacy mutator)
# ---------------------------------------------------------------------------


def wire_schedule_ready_items(
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
    review_state_port=None,
    board_port=None,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver=None,
    board_mutator=None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helpers
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    legacy_board_status_mutator_fn: Callable[..., Callable] | None = None,
) -> SchedulingDecision:
    """Wire port materialisation + legacy mutator, then delegate to core."""
    review_state_port = review_state_port or default_review_state_port_fn(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    board_port = board_port or default_board_mutation_port_fn(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    status_mutator = board_mutator
    if status_mutator is None and board_info_resolver is not None:
        status_mutator = legacy_board_status_mutator_fn(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    review_state_port = _wrap_review_state_port(
        review_state_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        board_info_resolver=board_info_resolver,
        gh_runner=gh_runner,
    )
    dependency_review_state_port = _wrap_review_state_port(
        review_state_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        status_resolver=status_resolver,
        gh_runner=gh_runner,
    )
    board_port = _wrap_board_port(
        board_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        board_info_resolver=board_info_resolver,
        board_mutator=status_mutator,
        gh_runner=gh_runner,
    )
    return schedule_ready_items(
        config,
        project_owner,
        project_number,
        review_state_port=review_state_port,
        dependency_review_state_port=dependency_review_state_port,
        board_port=board_port,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        mode=mode,
        per_executor_wip_limit=per_executor_wip_limit,
        automation_config=automation_config,
        missing_executor_block_cap=missing_executor_block_cap,
        dry_run=dry_run,
    )


# ---------------------------------------------------------------------------
# wire_claim_ready_issue (port materialisation + legacy mutator)
# ---------------------------------------------------------------------------


def wire_claim_ready_issue(
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
    review_state_port=None,
    board_port=None,
    status_resolver: Callable[..., str] | None = None,
    board_info_resolver=None,
    board_mutator=None,
    comment_checker: Callable[..., bool] | None = None,
    comment_poster: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
    # Injected board-automation helpers
    default_review_state_port_fn: Callable[..., object] | None = None,
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    legacy_board_status_mutator_fn: Callable[..., Callable] | None = None,
) -> ClaimReadyResult:
    """Wire port materialisation + legacy mutator, then delegate to core."""
    review_state_port = review_state_port or default_review_state_port_fn(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    board_port = board_port or default_board_mutation_port_fn(
        project_owner,
        project_number,
        config,
        gh_runner=gh_runner,
    )
    status_mutator = board_mutator
    if status_mutator is None and board_info_resolver is not None:
        status_mutator = legacy_board_status_mutator_fn(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )
    review_state_port = _wrap_review_state_port(
        review_state_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        board_info_resolver=board_info_resolver,
        comment_exists_fn=comment_checker,
        gh_runner=gh_runner,
    )
    dependency_review_state_port = _wrap_review_state_port(
        review_state_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        status_resolver=status_resolver,
        gh_runner=gh_runner,
    )
    board_port = _wrap_board_port(
        board_port,
        config=config,
        project_owner=project_owner,
        project_number=project_number,
        board_info_resolver=board_info_resolver,
        board_mutator=status_mutator,
        comment_poster=comment_poster,
        gh_runner=gh_runner,
    )
    return _app_claim_ready_issue(
        config,
        project_owner,
        project_number,
        review_state_port=review_state_port,
        dependency_review_state_port=dependency_review_state_port,
        board_port=board_port,
        executor=executor,
        issue_ref=issue_ref,
        next_issue=next_issue,
        this_repo_prefix=this_repo_prefix,
        all_prefixes=all_prefixes,
        per_executor_wip_limit=per_executor_wip_limit,
        automation_config=automation_config,
        dry_run=dry_run,
    )
