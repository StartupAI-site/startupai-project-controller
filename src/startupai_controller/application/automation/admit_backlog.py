"""Backlog admission use case."""

from __future__ import annotations

from startupai_controller.domain.models import AdmissionDecision
from startupai_controller.domain.scheduling_policy import admission_watermarks


def admit_backlog_items(
    *,
    config,
    automation_config,
    project_owner: str,
    project_number: int,
    dispatchable_repo_prefixes: tuple[str, ...] | None = None,
    active_lease_issue_refs: tuple[str, ...] = (),
    dry_run: bool = False,
    board_snapshot=None,
    review_state_port=None,
    pr_port=None,
    board_port=None,
    memo=None,
    gh_runner=None,
    protected_queue_executor_target,
    load_admission_source_items,
    partition_admission_source_items,
    build_provisional_candidates,
    evaluate_candidates,
    apply_candidates,
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

    target_executor = protected_queue_executor_target(automation_config)
    floor, cap = admission_watermarks(
        automation_config.global_concurrency,
        floor_multiplier=automation_config.admission.ready_floor_multiplier,
        cap_multiplier=automation_config.admission.ready_cap_multiplier,
    )
    if not automation_config.admission.enabled or target_executor is None:
        return AdmissionDecision(
            ready_count=0,
            ready_floor=floor,
            ready_cap=cap,
            needed=0,
            scanned_backlog=0,
        )

    items = load_admission_source_items(
        automation_config,
        review_state_port=review_state_port,
        board_snapshot=board_snapshot,
        gh_runner=gh_runner,
    )
    ready_count, backlog_items = partition_admission_source_items(
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

    provisional_candidates, skipped = build_provisional_candidates(
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

    (
        eligible,
        resolved,
        blocked,
        partial_failure,
        error,
        deep_evaluation_truncated,
    ) = evaluate_candidates(
        provisional_candidates,
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
    selected = eligible[:needed]
    admitted, mutation_partial_failure, mutation_error = apply_candidates(
        selected,
        executor=target_executor,
        assignment_owner=automation_config.admission.assignment_owner,
        board_port=board_port,
        project_owner=project_owner,
        project_number=project_number,
        config=config,
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
