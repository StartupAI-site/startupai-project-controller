"""Protected queue executor routing — normalize executor fields on queue items."""

from __future__ import annotations

from typing import Callable

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ExecutorRoutingDecision,
)
from startupai_controller.domain.scheduling_policy import (
    PROTECTED_QUEUE_ROUTING_STATUSES,
    protected_queue_executor_target as _domain_protected_queue_executor_target,
    snapshot_to_issue_ref as _snapshot_to_issue_ref,
)
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    parse_issue_ref,
)


def protected_queue_executor_target(
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
    # Injected board-automation helpers
    default_board_mutation_port_fn: Callable[..., object] | None = None,
    list_project_items_by_status_fn: Callable[..., list] | None = None,
    query_issue_board_info_fn: Callable[..., object] | None = None,
    set_single_select_field_fn: Callable[..., None] | None = None,
) -> ExecutorRoutingDecision:
    """Normalize protected Backlog/Ready queue items onto the local executor lane."""
    decision = ExecutorRoutingDecision()
    target_executor = protected_queue_executor_target(automation_config)
    if target_executor is None:
        decision.skipped.append(("*", "no-deterministic-executor-target"))
        return decision
    assert automation_config is not None
    board_port = None
    if not dry_run and default_board_mutation_port_fn is not None:
        board_port = default_board_mutation_port_fn(
            project_owner,
            project_number,
            config,
            gh_runner=gh_runner,
        )

    for status in statuses:
        if board_snapshot is not None:
            items = board_snapshot.items_with_status(status)
        elif list_project_items_by_status_fn is not None:
            items = list_project_items_by_status_fn(
                status,
                project_owner,
                project_number,
                gh_runner=gh_runner,
            )
        else:
            items = []
        for snapshot in items:
            issue_ref = _snapshot_to_issue_ref(snapshot.issue_ref, config.issue_prefixes)
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
                if query_issue_board_info_fn is not None:
                    info = query_issue_board_info_fn(
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
                else:
                    decision.skipped.append((issue_ref, "missing-board-ids"))
                    continue

            if not dry_run and board_port is not None and set_single_select_field_fn is not None:
                set_single_select_field_fn(
                    project_id,
                    item_id,
                    "Executor",
                    target_executor,
                    board_port=board_port,
                    project_owner=project_owner,
                    project_number=project_number,
                    config=config,
                    gh_runner=gh_runner,
                )
            decision.routed.append(issue_ref)

    return decision
