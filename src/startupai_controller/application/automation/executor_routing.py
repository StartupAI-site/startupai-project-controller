"""Protected queue executor routing — normalize executor fields on queue items."""

from __future__ import annotations

from collections.abc import Sequence

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    ExecutorRoutingDecision,
    IssueSnapshot,
    ProjectItemSnapshot,
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
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.review_state import ReviewStatePort


def protected_queue_executor_target(
    automation_config: BoardAutomationConfig | None,
) -> str | None:
    """Return the sole protected execution executor when routing is deterministic."""
    if automation_config is None:
        return None
    return _domain_protected_queue_executor_target(
        execution_authority_mode=automation_config.execution_authority_mode,
        execution_authority_executors=tuple(
            automation_config.execution_authority_executors
        ),
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
    review_state_port: ReviewStatePort | None = None,
    board_port: BoardMutationPort | None = None,
) -> ExecutorRoutingDecision:
    """Normalize protected Backlog/Ready queue items onto the local executor lane."""
    del project_owner, project_number
    decision = ExecutorRoutingDecision()
    target_executor = protected_queue_executor_target(automation_config)
    if target_executor is None:
        decision.skipped.append(("*", "no-deterministic-executor-target"))
        return decision
    assert automation_config is not None

    for status in statuses:
        items: Sequence[IssueSnapshot | ProjectItemSnapshot]
        if board_snapshot is not None:
            items = list(board_snapshot.items_with_status(status))
        elif review_state_port is not None:
            items = review_state_port.list_issues_by_status(status)
        else:
            items = []
        for snapshot in items:
            issue_ref = _snapshot_to_issue_ref(
                snapshot.issue_ref, config.issue_prefixes
            )
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
                if review_state_port is not None:
                    info = next(
                        (
                            item
                            for item in review_state_port.build_board_snapshot().items
                            if item.issue_ref == issue_ref
                        ),
                        None,
                    )
                    if info is None:
                        decision.skipped.append((issue_ref, "not-on-board"))
                        continue
                    project_id = info.project_id
                    item_id = info.item_id
                else:
                    decision.skipped.append((issue_ref, "missing-board-ids"))
                    continue

            if not dry_run and board_port is not None:
                board_port.set_project_single_select(
                    project_id,
                    item_id,
                    "Executor",
                    target_executor,
                )
            decision.routed.append(issue_ref)

    return decision
