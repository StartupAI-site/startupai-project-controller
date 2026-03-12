"""Interrupted-session recovery for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class RecoveryDeps:
    """Injected seams for interrupted-session recovery."""

    gh_query_error_type: type[Exception]
    load_automation_config: Callable[[Any], Any]
    resolve_issue_coordinates: Callable[..., tuple[str, str, int]]
    classify_open_pr_candidates: Callable[..., tuple[str, Any | None, str]]
    return_issue_to_ready: Callable[..., None]
    transition_issue_to_review: Callable[..., None]
    set_blocked_with_reason: Callable[..., None]


def recover_interrupted_sessions(
    config: Any,
    db: Any,
    *,
    recovered: list[Any] | None = None,
    cp_config: Any,
    deps: RecoveryDeps,
    automation_config: Any | None = None,
    pr_port: Any,
    review_state_port: Any,
    board_port: Any,
    log_error: Callable[[str, Exception], None] | None = None,
) -> list[Any]:
    """Recover leases left behind by a previous interrupted daemon process."""
    recovered_leases = recovered if recovered is not None else db.recover_interrupted_leases()
    if not recovered_leases:
        return []

    for lease in recovered_leases:
        try:
            owner, repo, number = deps.resolve_issue_coordinates(lease.issue_ref, cp_config)
            try:
                effective_automation_config = automation_config or deps.load_automation_config(
                    config.automation_config_path
                )
                classification, pr_match, _reason = deps.classify_open_pr_candidates(
                    lease.issue_ref,
                    owner,
                    repo,
                    number,
                    effective_automation_config,
                    expected_branch=lease.branch_name,
                    pr_port=pr_port,
                )
            except deps.gh_query_error_type:
                classification, pr_match = ("none", None)

            pr_url = lease.pr_url or (pr_match.url if pr_match is not None else None)
            if lease.session_kind == "repair":
                deps.return_issue_to_ready(
                    lease.issue_ref,
                    cp_config,
                    config.project_owner,
                    config.project_number,
                    from_statuses={"In Progress", "Review"},
                    review_state_port=review_state_port,
                    board_port=board_port,
                )
            elif pr_url or classification == "adoptable":
                deps.transition_issue_to_review(
                    lease.issue_ref,
                    cp_config,
                    config.project_owner,
                    config.project_number,
                    review_state_port=review_state_port,
                    board_port=board_port,
                )
            elif classification == "none":
                deps.return_issue_to_ready(
                    lease.issue_ref,
                    cp_config,
                    config.project_owner,
                    config.project_number,
                    review_state_port=review_state_port,
                    board_port=board_port,
                )
            else:
                deps.set_blocked_with_reason(
                    lease.issue_ref,
                    f"execution-authority:{classification}",
                    cp_config,
                    config.project_owner,
                    config.project_number,
                    review_state_port=review_state_port,
                    board_port=board_port,
                )
        except Exception as err:
            if log_error is not None:
                log_error(lease.issue_ref, err)

    return recovered_leases
