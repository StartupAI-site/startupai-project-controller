"""Interrupted-session recovery for the consumer application layer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.consumer_config import ConsumerConfig
from startupai_controller.domain.models import OpenPullRequestMatch
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.pull_requests import PullRequestPort
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig


class RecoveredLeaseInfo(Protocol):
    """Recovered interrupted session lease metadata needed for board recovery."""

    issue_ref: str
    branch_name: str | None
    pr_url: str | None
    repair_pr_url: str | None
    session_kind: str


class RecoveryStatePort(Protocol):
    """Persistence operations required by interrupted-session recovery."""

    def recover_interrupted_leases(
        self,
        *,
        failure_reasons_by_session_id: dict[str, str] | None = None,
    ) -> list[RecoveredLeaseInfo]:
        """Return and clear interrupted leases from local runtime state."""
        ...


class LoadAutomationConfigFn(Protocol):
    """Load board automation policy config from disk."""

    def __call__(self, path: Path) -> BoardAutomationConfig:
        """Return the parsed automation config."""
        ...


class ResolveIssueCoordinatesFn(Protocol):
    """Resolve one canonical issue ref into owner/repo/issue-number coordinates."""

    def __call__(
        self,
        issue_ref: str,
        config: CriticalPathConfig,
    ) -> tuple[str, str, int]:
        """Return (owner, repo, number) for an issue ref."""
        ...


class ClassifyOpenPrCandidatesFn(Protocol):
    """Classify open PR candidates for interrupted-session recovery."""

    def __call__(
        self,
        issue_ref: str,
        owner: str,
        repo: str,
        issue_number: int,
        automation_config: BoardAutomationConfig,
        *,
        expected_branch: str | None = None,
        pr_port: PullRequestPort,
    ) -> tuple[str, OpenPullRequestMatch | None, str]:
        """Return (classification, matching_pr, reason_code)."""
        ...


class ReturnIssueToReadyFn(Protocol):
    """Move an interrupted issue back to Ready."""

    def __call__(
        self,
        issue_ref: str,
        config: CriticalPathConfig,
        project_owner: str,
        project_number: int,
        *,
        from_statuses: set[str] | None = None,
        review_state_port: ReviewStatePort,
        board_port: BoardMutationPort,
    ) -> None:
        """Apply the Ready transition if allowed."""
        ...


class TransitionIssueToReviewFn(Protocol):
    """Move an interrupted issue into Review."""

    def __call__(
        self,
        issue_ref: str,
        config: CriticalPathConfig,
        project_owner: str,
        project_number: int,
        *,
        review_state_port: ReviewStatePort,
        board_port: BoardMutationPort,
    ) -> None:
        """Apply the Review transition."""
        ...


class SetBlockedWithReasonFn(Protocol):
    """Block an interrupted issue with a reason code."""

    def __call__(
        self,
        issue_ref: str,
        reason: str,
        config: CriticalPathConfig,
        project_owner: str,
        project_number: int,
        *,
        review_state_port: ReviewStatePort,
        board_port: BoardMutationPort,
    ) -> None:
        """Apply the Blocked transition."""
        ...


@dataclass(frozen=True)
class RecoveryDeps:
    """Injected seams for interrupted-session recovery."""

    gh_query_error_type: type[Exception]
    load_automation_config: LoadAutomationConfigFn
    resolve_issue_coordinates: ResolveIssueCoordinatesFn
    classify_open_pr_candidates: ClassifyOpenPrCandidatesFn
    return_issue_to_ready: ReturnIssueToReadyFn
    transition_issue_to_review: TransitionIssueToReviewFn
    set_blocked_with_reason: SetBlockedWithReasonFn


def recover_interrupted_sessions(
    config: ConsumerConfig,
    db: RecoveryStatePort,
    *,
    recovered: list[RecoveredLeaseInfo] | None = None,
    cp_config: CriticalPathConfig,
    deps: RecoveryDeps,
    automation_config: BoardAutomationConfig | None = None,
    pr_port: PullRequestPort,
    review_state_port: ReviewStatePort,
    board_port: BoardMutationPort,
    log_error: Callable[[str, Exception], None] | None = None,
) -> list[RecoveredLeaseInfo]:
    """Recover leases left behind by a previous interrupted daemon process."""
    recovered_leases = (
        recovered if recovered is not None else db.recover_interrupted_leases()
    )
    if not recovered_leases:
        return []

    for lease in recovered_leases:
        try:
            owner, repo, number = deps.resolve_issue_coordinates(
                lease.issue_ref, cp_config
            )
            try:
                effective_automation_config = (
                    automation_config
                    or deps.load_automation_config(config.automation_config_path)
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

            pr_url = (
                lease.pr_url
                or lease.repair_pr_url
                or (pr_match.url if pr_match is not None else None)
            )
            if lease.session_kind == "repair" and (
                pr_url is not None or classification == "adoptable"
            ):
                deps.transition_issue_to_review(
                    lease.issue_ref,
                    cp_config,
                    config.project_owner,
                    config.project_number,
                    review_state_port=review_state_port,
                    board_port=board_port,
                )
            elif lease.session_kind == "repair":
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
