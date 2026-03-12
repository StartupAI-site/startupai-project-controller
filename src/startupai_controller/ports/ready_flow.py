"""ReadyFlowPort — control-plane ready-queue routing and admission operations."""

from __future__ import annotations

from typing import Any, Protocol

from startupai_controller.domain.models import (
    AdmissionDecision,
    ClaimReadyResult,
    CycleBoardSnapshot,
    ExecutorRoutingDecision,
)


class ReadyFlowPort(Protocol):
    """Ready queue routing and backlog admission operations."""

    def route_protected_queue_executors(
        self,
        config: Any,
        automation_config: Any,
        project_owner: str,
        project_number: int,
        *,
        statuses: tuple[str, ...] = (),
        dry_run: bool = False,
        board_snapshot: CycleBoardSnapshot | None = None,
        gh_runner: Any = None,
    ) -> ExecutorRoutingDecision:
        """Normalize protected queue executor ownership."""
        ...

    def admit_backlog_items(
        self,
        config: Any,
        automation_config: Any,
        project_owner: str,
        project_number: int,
        *,
        dispatchable_repo_prefixes: tuple[str, ...] | None = None,
        active_lease_issue_refs=(),
        dry_run: bool = False,
        board_snapshot: CycleBoardSnapshot | None = None,
        github_bundle: Any = None,
        github_memo: Any = None,
        gh_runner: Any = None,
    ) -> AdmissionDecision:
        """Admit governed Backlog items into Ready."""
        ...

    def admission_summary_payload(
        self,
        decision: AdmissionDecision,
        *,
        enabled: bool,
    ) -> dict[str, object]:
        """Convert admission results into a JSON-friendly payload."""
        ...

    def claim_ready_issue(
        self,
        config: Any,
        project_owner: str,
        project_number: int,
        *,
        executor: str,
        issue_ref: str | None = None,
        next_issue: bool = False,
        this_repo_prefix: str | None = None,
        all_prefixes: bool = False,
        per_executor_wip_limit: int = 3,
        automation_config: Any = None,
        dry_run: bool = False,
        review_state_port: Any = None,
        board_port: Any = None,
        status_resolver: Any = None,
        board_info_resolver: Any = None,
        board_mutator: Any = None,
        comment_checker: Any = None,
        comment_poster: Any = None,
        gh_runner: Any = None,
    ) -> ClaimReadyResult:
        """Claim one Ready issue for a specific executor."""
        ...
