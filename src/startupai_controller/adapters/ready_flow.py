"""Adapter for ready-queue routing and backlog admission operations."""

from __future__ import annotations

from typing import Any

from startupai_controller.domain.models import (
    AdmissionDecision,
    ClaimReadyResult,
    CycleBoardSnapshot,
    ExecutorRoutingDecision,
)


class BoardAutomationReadyFlowAdapter:
    """Expose the existing ready-flow behavior behind a typed port."""

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
        from startupai_controller.board_automation import (
            route_protected_queue_executors,
        )

        return route_protected_queue_executors(
            config,
            automation_config,
            project_owner,
            project_number,
            statuses=statuses,
            dry_run=dry_run,
            board_snapshot=board_snapshot,
            gh_runner=gh_runner,
        )

    def admit_backlog_items(
        self,
        config: Any,
        automation_config: Any,
        project_owner: str,
        project_number: int,
        *,
        dispatchable_repo_prefixes: tuple[str, ...] | None = None,
        active_lease_issue_refs: tuple[str, ...] = (),
        dry_run: bool = False,
        board_snapshot: CycleBoardSnapshot | None = None,
        github_bundle: Any = None,
        github_memo: Any = None,
        gh_runner: Any = None,
    ) -> AdmissionDecision:
        from startupai_controller.board_automation import admit_backlog_items

        return admit_backlog_items(
            config=config,
            automation_config=automation_config,
            project_owner=project_owner,
            project_number=project_number,
            dispatchable_repo_prefixes=dispatchable_repo_prefixes,
            active_lease_issue_refs=active_lease_issue_refs,
            dry_run=dry_run,
            board_snapshot=board_snapshot,
            github_bundle=github_bundle,
            github_memo=github_memo,
            gh_runner=gh_runner,
        )

    def admission_summary_payload(
        self,
        decision: AdmissionDecision,
        *,
        enabled: bool,
    ) -> dict[str, object]:
        from startupai_controller.board_automation import admission_summary_payload

        return admission_summary_payload(decision, enabled=enabled)

    def claim_ready_issue(
        self,
        config: Any,
        project_owner: str,
        project_number: int,
        **kwargs: Any,
    ) -> ClaimReadyResult:
        from startupai_controller.automation_ready_review_wiring import (
            claim_ready_issue,
        )

        return claim_ready_issue(
            config,
            project_owner,
            project_number,
            **kwargs,
        )
