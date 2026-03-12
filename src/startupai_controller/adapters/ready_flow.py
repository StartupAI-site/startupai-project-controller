"""Adapter for ready-queue routing and backlog admission operations."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, cast

from startupai_controller.board_automation_config import BoardAutomationConfig
from startupai_controller.domain.models import (
    AdmissionDecision,
    ClaimReadyResult,
    CycleBoardSnapshot,
    ExecutorRoutingDecision,
)
from startupai_controller.ports.board_mutations import BoardMutationPort
from startupai_controller.ports.ready_flow import (
    BoardInfoResolverFn,
    BoardStatusMutatorFn,
    CommentCheckerFn,
    CommentPosterFn,
    GitHubBundleView,
    GitHubRunnerFn,
    StatusResolverFn,
)
from startupai_controller.ports.review_state import ReviewStatePort
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig

if TYPE_CHECKING:
    from startupai_controller.automation_port_helpers import BoardInfo
    from startupai_controller.runtime.wiring import GitHubPortBundle, GitHubRuntimeMemo


class BoardAutomationReadyFlowAdapter:
    """Expose the existing ready-flow behavior behind a typed port."""

    def route_protected_queue_executors(
        self,
        config: CriticalPathConfig,
        automation_config: BoardAutomationConfig | None,
        project_owner: str,
        project_number: int,
        *,
        statuses: tuple[str, ...] = (),
        dry_run: bool = False,
        board_snapshot: CycleBoardSnapshot | None = None,
        gh_runner: GitHubRunnerFn | None = None,
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
        config: CriticalPathConfig,
        automation_config: BoardAutomationConfig | None,
        project_owner: str,
        project_number: int,
        *,
        dispatchable_repo_prefixes: tuple[str, ...] | None = None,
        active_lease_issue_refs: tuple[str, ...] = (),
        dry_run: bool = False,
        board_snapshot: CycleBoardSnapshot | None = None,
        github_bundle: GitHubBundleView | None = None,
        github_memo: object | None = None,
        gh_runner: GitHubRunnerFn | None = None,
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
            github_bundle=cast("GitHubPortBundle | None", github_bundle),
            github_memo=cast("GitHubRuntimeMemo | None", github_memo),
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
        review_state_port: ReviewStatePort | None = None,
        board_port: BoardMutationPort | None = None,
        status_resolver: StatusResolverFn | None = None,
        board_info_resolver: BoardInfoResolverFn | None = None,
        board_mutator: BoardStatusMutatorFn | None = None,
        comment_checker: CommentCheckerFn | None = None,
        comment_poster: CommentPosterFn | None = None,
        gh_runner: GitHubRunnerFn | None = None,
    ) -> ClaimReadyResult:
        from startupai_controller.automation_ready_review_wiring import (
            claim_ready_issue,
        )

        return claim_ready_issue(
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
            board_info_resolver=cast(
                "Callable[..., BoardInfo] | None",
                board_info_resolver,
            ),
            board_mutator=board_mutator,
            comment_checker=comment_checker,
            comment_poster=comment_poster,
            gh_runner=gh_runner,
        )
