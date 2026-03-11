"""ReadyFlowPort — control-plane ready-queue routing and admission operations."""

from __future__ import annotations

from typing import Protocol


class ReadyFlowPort(Protocol):
    """Ready queue routing and backlog admission operations."""

    def route_protected_queue_executors(
        self,
        config,
        automation_config,
        project_owner: str,
        project_number: int,
        *,
        statuses=(),
        dry_run: bool = False,
        board_snapshot=None,
        gh_runner=None,
    ):
        """Normalize protected queue executor ownership."""
        ...

    def admit_backlog_items(
        self,
        config,
        automation_config,
        project_owner: str,
        project_number: int,
        *,
        dispatchable_repo_prefixes=None,
        active_lease_issue_refs=(),
        dry_run: bool = False,
        board_snapshot=None,
        github_bundle=None,
        github_memo=None,
        gh_runner=None,
    ):
        """Admit governed Backlog items into Ready."""
        ...

    def admission_summary_payload(self, decision, *, enabled: bool):
        """Convert admission results into a JSON-friendly payload."""
        ...

    def claim_ready_issue(
        self,
        config,
        project_owner: str,
        project_number: int,
        *,
        executor: str,
        issue_ref: str | None = None,
        next_issue: bool = False,
        this_repo_prefix: str | None = None,
        all_prefixes: bool = False,
        per_executor_wip_limit: int = 3,
        automation_config=None,
        dry_run: bool = False,
        review_state_port=None,
        board_port=None,
        status_resolver=None,
        board_info_resolver=None,
        board_mutator=None,
        comment_checker=None,
        comment_poster=None,
        gh_runner=None,
    ):
        """Claim one Ready issue for a specific executor."""
        ...
