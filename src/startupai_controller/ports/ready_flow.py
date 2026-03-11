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
