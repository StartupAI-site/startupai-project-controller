"""PullRequestPort — PR read/write operations.

Port protocol for interacting with pull requests. Returns domain types
from domain/models.py — no raw payloads or adapter DTOs.
"""

from __future__ import annotations

from typing import Protocol

from startupai_controller.domain.models import OpenPullRequest, PrGateStatus


class PullRequestPort(Protocol):
    """Pull request read and write operations."""

    def get_gate_status(self, pr_repo: str, pr_number: int) -> PrGateStatus:
        """Return the gate readiness snapshot for a PR."""
        ...

    def list_open_prs_for_issue(
        self, repo: str, issue_number: int
    ) -> list[OpenPullRequest]:
        """Return open PRs that reference the given issue."""
        ...

    def enable_automerge(
        self, pr_repo: str, pr_number: int, *, delete_branch: bool = False
    ) -> str:
        """Enable auto-merge on a PR. Returns status string."""
        ...

    def rerun_failed_check(
        self, pr_repo: str, check_name: str, run_id: int
    ) -> bool:
        """Re-run a failed/cancelled check. Returns True on success."""
        ...
