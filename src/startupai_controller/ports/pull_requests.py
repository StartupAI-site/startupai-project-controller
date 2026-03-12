"""PullRequestPort — PR read/write operations.

Port protocol for interacting with pull requests. Returns domain types
from domain/models.py — no raw payloads or adapter DTOs.
"""

from __future__ import annotations

from typing import Protocol

from startupai_controller.domain.models import (
    OpenPullRequest,
    PrGateStatus,
    ReviewSnapshot,
)


class PullRequestPort(Protocol):
    """Pull request read and write operations."""

    def list_open_prs(self, repo: str) -> list[OpenPullRequest]:
        """Return open PRs for a repository."""
        ...

    def get_pull_request(self, repo: str, number: int) -> OpenPullRequest | None:
        """Return one PR snapshot, or None if not found."""
        ...

    def linked_issue_refs(self, pr_repo: str, pr_number: int) -> tuple[str, ...]:
        """Return linked issue refs referenced by a PR body."""
        ...

    def has_copilot_review_signal(self, pr_repo: str, pr_number: int) -> bool:
        """Return True when Copilot review signal is present for the PR."""
        ...

    def get_gate_status(self, pr_repo: str, pr_number: int) -> PrGateStatus:
        """Return the gate readiness snapshot for a PR."""
        ...

    def required_status_checks(
        self, pr_repo: str, base_ref_name: str = "main"
    ) -> set[str]:
        """Return the required status-check contexts for a PR base branch."""
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

    def rerun_failed_check(self, pr_repo: str, check_name: str, run_id: int) -> bool:
        """Re-run a failed/cancelled check. Returns True on success."""
        ...

    def update_branch(self, pr_repo: str, pr_number: int) -> None:
        """Update a PR branch to the latest base branch."""
        ...

    def is_pull_request_open(self, pr_repo: str, pr_number: int) -> bool:
        """Return True when a PR exists and is open."""
        ...

    def is_pull_request_merged(self, pr_repo: str, pr_number: int) -> bool:
        """Return True when a PR exists and is merged."""
        ...

    def pull_request_updated_at(
        self,
        pr_repo: str,
        pr_number: int,
    ) -> str | None:
        """Return the PR updated timestamp as an ISO string, or None."""
        ...

    def pull_request_head_sha(self, pr_repo: str, pr_number: int) -> str | None:
        """Return the PR head SHA, or None when unavailable."""
        ...

    def failed_check_runs(
        self,
        pr_repo: str,
        head_sha: str,
    ) -> tuple[str, ...] | None:
        """Return failed check-run names for one commit head SHA."""
        ...

    def close_pull_request(
        self,
        pr_repo: str,
        pr_number: int,
        *,
        comment: str | None = None,
    ) -> None:
        """Close a pull request, optionally posting a closing comment."""
        ...

    def review_state_digests(
        self, pr_refs: list[tuple[str, int]]
    ) -> dict[tuple[str, int], str]:
        """Return lightweight review-state digests keyed by (repo, pr_number)."""
        ...

    def review_snapshots(
        self,
        review_refs_by_pr: dict[tuple[str, int], tuple[str, ...]],
        *,
        trusted_codex_actors: frozenset[str],
    ) -> dict[tuple[str, int], ReviewSnapshot]:
        """Return typed review snapshots keyed by (repo, pr_number)."""
        ...

    def post_codex_verdict_if_missing(self, pr_url: str, session_id: str) -> bool:
        """Post the trusted codex verdict marker if it is not already present."""
        ...
