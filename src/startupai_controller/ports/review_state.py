"""ReviewStatePort — board state queries for issue status and fields.

Port protocol for reading project board state. Hides GitHub Project
field IDs, GraphQL queries, and authentication details.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    IssueFields,
    IssueSnapshot,
)


class ReviewStatePort(Protocol):
    """Read-only access to issue board state."""

    def get_issue_status(self, issue_ref: str) -> str | None:
        """Return the current board status for an issue, or None if unknown."""
        ...

    def list_issues_by_status(self, status: str) -> list[IssueSnapshot]:
        """Return typed snapshots for all issues with the given status."""
        ...

    def get_issue_fields(self, issue_ref: str) -> IssueFields:
        """Return the full typed field bundle for an issue."""
        ...

    def search_open_issue_numbers_with_comment_marker(
        self, repo: str, marker: str
    ) -> tuple[int, ...]:
        """Return open issue numbers in a repo with comments matching a marker."""
        ...

    def list_issue_comment_bodies(
        self, repo: str, issue_number: int
    ) -> tuple[str, ...]:
        """Return all comment bodies for an issue."""
        ...

    def latest_matching_comment_timestamp(
        self,
        repo: str,
        issue_number: int,
        markers: tuple[str, ...],
    ) -> datetime | None:
        """Return the latest timestamp for comments containing any marker."""
        ...

    def comment_exists(self, repo: str, issue_number: int, marker: str) -> bool:
        """Return True when a marker comment already exists for the issue or PR."""
        ...

    def build_board_snapshot(self) -> CycleBoardSnapshot:
        """Return the thin board snapshot for the current project."""
        ...
