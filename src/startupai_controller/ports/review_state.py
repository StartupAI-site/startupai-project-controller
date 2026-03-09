"""ReviewStatePort — board state queries for issue status and fields.

Port protocol for reading project board state. Hides GitHub Project
field IDs, GraphQL queries, and authentication details.
"""

from __future__ import annotations

from typing import Protocol

from startupai_controller.domain.models import IssueFields, IssueSnapshot


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
