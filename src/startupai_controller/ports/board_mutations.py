"""BoardMutationPort — board state mutation operations.

Port protocol for writing to the project board. Hides field/option ID
resolution and GraphQL mutation details.
"""

from __future__ import annotations

from typing import Protocol


class BoardMutationPort(Protocol):
    """Write operations on the project board."""

    def set_issue_status(self, issue_ref: str, status: str) -> None:
        """Set the board status for an issue."""
        ...

    def set_issue_field(
        self, issue_ref: str, field_name: str, value: str
    ) -> None:
        """Set a text or single-select field on an issue."""
        ...

    def post_issue_comment(
        self, repo: str, issue_number: int, body: str
    ) -> None:
        """Post a comment on an issue."""
        ...

    def close_issue(self, repo: str, issue_number: int) -> None:
        """Close an issue."""
        ...
