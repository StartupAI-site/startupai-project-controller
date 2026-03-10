"""IssueContextPort — issue body/title/label reads for launch preparation."""

from __future__ import annotations

from typing import Protocol

from startupai_controller.domain.models import IssueContext


class IssueContextPort(Protocol):
    """Read issue context from the canonical upstream system."""

    def get_issue_context(self, owner: str, repo: str, number: int) -> IssueContext:
        """Return typed issue context for an issue."""
        ...
