"""Repair policy — requeue ceiling, PR identity, branch patterns, provenance.

Pure policy module: NO GitHub, SQLite, subprocess, or logging imports.
"""

from __future__ import annotations

import re
from typing import Any


# ---------------------------------------------------------------------------
# Branch and provenance patterns
# ---------------------------------------------------------------------------

MARKER_PREFIX = "startupai-board-bot"


def marker_for(kind: str, ref: str) -> str:
    """Build a deterministic machine marker for issue/PR comments."""
    return f"<!-- {MARKER_PREFIX}:{kind}:{ref} -->"


def deterministic_branch_pattern(issue_number: int) -> re.Pattern[str]:
    """Return the canonical issue branch pattern for PR adoption."""
    return re.compile(rf"^feat/{issue_number}-[a-z0-9-]+$")


def extract_acceptance_criteria(body: str) -> str:
    """Extract acceptance criteria section from issue body."""
    if not body:
        return ""
    patterns = [
        r"##\s*Acceptance\s+Criteria\s*\n(.*?)(?=\n##|\Z)",
        r"##\s*AC\s*\n(.*?)(?=\n##|\Z)",
        r"\*\*Acceptance\s+Criteria\*\*\s*\n(.*?)(?=\n\*\*|\Z)",
    ]
    for pattern in patterns:
        match = re.search(pattern, body, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return body.strip()


def consumer_provenance_marker(
    *,
    session_id: str,
    issue_ref: str,
    repo_prefix: str,
    branch_name: str,
    executor: str,
) -> str:
    """Build a machine-readable provenance marker for issues and PRs."""
    return (
        f"<!-- {MARKER_PREFIX}:consumer:session={session_id} issue={issue_ref} "
        f"repo={repo_prefix} branch={branch_name} executor={executor} -->"
    )


def parse_consumer_provenance(text: str) -> dict[str, str] | None:
    """Parse the consumer provenance marker from text."""
    match = re.search(
        rf"<!--\s*{re.escape(MARKER_PREFIX)}:consumer:"
        r"session=(?P<session>[^\s]+)\s+"
        r"issue=(?P<issue>[^\s]+)\s+"
        r"repo=(?P<repo>[^\s]+)\s+"
        r"branch=(?P<branch>[^\s]+)\s+"
        r"executor=(?P<executor>[^\s]+)\s*-->",
        text,
    )
    if not match:
        return None
    return {
        "session_id": match.group("session"),
        "issue_ref": match.group("issue"),
        "repo_prefix": match.group("repo"),
        "branch_name": match.group("branch"),
        "executor": match.group("executor"),
    }


# ---------------------------------------------------------------------------
# PR URL parsing — pure string extraction, no side effects
# ---------------------------------------------------------------------------


def parse_pr_url(pr_field: str) -> tuple[str, str, int] | None:
    """Extract owner/repo/pr_number from a GitHub PR URL in project field."""
    text = pr_field.strip()
    if not text:
        return None
    match = re.search(
        r"github\.com/([^/]+)/([^/]+)/pull/(\d+)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    owner = match.group(1)
    repo = match.group(2)
    number = int(match.group(3))
    return owner, repo, number


def repo_to_prefix_for_repo(repo: str) -> str:
    """Best-effort repo name to board prefix mapping."""
    mapping = {
        "startupai-crew": "crew",
        "app.startupai-site": "app",
        "startupai.site": "site",
    }
    return mapping.get(repo, "crew")
