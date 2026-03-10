"""Canonical GitHub adapter types and constants."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


COPILOT_CODING_AGENT_LOGINS = {
    "app/copilot-swe-agent",
    "copilot-swe-agent[bot]",
    "copilot",
}


@dataclass
class ProjectItemSnapshot:
    """A single project board item with key field values."""

    issue_ref: str
    status: str
    executor: str
    handoff_to: str
    priority: str = ""
    item_id: str = ""
    project_id: str = ""
    sprint: str = ""
    agent: str = ""
    owner_field: str = ""
    title: str = ""
    body: str = ""
    repo_slug: str = ""
    repo_name: str = ""
    repo_owner: str = ""
    issue_number: int = 0
    issue_updated_at: str = ""


@dataclass(frozen=True)
class CycleBoardSnapshot:
    """Thin per-cycle view of board items reused across hot-path phases."""

    items: tuple[ProjectItemSnapshot, ...]
    by_status: dict[str, tuple[ProjectItemSnapshot, ...]] = field(default_factory=dict)

    def items_with_status(self, status: str) -> tuple[ProjectItemSnapshot, ...]:
        """Return cached items in the given status."""
        return self.by_status.get(status, ())


@dataclass
class CycleGitHubMemo:
    """Cycle-local memoization for expensive GitHub reads."""

    issue_bodies: dict[tuple[str, str, int], str] = field(default_factory=dict)
    issue_comment_bodies: dict[tuple[str, str, int], list[str]] = field(default_factory=dict)
    open_pull_requests: dict[str, list["OpenPullRequest"]] = field(default_factory=dict)
    dependency_ready: dict[str, bool] = field(default_factory=dict)
    review_pull_requests: dict[tuple[str, int], "PullRequestViewPayload"] = field(
        default_factory=dict
    )
    review_state_probes: dict[tuple[str, int], "PullRequestStateProbe"] = field(
        default_factory=dict
    )
    required_status_checks: dict[tuple[str, str], set[str]] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class LinkedIssue:
    """One issue linked from a PR body."""

    owner: str
    repo: str
    number: int
    ref: str


@dataclass(frozen=True)
class CodexReviewVerdict:
    """Adapter-local codex verdict extracted from PR comments/reviews."""

    decision: str
    route: str
    source: str
    timestamp: str
    actor: str
    checklist: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PullRequestViewPayload:
    """Expanded PR payload used to make one review decision without requerying."""

    pr_repo: str
    pr_number: int
    author: str
    body: str
    state: str
    is_draft: bool
    merge_state_status: str
    mergeable: str
    base_ref_name: str
    auto_merge_enabled: bool
    comments: tuple[dict[str, Any], ...] = ()
    reviews: tuple[dict[str, Any], ...] = ()
    status_check_rollup: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class PullRequestStateProbe:
    """Lightweight PR state used to avoid rehydrating unchanged review items."""

    pr_repo: str
    pr_number: int
    state: str
    is_draft: bool
    merge_state_status: str
    mergeable: str
    base_ref_name: str
    auto_merge_enabled: bool
    head_ref_oid: str
    updated_at: str
    latest_comment_at: str
    latest_review_at: str
    status_check_rollup: tuple[dict[str, Any], ...] = ()


# Transitional compatibility aliases while legacy callers still expect underscored names.
_ProjectItemSnapshot = ProjectItemSnapshot
