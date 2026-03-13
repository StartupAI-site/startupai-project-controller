"""Canonical GitHub adapter types and constants."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TypeAlias, TypedDict

from startupai_controller.domain.models import (
    CycleBoardSnapshot,
    LinkedIssue,
    OpenPullRequest,
    ProjectItemSnapshot,
)

COPILOT_CODING_AGENT_LOGINS = {
    "app/copilot-swe-agent",
    "copilot-swe-agent[bot]",
    "copilot",
}


@dataclass
class CycleGitHubMemo:
    """Cycle-local memoization for expensive GitHub reads."""

    issue_bodies: dict[tuple[str, str, int], str] = field(default_factory=dict)
    issue_comment_bodies: dict[tuple[str, str, int], list[str]] = field(
        default_factory=dict
    )
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
class CodexReviewVerdict:
    """Adapter-local codex verdict extracted from PR comments/reviews."""

    decision: str
    route: str
    source: str
    timestamp: str
    actor: str
    checklist: list[str] = field(default_factory=list)


class GitHubActorPayload(TypedDict, total=False):
    """Minimal GitHub actor payload embedded in comment/review nodes."""

    login: str


class GitHubCommentNode(TypedDict, total=False):
    """Normalized PR comment payload used by review-state helpers."""

    body: str
    createdAt: str
    author: GitHubActorPayload
    user: GitHubActorPayload


class GitHubReviewNode(TypedDict, total=False):
    """Normalized PR review payload used by review-state helpers."""

    body: str
    submittedAt: str
    state: str
    author: GitHubActorPayload
    user: GitHubActorPayload


class GitHubCheckRunNode(TypedDict):
    """Normalized GraphQL CheckRun node from a status-check rollup."""

    __typename: Literal["CheckRun"]
    name: str
    status: str
    conclusion: str
    detailsUrl: str
    completedAt: str
    startedAt: str


class GitHubStatusContextNode(TypedDict):
    """Normalized GraphQL StatusContext node from a status-check rollup."""

    __typename: Literal["StatusContext"]
    context: str
    state: str
    targetUrl: str
    startedAt: str


GitHubStatusCheckRollupNode: TypeAlias = GitHubCheckRunNode | GitHubStatusContextNode


@dataclass(frozen=True)
class PullRequestViewPayload:
    """Expanded PR payload used to make one review decision without requerying."""

    pr_repo: str
    pr_number: int
    url: str
    head_ref_name: str
    author: str
    body: str
    state: str
    is_draft: bool
    merge_state_status: str
    mergeable: str
    base_ref_name: str
    merged_at: str
    auto_merge_enabled: bool
    comments: tuple[GitHubCommentNode, ...] = ()
    reviews: tuple[GitHubReviewNode, ...] = ()
    status_check_rollup: tuple[GitHubStatusCheckRollupNode, ...] = ()


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
    status_check_rollup: tuple[GitHubStatusCheckRollupNode, ...] = ()


# Transitional compatibility aliases while legacy callers still expect underscored names.
_ProjectItemSnapshot = ProjectItemSnapshot
