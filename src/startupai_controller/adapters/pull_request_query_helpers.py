"""Issue/PR query helpers for the pull-request capability adapter."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime
import json
import re

from startupai_controller.adapters.github_transport import _run_gh
from startupai_controller.adapters.github_types import (
    CycleGitHubMemo,
    GitHubCommentNode,
    GitHubReviewNode,
    GitHubStatusCheckRollupNode,
)
from startupai_controller.adapters.review_state import (
    _parse_github_timestamp,
    _query_latest_matching_comment_timestamp,
    _query_latest_non_automation_comment_timestamp,
)
from startupai_controller.domain.repair_policy import MARKER_PREFIX
from startupai_controller.domain.repair_policy import parse_pr_url as _parse_pr_url
from startupai_controller.validate_critical_path_promotion import GhQueryError


def _extract_run_id(details_url: str) -> int | None:
    """Extract a GitHub Actions run ID from a details URL when present."""
    match = re.search(r"/actions/runs/(\d+)", details_url)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _normalize_graphql_rollup_node(
    node: dict[str, object],
) -> GitHubStatusCheckRollupNode | None:
    """Normalize GraphQL status-check nodes into the shared rollup shape."""
    typename = str(node.get("__typename") or "")
    if typename == "CheckRun":
        return {
            "__typename": "CheckRun",
            "name": str(node.get("name") or ""),
            "status": str(node.get("status") or "").lower(),
            "conclusion": str(node.get("conclusion") or "").lower(),
            "detailsUrl": str(node.get("detailsUrl") or ""),
            "completedAt": str(node.get("completedAt") or ""),
            "startedAt": str(node.get("startedAt") or ""),
        }
    if typename == "StatusContext":
        return {
            "__typename": "StatusContext",
            "context": str(node.get("context") or ""),
            "state": str(node.get("state") or "").lower(),
            "targetUrl": str(node.get("targetUrl") or ""),
            "startedAt": str(node.get("createdAt") or ""),
        }
    return None


def _latest_node_timestamp(
    nodes: Sequence[GitHubCommentNode | GitHubReviewNode],
    *keys: str,
) -> str:
    """Return the latest available timestamp from a list of GraphQL nodes."""
    timestamps: list[str] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        for key in keys:
            value = str(node.get(key) or "")
            if value:
                timestamps.append(value)
    return max(timestamps) if timestamps else ""


def _query_latest_marker_timestamp(
    owner: str,
    repo: str,
    number: int,
    marker_prefix: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return most recent marker timestamp encoded in issue comments."""
    try:
        comments = _run_gh(
            [
                "api",
                f"repos/{owner}/{repo}/issues/{number}/comments",
                "--paginate",
                "-q",
                ".[].body",
            ],
            gh_runner=gh_runner,
        )
    except GhQueryError:
        return None

    pattern = re.compile(
        rf"{re.escape(marker_prefix)}:([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T"
        rf"[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}Z)"
    )
    latest: datetime | None = None
    for match in pattern.finditer(comments):
        raw = match.group(1)
        try:
            ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if latest is None or ts > latest:
            latest = ts
    return latest


def _query_issue_updated_at(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime:
    """Get issue updated_at timestamp (UTC)."""
    output = _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}",
            "-q",
            ".updated_at",
        ],
        gh_runner=gh_runner,
    ).strip()
    try:
        return datetime.fromisoformat(output.replace("Z", "+00:00"))
    except ValueError as error:
        raise GhQueryError(
            f"Invalid updated_at for {owner}/{repo}#{number}: {output}"
        ) from error


def query_issue_body(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Return the raw issue body."""
    output = _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}",
            "--jq",
            ".body",
        ],
        gh_runner=gh_runner,
        operation_type="query",
    )
    return str(output or "")


def memoized_query_issue_body(
    memo: CycleGitHubMemo,
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Return an issue body using the cycle-local cache."""
    key = (owner, repo, number)
    cached = memo.issue_bodies.get(key)
    if cached is not None:
        return cached
    body = query_issue_body(owner, repo, number, gh_runner=gh_runner)
    memo.issue_bodies[key] = body
    return body


def _query_open_pr_updated_at(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return updated_at for an open PR, or None when closed/unavailable."""
    try:
        output = _run_gh(
            [
                "api",
                f"repos/{owner}/{repo}/pulls/{pr_number}",
                "--jq",
                "{state: .state, updated_at: .updated_at}",
            ],
            gh_runner=gh_runner,
        )
    except GhQueryError:
        return None

    try:
        payload = json.loads(output)
    except json.JSONDecodeError:
        return None

    if str(payload.get("state") or "").upper() != "OPEN":
        return None
    return _parse_github_timestamp(str(payload.get("updated_at") or ""))


def _is_pr_open(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Return True when a PR exists and is open."""
    try:
        state = _run_gh(
            [
                "api",
                f"repos/{owner}/{repo}/pulls/{pr_number}",
                "-q",
                ".state",
            ],
            gh_runner=gh_runner,
        ).strip()
    except GhQueryError:
        return False
    return state.upper() == "OPEN"


def _query_failed_check_runs(
    owner: str,
    repo: str,
    head_sha: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str] | None:
    """Query failed check-run names for one commit head SHA."""
    try:
        output = _run_gh(
            ["api", f"repos/{owner}/{repo}/commits/{head_sha}/check-runs"],
            gh_runner=gh_runner,
        )
    except GhQueryError:
        return None

    try:
        data = json.loads(output)
    except json.JSONDecodeError:
        return None

    failed: list[str] = []
    for run in data.get("check_runs", []):
        if isinstance(run, dict) and run.get("conclusion") == "failure":
            name = run.get("name", "")
            if name:
                failed.append(name)
    return failed


def _query_pr_head_sha(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str | None:
    """Get the head SHA for one PR, or None on failure."""
    try:
        output = _run_gh(
            ["api", f"repos/{owner}/{repo}/pulls/{pr_number}"],
            gh_runner=gh_runner,
        )
    except GhQueryError:
        return None

    try:
        data = json.loads(output)
    except json.JSONDecodeError:
        return None

    head = data.get("head")
    if not isinstance(head, dict):
        return None
    sha = head.get("sha")
    return None if sha is None else str(sha)


def _query_latest_wip_activity_timestamp(
    issue_ref: str,
    owner: str,
    repo: str,
    number: int,
    pr_field: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return latest execution-relevant activity for a WIP issue."""
    candidates: list[datetime] = []

    parsed_pr = _parse_pr_url(pr_field)
    if parsed_pr is not None:
        pr_owner, pr_repo, pr_number = parsed_pr
        pr_ts = _query_open_pr_updated_at(
            pr_owner, pr_repo, pr_number, gh_runner=gh_runner
        )
        if pr_ts is not None:
            candidates.append(pr_ts)

    comment_ts = _query_latest_non_automation_comment_timestamp(
        owner, repo, number, gh_runner=gh_runner
    )
    if comment_ts is not None:
        candidates.append(comment_ts)

    baseline_ts = _query_latest_matching_comment_timestamp(
        owner,
        repo,
        number,
        (
            f"{MARKER_PREFIX}:claim-ready:{issue_ref}",
            f"{MARKER_PREFIX}:dispatch-agent:{issue_ref}",
        ),
        gh_runner=gh_runner,
    )
    if baseline_ts is not None:
        candidates.append(baseline_ts)

    return max(candidates) if candidates else None


def _query_issue_assignees(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Fetch current assignee logins for an issue."""
    output = _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}",
            "-q",
            ".assignees[].login",
        ],
        gh_runner=gh_runner,
    )
    return [line.strip() for line in output.splitlines() if line.strip()]


def _set_issue_assignees(
    owner: str,
    repo: str,
    number: int,
    assignees: list[str],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set issue assignees explicitly."""
    args = [
        "api",
        f"repos/{owner}/{repo}/issues/{number}",
        "-X",
        "PATCH",
    ]
    for login in assignees:
        args.extend(["-f", f"assignees[]={login}"])
    _run_gh(args, gh_runner=gh_runner)
