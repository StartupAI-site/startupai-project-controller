"""GitHub API interaction layer for board automation.

Extracted from board_automation.py (ADR-018 step 3). Contains all GitHub
I/O functions: gh CLI runner, comment operations, project field queries,
issue/PR queries, and codex/gate queries.

All functions preserve existing DI signatures (gh_runner, board_info_resolver,
board_mutator callables).
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Sequence


from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    ConfigError,
    GhQueryError,
    parse_issue_ref,
)
from startupai_controller.promote_ready import (
    BoardInfo,
)
from startupai_controller.domain.models import (
    CheckObservation,
    OpenPullRequest,
    PrGateStatus,
)
from startupai_controller.domain.repair_policy import marker_for
from startupai_controller.domain.scheduling_policy import (
    snapshot_to_issue_ref as _canonical_snapshot_to_issue_ref,
)
from startupai_controller.adapters.github_transport import (
    GhCommandError,
    _run_gh,
    gh_reason_code,
)
from startupai_controller.adapters.github_types import (
    COPILOT_CODING_AGENT_LOGINS,
    CycleBoardSnapshot,
    CycleGitHubMemo,
    CodexReviewVerdict,
    LinkedIssue,
    PullRequestStateProbe,
    PullRequestViewPayload,
    ProjectItemSnapshot as _ProjectItemSnapshot,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MARKER_PREFIX = "startupai-board-bot"
from startupai_controller.domain.scheduling_policy import (  # noqa: E402
    VALID_EXECUTORS,
    priority_rank as _priority_rank,  # re-export (compat)
)
_REQUIRED_STATUS_CHECKS_CACHE_TTL_SECONDS = 900
_required_status_checks_ttl_cache: dict[
    tuple[str, str],
    tuple[float, set[str]],
] = {}
_BOARD_SNAPSHOT_CACHE_TTL_SECONDS = 15
_cycle_board_snapshot_cache: dict[
    tuple[str, int],
    tuple[float, "CycleBoardSnapshot"],
] = {}


def rerun_actions_run(
    pr_repo: str,
    run_id: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Rerun a GitHub Actions workflow run."""
    _run_gh(
        ["run", "rerun", str(run_id), "--repo", pr_repo],
        gh_runner=gh_runner,
        operation_type="check_rerun",
    )


def update_pull_request_branch(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Update a PR branch from its base branch."""
    _run_gh(
        ["pr", "update-branch", str(pr_number), "--repo", pr_repo],
        gh_runner=gh_runner,
        operation_type="mutation",
    )


def enable_pull_request_automerge(
    pr_repo: str,
    pr_number: int,
    *,
    delete_branch: bool = False,
    confirm_retries: int = 3,
    confirm_delay_seconds: float = 1.0,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Compatibility wrapper for adapter-owned auto-merge enablement."""
    del confirm_retries, confirm_delay_seconds
    from startupai_controller.adapters.github_cli import enable_pull_request_automerge as _adapter_enable_pull_request_automerge

    return _adapter_enable_pull_request_automerge(
        pr_repo,
        pr_number,
        delete_branch=delete_branch,
        gh_runner=gh_runner or _run_gh,
    )


def close_pull_request(
    pr_repo: str,
    pr_number: int,
    *,
    comment: str,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Close a PR with a deterministic policy comment."""
    _run_gh(
        [
            "pr",
            "close",
            str(pr_number),
            "--repo",
            pr_repo,
            "--comment",
            comment,
        ],
        gh_runner=gh_runner,
        operation_type="mutation",
    )


def close_issue(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Close an issue without altering any other fields."""
    _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}",
            "-X",
            "PATCH",
            "-f",
            "state=closed",
        ],
        gh_runner=gh_runner,
        operation_type="mutation",
    )


def _is_copilot_coding_agent_actor(login: str) -> bool:
    """Return True when actor is Copilot coding-agent identity."""
    normalized = login.strip().lower()
    return normalized in COPILOT_CODING_AGENT_LOGINS


def _is_automation_login(login: str) -> bool:
    """Return True when a comment author is an automation identity."""
    normalized = login.strip().lower()
    if not normalized:
        return False
    return (
        normalized.endswith("[bot]")
        or normalized.startswith("app/")
        or normalized in COPILOT_CODING_AGENT_LOGINS
        or normalized in {"codex-bot", "codex", "claude"}
    )


def _parse_github_timestamp(raw: str) -> datetime | None:
    """Parse an ISO timestamp from GitHub payloads."""
    text = raw.strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Marker & comment operations
# ---------------------------------------------------------------------------


def _marker_for(kind: str, ref: str) -> str:
    """Generate an HTML comment marker for idempotency."""
    return marker_for(kind, ref)


def _comment_exists(
    owner: str,
    repo: str,
    number: int,
    marker: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Check if a comment with the given marker already exists on an issue."""
    try:
        output = _run_gh(
            [
                "api",
                f"repos/{owner}/{repo}/issues/{number}/comments",
                "--paginate",
                "-q",
                ".[].body",
            ],
            gh_runner=gh_runner,
        )
        return marker in output
    except GhQueryError:
        return False


def _post_comment(
    owner: str,
    repo: str,
    number: int,
    body: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Post a comment on a GitHub issue."""
    _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}/comments",
            "-f",
            f"body={body}",
        ],
        gh_runner=gh_runner,
    )


def list_issue_comment_bodies(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Return comment bodies for an issue in ascending API order."""
    output = _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}/comments",
            "--paginate",
        ],
        gh_runner=gh_runner,
        operation_type="query",
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise GhQueryError(
            f"Failed querying comments for {owner}/{repo}#{number}: invalid JSON."
        ) from error
    if not isinstance(payload, list):
        raise GhQueryError(
            f"Failed querying comments for {owner}/{repo}#{number}: invalid payload."
        )
    results: list[str] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        body = str(item.get("body") or "")
        if body:
            results.append(body)
    return results


def memoized_list_issue_comment_bodies(
    memo: CycleGitHubMemo,
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Return issue comment bodies using a cycle-local cache."""
    key = (owner, repo, number)
    cached = memo.issue_comment_bodies.get(key)
    if cached is not None:
        return list(cached)
    bodies = list_issue_comment_bodies(owner, repo, number, gh_runner=gh_runner)
    memo.issue_comment_bodies[key] = list(bodies)
    return bodies


def _query_issue_comments(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[dict]:
    """Compatibility wrapper for adapter-owned issue comment queries."""
    from startupai_controller.adapters.review_state import _query_issue_comments as _adapter_query_issue_comments

    return _adapter_query_issue_comments(
        owner,
        repo,
        number,
        gh_runner=gh_runner or _run_gh,
    )


def _comment_activity_timestamp(comment: dict) -> datetime | None:
    """Compatibility wrapper for adapter-owned comment activity timestamps."""
    from startupai_controller.adapters.review_state import _comment_activity_timestamp as _adapter_comment_activity_timestamp

    return _adapter_comment_activity_timestamp(comment)


def _query_latest_matching_comment_timestamp(
    owner: str,
    repo: str,
    number: int,
    markers: Sequence[str],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Compatibility wrapper for adapter-owned marker timestamp queries."""
    from startupai_controller.adapters.review_state import (
        _query_latest_matching_comment_timestamp as _adapter_query_latest_matching_comment_timestamp,
    )

    return _adapter_query_latest_matching_comment_timestamp(
        owner,
        repo,
        number,
        markers,
        gh_runner=gh_runner or _run_gh,
    )


def _query_latest_non_automation_comment_timestamp(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Compatibility wrapper for adapter-owned activity timestamp queries."""
    from startupai_controller.adapters.review_state import (
        _query_latest_non_automation_comment_timestamp as _adapter_query_latest_non_automation_comment_timestamp,
    )

    return _adapter_query_latest_non_automation_comment_timestamp(
        owner,
        repo,
        number,
        gh_runner=gh_runner or _run_gh,
    )


def _query_latest_marker_timestamp(
    owner: str,
    repo: str,
    number: int,
    marker_prefix: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Compatibility wrapper for adapter-owned marker timestamp queries."""
    from startupai_controller.adapters.github_cli import (
        _query_latest_marker_timestamp as _adapter_query_latest_marker_timestamp,
    )

    return _adapter_query_latest_marker_timestamp(
        owner,
        repo,
        number,
        marker_prefix,
        gh_runner=gh_runner or _run_gh,
    )


# ---------------------------------------------------------------------------
# Repo / issue-ref helpers
# ---------------------------------------------------------------------------


def _repo_to_prefix(
    full_repo: str,
    config: CriticalPathConfig,
) -> str | None:
    """Reverse-lookup issue_prefixes to find prefix for a full repo slug."""
    for prefix, repo_slug in config.issue_prefixes.items():
        if repo_slug == full_repo:
            return prefix
    return None


def _issue_ref_to_repo_parts(
    issue_ref: str,
    config: CriticalPathConfig,
) -> tuple[str, str, int]:
    """Parse issue_ref and return (owner, repo, number)."""
    parsed = parse_issue_ref(issue_ref)
    repo_slug = config.issue_prefixes.get(parsed.prefix)
    if not repo_slug:
        raise ConfigError(
            f"Missing repo mapping for prefix '{parsed.prefix}'."
        )
    owner, repo = repo_slug.split("/", maxsplit=1)
    return owner, repo, parsed.number


def _snapshot_to_issue_ref(
    snapshot: _ProjectItemSnapshot,
    config: CriticalPathConfig,
) -> str | None:
    """Compatibility wrapper for canonical domain issue-ref normalization."""
    return _canonical_snapshot_to_issue_ref(snapshot.issue_ref, config.issue_prefixes)


# ---------------------------------------------------------------------------
# Check-run / PR-SHA queries
# ---------------------------------------------------------------------------


def _query_failed_check_runs(
    owner: str,
    repo: str,
    head_sha: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str] | None:
    """Compatibility wrapper for adapter-owned failed-check queries."""
    from startupai_controller.adapters.github_cli import _query_failed_check_runs as _adapter_query_failed_check_runs

    return _adapter_query_failed_check_runs(
        owner,
        repo,
        head_sha,
        gh_runner=gh_runner or _run_gh,
    )


def _query_pr_head_sha(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str | None:
    """Compatibility wrapper for adapter-owned PR head SHA queries."""
    from startupai_controller.adapters.github_cli import _query_pr_head_sha as _adapter_query_pr_head_sha

    return _adapter_query_pr_head_sha(
        owner,
        repo,
        pr_number,
        gh_runner=gh_runner or _run_gh,
    )


# ---------------------------------------------------------------------------
# Project field operations
# ---------------------------------------------------------------------------


def _query_issue_board_info(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> BoardInfo:
    """Compatibility wrapper for adapter-owned board info resolution."""
    from startupai_controller.adapters.github_cli import (
        _query_issue_board_info as _adapter_query_issue_board_info,
    )

    return _adapter_query_issue_board_info(
        issue_ref,
        config,
        project_owner,
        project_number,
        gh_runner=gh_runner or _run_gh,
    )


def _query_status_field_option(
    project_id: str,
    option_name: str = "Ready",
    *,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, str]:
    """Compatibility wrapper for adapter-owned Status option resolution."""
    from startupai_controller.adapters.github_cli import (
        _query_status_field_option as _adapter_query_status_field_option,
    )

    return _adapter_query_status_field_option(
        project_id,
        option_name,
        gh_runner=gh_runner or _run_gh,
    )


def _set_board_status(
    project_id: str,
    item_id: str,
    field_id: str,
    option_id: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper for adapter-owned Status mutation."""
    from startupai_controller.adapters.github_cli import (
        _set_board_status as _adapter_set_board_status,
    )

    _adapter_set_board_status(
        project_id,
        item_id,
        field_id,
        option_id,
        gh_runner=gh_runner or _run_gh,
    )


def _query_project_item_field(
    issue_ref: str,
    field_name: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Compatibility wrapper for adapter-owned project field reads."""
    from startupai_controller.adapters.github_cli import (
        _query_project_item_field as _adapter_query_project_item_field,
    )

    return _adapter_query_project_item_field(
        issue_ref,
        field_name,
        config,
        project_owner,
        project_number,
        gh_runner=gh_runner or _run_gh,
    )


def _set_text_field(
    project_id: str,
    item_id: str,
    field_name: str,
    value: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper for adapter-owned text field mutation."""
    from startupai_controller.adapters.github_cli import (
        _set_text_field as _adapter_set_text_field,
    )

    _adapter_set_text_field(
        project_id,
        item_id,
        field_name,
        value,
        gh_runner=gh_runner or _run_gh,
    )


def _query_single_select_field_option(
    project_id: str,
    field_name: str,
    option_name: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, str]:
    """Compatibility wrapper for adapter-owned single-select resolution."""
    from startupai_controller.adapters.github_cli import (
        _query_single_select_field_option as _adapter_query_single_select_field_option,
    )

    return _adapter_query_single_select_field_option(
        project_id,
        field_name,
        option_name,
        gh_runner=gh_runner or _run_gh,
    )


def _set_single_select_field(
    project_id: str,
    item_id: str,
    field_name: str,
    option_name: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper for adapter-owned single-select mutation."""
    from startupai_controller.adapters.github_cli import (
        _set_single_select_field as _adapter_set_single_select_field,
    )

    _adapter_set_single_select_field(
        project_id,
        item_id,
        field_name,
        option_name,
        gh_runner=gh_runner or _run_gh,
    )


def _set_status_if_changed(
    issue_ref: str,
    from_statuses: set[str],
    to_status: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    board_info_resolver: Callable[..., BoardInfo] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[bool, str]:
    """Compatibility wrapper for adapter-owned safe status mutation."""
    from startupai_controller.adapters.github_cli import (
        _set_status_if_changed as _adapter_set_status_if_changed,
    )

    return _adapter_set_status_if_changed(
        issue_ref,
        from_statuses,
        to_status,
        config,
        project_owner,
        project_number,
        board_info_resolver=board_info_resolver or _query_issue_board_info,
        board_mutator=board_mutator,
        gh_runner=gh_runner or _run_gh,
    )


# ---------------------------------------------------------------------------
# Issue timestamp & PR queries
# ---------------------------------------------------------------------------


def _query_issue_updated_at(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime:
    """Compatibility wrapper for adapter-owned issue timestamp queries."""
    from startupai_controller.adapters.github_cli import _query_issue_updated_at as _adapter_query_issue_updated_at

    return _adapter_query_issue_updated_at(
        owner,
        repo,
        number,
        gh_runner=gh_runner or _run_gh,
    )


def _parse_pr_url(pr_field: str) -> tuple[str, str, int] | None:
    """Extract owner/repo/pr_number from a GitHub PR URL in project field."""
    from startupai_controller.domain.repair_policy import parse_pr_url  # canonical (M5)

    return parse_pr_url(pr_field)


def _is_pr_open(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Compatibility wrapper for adapter-owned PR-open queries."""
    from startupai_controller.adapters.github_cli import _is_pr_open as _adapter_is_pr_open

    return _adapter_is_pr_open(
        owner,
        repo,
        pr_number,
        gh_runner=gh_runner or _run_gh,
    )


def _query_open_pr_updated_at(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Compatibility wrapper for adapter-owned PR timestamp queries."""
    from startupai_controller.adapters.github_cli import _query_open_pr_updated_at as _adapter_query_open_pr_updated_at

    return _adapter_query_open_pr_updated_at(
        owner,
        repo,
        pr_number,
        gh_runner=gh_runner or _run_gh,
    )


def _extract_run_id(details_url: str) -> int | None:
    """Parse a GitHub Actions run id from a status details URL."""
    match = re.search(r"/actions/runs/(\d+)(?:/|$)", details_url or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def query_open_pull_requests(
    pr_repo: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[OpenPullRequest]:
    """Compatibility wrapper for adapter-owned open-PR listing."""
    from startupai_controller.adapters.github_cli import query_open_pull_requests as _adapter_query_open_pull_requests

    return _adapter_query_open_pull_requests(
        pr_repo,
        gh_runner=gh_runner or _run_gh,
    )


def memoized_query_open_pull_requests(
    memo: CycleGitHubMemo,
    pr_repo: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[OpenPullRequest]:
    """Compatibility wrapper for adapter-owned memoized open-PR listing."""
    from startupai_controller.adapters.github_cli import GitHubCliAdapter

    adapter = GitHubCliAdapter(
        project_owner="",
        project_number=0,
        github_memo=memo,
        gh_runner=gh_runner or _run_gh,
    )
    cached = adapter._github_memo.open_pull_requests.get(pr_repo)
    if cached is not None:
        return list(cached)
    results = adapter.list_open_prs(pr_repo)
    adapter._github_memo.open_pull_requests[pr_repo] = list(results)
    return results


def query_pull_request_view_payload(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> PullRequestViewPayload:
    """Compatibility wrapper for adapter-owned PR payload reads."""
    from startupai_controller.adapters.github_cli import query_pull_request_view_payload as _adapter_query_pull_request_view_payload

    return _adapter_query_pull_request_view_payload(
        pr_repo,
        pr_number,
        gh_runner=gh_runner or _run_gh,
    )


def _pull_request_view_payload_from_json(
    pr_repo: str,
    pr_number: int,
    payload: dict[str, Any],
) -> PullRequestViewPayload:
    """Normalize one PR payload into the shared review-rescue shape."""
    comments = tuple(
        item for item in (payload.get("comments", []) or []) if isinstance(item, dict)
    )
    reviews = tuple(
        item for item in (payload.get("reviews", []) or []) if isinstance(item, dict)
    )
    status_check_rollup = tuple(
        item
        for item in (payload.get("statusCheckRollup", []) or [])
        if isinstance(item, dict)
    )
    return PullRequestViewPayload(
        pr_repo=pr_repo,
        pr_number=pr_number,
        author=str(((payload.get("author") or {}).get("login") or "")).strip().lower(),
        body=str(payload.get("body") or ""),
        state=str(payload.get("state") or ""),
        is_draft=bool(payload.get("isDraft", False)),
        merge_state_status=str(payload.get("mergeStateStatus") or ""),
        mergeable=str(payload.get("mergeable") or ""),
        base_ref_name=str(payload.get("baseRefName") or "main"),
        auto_merge_enabled=payload.get("autoMergeRequest") is not None,
        comments=comments,
        reviews=reviews,
        status_check_rollup=status_check_rollup,
    )


def _normalize_graphql_rollup_node(node: dict[str, Any]) -> dict[str, Any] | None:
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


def _pull_request_view_payload_from_graphql_node(
    pr_repo: str,
    pr_number: int,
    node: dict[str, Any],
) -> PullRequestViewPayload:
    """Normalize one GraphQL PR node into the shared review-rescue shape."""
    review_nodes = (
        (node.get("reviews") or {}).get("nodes", [])
        if isinstance(node.get("reviews"), dict)
        else []
    )
    comment_nodes = (
        (node.get("comments") or {}).get("nodes", [])
        if isinstance(node.get("comments"), dict)
        else []
    )
    commit_nodes = (
        (node.get("commits") or {}).get("nodes", [])
        if isinstance(node.get("commits"), dict)
        else []
    )
    status_nodes: list[dict[str, Any]] = []
    if commit_nodes:
        latest_commit = commit_nodes[-1]
        rollup = (((latest_commit.get("commit") or {}).get("statusCheckRollup") or {}).get("contexts") or {})
        for item in rollup.get("nodes", []) or []:
            if not isinstance(item, dict):
                continue
            normalized = _normalize_graphql_rollup_node(item)
            if normalized is not None:
                status_nodes.append(normalized)

    return PullRequestViewPayload(
        pr_repo=pr_repo,
        pr_number=pr_number,
        author=str(((node.get("author") or {}).get("login") or "")).strip().lower(),
        body=str(node.get("body") or ""),
        state=str(node.get("state") or ""),
        is_draft=bool(node.get("isDraft", False)),
        merge_state_status=str(node.get("mergeStateStatus") or ""),
        mergeable=str(node.get("mergeable") or ""),
        base_ref_name=str(node.get("baseRefName") or "main"),
        auto_merge_enabled=node.get("autoMergeRequest") is not None,
        comments=tuple(item for item in comment_nodes if isinstance(item, dict)),
        reviews=tuple(item for item in review_nodes if isinstance(item, dict)),
        status_check_rollup=tuple(status_nodes),
    )


def _latest_node_timestamp(nodes: Sequence[dict[str, Any]], *keys: str) -> str:
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


def _pull_request_state_probe_from_graphql_node(
    pr_repo: str,
    pr_number: int,
    node: dict[str, Any],
) -> PullRequestStateProbe:
    """Normalize one lightweight GraphQL PR node into a review-state probe."""
    review_nodes = (
        (node.get("reviews") or {}).get("nodes", [])
        if isinstance(node.get("reviews"), dict)
        else []
    )
    comment_nodes = (
        (node.get("comments") or {}).get("nodes", [])
        if isinstance(node.get("comments"), dict)
        else []
    )
    commit_nodes = (
        (node.get("commits") or {}).get("nodes", [])
        if isinstance(node.get("commits"), dict)
        else []
    )
    status_nodes: list[dict[str, Any]] = []
    if commit_nodes:
        latest_commit = commit_nodes[-1]
        rollup = (
            (((latest_commit.get("commit") or {}).get("statusCheckRollup") or {}).get("contexts") or {})
        )
        for item in rollup.get("nodes", []) or []:
            if not isinstance(item, dict):
                continue
            normalized = _normalize_graphql_rollup_node(item)
            if normalized is not None:
                status_nodes.append(normalized)

    return PullRequestStateProbe(
        pr_repo=pr_repo,
        pr_number=pr_number,
        state=str(node.get("state") or ""),
        is_draft=bool(node.get("isDraft", False)),
        merge_state_status=str(node.get("mergeStateStatus") or ""),
        mergeable=str(node.get("mergeable") or ""),
        base_ref_name=str(node.get("baseRefName") or "main"),
        auto_merge_enabled=node.get("autoMergeRequest") is not None,
        head_ref_oid=str(node.get("headRefOid") or ""),
        updated_at=str(node.get("updatedAt") or ""),
        latest_comment_at=_latest_node_timestamp(comment_nodes, "createdAt"),
        latest_review_at=_latest_node_timestamp(review_nodes, "submittedAt"),
        status_check_rollup=tuple(status_nodes),
    )


def review_state_probe_from_payload(
    payload: PullRequestViewPayload,
) -> PullRequestStateProbe:
    """Build a lightweight review-state probe from an expanded PR payload."""
    latest_comment_at = ""
    if payload.comments:
        latest_comment_at = max(
            str(comment.get("createdAt") or "")
            for comment in payload.comments
            if isinstance(comment, dict)
        )
    latest_review_at = ""
    if payload.reviews:
        latest_review_at = max(
            str(review.get("submittedAt") or "")
            for review in payload.reviews
            if isinstance(review, dict)
        )
    return PullRequestStateProbe(
        pr_repo=payload.pr_repo,
        pr_number=payload.pr_number,
        state=payload.state,
        is_draft=payload.is_draft,
        merge_state_status=payload.merge_state_status,
        mergeable=payload.mergeable,
        base_ref_name=payload.base_ref_name,
        auto_merge_enabled=payload.auto_merge_enabled,
        head_ref_oid="",
        updated_at="",
        latest_comment_at=latest_comment_at,
        latest_review_at=latest_review_at,
        status_check_rollup=payload.status_check_rollup,
    )


def query_pull_request_state_probes(
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, PullRequestStateProbe]:
    """Compatibility wrapper for adapter-owned PR state probes."""
    from startupai_controller.adapters.github_cli import query_pull_request_state_probes as _adapter_query_pull_request_state_probes

    return _adapter_query_pull_request_state_probes(
        pr_repo,
        pr_numbers,
        gh_runner=gh_runner or _run_gh,
    )


def query_pull_request_view_payloads(
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, PullRequestViewPayload]:
    """Compatibility wrapper for adapter-owned batched PR payload reads."""
    from startupai_controller.adapters.github_cli import query_pull_request_view_payloads as _adapter_query_pull_request_view_payloads

    return _adapter_query_pull_request_view_payloads(
        pr_repo,
        pr_numbers,
        gh_runner=gh_runner or _run_gh,
    )


def memoized_query_pull_request_view_payload(
    memo: CycleGitHubMemo,
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> PullRequestViewPayload:
    """Return an expanded PR payload using cycle-local memoization."""
    payloads = memoized_query_pull_request_view_payloads(
        memo,
        pr_repo,
        (pr_number,),
        gh_runner=gh_runner,
    )
    payload = payloads.get(pr_number)
    if payload is None:
        raise GhQueryError(f"Failed querying PR {pr_repo}#{pr_number}: pull request not found.")
    return payload


def memoized_query_pull_request_view_payloads(
    memo: CycleGitHubMemo,
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, PullRequestViewPayload]:
    """Compatibility wrapper for adapter-owned memoized PR payload reads."""
    from startupai_controller.adapters.github_cli import memoized_query_pull_request_view_payloads as _adapter_memoized_query_pull_request_view_payloads

    return _adapter_memoized_query_pull_request_view_payloads(
        memo,
        pr_repo,
        pr_numbers,
        gh_runner=gh_runner or _run_gh,
    )


def memoized_query_pull_request_state_probes(
    memo: CycleGitHubMemo,
    pr_repo: str,
    pr_numbers: Sequence[int],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[int, PullRequestStateProbe]:
    """Compatibility wrapper for adapter-owned memoized PR state probes."""
    from startupai_controller.adapters.github_cli import memoized_query_pull_request_state_probes as _adapter_memoized_query_pull_request_state_probes

    return _adapter_memoized_query_pull_request_state_probes(
        memo,
        pr_repo,
        pr_numbers,
        gh_runner=gh_runner or _run_gh,
    )


def query_required_status_checks(
    pr_repo: str,
    base_ref_name: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> set[str]:
    """Compatibility wrapper for adapter-owned required-check policy reads."""
    from startupai_controller.adapters.github_cli import query_required_status_checks as _adapter_query_required_status_checks

    return _adapter_query_required_status_checks(
        pr_repo,
        base_ref_name,
        gh_runner=gh_runner or _run_gh,
    )


def memoized_query_required_status_checks(
    memo: CycleGitHubMemo,
    pr_repo: str,
    base_ref_name: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> set[str]:
    """Compatibility wrapper for adapter-owned memoized required-check reads."""
    from startupai_controller.adapters.github_cli import GitHubCliAdapter

    adapter = GitHubCliAdapter(
        project_owner="",
        project_number=0,
        github_memo=memo,
        gh_runner=gh_runner or _run_gh,
    )
    key = (pr_repo, base_ref_name)
    cached = adapter._github_memo.required_status_checks.get(key)
    if cached is not None:
        return set(cached)
    required = adapter._query_required_status_checks(pr_repo, base_ref_name)
    adapter._github_memo.required_status_checks[key] = set(required)
    return set(required)


def clear_required_status_checks_cache() -> None:
    """Compatibility wrapper for adapter-owned required-check TTL cache clearing."""
    from startupai_controller.adapters.github_cli import clear_required_status_checks_cache as _adapter_clear_required_status_checks_cache

    _adapter_clear_required_status_checks_cache()


def query_issue_body(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Compatibility wrapper for adapter-owned issue-body queries."""
    from startupai_controller.adapters.github_cli import query_issue_body as _adapter_query_issue_body

    return _adapter_query_issue_body(
        owner,
        repo,
        number,
        gh_runner=gh_runner or _run_gh,
    )


def memoized_query_issue_body(
    memo: CycleGitHubMemo,
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Compatibility wrapper for adapter-owned memoized issue-body queries."""
    from startupai_controller.adapters.github_cli import memoized_query_issue_body as _adapter_memoized_query_issue_body

    return _adapter_memoized_query_issue_body(
        memo,
        owner,
        repo,
        number,
        gh_runner=gh_runner or _run_gh,
    )


def _query_latest_wip_activity_timestamp(
    issue_ref: str,
    owner: str,
    repo: str,
    number: int,
    pr_field: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Compatibility wrapper for adapter-owned WIP activity queries."""
    from startupai_controller.adapters.github_cli import _query_latest_wip_activity_timestamp as _adapter_query_latest_wip_activity_timestamp

    return _adapter_query_latest_wip_activity_timestamp(
        issue_ref,
        owner,
        repo,
        number,
        pr_field,
        gh_runner=gh_runner or _run_gh,
    )


# ---------------------------------------------------------------------------
# Issue assignee operations
# ---------------------------------------------------------------------------


def _query_issue_assignees(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Compatibility wrapper for adapter-owned assignee reads."""
    from startupai_controller.adapters.github_cli import _query_issue_assignees as _adapter_query_issue_assignees

    return _adapter_query_issue_assignees(
        owner,
        repo,
        number,
        gh_runner=gh_runner or _run_gh,
    )


def _set_issue_assignees(
    owner: str,
    repo: str,
    number: int,
    assignees: list[str],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Compatibility wrapper for adapter-owned assignee writes."""
    from startupai_controller.adapters.github_cli import _set_issue_assignees as _adapter_set_issue_assignees

    _adapter_set_issue_assignees(
        owner,
        repo,
        number,
        assignees,
        gh_runner=gh_runner or _run_gh,
    )


# _priority_rank re-exported from domain.scheduling_policy (M5)


def _list_project_items_by_status(
    status: str,
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[_ProjectItemSnapshot]:
    """Compatibility wrapper for adapter-owned project status queries."""
    from startupai_controller.adapters.review_state import _list_project_items_by_status as _adapter_list_project_items_by_status

    return _adapter_list_project_items_by_status(
        status,
        project_owner,
        project_number,
        gh_runner=gh_runner or _run_gh,
    )


def build_cycle_board_snapshot(
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> CycleBoardSnapshot:
    """Compatibility wrapper for adapter-owned cycle board snapshots."""
    from startupai_controller.adapters.review_state import build_cycle_board_snapshot as _adapter_build_cycle_board_snapshot

    return _adapter_build_cycle_board_snapshot(
        project_owner,
        project_number,
        gh_runner=gh_runner or _run_gh,
    )


def clear_cycle_board_snapshot_cache() -> None:
    """Compatibility wrapper for adapter-owned snapshot cache clearing."""
    from startupai_controller.adapters.review_state import clear_cycle_board_snapshot_cache as _adapter_clear_cycle_board_snapshot_cache

    _adapter_clear_cycle_board_snapshot_cache()


def _list_project_items(
    project_owner: str,
    project_number: int,
    *,
    statuses: set[str] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[_ProjectItemSnapshot]:
    """Compatibility wrapper for adapter-owned rich project item reads."""
    from startupai_controller.adapters.github_cli import _list_project_items as _adapter_list_project_items

    return _adapter_list_project_items(
        project_owner,
        project_number,
        statuses=statuses,
        gh_runner=gh_runner or _run_gh,
    )


def query_closing_issues(
    pr_owner: str,
    pr_repo: str,
    pr_number: int,
    config: CriticalPathConfig,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[LinkedIssue]:
    """Compatibility wrapper for adapter-owned closing-issue resolution."""
    from startupai_controller.adapters.github_cli import query_closing_issues as _adapter_query_closing_issues

    return _adapter_query_closing_issues(
        pr_owner,
        pr_repo,
        pr_number,
        config,
        gh_runner=gh_runner or _run_gh,
    )


def _parse_codex_verdict_from_text(
    text: str,
) -> tuple[str | None, str | None, list[str]]:
    """Extract codex verdict markers from free text."""
    decision_match = re.search(r"\bcodex-review\s*:\s*(pass|fail)\b", text, re.I)
    route_match = re.search(
        r"\bcodex-route\s*:\s*(none|codex|executor|claude|human)\b",
        text,
        re.I,
    )
    checklist = re.findall(r"^\s*-\s*\[\s\]\s+(.+)$", text, flags=re.M)
    decision = decision_match.group(1).lower() if decision_match else None
    route = route_match.group(1).lower() if route_match else None
    return decision, route, checklist


def query_latest_codex_verdict(
    pr_repo: str,
    pr_number: int,
    *,
    trusted_actors: set[str] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> CodexReviewVerdict | None:
    """Compatibility wrapper for adapter-owned codex verdict lookup."""
    from startupai_controller.adapters.github_cli import query_latest_codex_verdict as _adapter_query_latest_codex_verdict

    return _adapter_query_latest_codex_verdict(
        pr_repo,
        pr_number,
        trusted_actors=trusted_actors,
        gh_runner=gh_runner or _run_gh,
    )


def latest_codex_verdict_from_payload(
    payload: PullRequestViewPayload,
    *,
    trusted_actors: set[str] | None = None,
) -> CodexReviewVerdict | None:
    """Compatibility wrapper for adapter-owned verdict parsing."""
    from startupai_controller.adapters.github_cli import latest_codex_verdict_from_payload as _adapter_latest_codex_verdict_from_payload

    return _adapter_latest_codex_verdict_from_payload(
        payload,
        trusted_actors=trusted_actors,
    )


def has_copilot_review_signal_from_payload(payload: PullRequestViewPayload) -> bool:
    """Return True when Copilot has submitted approved/commented review."""
    accepted_states = {"APPROVED", "COMMENTED"}
    for review in payload.reviews:
        state = str(review.get("state", "")).upper()
        actor = ((review.get("author") or {}).get("login") or "").lower()
        if "copilot" in actor and state in accepted_states:
            return True
    return False


def build_pr_gate_status_from_payload(
    payload: PullRequestViewPayload,
    *,
    required: set[str],
) -> PrGateStatus:
    """Compatibility wrapper for adapter-owned PR gate evaluation."""
    from startupai_controller.adapters.github_cli import build_pr_gate_status_from_payload as _adapter_build_pr_gate_status_from_payload

    return _adapter_build_pr_gate_status_from_payload(
        payload,
        required=required,
    )


def review_state_digest_from_probe(probe: PullRequestStateProbe) -> str:
    """Return a stable digest for the lightweight state of a review PR."""
    latest_checks: dict[str, tuple[str, str]] = {}
    for check in probe.status_check_rollup:
        typename = check.get("__typename", "")
        if typename == "CheckRun":
            name = str(check.get("name") or "")
            timestamp = str(check.get("completedAt") or check.get("startedAt") or "")
            status = str(check.get("status") or "").lower()
            conclusion = str(check.get("conclusion") or "").lower()
            result = (
                "pending"
                if status != "completed"
                else (
                    "pass"
                    if conclusion in {"success", "neutral", "skipped"}
                    else (
                        "cancelled"
                        if conclusion in {"cancelled", "startup_failure", "stale"}
                        else "fail"
                    )
                )
            )
        elif typename == "StatusContext":
            name = str(check.get("context") or "")
            timestamp = str(check.get("startedAt") or "")
            state = str(check.get("state") or "").lower()
            result = "pass" if state == "success" else ("fail" if state in {"error", "failure"} else "pending")
        else:
            continue
        if not name:
            continue
        previous = latest_checks.get(name)
        if previous is None or timestamp >= previous[0]:
            latest_checks[name] = (timestamp, result)

    payload = {
        "state": probe.state.strip().upper(),
        "is_draft": bool(probe.is_draft),
        "merge_state_status": probe.merge_state_status,
        "mergeable": probe.mergeable,
        "base_ref_name": probe.base_ref_name,
        "auto_merge_enabled": bool(probe.auto_merge_enabled),
        "head_ref_oid": probe.head_ref_oid,
        "updated_at": probe.updated_at,
        "latest_comment_at": probe.latest_comment_at,
        "latest_review_at": probe.latest_review_at,
        "checks": sorted((name, result) for name, (_ts, result) in latest_checks.items()),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def review_state_digest_from_payload(payload: PullRequestViewPayload) -> str:
    """Return a stable review-state digest from an expanded PR payload."""
    return review_state_digest_from_probe(review_state_probe_from_payload(payload))


def _query_pr_gate_status(
    pr_repo: str,
    pr_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> PrGateStatus:
    """Compatibility wrapper for adapter-owned PR gate-status reads."""
    from startupai_controller.adapters.github_cli import GitHubCliAdapter

    adapter = GitHubCliAdapter(project_owner="", project_number=0, gh_runner=gh_runner or _run_gh)
    return adapter.get_gate_status(pr_repo, pr_number)
