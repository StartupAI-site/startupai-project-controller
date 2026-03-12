"""Shared support helpers for the pull-request capability adapter."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime
import json
import re
from typing import Any

from startupai_controller.adapters.board_mutation import GitHubBoardMutationAdapter
from startupai_controller.adapters.github_transport import _run_gh
from startupai_controller.adapters.github_types import (
    CycleGitHubMemo,
    _ProjectItemSnapshot,
)
from startupai_controller.adapters.review_state import (
    _parse_github_timestamp,
    _query_latest_matching_comment_timestamp,
    _query_latest_non_automation_comment_timestamp,
)
from startupai_controller.domain.repair_policy import MARKER_PREFIX
from startupai_controller.domain.repair_policy import parse_pr_url as _parse_pr_url
from startupai_controller.promote_ready import BoardInfo
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
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
    """Read a single project field value through the adapter-owned mechanism."""
    adapter = GitHubBoardMutationAdapter(
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
    )
    return adapter._query_project_field_value(issue_ref, field_name)


def _query_single_select_field_option(
    project_id: str,
    field_name: str,
    option_name: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, str]:
    """Resolve a single-select field option through the adapter-owned mechanism."""
    adapter = GitHubBoardMutationAdapter(
        project_owner="",
        project_number=0,
        gh_runner=gh_runner,
    )
    return adapter._query_single_select_field_option(
        project_id,
        field_name,
        option_name,
    )


def _set_text_field(
    project_id: str,
    item_id: str,
    field_name: str,
    value: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set a project text field through the adapter-owned mechanism."""
    adapter = GitHubBoardMutationAdapter(
        project_owner="",
        project_number=0,
        gh_runner=gh_runner,
    )
    field_id = adapter._query_field_id(project_id, field_name)
    adapter._set_project_text_field(
        project_id=project_id,
        item_id=item_id,
        field_id=field_id,
        value=value,
    )


def _set_single_select_field(
    project_id: str,
    item_id: str,
    field_name: str,
    option_name: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set a project single-select field through the adapter-owned mechanism."""
    adapter = GitHubBoardMutationAdapter(
        project_owner="",
        project_number=0,
        gh_runner=gh_runner,
    )
    field_id, option_id = adapter._query_single_select_field_option(
        project_id,
        field_name,
        option_name,
    )
    adapter._set_project_single_select(
        project_id=project_id,
        item_id=item_id,
        field_id=field_id,
        option_id=option_id,
    )


def _set_status_if_changed(
    issue_ref: str,
    from_statuses: set[str],
    to_status: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    board_info_resolver: Callable[..., Any] | None = None,
    board_mutator: Callable[..., None] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[bool, str]:
    """Safely transition status through the adapter-owned board mutation path."""
    adapter = GitHubBoardMutationAdapter(
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
    )
    if board_info_resolver is None:
        info = adapter._query_board_info(issue_ref)
    else:
        info = board_info_resolver(issue_ref, config, project_owner, project_number)

    if info.status not in from_statuses:
        return False, info.status

    if board_mutator is None:
        field_id, option_id = adapter._query_single_select_field_option(
            info.project_id,
            "Status",
            to_status,
        )
        adapter._set_project_single_select(
            project_id=info.project_id,
            item_id=info.item_id,
            field_id=field_id,
            option_id=option_id,
        )
    else:
        board_mutator(info.project_id, info.item_id)

    return True, info.status


def _query_issue_board_info(
    issue_ref: str,
    config: CriticalPathConfig,
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> BoardInfo:
    """Query board identity/status through the adapter-owned mechanism."""
    adapter = GitHubBoardMutationAdapter(
        project_owner=project_owner,
        project_number=project_number,
        config=config,
        gh_runner=gh_runner,
    )
    info = adapter._query_board_info(issue_ref)
    return BoardInfo(
        status=info.status,
        item_id=info.item_id,
        project_id=info.project_id,
    )


def _query_status_field_option(
    project_id: str,
    option_name: str = "Ready",
    *,
    gh_runner: Callable[..., str] | None = None,
) -> tuple[str, str]:
    """Resolve Status field option IDs through the adapter-owned mechanism."""
    return _query_single_select_field_option(
        project_id,
        "Status",
        option_name,
        gh_runner=gh_runner,
    )


def _set_board_status(
    project_id: str,
    item_id: str,
    field_id: str,
    option_id: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Mutate Status via the adapter-owned project single-select path."""
    adapter = GitHubBoardMutationAdapter(
        project_owner="",
        project_number=0,
        gh_runner=gh_runner,
    )
    adapter._set_project_single_select(
        project_id=project_id,
        item_id=item_id,
        field_id=field_id,
        option_id=option_id,
    )


def _extract_run_id(details_url: str) -> int | None:
    """Extract a GitHub Actions run ID from a details URL when present."""
    match = re.search(r"/actions/runs/(\d+)", details_url)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


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

    return data.get("head", {}).get("sha")


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


def _list_project_items(
    project_owner: str,
    project_number: int,
    *,
    statuses: set[str] | None = None,
    gh_runner: Callable[..., str] | None = None,
) -> list[_ProjectItemSnapshot]:
    """List issue-backed project items with richer field snapshots."""
    query = """
query($owner: String!, $number: Int!, $cursor: String) {
  organization(login: $owner) {
    projectV2(number: $number) {
      id
      items(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          statusField: fieldValueByName(name: "Status") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          executorField: fieldValueByName(name: "Executor") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          handoffField: fieldValueByName(name: "Handoff To") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          priorityField: fieldValueByName(name: "Priority") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          sprintField: fieldValueByName(name: "Sprint") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          agentField: fieldValueByName(name: "Agent") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          ownerField: fieldValueByName(name: "Owner") {
            ... on ProjectV2ItemFieldTextValue { text }
          }
          content {
            ... on Issue {
              number
              title
              body
              repository {
                name
                nameWithOwner
                owner { login }
              }
            }
          }
        }
      }
    }
  }
}
"""

    items: list[_ProjectItemSnapshot] = []
    cursor = ""
    has_next = True

    while has_next:
        gh_args = [
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-f",
            f"owner={project_owner}",
            "-F",
            f"number={project_number}",
        ]
        gh_args.extend(["-f", f"cursor={cursor}" if cursor else "cursor="])
        output = _run_gh(gh_args, gh_runner=gh_runner)

        try:
            payload = json.loads(output)
        except json.JSONDecodeError as error:
            raise GhQueryError("Failed listing project items: invalid JSON.") from error

        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            messages = [
                err.get("message", "unknown GraphQL error")
                for err in errors
                if isinstance(err, dict)
            ]
            joined = "; ".join(messages) if messages else "unknown GraphQL error"
            raise GhQueryError(f"Failed listing project items: {joined}")

        project_data = (
            payload.get("data", {}).get("organization", {}).get("projectV2", {})
        )
        project_id = str(project_data.get("id") or "")
        items_data = project_data.get("items", {})
        page_info = items_data.get("pageInfo", {})
        has_next = bool(page_info.get("hasNextPage", False))
        cursor = str(page_info.get("endCursor") or "")

        for node in items_data.get("nodes", []):
            status = str((node.get("statusField") or {}).get("name") or "")
            if statuses is not None and status not in statuses:
                continue
            content = node.get("content") or {}
            issue_number = content.get("number")
            repo = content.get("repository") or {}
            repo_with_owner = str(repo.get("nameWithOwner") or "")
            repo_name = str(repo.get("name") or "")
            repo_owner = str((repo.get("owner") or {}).get("login") or "")
            if not issue_number or not repo_with_owner:
                continue
            items.append(
                _ProjectItemSnapshot(
                    issue_ref=f"{repo_with_owner}#{issue_number}",
                    status=status,
                    executor=str((node.get("executorField") or {}).get("name") or ""),
                    handoff_to=str((node.get("handoffField") or {}).get("name") or ""),
                    priority=str((node.get("priorityField") or {}).get("name") or ""),
                    item_id=str(node.get("id") or ""),
                    project_id=project_id,
                    sprint=str((node.get("sprintField") or {}).get("name") or ""),
                    agent=str((node.get("agentField") or {}).get("name") or ""),
                    owner_field=str((node.get("ownerField") or {}).get("text") or ""),
                    title=str(content.get("title") or ""),
                    body=str(content.get("body") or ""),
                    repo_slug=repo_with_owner,
                    repo_name=repo_name,
                    repo_owner=repo_owner,
                    issue_number=int(issue_number),
                )
            )
    return items
