"""GitHub review-state and board-read adapter.

Owns board snapshot reads, board field/status reads, and issue comment/query
mechanics for the ReviewStatePort capability.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime
import json
import time

from startupai_controller.adapters.board_mutation import GitHubBoardMutationAdapter
from startupai_controller.adapters.github_transport import _run_gh
from startupai_controller.adapters.github_types import (
    COPILOT_CODING_AGENT_LOGINS,
    CycleBoardSnapshot,
    CycleGitHubMemo,
    ProjectItemSnapshot as _ProjectItemSnapshot,
)
from startupai_controller.domain.models import IssueFields, IssueSnapshot
from startupai_controller.domain.repair_policy import MARKER_PREFIX
from startupai_controller.validate_critical_path_promotion import GhQueryError


_BOARD_SNAPSHOT_CACHE_TTL_SECONDS = 15
_cycle_board_snapshot_cache: dict[
    tuple[str, int, int],
    tuple[float, CycleBoardSnapshot],
] = {}


def _is_automation_login(login: str) -> bool:
    """Return True when a login belongs to automation."""
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
    """Parse an ISO timestamp returned by GitHub payloads."""
    text = raw.strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _query_issue_comments(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[dict]:
    """Fetch issue comments as parsed JSON objects."""
    output = _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}/comments",
            "--paginate",
            "--slurp",
        ],
        gh_runner=gh_runner,
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise GhQueryError(
            f"Invalid comments payload for {owner}/{repo}#{number}."
        ) from error

    comments: list[dict] = []
    if isinstance(payload, list):
        for entry in payload:
            if isinstance(entry, list):
                comments.extend(
                    comment for comment in entry if isinstance(comment, dict)
                )
            elif isinstance(entry, dict):
                comments.append(entry)
    return comments


def _comment_activity_timestamp(comment: dict) -> datetime | None:
    """Return the best activity timestamp from a GitHub issue comment payload."""
    return _parse_github_timestamp(
        str(comment.get("updated_at") or comment.get("created_at") or "")
    )


def _query_latest_matching_comment_timestamp(
    owner: str,
    repo: str,
    number: int,
    markers: Sequence[str],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return the latest timestamp for comments containing any marker fragment."""
    try:
        comments = _query_issue_comments(owner, repo, number, gh_runner=gh_runner)
    except GhQueryError:
        return None

    latest: datetime | None = None
    for comment in comments:
        body = str(comment.get("body") or "")
        if not any(marker in body for marker in markers):
            continue
        ts = _comment_activity_timestamp(comment)
        if ts is None:
            continue
        if latest is None or ts > latest:
            latest = ts
    return latest


def _query_latest_non_automation_comment_timestamp(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> datetime | None:
    """Return latest issue-comment timestamp from a non-automation actor."""
    try:
        comments = _query_issue_comments(owner, repo, number, gh_runner=gh_runner)
    except GhQueryError:
        return None

    latest: datetime | None = None
    for comment in comments:
        body = str(comment.get("body") or "")
        if MARKER_PREFIX in body:
            continue
        user = comment.get("user") or {}
        login = str(user.get("login") or "")
        if _is_automation_login(login):
            continue
        ts = _comment_activity_timestamp(comment)
        if ts is None:
            continue
        if latest is None or ts > latest:
            latest = ts
    return latest


class GitHubReviewStateAdapter(GitHubBoardMutationAdapter):
    """Capability adapter implementing board-read and review-state mechanics."""

    def get_issue_status(self, issue_ref: str) -> str | None:
        info = self._query_board_info(issue_ref)
        return info.status if info else None

    def list_issues_by_status(self, status: str) -> list[IssueSnapshot]:
        query = """
query($owner: String!, $number: Int!, $cursor: String) {
  organization(login: $owner) {
    projectV2(number: $number) {
      id
      items(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          fieldValueByName(name: "Status") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          executorField: fieldValueByName(name: "Executor") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          priorityField: fieldValueByName(name: "Priority") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          content {
            ... on Issue {
              number
              title
              repository { nameWithOwner }
            }
          }
        }
      }
    }
  }
}
"""
        results: list[IssueSnapshot] = []
        cursor = ""
        has_next = True
        while has_next:
            payload = self._graphql(
                query,
                fields=[
                    "-f",
                    f"owner={self._project_owner}",
                    "-F",
                    f"number={self._project_number}",
                    "-f",
                    f"cursor={cursor}",
                ],
            )
            project_data = (
                payload.get("data", {}).get("organization", {}).get("projectV2", {})
            )
            project_id = str(project_data.get("id") or "")
            items_data = project_data.get("items", {})
            page_info = items_data.get("pageInfo", {})
            has_next = bool(page_info.get("hasNextPage", False))
            cursor = str(page_info.get("endCursor") or "")

            for node in items_data.get("nodes", []):
                node_status = ((node.get("fieldValueByName") or {}).get("name") or "")
                if node_status != status:
                    continue
                content = node.get("content") or {}
                issue_number = content.get("number")
                repo_with_owner = (
                    (content.get("repository") or {}).get("nameWithOwner") or ""
                )
                if not issue_number or not repo_with_owner:
                    continue
                results.append(
                    IssueSnapshot(
                        issue_ref=self._issue_ref_from_repo_slug(
                            repo_with_owner,
                            int(issue_number),
                        ),
                        status=node_status or status,
                        executor=str((node.get("executorField") or {}).get("name") or ""),
                        priority=str((node.get("priorityField") or {}).get("name") or ""),
                        title=str(content.get("title") or ""),
                        item_id=str(node.get("id") or ""),
                        project_id=project_id,
                    )
                )
        return results

    def get_issue_fields(self, issue_ref: str) -> IssueFields:
        def field(name: str) -> str:
            return self._query_project_field_value(issue_ref, name) or ""

        return IssueFields(
            issue_ref=issue_ref,
            status=field("Status"),
            priority=field("Priority"),
            sprint=field("Sprint"),
            executor=field("Executor"),
            owner=field("Owner"),
            handoff_to=field("Handoff To"),
            blocked_reason=field("Blocked Reason"),
        )

    def search_open_issue_numbers_with_comment_marker(
        self,
        repo: str,
        marker: str,
    ) -> tuple[int, ...]:
        search_query = f'repo:{repo} is:issue is:open in:comments "{marker}"'
        payload = self._gh_json(
            [
                "api",
                "search/issues",
                "-X",
                "GET",
                "-f",
                f"q={search_query}",
                "-f",
                "per_page=100",
            ],
            error_message=f"Invalid issue search payload for repo:{repo}.",
        )
        if not isinstance(payload, dict):
            raise GhQueryError(f"Invalid issue search payload for repo:{repo}.")
        numbers: list[int] = []
        for item in payload.get("items", []):
            number = item.get("number") if isinstance(item, dict) else None
            if number is None:
                continue
            try:
                numbers.append(int(number))
            except (TypeError, ValueError):
                continue
        return tuple(numbers)

    def _list_issue_comment_bodies(
        self,
        owner: str,
        repo: str,
        number: int,
    ) -> list[str]:
        """Return issue comment bodies using the cycle-local memo cache."""
        key = (owner, repo, number)
        cached = self._github_memo.issue_comment_bodies.get(key)
        if cached is not None:
            return list(cached)
        payload = self._gh_json(
            [
                "api",
                f"repos/{owner}/{repo}/issues/{number}/comments",
                "--paginate",
            ],
            error_message=(
                f"Failed querying comments for {owner}/{repo}#{number}: invalid JSON."
            ),
        )
        if not isinstance(payload, list):
            raise GhQueryError(
                f"Failed querying comments for {owner}/{repo}#{number}: invalid payload."
            )
        bodies = [
            str(item.get("body") or "")
            for item in payload
            if isinstance(item, dict) and "body" in item
        ]
        self._github_memo.issue_comment_bodies[key] = list(bodies)
        return list(bodies)

    def _comment_exists(self, owner: str, repo: str, number: int, marker: str) -> bool:
        """Return True when a marker comment already exists on the issue/PR."""
        try:
            return any(
                marker in body
                for body in self._list_issue_comment_bodies(owner, repo, number)
            )
        except GhQueryError:
            return False

    def list_issue_comment_bodies(
        self,
        repo: str,
        issue_number: int,
    ) -> tuple[str, ...]:
        owner, repo_name = repo.split("/", maxsplit=1)
        return tuple(self._list_issue_comment_bodies(owner, repo_name, issue_number))

    def latest_matching_comment_timestamp(
        self,
        repo: str,
        issue_number: int,
        markers: tuple[str, ...],
    ) -> datetime | None:
        owner, repo_name = repo.split("/", maxsplit=1)
        return _query_latest_matching_comment_timestamp(
            owner,
            repo_name,
            issue_number,
            markers,
            gh_runner=self._gh_runner,
        )

    def comment_exists(self, repo: str, issue_number: int, marker: str) -> bool:
        owner, repo_name = repo.split("/", maxsplit=1)
        return self._comment_exists(owner, repo_name, issue_number, marker)

    def build_board_snapshot(self) -> CycleBoardSnapshot:
        return build_cycle_board_snapshot(
            self._project_owner,
            self._project_number,
            gh_runner=self._gh_runner,
        )


def _list_project_items_by_status(
    status: str,
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[_ProjectItemSnapshot]:
    """List board items in a target status."""
    query = """
query($owner: String!, $number: Int!, $cursor: String) {
  organization(login: $owner) {
    projectV2(number: $number) {
      id
      items(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          fieldValueByName(name: "Status") {
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
          content {
            ... on Issue {
              number
              repository { nameWithOwner }
            }
          }
        }
      }
    }
  }
}
"""

    items: list[_ProjectItemSnapshot] = []
    adapter = GitHubReviewStateAdapter(
        project_owner=project_owner,
        project_number=project_number,
        gh_runner=gh_runner,
    )
    cursor = ""
    has_next = True

    while has_next:
        payload = adapter._graphql(
            query,
            fields=[
                "-f",
                f"owner={project_owner}",
                "-F",
                f"number={project_number}",
                "-f",
                f"cursor={cursor}",
            ],
        )

        project_data = (
            payload.get("data", {}).get("organization", {}).get("projectV2", {})
        )
        project_id = str(project_data.get("id") or "")
        items_data = project_data.get("items", {})
        page_info = items_data.get("pageInfo", {})
        has_next = bool(page_info.get("hasNextPage", False))
        cursor = str(page_info.get("endCursor") or "")

        for node in items_data.get("nodes", []):
            node_status = ((node.get("fieldValueByName") or {}).get("name") or "")
            if node_status != status:
                continue
            content = node.get("content") or {}
            issue_number = content.get("number")
            repo_with_owner = (
                (content.get("repository") or {}).get("nameWithOwner") or ""
            )
            if not issue_number or not repo_with_owner:
                continue
            items.append(
                _ProjectItemSnapshot(
                    issue_ref=f"{repo_with_owner}#{issue_number}",
                    status=node_status,
                    executor=str((node.get("executorField") or {}).get("name") or ""),
                    handoff_to=str((node.get("handoffField") or {}).get("name") or ""),
                    priority=str((node.get("priorityField") or {}).get("name") or ""),
                    item_id=str(node.get("id") or ""),
                    project_id=project_id,
                    repo_slug=repo_with_owner,
                    issue_number=int(issue_number),
                )
            )
    return items


def build_cycle_board_snapshot(
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> CycleBoardSnapshot:
    """Build one thin board snapshot for a consumer/control-plane cycle."""
    cache_key = (
        project_owner,
        project_number,
        id(gh_runner) if gh_runner is not None else 0,
    )
    now_monotonic = time.monotonic()
    cached = _cycle_board_snapshot_cache.get(cache_key)
    if cached is not None:
        expires_at, snapshot = cached
        if expires_at > now_monotonic:
            return snapshot

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
              updatedAt
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
    adapter = GitHubReviewStateAdapter(
        project_owner=project_owner,
        project_number=project_number,
        gh_runner=gh_runner,
    )
    cursor = ""
    has_next = True

    while has_next:
        payload = adapter._graphql(
            query,
            fields=[
                "-f",
                f"owner={project_owner}",
                "-F",
                f"number={project_number}",
                "-f",
                f"cursor={cursor}",
            ],
        )

        project_data = (
            payload.get("data", {}).get("organization", {}).get("projectV2", {})
        )
        project_id = str(project_data.get("id") or "")
        items_data = project_data.get("items", {})
        page_info = items_data.get("pageInfo", {})
        has_next = bool(page_info.get("hasNextPage", False))
        cursor = str(page_info.get("endCursor") or "")

        for node in items_data.get("nodes", []):
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
                    status=str((node.get("statusField") or {}).get("name") or ""),
                    executor=str((node.get("executorField") or {}).get("name") or ""),
                    handoff_to=str((node.get("handoffField") or {}).get("name") or ""),
                    priority=str((node.get("priorityField") or {}).get("name") or ""),
                    item_id=str(node.get("id") or ""),
                    project_id=project_id,
                    sprint=str((node.get("sprintField") or {}).get("name") or ""),
                    agent=str((node.get("agentField") or {}).get("name") or ""),
                    owner_field=str((node.get("ownerField") or {}).get("text") or ""),
                    title=str(content.get("title") or ""),
                    repo_slug=repo_with_owner,
                    repo_name=repo_name,
                    repo_owner=repo_owner,
                    issue_number=int(issue_number),
                    issue_updated_at=str(content.get("updatedAt") or ""),
                )
            )

    by_status: dict[str, list[_ProjectItemSnapshot]] = {}
    for item in items:
        by_status.setdefault(item.status, []).append(item)

    snapshot = CycleBoardSnapshot(
        items=tuple(items),
        by_status={status: tuple(group) for status, group in by_status.items()},
    )
    _cycle_board_snapshot_cache[cache_key] = (
        now_monotonic + _BOARD_SNAPSHOT_CACHE_TTL_SECONDS,
        snapshot,
    )
    return snapshot


def clear_cycle_board_snapshot_cache() -> None:
    """Clear the process-local thin board snapshot cache."""
    _cycle_board_snapshot_cache.clear()


def list_issue_comment_bodies(
    owner: str,
    repo: str,
    number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[str]:
    """Compatibility wrapper implemented on the adapter-owned comment path."""
    adapter = GitHubReviewStateAdapter(
        project_owner="",
        project_number=0,
        gh_runner=gh_runner,
    )
    return adapter._list_issue_comment_bodies(owner, repo, number)


def _comment_exists(
    owner: str,
    repo: str,
    number: int,
    marker: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> bool:
    """Compatibility wrapper implemented on the adapter-owned comment path."""
    adapter = GitHubReviewStateAdapter(
        project_owner="",
        project_number=0,
        gh_runner=gh_runner,
    )
    return adapter._comment_exists(owner, repo, number, marker)
