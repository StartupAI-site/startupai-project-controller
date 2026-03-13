"""Board/project helper seams for the pull-request capability adapter."""

from __future__ import annotations

from collections.abc import Callable
import json
from typing import Protocol

from startupai_controller.adapters.board_mutation import GitHubBoardMutationAdapter
from startupai_controller.adapters.github_transport import _run_gh
from startupai_controller.adapters.github_types import _ProjectItemSnapshot
from startupai_controller.promote_ready import BoardInfo
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
)


class BoardInfoResolver(Protocol):
    """Typed compatibility seam for board-info lookups."""

    def __call__(
        self,
        issue_ref: str,
        config: CriticalPathConfig,
        project_owner: str,
        project_number: int,
    ) -> BoardInfo: ...


class BoardStatusMutator(Protocol):
    """Typed compatibility seam for direct board-status mutations."""

    def __call__(self, project_id: str, item_id: str) -> None: ...


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
    board_info_resolver: BoardInfoResolver | None = None,
    board_mutator: BoardStatusMutator | None = None,
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
        item_info = adapter._query_board_info(issue_ref)
        info = BoardInfo(
            status=item_info.status,
            item_id=item_info.item_id,
            project_id=item_info.project_id,
        )
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
