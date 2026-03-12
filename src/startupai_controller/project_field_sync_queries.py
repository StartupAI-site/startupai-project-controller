"""GitHub query helpers for project field synchronization."""

from __future__ import annotations

import json
import subprocess
from typing import Any, Callable

from startupai_controller.project_field_sync_core import (
    FieldSpec,
    IssueItem,
    LinkedPull,
    ProjectSchema,
)
from startupai_controller.validate_critical_path_promotion import GhQueryError


def _run_gh(
    args: list[str],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> str:
    """Run a gh command and return stdout."""
    if gh_runner is not None:
        return gh_runner(args)
    try:
        return subprocess.check_output(
            ["gh"] + args,
            text=True,
            stderr=subprocess.STDOUT,
        )
    except OSError as error:
        raise GhQueryError(
            f"Failed running gh {' '.join(args[:3])}: {error}"
        ) from error
    except subprocess.CalledProcessError as error:
        raise GhQueryError(
            f"Failed running gh {' '.join(args[:3])}: {error.output.strip()}"
        ) from error


def _run_gh_json(
    args: list[str],
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[str, Any]:
    """Run gh and decode JSON payload."""
    output = _run_gh(args, gh_runner=gh_runner)
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise GhQueryError("Failed parsing gh JSON output.") from error
    return payload


def _must_get_graphql_data(payload: dict[str, Any], context: str) -> dict[str, Any]:
    """Validate GraphQL response and return data node."""
    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        messages = [
            err.get("message", "unknown GraphQL error")
            for err in errors
            if isinstance(err, dict)
        ]
        joined = "; ".join(messages) if messages else "unknown GraphQL error"
        raise GhQueryError(f"{context}: {joined}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise GhQueryError(f"{context}: missing data payload")
    return data


def query_project_schema(
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> ProjectSchema:
    """Fetch project ID and field metadata."""
    query = """
query($owner: String!, $number: Int!) {
  organization(login: $owner) {
    projectV2(number: $number) {
      id
      fields(first: 100) {
        nodes {
          ... on ProjectV2FieldCommon {
            id
            name
            dataType
          }
          ... on ProjectV2SingleSelectField {
            options {
              id
              name
            }
          }
        }
      }
    }
  }
}
"""

    payload = _run_gh_json(
        [
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-f",
            f"owner={project_owner}",
            "-F",
            f"number={project_number}",
        ],
        gh_runner=gh_runner,
    )
    data = _must_get_graphql_data(payload, "Failed querying project schema")
    project = data.get("organization", {}).get("projectV2") or {}

    project_id = project.get("id", "")
    if not project_id:
        raise GhQueryError("Project not found for schema query.")

    fields: dict[str, FieldSpec] = {}
    for node in (project.get("fields") or {}).get("nodes", []):
        name = str(node.get("name", "")).strip()
        field_id = str(node.get("id", "")).strip()
        data_type = str(node.get("dataType", "")).strip()
        if not name or not field_id or not data_type:
            continue

        options = {
            str(option.get("name", "")).strip(): str(option.get("id", "")).strip()
            for option in node.get("options", []) or []
            if str(option.get("name", "")).strip() and str(option.get("id", "")).strip()
        }

        fields[name] = FieldSpec(
            id=field_id,
            name=name,
            data_type=data_type,
            options=options,
        )

    return ProjectSchema(project_id=project_id, fields=fields)


def list_project_issue_items(
    project_owner: str,
    project_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> list[IssueItem]:
    """List all issue-backed project items with required field snapshots."""
    query = """
query($owner: String!, $number: Int!, $cursor: String) {
  organization(login: $owner) {
    projectV2(number: $number) {
      items(first: 100, after: $cursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          id
          statusField: fieldValueByName(name: "Status") {
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
          executorField: fieldValueByName(name: "Executor") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          ownerField: fieldValueByName(name: "Owner") {
            ... on ProjectV2ItemFieldTextValue { text }
          }
          handoffField: fieldValueByName(name: "Handoff To") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          prField: fieldValueByName(name: "PR") {
            ... on ProjectV2ItemFieldTextValue { text }
          }
          ciField: fieldValueByName(name: "CI") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
          sourceField: fieldValueByName(name: "Source") {
            ... on ProjectV2ItemFieldTextValue { text }
          }
          blockedReasonField: fieldValueByName(name: "Blocked Reason") {
            ... on ProjectV2ItemFieldTextValue { text }
          }
          startDateField: fieldValueByName(name: "Start Date") {
            ... on ProjectV2ItemFieldDateValue { date }
          }
          targetDateField: fieldValueByName(name: "Target Date") {
            ... on ProjectV2ItemFieldDateValue { date }
          }
          content {
            ... on Issue {
              number
              state
              repository {
                name
                nameWithOwner
                owner { login }
              }
              assignees(first: 10) {
                nodes { login }
              }
              milestone {
                title
              }
              closedByPullRequestsReferences(first: 20) {
                nodes {
                  url
                  state
                  mergedAt
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

    items: list[IssueItem] = []
    cursor = ""
    has_next = True

    while has_next:
        args = [
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-f",
            f"owner={project_owner}",
            "-F",
            f"number={project_number}",
            "-f",
            f"cursor={cursor}",
        ]

        payload = _run_gh_json(args, gh_runner=gh_runner)
        data = _must_get_graphql_data(payload, "Failed listing project items")

        project_items = (
            data.get("organization", {}).get("projectV2", {}).get("items", {})
        )
        page_info = project_items.get("pageInfo", {})
        has_next = bool(page_info.get("hasNextPage"))
        cursor = str(page_info.get("endCursor", ""))

        for node in project_items.get("nodes", []):
            content = node.get("content") or {}
            number = content.get("number")
            if number is None:
                continue

            repo_data = content.get("repository") or {}
            repo_slug = str(repo_data.get("nameWithOwner", "")).strip()
            repo_name = str(repo_data.get("name", "")).strip()
            owner = str((repo_data.get("owner") or {}).get("login", "")).strip()
            if not repo_slug or not repo_name or not owner:
                continue

            state = str(content.get("state", "")).strip()
            if state == "CLOSED":
                pass

            linked_pulls = [
                LinkedPull(
                    url=str(pr.get("url", "")).strip(),
                    state=str(pr.get("state", "")).strip(),
                    merged_at=pr.get("mergedAt"),
                )
                for pr in (content.get("closedByPullRequestsReferences") or {}).get(
                    "nodes", []
                )
                if str(pr.get("url", "")).strip()
            ]

            items.append(
                IssueItem(
                    item_id=str(node.get("id", "")).strip(),
                    issue_ref=f"{repo_slug}#{number}",
                    repo_slug=repo_slug,
                    owner=owner,
                    repo=repo_name,
                    number=int(number),
                    status=str((node.get("statusField") or {}).get("name", "")).strip(),
                    priority=str(
                        (node.get("priorityField") or {}).get("name", "")
                    ).strip(),
                    sprint=str((node.get("sprintField") or {}).get("name", "")).strip(),
                    agent=str((node.get("agentField") or {}).get("name", "")).strip(),
                    executor=str(
                        (node.get("executorField") or {}).get("name", "")
                    ).strip(),
                    owner_field=str(
                        (node.get("ownerField") or {}).get("text", "")
                    ).strip(),
                    handoff_to=str(
                        (node.get("handoffField") or {}).get("name", "")
                    ).strip(),
                    pr_field=str((node.get("prField") or {}).get("text", "")).strip(),
                    ci=str((node.get("ciField") or {}).get("name", "")).strip(),
                    source=str((node.get("sourceField") or {}).get("text", "")).strip(),
                    blocked_reason=str(
                        (node.get("blockedReasonField") or {}).get("text", "")
                    ).strip(),
                    start_date=str(
                        (node.get("startDateField") or {}).get("date", "")
                    ).strip(),
                    target_date=str(
                        (node.get("targetDateField") or {}).get("date", "")
                    ).strip(),
                    assignees=[
                        str(user.get("login", "")).strip()
                        for user in (content.get("assignees") or {}).get("nodes", [])
                        if str(user.get("login", "")).strip()
                    ],
                    milestone_title=str(
                        (content.get("milestone") or {}).get("title", "")
                    ).strip(),
                    linked_pulls=linked_pulls,
                )
            )

    return items
