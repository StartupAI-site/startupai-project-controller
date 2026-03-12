"""GitHub mutation helpers for project field synchronization."""

from __future__ import annotations

import json
from typing import Callable

from startupai_controller.project_field_sync_core import ProjectSchema
from startupai_controller.project_field_sync_queries import (
    _must_get_graphql_data,
    _run_gh,
    _run_gh_json,
)
from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    GhQueryError,
)


def _set_project_field_value(
    schema: ProjectSchema,
    item_id: str,
    field_name: str,
    value: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set a project field value by name, dispatching on field data type."""
    field = schema.fields.get(field_name)
    if field is None:
        raise GhQueryError(f"Project field '{field_name}' not found")

    if field.data_type == "SINGLE_SELECT":
        option_id = field.options.get(value)
        if not option_id:
            options = ", ".join(sorted(field.options.keys()))
            raise ConfigError(
                f"Invalid option '{value}' for field '{field_name}'. "
                f"Expected one of: {options}"
            )
        mutation = """
mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $projectId,
    itemId: $itemId,
    fieldId: $fieldId,
    value: { singleSelectOptionId: $optionId }
  }) {
    projectV2Item { id }
  }
}
"""
        payload = _run_gh_json(
            [
                "api",
                "graphql",
                "-f",
                f"query={mutation}",
                "-f",
                f"projectId={schema.project_id}",
                "-f",
                f"itemId={item_id}",
                "-f",
                f"fieldId={field.id}",
                "-f",
                f"optionId={option_id}",
            ],
            gh_runner=gh_runner,
        )
        _must_get_graphql_data(payload, f"Failed setting field '{field_name}'")
        return

    if field.data_type == "TEXT":
        mutation = """
mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $textValue: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $projectId,
    itemId: $itemId,
    fieldId: $fieldId,
    value: { text: $textValue }
  }) {
    projectV2Item { id }
  }
}
"""
        payload = _run_gh_json(
            [
                "api",
                "graphql",
                "-f",
                f"query={mutation}",
                "-f",
                f"projectId={schema.project_id}",
                "-f",
                f"itemId={item_id}",
                "-f",
                f"fieldId={field.id}",
                "-f",
                f"textValue={value}",
            ],
            gh_runner=gh_runner,
        )
        _must_get_graphql_data(payload, f"Failed setting field '{field_name}'")
        return

    if field.data_type == "DATE":
        mutation = """
mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $dateValue: Date!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $projectId,
    itemId: $itemId,
    fieldId: $fieldId,
    value: { date: $dateValue }
  }) {
    projectV2Item { id }
  }
}
"""
        payload = _run_gh_json(
            [
                "api",
                "graphql",
                "-f",
                f"query={mutation}",
                "-f",
                f"projectId={schema.project_id}",
                "-f",
                f"itemId={item_id}",
                "-f",
                f"fieldId={field.id}",
                "-f",
                f"dateValue={value}",
            ],
            gh_runner=gh_runner,
        )
        _must_get_graphql_data(payload, f"Failed setting field '{field_name}'")
        return

    raise ConfigError(f"Unsupported field type '{field.data_type}' for '{field_name}'")


def _list_repo_milestones(
    owner: str,
    repo: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> dict[str, int]:
    """List milestones by title for a repository."""
    title_to_number: dict[str, int] = {}
    for state in ("open", "closed"):
        output = _run_gh(
            [
                "api",
                f"repos/{owner}/{repo}/milestones",
                "-X",
                "GET",
                "--paginate",
                "-f",
                f"state={state}",
            ],
            gh_runner=gh_runner,
        )

        try:
            payload = json.loads(output)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, list):
            continue

        for milestone in payload:
            if not isinstance(milestone, dict):
                continue
            title = str(milestone.get("title", "")).strip()
            number = milestone.get("number")
            if title and isinstance(number, int):
                title_to_number[title] = number

    return title_to_number


def _create_repo_milestone(
    owner: str,
    repo: str,
    title: str,
    due_date: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> int:
    """Create milestone and return milestone number."""
    args = [
        "api",
        f"repos/{owner}/{repo}/milestones",
        "-X",
        "POST",
        "-f",
        f"title={title}",
    ]
    if due_date:
        args.extend(["-f", f"due_on={due_date}T00:00:00Z"])

    payload = _run_gh_json(args, gh_runner=gh_runner)
    number = payload.get("number")
    if not isinstance(number, int):
        raise GhQueryError(
            f"Failed creating milestone '{title}' in {owner}/{repo}: missing number"
        )
    return number


def _set_issue_milestone(
    owner: str,
    repo: str,
    number: int,
    milestone_number: int,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Set issue milestone."""
    _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}",
            "-X",
            "PATCH",
            "-F",
            f"milestone={milestone_number}",
        ],
        gh_runner=gh_runner,
    )


def _add_issue_assignee(
    owner: str,
    repo: str,
    number: int,
    assignee: str,
    *,
    gh_runner: Callable[..., str] | None = None,
) -> None:
    """Add an assignee to an issue."""
    _run_gh(
        [
            "api",
            f"repos/{owner}/{repo}/issues/{number}/assignees",
            "-X",
            "POST",
            "-f",
            f"assignees[]={assignee}",
        ],
        gh_runner=gh_runner,
    )
