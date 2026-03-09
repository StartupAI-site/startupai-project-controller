#!/usr/bin/env python3
"""Project field completeness synchronization for GitHub Project boards.

This script keeps board metadata populated so table/roadmap views stay usable:
- Fills custom fields (Priority, Sprint, Agent, Executor, Owner, etc.)
- Syncs native issue fields (Assignees, Milestone)
- Fills Start Date / Target Date from sprint windows
- Syncs PR / CI fields from linked pull request state

Exit codes:
  0 - success
  2 - strict audit failed (missing required fields)
  3 - config error
  4 - GitHub API error
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Callable


from startupai_controller.validate_critical_path_promotion import ConfigError, GhQueryError

DEFAULT_PROJECT_OWNER = "StartupAI-site"
DEFAULT_PROJECT_NUMBER = 1
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_SYNC_CONFIG_PATH = str(
    _REPO_ROOT / "config" / "project-field-sync-config.json"
)
VALID_EXECUTORS = {"claude", "codex", "human"}
ACTIVE_STATUSES = {"Ready", "In Progress", "Review", "Blocked"}


@dataclass(frozen=True)
class FieldSpec:
    """Project field metadata needed to mutate values."""

    id: str
    name: str
    data_type: str
    options: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ProjectSchema:
    """Schema for a GitHub Project board."""

    project_id: str
    fields: dict[str, FieldSpec]


@dataclass
class LinkedPull:
    """Linked pull request metadata used for PR/CI sync."""

    url: str
    state: str
    merged_at: str | None


@dataclass
class IssueItem:
    """Snapshot of board + issue metadata for one project issue item."""

    item_id: str
    issue_ref: str
    repo_slug: str
    owner: str
    repo: str
    number: int
    status: str
    priority: str
    sprint: str
    agent: str
    executor: str
    owner_field: str
    handoff_to: str
    pr_field: str
    ci: str
    source: str
    blocked_reason: str
    start_date: str
    target_date: str
    assignees: list[str]
    milestone_title: str
    linked_pulls: list[LinkedPull]


@dataclass(frozen=True)
class SprintWindow:
    """Date window for a sprint."""

    start: str
    target: str


@dataclass
class SyncConfig:
    """Policy config for deterministic field auto-population."""

    default_assignee: str
    executor_assignee_map: dict[str, str]
    defaults: dict[str, str]
    status_defaults: dict[str, dict[str, str]]
    sprint_to_milestone: dict[str, str]
    sprint_windows: dict[str, SprintWindow]


@dataclass
class SyncStats:
    """Mutable counters for reporting sync actions."""

    changed_fields: dict[str, int] = field(default_factory=dict)
    changed_issue_assignees: int = 0
    changed_issue_milestones: int = 0
    created_milestones: int = 0
    processed_issues: int = 0

    def bump_field(self, field_name: str) -> None:
        self.changed_fields[field_name] = self.changed_fields.get(field_name, 0) + 1

    def merge(self, other: "SyncStats") -> None:
        for name, count in other.changed_fields.items():
            self.changed_fields[name] = self.changed_fields.get(name, 0) + count
        self.changed_issue_assignees += other.changed_issue_assignees
        self.changed_issue_milestones += other.changed_issue_milestones
        self.created_milestones += other.created_milestones


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
        raise GhQueryError(f"Failed running gh {' '.join(args[:3])}: {error}") from error
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


def load_sync_config(path: Path) -> SyncConfig:
    """Load and validate field sync policy config."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ConfigError(f"Cannot read sync config '{path}': {error}") from error
    except json.JSONDecodeError as error:
        raise ConfigError(f"Invalid JSON in sync config '{path}': {error}") from error

    version = payload.get("version")
    if version != 1:
        raise ConfigError(
            f"Unsupported sync config version '{version}'. Expected 1."
        )

    default_assignee = str(payload.get("default_assignee", "")).strip()
    if not default_assignee:
        raise ConfigError("sync config default_assignee is required")

    executor_assignee_map = payload.get("executor_assignee_map") or {}
    if not isinstance(executor_assignee_map, dict):
        raise ConfigError("sync config executor_assignee_map must be an object")

    defaults = payload.get("defaults") or {}
    if not isinstance(defaults, dict):
        raise ConfigError("sync config defaults must be an object")

    required_defaults = {
        "priority",
        "sprint",
        "agent",
        "executor",
        "owner",
        "handoff_to",
        "ci",
        "pr",
        "source",
        "blocked_reason",
    }
    missing = [key for key in sorted(required_defaults) if not defaults.get(key)]
    if missing:
        raise ConfigError(f"sync config defaults missing keys: {', '.join(missing)}")

    status_defaults = payload.get("status_defaults") or {}
    if not isinstance(status_defaults, dict):
        raise ConfigError("sync config status_defaults must be an object")

    sprint_to_milestone = payload.get("sprint_to_milestone") or {}
    if not isinstance(sprint_to_milestone, dict):
        raise ConfigError("sync config sprint_to_milestone must be an object")

    sprint_windows_payload = payload.get("sprint_windows") or {}
    if not isinstance(sprint_windows_payload, dict):
        raise ConfigError("sync config sprint_windows must be an object")

    sprint_windows: dict[str, SprintWindow] = {}
    for sprint_name, window_payload in sprint_windows_payload.items():
        if not isinstance(window_payload, dict):
            raise ConfigError(f"sprint_windows.{sprint_name} must be an object")
        sprint_windows[sprint_name] = SprintWindow(
            start=str(window_payload.get("start", "")).strip(),
            target=str(window_payload.get("target", "")).strip(),
        )

    return SyncConfig(
        default_assignee=default_assignee,
        executor_assignee_map={
            str(k): str(v)
            for k, v in executor_assignee_map.items()
            if str(k).strip() and str(v).strip()
        },
        defaults={str(k): str(v) for k, v in defaults.items()},
        status_defaults={
            str(k): {str(sk): str(sv) for sk, sv in (v or {}).items()}
            for k, v in status_defaults.items()
            if isinstance(v, dict)
        },
        sprint_to_milestone={str(k): str(v) for k, v in sprint_to_milestone.items()},
        sprint_windows=sprint_windows,
    )


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
            if str(option.get("name", "")).strip()
            and str(option.get("id", "")).strip()
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
            data.get("organization", {})
            .get("projectV2", {})
            .get("items", {})
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
                # Closed issues may still appear on the board; skip issue-level edits.
                # Project fields can still be synced, so keep them.
                pass

            linked_pulls = [
                LinkedPull(
                    url=str(pr.get("url", "")).strip(),
                    state=str(pr.get("state", "")).strip(),
                    merged_at=pr.get("mergedAt"),
                )
                    for pr in (
                        content.get("closedByPullRequestsReferences") or {}
                    ).get("nodes", [])
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
                    priority=str((node.get("priorityField") or {}).get("name", "")).strip(),
                    sprint=str((node.get("sprintField") or {}).get("name", "")).strip(),
                    agent=str((node.get("agentField") or {}).get("name", "")).strip(),
                    executor=str((node.get("executorField") or {}).get("name", "")).strip(),
                    owner_field=str((node.get("ownerField") or {}).get("text", "")).strip(),
                    handoff_to=str((node.get("handoffField") or {}).get("name", "")).strip(),
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


def _derive_sprint(item: IssueItem, config: SyncConfig) -> str:
    if item.sprint:
        return item.sprint
    return config.defaults["sprint"]


def _derive_executor(item: IssueItem, config: SyncConfig) -> str:
    if item.executor in VALID_EXECUTORS:
        return item.executor

    owner_prefix = item.owner_field.split(":", maxsplit=1)[0].strip().lower()
    if owner_prefix in VALID_EXECUTORS:
        return owner_prefix

    default_executor = config.defaults["executor"]
    if default_executor not in VALID_EXECUTORS:
        raise ConfigError(
            f"Invalid defaults.executor '{default_executor}' in sync config"
        )
    return default_executor


def _derive_owner(item: IssueItem, executor: str, config: SyncConfig) -> str:
    if item.owner_field:
        return item.owner_field
    agent = item.agent or config.defaults["agent"]
    return f"{executor}:{agent}"


def _derive_assignee_login(
    item: IssueItem,
    executor: str,
    config: SyncConfig,
) -> str | None:
    if item.assignees:
        return None

    owner_prefix = item.owner_field.split(":", maxsplit=1)[0].strip().lower()
    if owner_prefix and owner_prefix in config.executor_assignee_map:
        return config.executor_assignee_map[owner_prefix]

    if item.executor in config.executor_assignee_map:
        return config.executor_assignee_map[item.executor]

    if executor in config.executor_assignee_map:
        return config.executor_assignee_map[executor]

    return config.default_assignee


def _status_default_ci(status: str, config: SyncConfig) -> str:
    status_defaults = config.status_defaults.get(status, {})
    return status_defaults.get("ci", config.defaults["ci"])


def _derive_milestone_title(item: IssueItem, config: SyncConfig) -> str:
    sprint = _derive_sprint(item, config)
    title = config.sprint_to_milestone.get(sprint, "").strip()
    if title:
        return title
    fallback = config.sprint_to_milestone.get(config.defaults["sprint"], "").strip()
    return fallback


def _choose_latest_merged(prs: list[LinkedPull]) -> LinkedPull | None:
    merged = [pr for pr in prs if pr.state == "MERGED"]
    if not merged:
        return None
    merged.sort(key=lambda pr: pr.merged_at or "", reverse=True)
    return merged[0]


def derive_pr_and_ci(item: IssueItem, config: SyncConfig) -> tuple[str, str]:
    """Derive canonical PR URL and CI state from linked PRs + board status."""
    default_ci = _status_default_ci(item.status, config)
    default_pr = config.defaults["pr"]

    if not item.linked_pulls:
        return default_pr, default_ci

    open_pr = next((pr for pr in item.linked_pulls if pr.state == "OPEN"), None)
    if open_pr is not None:
        return open_pr.url, "pending"

    merged_pr = _choose_latest_merged(item.linked_pulls)
    if merged_pr is not None:
        return merged_pr.url, "passing"

    closed_pr = next((pr for pr in item.linked_pulls if pr.state == "CLOSED"), None)
    if closed_pr is not None:
        return closed_pr.url, "failing"

    return default_pr, default_ci


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

    raise ConfigError(
        f"Unsupported field type '{field.data_type}' for '{field_name}'"
    )


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


def sync_custom_fields(
    items: list[IssueItem],
    schema: ProjectSchema,
    config: SyncConfig,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
) -> SyncStats:
    """Fill missing custom project fields using deterministic defaults."""
    stats = SyncStats(processed_issues=len(items))

    for item in items:
        executor = _derive_executor(item, config)
        owner_value = _derive_owner(item, executor, config)

        desired = {
            "Priority": item.priority or config.defaults["priority"],
            "Sprint": item.sprint or config.defaults["sprint"],
            "Agent": item.agent or config.defaults["agent"],
            "Executor": item.executor or executor,
            "Owner": item.owner_field or owner_value,
            "Handoff To": item.handoff_to or config.defaults["handoff_to"],
            "Source": item.source or config.defaults["source"],
            "CI": item.ci or _status_default_ci(item.status, config),
            "PR": item.pr_field or config.defaults["pr"],
        }

        if item.status == "Blocked" and not item.blocked_reason:
            desired["Blocked Reason"] = config.defaults["blocked_reason"]

        for field_name, field_value in desired.items():
            if not field_value:
                continue

            current_value = ""
            if field_name == "Priority":
                current_value = item.priority
            elif field_name == "Sprint":
                current_value = item.sprint
            elif field_name == "Agent":
                current_value = item.agent
            elif field_name == "Executor":
                current_value = item.executor
            elif field_name == "Owner":
                current_value = item.owner_field
            elif field_name == "Handoff To":
                current_value = item.handoff_to
            elif field_name == "Source":
                current_value = item.source
            elif field_name == "CI":
                current_value = item.ci
            elif field_name == "PR":
                current_value = item.pr_field
            elif field_name == "Blocked Reason":
                current_value = item.blocked_reason

            if current_value:
                continue

            if not dry_run:
                _set_project_field_value(
                    schema,
                    item.item_id,
                    field_name,
                    field_value,
                    gh_runner=gh_runner,
                )

            if field_name == "Priority":
                item.priority = field_value
            elif field_name == "Sprint":
                item.sprint = field_value
            elif field_name == "Agent":
                item.agent = field_value
            elif field_name == "Executor":
                item.executor = field_value
            elif field_name == "Owner":
                item.owner_field = field_value
            elif field_name == "Handoff To":
                item.handoff_to = field_value
            elif field_name == "Source":
                item.source = field_value
            elif field_name == "CI":
                item.ci = field_value
            elif field_name == "PR":
                item.pr_field = field_value
            elif field_name == "Blocked Reason":
                item.blocked_reason = field_value

            stats.bump_field(field_name)

    return stats


def sync_milestones(
    items: list[IssueItem],
    config: SyncConfig,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
) -> SyncStats:
    """Populate issue milestones from sprint mapping."""
    stats = SyncStats(processed_issues=len(items))
    repo_milestones_cache: dict[str, dict[str, int]] = {}

    for item in items:
        if item.milestone_title:
            continue

        milestone_title = _derive_milestone_title(item, config)
        if not milestone_title:
            continue

        cache = repo_milestones_cache.get(item.repo_slug)
        if cache is None:
            cache = _list_repo_milestones(item.owner, item.repo, gh_runner=gh_runner)
            repo_milestones_cache[item.repo_slug] = cache

        milestone_number = cache.get(milestone_title)
        if milestone_number is None:
            due_date = ""
            sprint = _derive_sprint(item, config)
            window = config.sprint_windows.get(sprint)
            if window is not None:
                due_date = window.target

            if dry_run:
                milestone_number = -1
            else:
                milestone_number = _create_repo_milestone(
                    item.owner,
                    item.repo,
                    milestone_title,
                    due_date,
                    gh_runner=gh_runner,
                )
            cache[milestone_title] = milestone_number
            stats.created_milestones += 1

        if not dry_run:
            _set_issue_milestone(
                item.owner,
                item.repo,
                item.number,
                milestone_number,
                gh_runner=gh_runner,
            )

        item.milestone_title = milestone_title
        stats.changed_issue_milestones += 1

    return stats


def sync_dates(
    items: list[IssueItem],
    schema: ProjectSchema,
    config: SyncConfig,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
) -> SyncStats:
    """Populate Start Date and Target Date from sprint windows."""
    stats = SyncStats(processed_issues=len(items))

    for item in items:
        sprint = _derive_sprint(item, config)
        window = config.sprint_windows.get(sprint)
        if window is None:
            continue

        if not item.start_date and window.start:
            if not dry_run:
                _set_project_field_value(
                    schema,
                    item.item_id,
                    "Start Date",
                    window.start,
                    gh_runner=gh_runner,
                )
            item.start_date = window.start
            stats.bump_field("Start Date")

        if not item.target_date and window.target:
            if not dry_run:
                _set_project_field_value(
                    schema,
                    item.item_id,
                    "Target Date",
                    window.target,
                    gh_runner=gh_runner,
                )
            item.target_date = window.target
            stats.bump_field("Target Date")

    return stats


def sync_assignees(
    items: list[IssueItem],
    config: SyncConfig,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
) -> SyncStats:
    """Populate missing issue assignees from executor/owner mapping."""
    stats = SyncStats(processed_issues=len(items))

    for item in items:
        executor = _derive_executor(item, config)
        assignee = _derive_assignee_login(item, executor, config)
        if not assignee:
            continue

        if not dry_run:
            _add_issue_assignee(
                item.owner,
                item.repo,
                item.number,
                assignee,
                gh_runner=gh_runner,
            )

        item.assignees = [assignee]
        stats.changed_issue_assignees += 1

    return stats


def sync_pr_ci(
    items: list[IssueItem],
    schema: ProjectSchema,
    config: SyncConfig,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
) -> SyncStats:
    """Synchronize PR and CI project fields from linked pull requests."""
    stats = SyncStats(processed_issues=len(items))

    for item in items:
        desired_pr, desired_ci = derive_pr_and_ci(item, config)

        if desired_pr and item.pr_field != desired_pr:
            if not dry_run:
                _set_project_field_value(
                    schema,
                    item.item_id,
                    "PR",
                    desired_pr,
                    gh_runner=gh_runner,
                )
            item.pr_field = desired_pr
            stats.bump_field("PR")

        if desired_ci and item.ci != desired_ci:
            if not dry_run:
                _set_project_field_value(
                    schema,
                    item.item_id,
                    "CI",
                    desired_ci,
                    gh_runner=gh_runner,
                )
            item.ci = desired_ci
            stats.bump_field("CI")

    return stats


def build_audit_report(items: list[IssueItem]) -> dict[str, Any]:
    """Build completeness report for all project issues."""
    status_counts: dict[str, int] = {}
    missing = {
        "priority": 0,
        "sprint": 0,
        "agent": 0,
        "executor": 0,
        "owner": 0,
        "handoff_to": 0,
        "pr": 0,
        "ci": 0,
        "source": 0,
        "blocked_reason": 0,
        "start_date": 0,
        "target_date": 0,
        "assignees": 0,
        "milestone": 0,
    }
    active_missing = {key: 0 for key in missing}

    for item in items:
        status = item.status or "UNSET"
        status_counts[status] = status_counts.get(status, 0) + 1
        is_active = status in ACTIVE_STATUSES

        def mark(name: str) -> None:
            missing[name] += 1
            if is_active:
                active_missing[name] += 1

        if not item.priority:
            mark("priority")
        if not item.sprint:
            mark("sprint")
        if not item.agent:
            mark("agent")
        if not item.executor:
            mark("executor")
        if not item.owner_field:
            mark("owner")
        if not item.handoff_to:
            mark("handoff_to")
        if not item.pr_field:
            mark("pr")
        if not item.ci:
            mark("ci")
        if not item.source:
            mark("source")
        if status == "Blocked" and not item.blocked_reason:
            mark("blocked_reason")
        if not item.start_date:
            mark("start_date")
        if not item.target_date:
            mark("target_date")
        if not item.assignees:
            mark("assignees")
        if not item.milestone_title:
            mark("milestone")

    return {
        "total_issues": len(items),
        "status_counts": status_counts,
        "missing": missing,
        "active_missing": active_missing,
    }


def print_sync_stats(
    command_name: str,
    stats: SyncStats,
    report: dict[str, Any] | None = None,
) -> None:
    """Print deterministic JSON summary for scripts/workflows."""
    summary: dict[str, Any] = {
        "command": command_name,
        "processed_issues": stats.processed_issues,
        "changed_fields": stats.changed_fields,
        "changed_issue_assignees": stats.changed_issue_assignees,
        "changed_issue_milestones": stats.changed_issue_milestones,
        "created_milestones": stats.created_milestones,
    }
    if report is not None:
        summary["audit"] = report

    print(json.dumps(summary, indent=2, sort_keys=True))


def _run_single_sync(
    command: str,
    project_owner: str,
    project_number: int,
    sync_config: SyncConfig,
    *,
    dry_run: bool,
) -> tuple[int, str]:
    """Execute one sync command and return (exit_code, output_json)."""
    schema = query_project_schema(project_owner, project_number)
    items = list_project_issue_items(project_owner, project_number)

    if command == "audit-completeness":
        report = build_audit_report(items)
        output = json.dumps(report, indent=2, sort_keys=True)
        return 0, output

    stats = SyncStats(processed_issues=len(items))

    if command == "sync-custom-fields":
        stats = sync_custom_fields(items, schema, sync_config, dry_run=dry_run)
    elif command == "sync-milestones":
        stats = sync_milestones(items, sync_config, dry_run=dry_run)
    elif command == "sync-dates":
        stats = sync_dates(items, schema, sync_config, dry_run=dry_run)
    elif command == "sync-assignees":
        stats = sync_assignees(items, sync_config, dry_run=dry_run)
    elif command == "sync-pr-ci":
        stats = sync_pr_ci(items, schema, sync_config, dry_run=dry_run)
    elif command == "sync-all":
        all_stats = SyncStats(processed_issues=len(items))
        all_stats.merge(
            sync_custom_fields(items, schema, sync_config, dry_run=dry_run)
        )
        all_stats.merge(sync_milestones(items, sync_config, dry_run=dry_run))
        all_stats.merge(sync_dates(items, schema, sync_config, dry_run=dry_run))
        all_stats.merge(sync_assignees(items, sync_config, dry_run=dry_run))
        all_stats.merge(sync_pr_ci(items, schema, sync_config, dry_run=dry_run))
        all_stats.processed_issues = len(items)
        stats = all_stats
    else:
        raise ConfigError(f"Unknown command '{command}'")

    report = build_audit_report(items)
    summary = {
        "command": command,
        "processed_issues": stats.processed_issues,
        "changed_fields": stats.changed_fields,
        "changed_issue_assignees": stats.changed_issue_assignees,
        "changed_issue_milestones": stats.changed_issue_milestones,
        "created_milestones": stats.created_milestones,
        "audit": report,
    }
    return 0, json.dumps(summary, indent=2, sort_keys=True)


def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser."""
    parser = argparse.ArgumentParser(
        description="Populate and keep GitHub Project fields complete."
    )
    parser.add_argument(
        "--project-owner",
        default=DEFAULT_PROJECT_OWNER,
        help="Project owner login (default: StartupAI-site)",
    )
    parser.add_argument(
        "--project-number",
        type=int,
        default=DEFAULT_PROJECT_NUMBER,
        help="Project number (default: 1)",
    )
    parser.add_argument(
        "--sync-config",
        default=DEFAULT_SYNC_CONFIG_PATH,
        help="Path to sync policy JSON",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    audit = subparsers.add_parser("audit-completeness", help="Report missing fields")
    audit.add_argument(
        "--strict",
        action="store_true",
        help="Exit 2 when active issues still have missing required fields",
    )

    for name, help_text in [
        ("sync-custom-fields", "Fill missing custom project fields"),
        ("sync-milestones", "Fill missing issue milestones"),
        ("sync-dates", "Fill missing start/target dates"),
        ("sync-assignees", "Fill missing issue assignees"),
        ("sync-pr-ci", "Sync PR and CI fields from linked PRs"),
        ("sync-all", "Run all sync passes in order"),
    ]:
        sub = subparsers.add_parser(name, help=help_text)
        sub.add_argument("--dry-run", action="store_true", help="No mutations")

    return parser


def main() -> int:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()

    sync_config = load_sync_config(Path(args.sync_config))

    if args.command == "audit-completeness":
        _, output = _run_single_sync(
            args.command,
            args.project_owner,
            args.project_number,
            sync_config,
            dry_run=True,
        )
        print(output)

        if args.strict:
            report = json.loads(output)
            active_missing = report.get("active_missing", {})
            has_active_gaps = any(count > 0 for count in active_missing.values())
            if has_active_gaps:
                return 2
        return 0

    dry_run = bool(getattr(args, "dry_run", False))
    _, output = _run_single_sync(
        args.command,
        args.project_owner,
        args.project_number,
        sync_config,
        dry_run=dry_run,
    )
    print(output)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ConfigError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(3)
    except GhQueryError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(4)
