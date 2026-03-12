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
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Callable

from startupai_controller.project_field_sync_core import (
    ACTIVE_STATUSES,
    VALID_EXECUTORS,
    FieldSpec,
    IssueItem,
    LinkedPull,
    ProjectSchema,
    SprintWindow,
    SyncConfig,
    SyncStats,
    _derive_assignee_login,
    _derive_executor,
    _derive_milestone_title,
    _derive_owner,
    _derive_sprint,
    _status_default_ci,
    build_audit_report,
    derive_pr_and_ci,
    load_sync_config,
)
from startupai_controller.project_field_sync_queries import (
    _must_get_graphql_data,
    _run_gh,
    _run_gh_json,
    list_project_issue_items,
    query_project_schema,
)

from startupai_controller.validate_critical_path_promotion import (
    ConfigError,
    GhQueryError,
)

DEFAULT_PROJECT_OWNER = "StartupAI-site"
DEFAULT_PROJECT_NUMBER = 1
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_SYNC_CONFIG_PATH = str(_REPO_ROOT / "config" / "project-field-sync-config.json")


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
        all_stats.merge(sync_custom_fields(items, schema, sync_config, dry_run=dry_run))
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
