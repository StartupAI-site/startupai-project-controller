"""Sync-pass operations and reporting for project field synchronization."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Callable

from startupai_controller.project_field_sync_core import (
    IssueItem,
    ProjectSchema,
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
)
from startupai_controller.project_field_sync_mutations import (
    _add_issue_assignee,
    _create_repo_milestone,
    _list_repo_milestones,
    _set_issue_milestone,
    _set_project_field_value,
)
from startupai_controller.project_field_sync_queries import (
    list_project_issue_items,
    query_project_schema,
)
from startupai_controller.validate_critical_path_promotion import ConfigError


@dataclass(frozen=True)
class RunSingleSyncDeps:
    """Dependency bundle for one shell-triggered field-sync run."""

    query_project_schema: Callable[..., ProjectSchema]
    list_project_issue_items: Callable[..., list[IssueItem]]
    build_audit_report: Callable[[list[IssueItem]], dict[str, Any]]
    sync_custom_fields: Callable[..., SyncStats]
    sync_milestones: Callable[..., SyncStats]
    sync_dates: Callable[..., SyncStats]
    sync_assignees: Callable[..., SyncStats]
    sync_pr_ci: Callable[..., SyncStats]
    sync_stats_factory: type[SyncStats]


def sync_custom_fields(
    items: list[IssueItem],
    schema: ProjectSchema,
    config: SyncConfig,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
    set_project_field_value_fn: Callable[..., None] = _set_project_field_value,
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
                set_project_field_value_fn(
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
    list_repo_milestones_fn: Callable[..., dict[str, int]] = _list_repo_milestones,
    create_repo_milestone_fn: Callable[..., int] = _create_repo_milestone,
    set_issue_milestone_fn: Callable[..., None] = _set_issue_milestone,
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
            cache = list_repo_milestones_fn(item.owner, item.repo, gh_runner=gh_runner)
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
                milestone_number = create_repo_milestone_fn(
                    item.owner,
                    item.repo,
                    milestone_title,
                    due_date,
                    gh_runner=gh_runner,
                )
            cache[milestone_title] = milestone_number
            stats.created_milestones += 1

        if not dry_run:
            set_issue_milestone_fn(
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
    set_project_field_value_fn: Callable[..., None] = _set_project_field_value,
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
                set_project_field_value_fn(
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
                set_project_field_value_fn(
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
    add_issue_assignee_fn: Callable[..., None] = _add_issue_assignee,
) -> SyncStats:
    """Populate missing issue assignees from executor/owner mapping."""
    stats = SyncStats(processed_issues=len(items))

    for item in items:
        executor = _derive_executor(item, config)
        assignee = _derive_assignee_login(item, executor, config)
        if not assignee:
            continue

        if not dry_run:
            add_issue_assignee_fn(
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
    set_project_field_value_fn: Callable[..., None] = _set_project_field_value,
) -> SyncStats:
    """Synchronize PR and CI project fields from linked pull requests."""
    stats = SyncStats(processed_issues=len(items))

    for item in items:
        desired_pr, desired_ci = derive_pr_and_ci(item, config)

        if desired_pr and item.pr_field != desired_pr:
            if not dry_run:
                set_project_field_value_fn(
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
                set_project_field_value_fn(
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


def run_single_sync(
    command: str,
    project_owner: str,
    project_number: int,
    sync_config: SyncConfig,
    *,
    dry_run: bool,
    deps: RunSingleSyncDeps | None = None,
) -> tuple[int, str]:
    """Execute one sync command and return (exit_code, output_json)."""
    effective_deps = deps or RunSingleSyncDeps(
        query_project_schema=query_project_schema,
        list_project_issue_items=list_project_issue_items,
        build_audit_report=build_audit_report,
        sync_custom_fields=sync_custom_fields,
        sync_milestones=sync_milestones,
        sync_dates=sync_dates,
        sync_assignees=sync_assignees,
        sync_pr_ci=sync_pr_ci,
        sync_stats_factory=SyncStats,
    )
    schema = effective_deps.query_project_schema(project_owner, project_number)
    items = effective_deps.list_project_issue_items(project_owner, project_number)

    if command == "audit-completeness":
        report = effective_deps.build_audit_report(items)
        output = json.dumps(report, indent=2, sort_keys=True)
        return 0, output

    stats = effective_deps.sync_stats_factory(processed_issues=len(items))

    if command == "sync-custom-fields":
        stats = effective_deps.sync_custom_fields(
            items, schema, sync_config, dry_run=dry_run
        )
    elif command == "sync-milestones":
        stats = effective_deps.sync_milestones(items, sync_config, dry_run=dry_run)
    elif command == "sync-dates":
        stats = effective_deps.sync_dates(items, schema, sync_config, dry_run=dry_run)
    elif command == "sync-assignees":
        stats = effective_deps.sync_assignees(items, sync_config, dry_run=dry_run)
    elif command == "sync-pr-ci":
        stats = effective_deps.sync_pr_ci(items, schema, sync_config, dry_run=dry_run)
    elif command == "sync-all":
        all_stats = effective_deps.sync_stats_factory(processed_issues=len(items))
        all_stats.merge(
            effective_deps.sync_custom_fields(
                items, schema, sync_config, dry_run=dry_run
            )
        )
        all_stats.merge(
            effective_deps.sync_milestones(items, sync_config, dry_run=dry_run)
        )
        all_stats.merge(
            effective_deps.sync_dates(items, schema, sync_config, dry_run=dry_run)
        )
        all_stats.merge(
            effective_deps.sync_assignees(items, sync_config, dry_run=dry_run)
        )
        all_stats.merge(
            effective_deps.sync_pr_ci(items, schema, sync_config, dry_run=dry_run)
        )
        all_stats.processed_issues = len(items)
        stats = all_stats
    else:
        raise ConfigError(f"Unknown command '{command}'")

    report = effective_deps.build_audit_report(items)
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
