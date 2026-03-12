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
import sys
from typing import Callable

import startupai_controller.project_field_sync_operations as _project_field_sync_operations
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
from startupai_controller.project_field_sync_mutations import (
    _add_issue_assignee,
    _create_repo_milestone,
    _list_repo_milestones,
    _set_issue_milestone,
    _set_project_field_value,
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


def sync_custom_fields(
    items: list[IssueItem],
    schema: ProjectSchema,
    config: SyncConfig,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
) -> SyncStats:
    """Fill missing custom project fields using deterministic defaults."""
    return _project_field_sync_operations.sync_custom_fields(
        items,
        schema,
        config,
        dry_run=dry_run,
        gh_runner=gh_runner,
        set_project_field_value_fn=_set_project_field_value,
    )


def sync_milestones(
    items: list[IssueItem],
    config: SyncConfig,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
) -> SyncStats:
    """Populate issue milestones from sprint mapping."""
    return _project_field_sync_operations.sync_milestones(
        items,
        config,
        dry_run=dry_run,
        gh_runner=gh_runner,
        list_repo_milestones_fn=_list_repo_milestones,
        create_repo_milestone_fn=_create_repo_milestone,
        set_issue_milestone_fn=_set_issue_milestone,
    )


def sync_dates(
    items: list[IssueItem],
    schema: ProjectSchema,
    config: SyncConfig,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
) -> SyncStats:
    """Populate Start Date and Target Date from sprint windows."""
    return _project_field_sync_operations.sync_dates(
        items,
        schema,
        config,
        dry_run=dry_run,
        gh_runner=gh_runner,
        set_project_field_value_fn=_set_project_field_value,
    )


def sync_assignees(
    items: list[IssueItem],
    config: SyncConfig,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
) -> SyncStats:
    """Populate missing issue assignees from executor/owner mapping."""
    return _project_field_sync_operations.sync_assignees(
        items,
        config,
        dry_run=dry_run,
        gh_runner=gh_runner,
        add_issue_assignee_fn=_add_issue_assignee,
    )


def sync_pr_ci(
    items: list[IssueItem],
    schema: ProjectSchema,
    config: SyncConfig,
    *,
    dry_run: bool = False,
    gh_runner: Callable[..., str] | None = None,
) -> SyncStats:
    """Synchronize PR and CI project fields from linked pull requests."""
    return _project_field_sync_operations.sync_pr_ci(
        items,
        schema,
        config,
        dry_run=dry_run,
        gh_runner=gh_runner,
        set_project_field_value_fn=_set_project_field_value,
    )


print_sync_stats = _project_field_sync_operations.print_sync_stats


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
