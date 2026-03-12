"""Core models and pure policy helpers for project field synchronization."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any

from startupai_controller.validate_critical_path_promotion import ConfigError

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
        raise ConfigError(f"Unsupported sync config version '{version}'. Expected 1.")

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
