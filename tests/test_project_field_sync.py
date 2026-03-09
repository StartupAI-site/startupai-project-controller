"""Contract tests for project_field_sync automation.

All tests are offline and deterministic.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import startupai_controller.project_field_sync as pfs


def _sync_config_payload() -> dict:
    return {
        "version": 1,
        "default_assignee": "chris00walker",
        "executor_assignee_map": {
            "claude": "chris00walker",
            "codex": "chris00walker",
            "human": "chris00walker",
        },
        "defaults": {
            "priority": "P3 Low",
            "sprint": "Backlog",
            "agent": "project-manager",
            "executor": "human",
            "owner": "human:project-manager",
            "handoff_to": "none",
            "ci": "n/a",
            "pr": "n/a",
            "source": "project-board-sync",
            "blocked_reason": "awaiting-triage",
        },
        "status_defaults": {
            "Backlog": {"ci": "n/a"},
            "Ready": {"ci": "pending"},
            "In Progress": {"ci": "pending"},
            "Review": {"ci": "pending"},
            "Blocked": {"ci": "failing"},
            "Done": {"ci": "passing"},
        },
        "sprint_to_milestone": {
            "M1 Foundation": "M1 Foundation",
            "M2 Core": "M2 Core",
            "M3 Integration": "M3 Integration",
            "M4 Launch": "M4 Launch",
            "Backlog": "Backlog",
        },
        "sprint_windows": {
            "M2 Core": {"start": "2026-02-16", "target": "2026-04-15"},
            "Backlog": {"start": "2026-01-01", "target": "2026-12-31"},
        },
    }


def _write_sync_config(tmp_path: Path, payload: dict | None = None) -> Path:
    path = tmp_path / "project-field-sync-config.json"
    data = payload if payload is not None else _sync_config_payload()
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def _load(tmp_path: Path) -> pfs.SyncConfig:
    return pfs.load_sync_config(_write_sync_config(tmp_path))


def _issue(**overrides) -> pfs.IssueItem:
    base = {
        "item_id": "ITEM_1",
        "issue_ref": "StartupAI-site/startupai-crew#88",
        "repo_slug": "StartupAI-site/startupai-crew",
        "owner": "StartupAI-site",
        "repo": "startupai-crew",
        "number": 88,
        "status": "Ready",
        "priority": "",
        "sprint": "",
        "agent": "",
        "executor": "",
        "owner_field": "",
        "handoff_to": "",
        "pr_field": "",
        "ci": "",
        "source": "",
        "blocked_reason": "",
        "start_date": "",
        "target_date": "",
        "assignees": [],
        "milestone_title": "",
        "linked_pulls": [],
    }
    base.update(overrides)
    return pfs.IssueItem(**base)


def _schema() -> pfs.ProjectSchema:
    options = lambda *vals: {v: f"ID_{v}" for v in vals}
    return pfs.ProjectSchema(
        project_id="PROJ_1",
        fields={
            "Priority": pfs.FieldSpec("F1", "Priority", "SINGLE_SELECT", options("P3 Low")),
            "Sprint": pfs.FieldSpec("F2", "Sprint", "SINGLE_SELECT", options("Backlog", "M2 Core")),
            "Agent": pfs.FieldSpec("F3", "Agent", "SINGLE_SELECT", options("project-manager")),
            "Executor": pfs.FieldSpec("F4", "Executor", "SINGLE_SELECT", options("claude", "codex", "human")),
            "Owner": pfs.FieldSpec("F5", "Owner", "TEXT", {}),
            "Handoff To": pfs.FieldSpec("F6", "Handoff To", "SINGLE_SELECT", options("none", "claude", "codex", "human")),
            "PR": pfs.FieldSpec("F7", "PR", "TEXT", {}),
            "CI": pfs.FieldSpec("F8", "CI", "SINGLE_SELECT", options("pending", "passing", "failing", "n/a")),
            "Source": pfs.FieldSpec("F9", "Source", "TEXT", {}),
            "Blocked Reason": pfs.FieldSpec("F10", "Blocked Reason", "TEXT", {}),
            "Start Date": pfs.FieldSpec("F11", "Start Date", "DATE", {}),
            "Target Date": pfs.FieldSpec("F12", "Target Date", "DATE", {}),
        },
    )


def test_load_sync_config(tmp_path: Path) -> None:
    cfg = _load(tmp_path)
    assert cfg.default_assignee == "chris00walker"
    assert cfg.defaults["executor"] == "human"
    assert cfg.sprint_to_milestone["M2 Core"] == "M2 Core"


def test_load_sync_config_requires_defaults(tmp_path: Path) -> None:
    payload = _sync_config_payload()
    payload["defaults"].pop("executor")
    with pytest.raises(pfs.ConfigError):
        pfs.load_sync_config(_write_sync_config(tmp_path, payload))


def test_derive_executor_from_owner_prefix(tmp_path: Path) -> None:
    cfg = _load(tmp_path)
    item = _issue(owner_field="codex:qa-engineer")
    assert pfs._derive_executor(item, cfg) == "codex"


def test_derive_owner_fallback(tmp_path: Path) -> None:
    cfg = _load(tmp_path)
    item = _issue(agent="qa-engineer")
    assert pfs._derive_owner(item, "claude", cfg) == "claude:qa-engineer"


def test_derive_assignee_from_executor_mapping(tmp_path: Path) -> None:
    cfg = _load(tmp_path)
    item = _issue(executor="claude")
    assert pfs._derive_assignee_login(item, "claude", cfg) == "chris00walker"


def test_derive_assignee_none_if_present(tmp_path: Path) -> None:
    cfg = _load(tmp_path)
    item = _issue(assignees=["chris00walker"], executor="codex")
    assert pfs._derive_assignee_login(item, "codex", cfg) is None


def test_derive_pr_and_ci_open_pr(tmp_path: Path) -> None:
    cfg = _load(tmp_path)
    item = _issue(
        linked_pulls=[pfs.LinkedPull(url="https://github.com/pr/1", state="OPEN", merged_at=None)]
    )
    pr, ci = pfs.derive_pr_and_ci(item, cfg)
    assert pr.endswith("/1")
    assert ci == "pending"


def test_derive_pr_and_ci_merged_pr(tmp_path: Path) -> None:
    cfg = _load(tmp_path)
    item = _issue(
        linked_pulls=[
            pfs.LinkedPull(
                url="https://github.com/pr/1",
                state="MERGED",
                merged_at="2026-03-04T10:00:00Z",
            ),
            pfs.LinkedPull(
                url="https://github.com/pr/2",
                state="MERGED",
                merged_at="2026-03-04T11:00:00Z",
            ),
        ]
    )
    pr, ci = pfs.derive_pr_and_ci(item, cfg)
    assert pr.endswith("/2")
    assert ci == "passing"


def test_derive_pr_and_ci_no_pr_uses_status_default(tmp_path: Path) -> None:
    cfg = _load(tmp_path)
    item = _issue(status="Blocked", linked_pulls=[])
    pr, ci = pfs.derive_pr_and_ci(item, cfg)
    assert pr == "n/a"
    assert ci == "failing"


def test_build_audit_report_counts_missing_fields() -> None:
    report = pfs.build_audit_report([
        _issue(status="Ready"),
        _issue(status="Done", assignees=["chris00walker"], milestone_title="M2 Core"),
    ])
    assert report["total_issues"] == 2
    assert report["missing"]["assignees"] == 1
    assert report["active_missing"]["assignees"] == 1


def test_sync_custom_fields_dry_run_populates_missing(tmp_path: Path) -> None:
    cfg = _load(tmp_path)
    item = _issue(status="Blocked")
    stats = pfs.sync_custom_fields([item], _schema(), cfg, dry_run=True)

    assert stats.changed_fields["Executor"] == 1
    assert stats.changed_fields["Owner"] == 1
    assert stats.changed_fields["Blocked Reason"] == 1
    assert item.executor == "human"
    assert item.owner_field == "human:project-manager"
    assert item.blocked_reason == "awaiting-triage"


def test_sync_dates_dry_run_sets_start_and_target(tmp_path: Path) -> None:
    cfg = _load(tmp_path)
    item = _issue(sprint="M2 Core")
    stats = pfs.sync_dates([item], _schema(), cfg, dry_run=True)
    assert stats.changed_fields["Start Date"] == 1
    assert stats.changed_fields["Target Date"] == 1
    assert item.start_date == "2026-02-16"
    assert item.target_date == "2026-04-15"


def test_sync_assignees_dry_run_sets_default(tmp_path: Path) -> None:
    cfg = _load(tmp_path)
    item = _issue(executor="codex")
    stats = pfs.sync_assignees([item], cfg, dry_run=True)
    assert stats.changed_issue_assignees == 1
    assert item.assignees == ["chris00walker"]


def test_sync_pr_ci_dry_run_updates_fields(tmp_path: Path) -> None:
    cfg = _load(tmp_path)
    item = _issue(
        pr_field="n/a",
        ci="n/a",
        linked_pulls=[
            pfs.LinkedPull(
                url="https://github.com/StartupAI-site/startupai-crew/pull/123",
                state="MERGED",
                merged_at="2026-03-04T12:00:00Z",
            )
        ],
    )

    stats = pfs.sync_pr_ci([item], _schema(), cfg, dry_run=True)
    assert stats.changed_fields["PR"] == 1
    assert stats.changed_fields["CI"] == 1
    assert item.ci == "passing"
    assert item.pr_field.endswith("/123")


def test_set_project_field_value_rejects_unknown_option() -> None:
    schema = _schema()
    with pytest.raises(pfs.ConfigError):
        pfs._set_project_field_value(
            schema,
            "ITEM_1",
            "Priority",
            "P1 High",
            gh_runner=lambda _args: "{}",
        )


def test_list_repo_milestones_combines_open_and_closed() -> None:
    calls: list[list[str]] = []

    def fake_gh(args: list[str]) -> str:
        calls.append(args)
        joined = " ".join(args)
        if "state=open" in joined:
            return json.dumps([{"title": "M2 Core", "number": 2}])
        if "state=closed" in joined:
            return json.dumps([{"title": "M1 Foundation", "number": 1}])
        return "[]"

    milestones = pfs._list_repo_milestones(
        "StartupAI-site",
        "startupai-crew",
        gh_runner=fake_gh,
    )
    assert milestones == {"M2 Core": 2, "M1 Foundation": 1}
    assert any("-X" in call and "GET" in call for call in calls)
