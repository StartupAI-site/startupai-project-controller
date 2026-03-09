from __future__ import annotations

from pathlib import Path

import pytest

from startupai_controller.consumer_workflow import (
    WorkflowConfigError,
    effective_poll_interval,
    load_repo_workflows,
    load_workflow_definition,
    read_workflow_snapshot,
    render_workflow_prompt,
    snapshot_from_statuses,
    workflow_status_payload,
    write_workflow_snapshot,
)


def _write_workflow(
    path: Path,
    *,
    config_block: str = "",
    body: str = "Run {{ issue_ref }} in {{ worktree_path }}",
) -> Path:
    path.write_text(
        f"---\nstartupai_consumer:\n{config_block}---\n{body}\n",
        encoding="utf-8",
    )
    return path


def test_load_workflow_definition_parses_runtime_and_prompt(tmp_path: Path) -> None:
    workflow_path = _write_workflow(
        tmp_path / "WORKFLOW.md",
        config_block=(
            "  poll_interval_seconds: 120\n"
            "  codex_timeout_seconds: 900\n"
            "  max_retries: 4\n"
            "  retry_backoff_base_seconds: 45\n"
            "  retry_backoff_seconds: 600\n"
            "  validation_cmd: make test\n"
            "  workspace_hooks:\n"
            "    after_create:\n"
            "      - echo after\n"
        ),
        body="Repo prompt for {{ issue_ref }}",
    )

    workflow = load_workflow_definition(
        workflow_path,
        repo_prefix="crew",
        source_kind="main",
    )

    assert workflow.runtime.poll_interval_seconds == 120
    assert workflow.runtime.codex_timeout_seconds == 900
    assert workflow.runtime.max_retries == 4
    assert workflow.runtime.retry_backoff_base_seconds == 45
    assert workflow.runtime.retry_backoff_seconds == 600
    assert workflow.runtime.validation_cmd == "make test"
    assert workflow.runtime.workspace_hooks["after_create"] == ("echo after",)
    assert workflow.prompt_template == "Repo prompt for {{ issue_ref }}"


def test_load_workflow_definition_rejects_unknown_keys(tmp_path: Path) -> None:
    workflow_path = _write_workflow(
        tmp_path / "WORKFLOW.md",
        config_block="  surprise_key: true\n",
    )

    with pytest.raises(WorkflowConfigError, match="unknown-keys"):
        load_workflow_definition(
            workflow_path,
            repo_prefix="crew",
            source_kind="main",
        )


def test_load_workflow_definition_requires_file(tmp_path: Path) -> None:
    with pytest.raises(WorkflowConfigError, match="missing:"):
        load_workflow_definition(
            tmp_path / "WORKFLOW.md",
            repo_prefix="crew",
            source_kind="main",
        )


def test_render_workflow_prompt_replaces_known_placeholders(tmp_path: Path) -> None:
    workflow = load_workflow_definition(
        _write_workflow(tmp_path / "WORKFLOW.md"),
        repo_prefix="app",
        source_kind="main",
    )

    rendered = render_workflow_prompt(
        workflow,
        {
            "issue_ref": "app#71",
            "worktree_path": "/tmp/app",
        },
    )

    assert rendered == "Run app#71 in /tmp/app"


def test_load_repo_workflows_disables_only_missing_repo(tmp_path: Path) -> None:
    crew_root = tmp_path / "crew"
    app_root = tmp_path / "app"
    crew_root.mkdir()
    app_root.mkdir()
    _write_workflow(crew_root / "WORKFLOW.md")

    workflows, statuses = load_repo_workflows(
        ("crew", "app"),
        {"crew": crew_root, "app": app_root},
    )

    assert set(workflows) == {"crew"}
    assert statuses["crew"].available is True
    assert statuses["app"].available is False
    assert statuses["app"].disabled_reason is not None


def test_effective_poll_interval_uses_lowest_repo_override(tmp_path: Path) -> None:
    crew_root = tmp_path / "crew"
    app_root = tmp_path / "app"
    crew_root.mkdir()
    app_root.mkdir()
    _write_workflow(
        crew_root / "WORKFLOW.md",
        config_block="  poll_interval_seconds: 180\n",
    )
    _write_workflow(
        app_root / "WORKFLOW.md",
        config_block="  poll_interval_seconds: 60\n",
    )
    workflows, _statuses = load_repo_workflows(
        ("crew", "app"),
        {"crew": crew_root, "app": app_root},
    )

    assert effective_poll_interval(workflows, default_seconds=300) == 60


def test_workflow_snapshot_round_trip(tmp_path: Path) -> None:
    crew_root = tmp_path / "crew"
    crew_root.mkdir()
    _write_workflow(crew_root / "WORKFLOW.md")
    _workflows, statuses = load_repo_workflows(("crew",), {"crew": crew_root})
    snapshot = snapshot_from_statuses(
        statuses,
        effective_poll_interval_seconds=180,
    )
    snapshot_path = tmp_path / "workflow-state.json"

    write_workflow_snapshot(snapshot_path, snapshot)
    loaded = read_workflow_snapshot(snapshot_path)

    assert loaded is not None
    assert loaded.effective_poll_interval_seconds == 180
    assert loaded.repos["crew"].available is True
    assert workflow_status_payload(loaded.repos["crew"])["source_path"].endswith(
        "WORKFLOW.md"
    )
