"""Fixture-backed contract tests for machine-consumed controller outputs."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import startupai_controller.board_control_plane as control_plane
import startupai_controller.control_plane_tick_runtime as tick_runtime
import startupai_controller.project_field_sync as project_field_sync
from startupai_controller.adapters.github_types import (
    CycleBoardSnapshot,
    CycleGitHubMemo,
)
from startupai_controller.board_automation import (
    AdmissionConfig,
    BoardAutomationConfig,
    ExecutorRoutingDecision,
)
from startupai_controller.board_graph import AdmissionDecision
from tests.test_board_consumer import (
    _cmd_drain,
    _cmd_status,
    _make_consumer_config,
    _make_db,
)
from tests.test_project_field_sync import _issue, _schema, _sync_config_payload

FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures" / "contracts"


def _load_fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURES_ROOT / name).read_text(encoding="utf-8"))


def _normalize_status_payload(payload: dict[str, object]) -> dict[str, object]:
    normalized = deepcopy(payload)
    normalized["drain_path"] = "<drain-path>"
    if normalized.get("drain_requested_at") is not None:
        normalized["drain_requested_at"] = "<iso8601>"
    normalized["workflow_state_path"] = "<workflow-state-path>"
    if normalized.get("oldest_deferred_action_age_seconds") is not None:
        normalized["oldest_deferred_action_age_seconds"] = "<positive-float>"

    workers = []
    for worker in normalized["workers"]:
        rendered = dict(worker)
        rendered["id"] = "<session-id:running>"
        if rendered.get("drain_observed_at") is not None:
            rendered["drain_observed_at"] = "<iso8601>"
        if rendered.get("shutdown_signal_sent_at") is not None:
            rendered["shutdown_signal_sent_at"] = "<iso8601>"
        if rendered.get("last_external_event_before_shutdown_signal_at") is not None:
            rendered["last_external_event_before_shutdown_signal_at"] = "<iso8601>"
        workers.append(rendered)
    normalized["workers"] = sorted(workers, key=lambda item: item["issue_ref"])

    drain_blockers = []
    for blocker in normalized.get("drain_blockers", []):
        rendered = dict(blocker)
        if rendered.get("session_id") is not None:
            rendered["session_id"] = "<session-id:running>"
        if rendered.get("drain_observed_at") is not None:
            rendered["drain_observed_at"] = "<iso8601>"
        if rendered.get("shutdown_signal_sent_at") is not None:
            rendered["shutdown_signal_sent_at"] = "<iso8601>"
        if rendered.get("last_external_event_before_shutdown_signal_at") is not None:
            rendered["last_external_event_before_shutdown_signal_at"] = "<iso8601>"
        drain_blockers.append(rendered)
    normalized["drain_blockers"] = sorted(
        drain_blockers, key=lambda item: item["issue_ref"]
    )

    sessions = []
    for session in normalized["recent_sessions"]:
        rendered = dict(session)
        rendered["id"] = (
            "<session-id:running>"
            if rendered["issue_ref"] == "crew#84"
            else "<session-id:failed>"
        )
        if rendered["completed_at"] is not None:
            rendered["completed_at"] = "<iso8601>"
        if rendered.get("drain_observed_at") is not None:
            rendered["drain_observed_at"] = "<iso8601>"
        if rendered.get("shutdown_signal_sent_at") is not None:
            rendered["shutdown_signal_sent_at"] = "<iso8601>"
        if rendered.get("last_external_event_before_shutdown_signal_at") is not None:
            rendered["last_external_event_before_shutdown_signal_at"] = "<iso8601>"
        if rendered["next_retry_at"] is not None:
            rendered["next_retry_at"] = "<iso8601>"
        if rendered["retry_remaining_seconds"] is not None:
            rendered["retry_remaining_seconds"] = "<non-negative-int>"
        sessions.append(rendered)
    normalized["recent_sessions"] = sorted(sessions, key=lambda item: item["issue_ref"])

    repo_workflows = {}
    for repo_prefix, status in normalized["repo_workflows"].items():
        rendered = dict(status)
        rendered["loaded_at"] = "<iso8601>"
        rendered["source_path"] = "<workflow-path>"
        rendered["workflow_hash"] = "<workflow-hash>"
        repo_workflows[repo_prefix] = rendered
    normalized["repo_workflows"] = repo_workflows

    return normalized


def _normalize_control_plane_payload(payload: dict[str, object]) -> dict[str, object]:
    normalized = deepcopy(payload)
    normalized["timings_ms"] = {
        key: "<non-negative-int>" for key in sorted(normalized["timings_ms"])
    }
    return normalized


def _field_sync_items() -> list[project_field_sync.IssueItem]:
    return [
        _issue(status="Ready"),
        _issue(
            item_id="ITEM_2",
            issue_ref="StartupAI-site/startupai-crew#89",
            number=89,
            status="Blocked",
            blocked_reason="",
            linked_pulls=[
                project_field_sync.LinkedPull(
                    url="https://github.com/pr/5",
                    state="OPEN",
                    merged_at=None,
                )
            ],
        ),
    ]


def _write_sync_config(tmp_path: Path) -> project_field_sync.SyncConfig:
    path = tmp_path / "project-field-sync-config.json"
    path.write_text(json.dumps(_sync_config_payload()), encoding="utf-8")
    return project_field_sync.load_sync_config(path)


def test_status_json_contract_fixture(tmp_path: Path) -> None:
    config = _make_consumer_config(tmp_path)
    db = _make_db(tmp_path)
    session_id = db.create_session(
        "crew#84",
        "codex",
        slot_id=1,
        session_kind="repair",
        repair_pr_url="https://github.com/O/R/pull/10",
    )
    db.acquire_lease("crew#84", session_id, slot_id=1, now=datetime.now(timezone.utc))
    db.update_session(session_id, status="running", slot_id=1, phase="running")
    failed_session_id = db.create_session("crew#85", "codex")
    db.update_session(
        failed_session_id,
        status="failed",
        phase="blocked",
        completed_at=datetime.now(timezone.utc).isoformat(),
        failure_reason="api_error",
        retry_count=2,
    )
    db.queue_deferred_action(
        "crew#84",
        "set_status",
        {
            "issue_ref": "crew#84",
            "to_status": "Ready",
            "from_statuses": ["In Progress"],
        },
    )
    db.set_control_value("degraded", "true")
    db.set_control_value("degraded_reason", "selection-error:test")

    with patch("sys.stdout", new=io.StringIO()):
        assert _cmd_drain(config) == 0

    buf = io.StringIO()
    with patch("sys.stdout", new=buf):
        assert _cmd_status(config, as_json=True, local_only=True) == 0

    payload = json.loads(buf.getvalue())
    assert _normalize_status_payload(payload) == _load_fixture("status_local_only.json")


def test_control_plane_tick_contract_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeDB:
        def __init__(self, db_path: Path) -> None:
            self.db_path = db_path

        def control_state_snapshot(self) -> dict[str, str]:
            return {}

        def deferred_action_count(self) -> int:
            return 0

        def oldest_deferred_action_age_seconds(self):
            return None

        def active_lease_issue_refs(self) -> list[str]:
            return []

        def set_control_value(self, key: str, value: str) -> None:
            return None

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        tick_runtime, "open_consumer_db", lambda db_path: FakeDB(db_path)
    )
    monkeypatch.setattr(tick_runtime, "load_config", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        tick_runtime,
        "load_automation_config",
        lambda *_args, **_kwargs: BoardAutomationConfig(
            wip_limits={"codex": {"crew": 2}},
            freshness_hours=24,
            stale_confirmation_cycles=2,
            trusted_codex_actors={"codex"},
            trusted_local_authors={"codex"},
            execution_authority_mode="single_machine",
            execution_authority_repos=("crew",),
            execution_authority_executors=("codex",),
            global_concurrency=2,
            admission=AdmissionConfig(enabled=True),
        ),
    )
    monkeypatch.setattr(
        tick_runtime, "_apply_automation_runtime", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        tick_runtime, "_current_main_workflows", lambda *_args, **_kwargs: ({}, {}, 180)
    )
    monkeypatch.setattr(
        tick_runtime,
        "build_github_port_bundle",
        lambda *_args, **_kwargs: SimpleNamespace(
            pull_requests=SimpleNamespace(),
            review_state=SimpleNamespace(list_issues_by_status=lambda _status: []),
            board_mutations=SimpleNamespace(),
            issue_context=SimpleNamespace(),
            github_memo=CycleGitHubMemo(),
        ),
    )
    monkeypatch.setattr(
        tick_runtime, "_replay_deferred_actions", lambda *_args, **_kwargs: ()
    )
    monkeypatch.setattr(
        tick_runtime,
        "build_ready_flow_port",
        lambda: SimpleNamespace(
            route_protected_queue_executors=lambda *_args, **_kwargs: ExecutorRoutingDecision(
                routed=["crew#19"],
                unchanged=["crew#32"],
                skipped=[("app#109", "repo-not-governed")],
            ),
            admit_backlog_items=lambda *_args, **_kwargs: AdmissionDecision(
                ready_count=1,
                ready_floor=4,
                ready_cap=6,
                needed=2,
                scanned_backlog=3,
            ),
            admission_summary_payload=lambda *_args, **_kwargs: {
                "enabled": True,
                "ready_count": 1,
                "ready_floor": 4,
                "ready_cap": 6,
                "needed": 2,
                "scanned_backlog": 3,
                "eligible_count": 0,
                "admitted": [],
                "skip_reason_counts": {},
                "top_candidates": [],
                "top_skipped": [],
                "partial_failure": False,
                "error": None,
                "controller_owned_admission_rejections": 0,
            },
        ),
    )
    monkeypatch.setattr(
        tick_runtime, "_persist_admission_summary", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        tick_runtime,
        "_drain_review_queue",
        lambda *_args, **_kwargs: (
            SimpleNamespace(
                queued_count=0,
                due_count=0,
                seeded=(),
                removed=(),
                verdict_backfilled=(),
                rerun=(),
                auto_merge_enabled=(),
                requeued=(),
                blocked=(),
                skipped=("control-plane:no-review-items",),
                partial_failure=False,
                error=None,
            ),
            CycleBoardSnapshot(items=(), by_status={}),
        ),
    )
    monkeypatch.setattr(
        tick_runtime, "_record_successful_board_sync", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(tick_runtime, "_clear_degraded", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        tick_runtime,
        "build_service_control_port",
        lambda: SimpleNamespace(is_active=lambda _service_name, user=True: True),
    )
    monkeypatch.setattr(
        tick_runtime,
        "_control_plane_health_summary",
        lambda *_args, **_kwargs: {"health": "healthy", "reason_code": "none"},
    )

    args = SimpleNamespace(
        file="critical-paths.json",
        automation_config="board-automation-config.json",
        project_owner="StartupAI-site",
        project_number=1,
        db_path=str(tmp_path / "consumer.db"),
        schema_path=str(tmp_path / "schema.json"),
        output_dir=str(tmp_path / "out"),
        drain_path=str(tmp_path / "consumer.drain"),
        workflow_state_path=str(tmp_path / "workflow-state.json"),
        dry_run=False,
        json=True,
    )

    code, payload = control_plane._tick(args)

    assert code == 0
    assert _normalize_control_plane_payload(payload) == _load_fixture(
        "control_plane_tick.json"
    )


@pytest.mark.parametrize(
    ("command", "fixture_name"),
    [
        ("audit-completeness", "project_field_sync_audit.json"),
        ("sync-all", "project_field_sync_sync_all_dry_run.json"),
    ],
)
def test_project_field_sync_contract_outputs(
    tmp_path: Path, command: str, fixture_name: str
) -> None:
    sync_config = _write_sync_config(tmp_path)

    with (
        patch.object(
            project_field_sync,
            "query_project_schema",
            lambda *_args, **_kwargs: _schema(),
        ),
        patch.object(
            project_field_sync,
            "list_project_issue_items",
            lambda *_args, **_kwargs: _field_sync_items(),
        ),
        patch.object(
            project_field_sync,
            "_list_repo_milestones",
            lambda *_args, **_kwargs: {"Backlog": 7},
        ),
        patch.object(
            project_field_sync, "_create_repo_milestone", lambda *_args, **_kwargs: 11
        ),
        patch.object(
            project_field_sync, "_set_issue_milestone", lambda *_args, **_kwargs: None
        ),
        patch.object(
            project_field_sync, "_add_issue_assignee", lambda *_args, **_kwargs: None
        ),
        patch.object(
            project_field_sync,
            "_set_project_field_value",
            lambda *_args, **_kwargs: None,
        ),
    ):
        code, output = project_field_sync._run_single_sync(
            command,
            "StartupAI-site",
            1,
            sync_config,
            dry_run=True,
        )

    assert code == 0
    assert json.loads(output) == _load_fixture(fixture_name)
