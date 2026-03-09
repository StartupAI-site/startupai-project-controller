"""Unit tests for repo-tracked control-plane helpers."""

from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace

import pytest

import startupai_controller.board_control_plane as control_plane
from startupai_controller.board_automation import BoardAutomationConfig, ExecutorRoutingDecision, AdmissionConfig
from startupai_controller.board_graph import AdmissionDecision
from startupai_controller.board_io import CycleBoardSnapshot, _ProjectItemSnapshot
from startupai_controller.validate_critical_path_promotion import CriticalPathConfig


def test_review_scope_refs_filters_to_executor_and_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = control_plane.ConsumerConfig(
        critical_paths_path=Path("critical-paths.json"),
        automation_config_path=Path("board-automation-config.json"),
        repo_prefixes=("crew",),
        executor="codex",
    )
    critical_path_config = CriticalPathConfig(
        issue_prefixes={
            "app": "StartupAI-site/app.startupai-site",
            "crew": "StartupAI-site/startupai-crew",
        },
        critical_paths={},
    )
    snapshots = [
        _ProjectItemSnapshot(
            issue_ref="StartupAI-site/startupai-crew#88",
            status="Review",
            executor="codex",
            handoff_to="none",
        ),
        _ProjectItemSnapshot(
            issue_ref="StartupAI-site/startupai-crew#89",
            status="Review",
            executor="human",
            handoff_to="none",
        ),
        _ProjectItemSnapshot(
            issue_ref="StartupAI-site/app.startupai-site#71",
            status="Review",
            executor="codex",
            handoff_to="none",
        ),
    ]
    monkeypatch.setattr(
        control_plane,
        "_list_project_items_by_status",
        lambda *_args, **_kwargs: snapshots,
    )

    review_refs = control_plane._review_scope_refs(config, critical_path_config)

    assert review_refs == ["crew#88"]


def test_tick_skips_review_rescue_when_no_review_items(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    called = {"drain_review_queue": False}

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

    monkeypatch.setattr(control_plane, "ConsumerDB", FakeDB)
    monkeypatch.setattr(control_plane, "load_config", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        control_plane,
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
    monkeypatch.setattr(control_plane, "_apply_automation_runtime", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        control_plane,
        "_current_main_workflows",
        lambda *_args, **_kwargs: ({}, {}, 180),
    )
    monkeypatch.setattr(
        control_plane,
        "build_cycle_board_snapshot",
        lambda *_args, **_kwargs: CycleBoardSnapshot(items=(), by_status={}),
    )
    monkeypatch.setattr(
        control_plane, "_replay_deferred_actions", lambda *_args, **_kwargs: ()
    )
    monkeypatch.setattr(
        control_plane,
        "route_protected_queue_executors",
        lambda *_args, **_kwargs: ExecutorRoutingDecision(
            routed=["crew#19"],
            unchanged=["crew#32"],
            skipped=[("app#109", "repo-not-governed")],
        ),
    )
    monkeypatch.setattr(
        control_plane,
        "admit_backlog_items",
        lambda *_args, **_kwargs: AdmissionDecision(
            ready_count=1,
            ready_floor=4,
            ready_cap=6,
            needed=2,
            scanned_backlog=3,
        ),
    )
    monkeypatch.setattr(
        control_plane,
        "admission_summary_payload",
        lambda *_args, **_kwargs: {
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
    )
    monkeypatch.setattr(control_plane, "_persist_admission_summary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        control_plane,
        "_drain_review_queue",
        lambda *_args, **_kwargs: (
            called.__setitem__("drain_review_queue", True) or True
        )
        and (
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
    monkeypatch.setattr(control_plane, "_record_successful_board_sync", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control_plane, "_clear_degraded", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control_plane, "_consumer_service_active", lambda: True)
    monkeypatch.setattr(
        control_plane,
        "_control_plane_health_summary",
        lambda *_args, **_kwargs: {"health": "healthy", "reason_code": "none"},
    )

    args = argparse.Namespace(
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
    assert called["drain_review_queue"] is True
    assert payload["review_rescue"]["queued_count"] == 0
    assert payload["review_rescue"]["due_count"] == 0
    assert payload["review_rescue"]["skipped"] == ["control-plane:no-review-items"]
    assert payload["verdict_backfill"] == []
    assert payload["admission"]["ready_floor"] == 4
    assert "timings_ms" in payload
    assert "github_request_counts" in payload
    assert payload["executor_routing"]["routed"] == ["crew#19"]
    assert payload["executor_routing"]["unchanged"] == ["crew#32"]
    assert payload["executor_routing"]["skipped"] == [
        {"issue_ref": "app#109", "reason": "repo-not-governed"}
    ]
