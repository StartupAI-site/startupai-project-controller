"""Unit tests for repo-tracked control-plane helpers."""

from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace

import pytest

import startupai_controller.board_control_plane as control_plane
import startupai_controller.control_plane_tick_runtime as tick_runtime
from startupai_controller.board_automation import (
    BoardAutomationConfig,
    ExecutorRoutingDecision,
    AdmissionConfig,
)
from startupai_controller.board_graph import AdmissionDecision
from startupai_controller.adapters.github_types import (
    CycleBoardSnapshot,
    CycleGitHubMemo,
)
from startupai_controller.domain.models import IssueSnapshot
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
        IssueSnapshot(
            issue_ref="crew#88",
            status="Review",
            executor="codex",
            priority="P1",
            title="Crew review",
            item_id="ITEM_88",
            project_id="PROJ_1",
        ),
        IssueSnapshot(
            issue_ref="crew#89",
            status="Review",
            executor="human",
            priority="P1",
            title="Crew human review",
            item_id="ITEM_89",
            project_id="PROJ_1",
        ),
        IssueSnapshot(
            issue_ref="app#71",
            status="Review",
            executor="codex",
            priority="P1",
            title="App review",
            item_id="ITEM_71",
            project_id="PROJ_1",
        ),
    ]
    review_state_port = SimpleNamespace(
        list_issues_by_status=lambda status: snapshots if status == "Review" else []
    )

    review_refs = control_plane._review_scope_refs(
        config,
        critical_path_config,
        review_state_port,
    )

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

    monkeypatch.setattr(
        tick_runtime,
        "open_consumer_db",
        lambda db_path: FakeDB(db_path),
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
        tick_runtime,
        "_current_main_workflows",
        lambda *_args, **_kwargs: ({}, {}, 180),
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


def test_build_tick_runtime_assembles_tick_deps_and_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    closed: list[bool] = []

    class FakeDB:
        def close(self) -> None:
            closed.append(True)

    monkeypatch.setattr(tick_runtime, "open_consumer_db", lambda _db_path: FakeDB())
    monkeypatch.setattr(tick_runtime, "begin_runtime_request_stats", lambda: "token")
    monkeypatch.setattr(
        tick_runtime,
        "end_runtime_request_stats",
        lambda _token: SimpleNamespace(graphql=3, rest=5),
    )
    monkeypatch.setattr(tick_runtime, "build_ready_flow_port", lambda: "ready-port")
    monkeypatch.setattr(
        tick_runtime,
        "build_service_control_port",
        lambda: SimpleNamespace(is_active=lambda _name, user=True: True),
    )

    args = argparse.Namespace()
    config = control_plane.ConsumerConfig(
        critical_paths_path=Path("critical-paths.json"),
        automation_config_path=Path("board-automation-config.json"),
        db_path=tmp_path / "consumer.db",
    )

    runtime = tick_runtime.build_tick_runtime(args=args, config=config)

    assert runtime.deps.ready_flow_port == "ready-port"
    payload = runtime.finalize_payload({})
    assert payload["github_request_counts"] == {"graphql": 3, "rest": 5}

    runtime.cleanup()
    assert closed == [True]
