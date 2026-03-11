"""Characterization tests for hot-path behavior prior to application-layer extraction.

These tests lock the observed contract of three subsystems that are
scheduled for extraction by Codex:

  1. Deferred action replay loop (control_plane_rescue._replay_deferred_actions)
  2. Deferred action queue dedupe/supersede (consumer_db.queue_deferred_action)
  3. Interrupted-session recovery decision tree
  4. Multi-worker dispatch loop

All tests are behavior-based: they assert observable outcomes through the
public/shell-facing surface, not internal helper call graphs.
"""

from __future__ import annotations

import json
from concurrent.futures import Future
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from startupai_controller.board_consumer_compat import (
    ActiveWorkerTask,
    ConsumerConfig,
    CycleResult,
    _dispatch_multi_worker_launches,
    _log_completed_worker_results,
    _next_available_slots,
    _recover_interrupted_sessions,
    _replay_deferred_actions,
    run_daemon_loop,
)
from startupai_controller.board_automation import load_automation_config
from startupai_controller.consumer_db import ConsumerDB, DeferredAction
from startupai_controller.validate_critical_path_promotion import (
    CriticalPathConfig,
    GhQueryError,
    load_config,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _valid_payload() -> dict:
    return {
        "issue_prefixes": {
            "app": "StartupAI-site/app.startupai-site",
            "crew": "StartupAI-site/startupai-crew",
            "site": "StartupAI-site/startupai.site",
        },
        "critical_paths": {
            "planning-surface": {
                "goal": "Founder reviews and approves experiment plan",
                "first_value_at": "app#149",
                "edges": [
                    ["app#149", "crew#84"],
                    ["crew#84", "crew#85"],
                ],
            }
        },
    }


def _write_config(tmp_path: Path, payload: dict | None = None) -> Path:
    config_path = tmp_path / "critical-paths.json"
    data = payload if payload is not None else _valid_payload()
    config_path.write_text(json.dumps(data), encoding="utf-8")
    return config_path


def _write_automation_config(tmp_path: Path) -> Path:
    path = tmp_path / "board-automation-config.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "wip_limits": {
                    "codex": {"crew": 1, "app": 1, "site": 1},
                    "claude": {"crew": 3, "app": 3, "site": 3},
                    "human": {"crew": 3, "app": 3, "site": 3},
                },
                "freshness_hours": 24,
                "stale_confirmation_cycles": 2,
                "trusted_codex_actors": ["codex", "codex[bot]"],
                "dispatch": {"target": "executor"},
                "canary_thresholds": {},
            }
        ),
        encoding="utf-8",
    )
    return path


def _write_repo_workflow(repo_root: Path) -> Path:
    repo_root.mkdir(parents=True, exist_ok=True)
    path = repo_root / "WORKFLOW.md"
    path.write_text(
        "\n".join(
            [
                "---",
                "startupai_consumer:",
                "  poll_interval_seconds: 180",
                "  codex_timeout_seconds: 1800",
                "  max_retries: 3",
                "  retry_backoff_seconds: 300",
                "  validation_cmd: make test",
                "  workspace_hooks:",
                "    after_create: []",
                "    before_run: []",
                "---",
                "Follow repo instructions for {{ issue_ref }} in {{ worktree_path }}.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _make_consumer_config(tmp_path: Path) -> ConsumerConfig:
    cp_path = _write_config(tmp_path)
    auto_path = _write_automation_config(tmp_path)
    repo_roots = {
        "crew": tmp_path / "repos" / "crew",
        "app": tmp_path / "repos" / "app",
        "site": tmp_path / "repos" / "site",
    }
    for root in repo_roots.values():
        root.mkdir(parents=True, exist_ok=True)
    for root in repo_roots.values():
        _write_repo_workflow(root)
    return ConsumerConfig(
        critical_paths_path=cp_path,
        automation_config_path=auto_path,
        db_path=tmp_path / "test.db",
        output_dir=tmp_path / "outputs",
        drain_path=tmp_path / "consumer.drain",
        schema_path=Path(__file__).resolve().parent.parent
        / "config"
        / "codex_session_result.schema.json",
        workflow_state_path=tmp_path / "workflow-state.json",
        repo_roots=repo_roots,
    )


def _make_db(tmp_path: Path) -> ConsumerDB:
    return ConsumerDB(db_path=tmp_path / "test.db")


# ===========================================================================
# 1. Deferred Action Replay Loop
# ===========================================================================


class TestDeferredActionReplayLoop:
    """Characterize control_plane_rescue._replay_deferred_actions behavior."""

    def test_empty_queue_returns_empty_tuple(self, tmp_path: Path) -> None:
        """Empty deferred queue returns () with no side effects."""
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        cp_config = load_config(config.critical_paths_path)

        result = _replay_deferred_actions(db, config, cp_config)

        assert result == ()

    def test_fifo_ordering_across_scopes(self, tmp_path: Path) -> None:
        """Actions replay in created_at ASC, id ASC order across scopes."""
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        cp_config = load_config(config.critical_paths_path)

        t0 = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
        db.queue_deferred_action(
            "crew#84",
            "post_issue_comment",
            {"issue_ref": "crew#84", "body": "first"},
            now=t0,
        )
        db.queue_deferred_action(
            "app#149",
            "post_issue_comment",
            {"issue_ref": "app#149", "body": "second"},
            now=t0 + timedelta(seconds=1),
        )
        db.queue_deferred_action(
            "crew#84",
            "post_issue_comment",
            {"issue_ref": "crew#84", "body": "third"},
            now=t0 + timedelta(seconds=2),
        )

        replay_order: list[str] = []
        board_port = SimpleNamespace(
            post_issue_comment=lambda repo, number, body: replay_order.append(body),
            close_issue=lambda repo, number: None,
            set_issue_status=lambda issue_ref, status: None,
        )

        replayed = _replay_deferred_actions(
            db, config, cp_config, board_port=board_port
        )

        assert len(replayed) == 3
        assert replay_order == ["first", "second", "third"]

    def test_all_six_action_types_dispatch(self, tmp_path: Path) -> None:
        """Each of the 6 action types reaches the correct handler."""
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        cp_config = load_config(config.critical_paths_path)

        dispatched: dict[str, list] = {
            "set_status": [],
            "post_verdict_marker": [],
            "post_issue_comment": [],
            "close_issue": [],
            "rerun_check": [],
            "enable_automerge": [],
        }

        db.queue_deferred_action(
            "crew#84",
            "set_status",
            {"issue_ref": "crew#84", "to_status": "Ready", "from_statuses": ["In Progress"]},
        )
        db.queue_deferred_action(
            "crew#84",
            "post_verdict_marker",
            {"pr_url": "https://github.com/O/R/pull/1", "session_id": "s1"},
        )
        db.queue_deferred_action(
            "crew#84",
            "post_issue_comment",
            {"issue_ref": "crew#84", "body": "test"},
        )
        db.queue_deferred_action(
            "crew#84",
            "close_issue",
            {"issue_ref": "crew#84"},
        )
        db.queue_deferred_action(
            "crew#84",
            "rerun_check",
            {"pr_repo": "StartupAI-site/startupai-crew", "check_name": "ci", "run_id": 42},
        )
        db.queue_deferred_action(
            "crew#84",
            "enable_automerge",
            {"pr_repo": "StartupAI-site/startupai-crew", "pr_number": 77},
        )

        review_state_port = SimpleNamespace(
            get_issue_status=lambda issue_ref: "In Progress",
        )

        def track_status(issue_ref, status):
            dispatched["set_status"].append(issue_ref)

        board_port = SimpleNamespace(
            post_issue_comment=lambda repo, number, body: dispatched["post_issue_comment"].append(repo),
            close_issue=lambda repo, number: dispatched["close_issue"].append(repo),
            set_issue_status=track_status,
        )
        pr_port = SimpleNamespace(
            rerun_failed_check=lambda repo, check_name, run_id: (
                dispatched["rerun_check"].append(repo) or True
            ),
            enable_automerge=lambda repo, pr_number: dispatched["enable_automerge"].append(repo),
        )

        def fake_comment_checker(*args, **kwargs):
            return False

        def fake_comment_poster(*args, **kwargs):
            dispatched["post_verdict_marker"].append("posted")

        replayed = _replay_deferred_actions(
            db,
            config,
            cp_config,
            pr_port=pr_port,
            review_state_port=review_state_port,
            board_port=board_port,
            comment_checker=fake_comment_checker,
            comment_poster=fake_comment_poster,
        )

        assert len(replayed) == 6
        for action_type, calls in dispatched.items():
            assert len(calls) >= 1, f"{action_type} was not dispatched"

    def test_stop_on_first_failure_raises_gh_query_error(
        self, tmp_path: Path
    ) -> None:
        """First handler failure stops replay and raises GhQueryError."""
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        cp_config = load_config(config.critical_paths_path)

        db.queue_deferred_action(
            "crew#84",
            "post_issue_comment",
            {"issue_ref": "crew#84", "body": "will succeed"},
        )
        db.queue_deferred_action(
            "crew#84",
            "close_issue",
            {"issue_ref": "crew#84"},
        )
        db.queue_deferred_action(
            "crew#84",
            "post_issue_comment",
            {"issue_ref": "crew#84", "body": "never reached"},
        )

        call_count = 0

        def failing_close(repo, number):
            raise RuntimeError("simulated GitHub failure")

        def counting_comment(repo, number, body):
            nonlocal call_count
            call_count += 1

        board_port = SimpleNamespace(
            post_issue_comment=counting_comment,
            close_issue=failing_close,
            set_issue_status=lambda *a, **k: None,
        )

        with pytest.raises(GhQueryError, match="simulated GitHub failure"):
            _replay_deferred_actions(db, config, cp_config, board_port=board_port)

        # First action succeeded, second failed, third never reached
        assert call_count == 1
        # First action was deleted; second and third remain queued
        remaining = db.list_deferred_actions()
        assert len(remaining) == 2
        assert remaining[0].action_type == "close_issue"
        assert remaining[1].action_type == "post_issue_comment"

    def test_successful_replay_clears_all_actions(self, tmp_path: Path) -> None:
        """After successful replay all actions are deleted from DB."""
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        cp_config = load_config(config.critical_paths_path)

        db.queue_deferred_action(
            "crew#84",
            "post_issue_comment",
            {"issue_ref": "crew#84", "body": "a"},
        )
        db.queue_deferred_action(
            "app#149",
            "post_issue_comment",
            {"issue_ref": "app#149", "body": "b"},
        )

        board_port = SimpleNamespace(
            post_issue_comment=lambda repo, number, body: None,
            close_issue=lambda *a: None,
            set_issue_status=lambda *a: None,
        )

        replayed = _replay_deferred_actions(
            db, config, cp_config, board_port=board_port
        )

        assert len(replayed) == 2
        assert db.list_deferred_actions() == []
        assert db.deferred_action_count() == 0

    def test_unsupported_action_type_raises_gh_query_error(
        self, tmp_path: Path
    ) -> None:
        """Unknown action_type in the queue raises GhQueryError."""
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        cp_config = load_config(config.critical_paths_path)

        # Insert directly to bypass queue_deferred_action validation
        conn = db._get_connection()
        conn.execute(
            "INSERT INTO deferred_actions (scope_key, action_type, payload_json, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("crew#84", "unknown_type", "{}", datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()

        with pytest.raises(GhQueryError, match="Unsupported deferred action type"):
            _replay_deferred_actions(db, config, cp_config)


# ===========================================================================
# 2. Deferred Action Queue Dedupe/Supersede (consumer_db)
# ===========================================================================


class TestDeferredActionQueueContract:
    """Characterize consumer_db.queue_deferred_action dedupe/supersede behavior."""

    def test_exact_duplicate_returns_existing_id(self, tmp_path: Path) -> None:
        """Exact duplicate (same scope, type, payload) returns original row id."""
        db = _make_db(tmp_path)
        first_id = db.queue_deferred_action(
            "crew#84",
            "post_issue_comment",
            {"issue_ref": "crew#84", "body": "hello"},
        )
        second_id = db.queue_deferred_action(
            "crew#84",
            "post_issue_comment",
            {"issue_ref": "crew#84", "body": "hello"},
        )

        assert first_id == second_id
        assert db.deferred_action_count() == 1

    def test_different_payload_not_deduped(self, tmp_path: Path) -> None:
        """Same scope and type but different payload creates a new action."""
        db = _make_db(tmp_path)
        first_id = db.queue_deferred_action(
            "crew#84",
            "post_issue_comment",
            {"issue_ref": "crew#84", "body": "hello"},
        )
        second_id = db.queue_deferred_action(
            "crew#84",
            "post_issue_comment",
            {"issue_ref": "crew#84", "body": "different"},
        )

        assert first_id != second_id
        assert db.deferred_action_count() == 2

    def test_set_status_supersedes_older_set_status_same_scope(
        self, tmp_path: Path
    ) -> None:
        """Newer set_status deletes older set_status for the same scope."""
        db = _make_db(tmp_path)
        t0 = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)

        db.queue_deferred_action(
            "crew#84",
            "set_status",
            {"issue_ref": "crew#84", "to_status": "In Progress"},
            now=t0,
        )
        db.queue_deferred_action(
            "crew#84",
            "set_status",
            {"issue_ref": "crew#84", "to_status": "Ready"},
            now=t0 + timedelta(seconds=1),
        )

        actions = db.list_deferred_actions()
        assert len(actions) == 1
        assert actions[0].payload["to_status"] == "Ready"

    def test_set_status_does_not_supersede_different_scope(
        self, tmp_path: Path
    ) -> None:
        """set_status supersede only applies within the same scope_key."""
        db = _make_db(tmp_path)

        db.queue_deferred_action(
            "crew#84",
            "set_status",
            {"issue_ref": "crew#84", "to_status": "In Progress"},
        )
        db.queue_deferred_action(
            "app#149",
            "set_status",
            {"issue_ref": "app#149", "to_status": "Ready"},
        )

        assert db.deferred_action_count() == 2

    def test_set_status_does_not_supersede_non_status_actions(
        self, tmp_path: Path
    ) -> None:
        """set_status supersede only removes other set_status actions, not other types."""
        db = _make_db(tmp_path)

        db.queue_deferred_action(
            "crew#84",
            "post_issue_comment",
            {"issue_ref": "crew#84", "body": "hello"},
        )
        db.queue_deferred_action(
            "crew#84",
            "set_status",
            {"issue_ref": "crew#84", "to_status": "Ready"},
        )

        assert db.deferred_action_count() == 2

    def test_list_deferred_actions_fifo_order(self, tmp_path: Path) -> None:
        """list_deferred_actions returns created_at ASC, id ASC."""
        db = _make_db(tmp_path)
        t0 = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)

        db.queue_deferred_action(
            "crew#84", "close_issue", {"issue_ref": "crew#84"}, now=t0,
        )
        db.queue_deferred_action(
            "app#149", "close_issue", {"issue_ref": "app#149"},
            now=t0 + timedelta(seconds=1),
        )
        db.queue_deferred_action(
            "crew#85", "close_issue", {"issue_ref": "crew#85"},
            now=t0 + timedelta(seconds=2),
        )

        actions = db.list_deferred_actions()
        assert [a.payload["issue_ref"] for a in actions] == [
            "crew#84", "app#149", "crew#85",
        ]

    def test_delete_deferred_action_removes_single_row(
        self, tmp_path: Path
    ) -> None:
        """delete_deferred_action removes only the target row."""
        db = _make_db(tmp_path)

        id1 = db.queue_deferred_action(
            "crew#84", "close_issue", {"issue_ref": "crew#84"},
        )
        id2 = db.queue_deferred_action(
            "app#149", "close_issue", {"issue_ref": "app#149"},
        )

        db.delete_deferred_action(id1)

        remaining = db.list_deferred_actions()
        assert len(remaining) == 1
        assert remaining[0].id == id2


# ===========================================================================
# 3. Interrupted-Session Recovery
# ===========================================================================


class TestInterruptedSessionRecovery:
    """Characterize _recover_interrupted_sessions decision tree."""

    def test_no_leases_returns_empty(self, tmp_path: Path) -> None:
        """No active leases => returns empty list, no board mutations."""
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)

        recovered = _recover_interrupted_sessions(config, db)

        assert recovered == []

    def test_repair_session_returns_to_ready(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Repair sessions always requeue to Ready, never Review."""
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        session_id = db.create_session(
            "crew#84",
            "codex",
            session_kind="repair",
            repair_pr_url="https://github.com/O/R/pull/10",
        )
        db.acquire_lease("crew#84", session_id, now=datetime.now(timezone.utc))
        db.update_session(
            session_id,
            status="running",
            worktree_path="/tmp/wt",
            branch_name="feat/84-repair",
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        requeued: list[str] = []
        transitioned_to_review: list[str] = []

        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._return_issue_to_ready",
            lambda issue_ref, *args, **kwargs: requeued.append(issue_ref),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._transition_issue_to_review",
            lambda issue_ref, *args, **kwargs: transitioned_to_review.append(issue_ref),
        )

        recovered = _recover_interrupted_sessions(config, db)

        assert [l.issue_ref for l in recovered] == ["crew#84"]
        assert requeued == ["crew#84"]
        assert transitioned_to_review == []

    def test_new_work_with_pr_transitions_to_review(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """new_work session with a PR URL transitions to Review."""
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        session_id = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", session_id, now=datetime.now(timezone.utc))
        db.update_session(
            session_id,
            status="running",
            worktree_path="/tmp/wt",
            branch_name="feat/84-test",
            pr_url="https://github.com/O/R/pull/5",
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        transitioned: list[str] = []
        requeued: list[str] = []

        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._return_issue_to_ready",
            lambda issue_ref, *args, **kwargs: requeued.append(issue_ref),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._transition_issue_to_review",
            lambda issue_ref, *args, **kwargs: transitioned.append(issue_ref),
        )

        recovered = _recover_interrupted_sessions(config, db)

        assert [l.issue_ref for l in recovered] == ["crew#84"]
        assert transitioned == ["crew#84"]
        assert requeued == []

    def test_new_work_without_pr_returns_to_ready(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """new_work session without PR and classification='none' → Ready."""
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        session_id = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", session_id, now=datetime.now(timezone.utc))
        db.update_session(
            session_id,
            status="running",
            worktree_path="/tmp/wt",
            branch_name="feat/84-test",
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        requeued: list[str] = []
        transitioned: list[str] = []

        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._return_issue_to_ready",
            lambda issue_ref, *args, **kwargs: requeued.append(issue_ref),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._transition_issue_to_review",
            lambda issue_ref, *args, **kwargs: transitioned.append(issue_ref),
        )

        recovered = _recover_interrupted_sessions(config, db)

        assert [l.issue_ref for l in recovered] == ["crew#84"]
        assert requeued == ["crew#84"]
        assert transitioned == []

    def test_new_work_with_conflict_classification_blocks(
        self, tmp_path: Path
    ) -> None:
        """new_work with conflict classification → Blocked."""
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        session_id = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", session_id, now=datetime.now(timezone.utc))
        db.update_session(
            session_id,
            status="running",
            worktree_path="/tmp/wt",
            branch_name="feat/84-test",
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        auto_config = load_automation_config(config.automation_config_path)
        blocked: list[tuple[str, str]] = []
        requeued: list[str] = []
        transitioned: list[str] = []

        pr_port = SimpleNamespace(
            list_open_prs_for_issue=lambda *args, **kwargs: [
                SimpleNamespace(
                    url="https://github.com/O/R/pull/5",
                    number=5,
                    author="someone-else",
                    body="unrelated",
                    branch_name="other-branch",
                    provenance=None,
                )
            ],
        )
        review_state_port = SimpleNamespace(
            get_issue_status=lambda issue_ref: "In Progress",
        )

        def fake_set_blocked(issue_ref, reason, *args, **kwargs):
            blocked.append((issue_ref, reason))

        def fake_return_ready(issue_ref, *args, **kwargs):
            requeued.append(issue_ref)

        def fake_transition_review(issue_ref, *args, **kwargs):
            transitioned.append(issue_ref)

        board_port = SimpleNamespace(
            set_issue_status=lambda *a, **k: None,
        )

        from unittest.mock import patch

        with patch(
            "startupai_controller.board_consumer_compat._return_issue_to_ready",
            side_effect=fake_return_ready,
        ), patch(
            "startupai_controller.board_consumer_compat._transition_issue_to_review",
            side_effect=fake_transition_review,
        ), patch(
            "startupai_controller.board_consumer_compat._set_blocked_with_reason",
            side_effect=fake_set_blocked,
        ), patch(
            "startupai_controller.board_consumer_compat._classify_open_pr_candidates",
            return_value=("conflict", None, "branch mismatch"),
        ):
            recovered = _recover_interrupted_sessions(
                config,
                db,
                automation_config=auto_config,
                pr_port=pr_port,
                review_state_port=review_state_port,
                board_port=board_port,
            )

        assert [l.issue_ref for l in recovered] == ["crew#84"]
        assert blocked == [("crew#84", "execution-authority:conflict")]
        assert requeued == []
        assert transitioned == []

    def test_db_contract_leases_cleared_sessions_aborted(
        self, tmp_path: Path
    ) -> None:
        """recover_interrupted_leases clears leases and aborts active sessions."""
        db = _make_db(tmp_path)
        t0 = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
        sess_id = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", sess_id, now=t0)
        db.update_session(sess_id, status="running", started_at=t0.isoformat())

        recovered = db.recover_interrupted_leases(now=t0 + timedelta(minutes=1))

        assert len(recovered) == 1
        assert recovered[0].issue_ref == "crew#84"
        assert recovered[0].session_status == "running"
        assert recovered[0].session_kind == "new_work"
        assert db.active_lease_count() == 0

        session = db.get_session(sess_id)
        assert session is not None
        assert session.status == "aborted"
        assert session.failure_reason == "interrupted_restart"

    def test_db_contract_completed_session_preserves_status(
        self, tmp_path: Path
    ) -> None:
        """Already-completed sessions keep their terminal status through recovery."""
        db = _make_db(tmp_path)
        t0 = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
        sess_id = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", sess_id, now=t0)
        db.update_session(sess_id, status="success", completed_at=t0.isoformat())

        recovered = db.recover_interrupted_leases(now=t0 + timedelta(minutes=1))

        assert recovered[0].session_status == "success"
        session = db.get_session(sess_id)
        assert session is not None
        assert session.status == "success"

    def test_recovery_error_for_one_lease_does_not_abort_others(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If board recovery fails for one lease, others still process."""
        config = _make_consumer_config(tmp_path)
        db = _make_db(tmp_path)
        t0 = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)

        s1 = db.create_session("crew#84", "codex")
        db.acquire_lease("crew#84", s1, now=t0)
        db.update_session(s1, status="running", started_at=t0.isoformat())

        s2 = db.create_session("crew#85", "codex")
        db.acquire_lease("crew#85", s2, now=t0)
        db.update_session(s2, status="running", started_at=t0.isoformat())

        requeued: list[str] = []
        call_count = 0

        def return_ready_or_fail(issue_ref, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("simulated board failure")
            requeued.append(issue_ref)

        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._return_issue_to_ready",
            return_ready_or_fail,
        )

        recovered = _recover_interrupted_sessions(config, db)

        # Both leases recovered from DB, even though first board mutation failed
        assert len(recovered) == 2
        # Second lease was still processed
        assert requeued == ["crew#85"]


# ===========================================================================
# 4. Multi-Worker Dispatch
# ===========================================================================


class TestMultiWorkerDispatch:
    """Characterize multi-worker slot allocation and dispatch behavior."""

    def test_next_available_slots_returns_lowest_unoccupied(
        self, tmp_path: Path
    ) -> None:
        """Slots are deterministically lowest-available."""
        db = _make_db(tmp_path)
        # No occupied slots — should return [1, 2, 3]
        result = _next_available_slots(db, 3)
        assert result == [1, 2, 3]

    def test_next_available_slots_skips_occupied(self, tmp_path: Path) -> None:
        """Occupied slots (via active leases) are excluded."""
        db = _make_db(tmp_path)
        sess_id = db.create_session("crew#84", "codex", slot_id=2)
        db.acquire_lease("crew#84", sess_id, slot_id=2, now=datetime.now(timezone.utc))
        db.update_session(sess_id, status="running", slot_id=2)

        result = _next_available_slots(db, 3)
        assert result == [1, 3]

    def test_next_available_slots_respects_reserved_set(
        self, tmp_path: Path
    ) -> None:
        """Reserved slots (from in-flight futures) are also excluded."""
        db = _make_db(tmp_path)
        result = _next_available_slots(db, 3, reserved_slots={1, 3})
        assert result == [2]

    def test_dispatch_respects_hydration_budget(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Dispatch launches at most hydration_budget candidates per cycle."""
        config = _make_consumer_config(tmp_path)
        config.launch_hydration_concurrency = 1
        config.global_concurrency = 3
        db = _make_db(tmp_path)
        cp_config = load_config(config.critical_paths_path)

        candidates = iter(["crew#84", "crew#85", "crew#86", None])
        launched: list[tuple[str, int]] = []

        prepared = SimpleNamespace(
            cp_config=cp_config,
            auto_config=None,
            main_workflows={},
            workflow_statuses={"crew": SimpleNamespace(available=True)},
            global_limit=3,
            effective_interval=1,
            dispatchable_repo_prefixes=("crew",),
            board_snapshot=SimpleNamespace(items=(), items_with_status=lambda *a, **k: ()),
            github_memo=SimpleNamespace(),
            admission_summary={},
            timings_ms={},
            github_request_counts={},
        )

        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._select_candidate_for_cycle",
            lambda *args, **kwargs: next(candidates),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._prepare_launch_candidate",
            lambda issue_ref, **kwargs: SimpleNamespace(issue_ref=issue_ref),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._record_metric",
            lambda *args, **kwargs: None,
        )

        class _FakeExecutor:
            def submit(self, fn, *args, **kwargs):
                f: Future = Future()
                result = fn(*args, **kwargs)
                f.set_result(result)
                return f

        def fake_worker(config, *, target_issue, slot_id, prepared,
                        launch_context=None, dry_run=False, di_kwargs=None):
            launched.append((target_issue, slot_id))
            return CycleResult(action="claimed", issue_ref=target_issue)

        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._run_worker_cycle",
            fake_worker,
        )

        active_tasks: dict[Future[CycleResult], ActiveWorkerTask] = {}
        count = _dispatch_multi_worker_launches(
            _FakeExecutor(),
            config,
            db,
            prepared=prepared,
            available_slots=[1, 2, 3],
            active_issue_refs=set(),
            active_tasks=active_tasks,
            dry_run=False,
            di_kwargs={},
        )

        # Budget is 1 — only one launch
        assert count == 1
        assert len(launched) == 1

    def test_dispatch_stops_when_no_candidates(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Dispatch stops when candidate selection returns None."""
        config = _make_consumer_config(tmp_path)
        config.launch_hydration_concurrency = 5
        config.global_concurrency = 3
        db = _make_db(tmp_path)
        cp_config = load_config(config.critical_paths_path)

        candidates = iter(["crew#84", None])
        launched: list[str] = []

        prepared = SimpleNamespace(
            cp_config=cp_config,
            auto_config=None,
            main_workflows={},
            workflow_statuses={"crew": SimpleNamespace(available=True)},
            global_limit=3,
            effective_interval=1,
            dispatchable_repo_prefixes=("crew",),
            board_snapshot=SimpleNamespace(items=(), items_with_status=lambda *a, **k: ()),
            github_memo=SimpleNamespace(),
            admission_summary={},
            timings_ms={},
            github_request_counts={},
        )

        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._select_candidate_for_cycle",
            lambda *args, **kwargs: next(candidates),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._prepare_launch_candidate",
            lambda issue_ref, **kwargs: SimpleNamespace(issue_ref=issue_ref),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._record_metric",
            lambda *args, **kwargs: None,
        )

        class _FakeExecutor:
            def submit(self, fn, *args, **kwargs):
                f: Future = Future()
                result = fn(*args, **kwargs)
                f.set_result(result)
                return f

        def fake_worker(config, *, target_issue, slot_id, prepared,
                        launch_context=None, dry_run=False, di_kwargs=None):
            launched.append(target_issue)
            return CycleResult(action="claimed", issue_ref=target_issue)

        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._run_worker_cycle",
            fake_worker,
        )

        active_tasks: dict[Future[CycleResult], ActiveWorkerTask] = {}
        count = _dispatch_multi_worker_launches(
            _FakeExecutor(),
            config,
            db,
            prepared=prepared,
            available_slots=[1, 2, 3],
            active_issue_refs=set(),
            active_tasks=active_tasks,
            dry_run=False,
            di_kwargs={},
        )

        assert count == 1
        assert launched == ["crew#84"]

    def test_dispatch_gh_error_marks_degraded_and_stops(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """GhQueryError during candidate selection marks degraded and stops dispatch."""
        config = _make_consumer_config(tmp_path)
        config.launch_hydration_concurrency = 5
        config.global_concurrency = 3
        db = _make_db(tmp_path)
        cp_config = load_config(config.critical_paths_path)

        prepared = SimpleNamespace(
            cp_config=cp_config,
            auto_config=None,
            main_workflows={},
            workflow_statuses={"crew": SimpleNamespace(available=True)},
            global_limit=3,
            effective_interval=1,
            dispatchable_repo_prefixes=("crew",),
            board_snapshot=SimpleNamespace(items=(), items_with_status=lambda *a, **k: ()),
            github_memo=SimpleNamespace(),
            admission_summary={},
            timings_ms={},
            github_request_counts={},
        )

        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._select_candidate_for_cycle",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                GhQueryError("simulated API failure")
            ),
        )
        degraded_reasons: list[str] = []
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._mark_degraded",
            lambda db, reason: degraded_reasons.append(reason),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat.gh_reason_code",
            lambda err: "test-reason",
        )

        active_tasks: dict[Future[CycleResult], ActiveWorkerTask] = {}
        count = _dispatch_multi_worker_launches(
            SimpleNamespace(),  # executor — never used
            config,
            db,
            prepared=prepared,
            available_slots=[1, 2, 3],
            active_issue_refs=set(),
            active_tasks=active_tasks,
            dry_run=False,
            di_kwargs={},
        )

        assert count == 0
        assert len(degraded_reasons) == 1
        assert "test-reason" in degraded_reasons[0]

    def test_log_completed_worker_results_cleans_done_futures(
        self, tmp_path: Path
    ) -> None:
        """Completed futures are removed from active_tasks dict."""
        done_future: Future[CycleResult] = Future()
        done_future.set_result(
            CycleResult(action="claimed", issue_ref="crew#84", session_id="s1")
        )
        pending_future: Future[CycleResult] = Future()  # not done

        active_tasks = {
            done_future: ActiveWorkerTask(
                issue_ref="crew#84", slot_id=1,
                launched_at=datetime.now(timezone.utc).isoformat(),
            ),
            pending_future: ActiveWorkerTask(
                issue_ref="crew#85", slot_id=2,
                launched_at=datetime.now(timezone.utc).isoformat(),
            ),
        }

        _log_completed_worker_results(active_tasks)

        assert done_future not in active_tasks
        assert pending_future in active_tasks
        assert len(active_tasks) == 1

    def test_log_completed_worker_handles_exception_futures(
        self, tmp_path: Path
    ) -> None:
        """Failed futures are also cleaned from active_tasks."""
        failed_future: Future[CycleResult] = Future()
        failed_future.set_exception(RuntimeError("worker crashed"))

        active_tasks = {
            failed_future: ActiveWorkerTask(
                issue_ref="crew#84", slot_id=1,
                launched_at=datetime.now(timezone.utc).isoformat(),
            ),
        }

        _log_completed_worker_results(active_tasks)

        assert len(active_tasks) == 0

    def test_daemon_loop_adaptive_sleep(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Daemon loop sleeps 1s when active, poll_interval when idle."""
        config = _make_consumer_config(tmp_path)
        config.multi_worker_enabled = True
        config.global_concurrency = 2
        config.launch_hydration_concurrency = 2
        config.poll_interval_seconds = 60
        db = _make_db(tmp_path)

        sleep_durations: list[float] = []
        cycle_count = 0

        class _ImmediateFuture:
            def __init__(self, result):
                self._result = result

            def done(self) -> bool:
                return True

            def result(self):
                return self._result

        class _FakeExecutor:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def submit(self, fn, *args, **kwargs):
                return _ImmediateFuture(fn(*args, **kwargs))

        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat.ThreadPoolExecutor", _FakeExecutor
        )

        # First cycle: return one candidate (active → sleep 1s)
        # Second cycle: return no candidates (idle → sleep poll_interval)
        candidate_sequences = [iter(["crew#84", None]), iter([None])]

        def select_candidate(*args, **kwargs):
            nonlocal cycle_count
            return next(candidate_sequences[min(cycle_count, len(candidate_sequences) - 1)])

        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._select_candidate_for_cycle",
            select_candidate,
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._prepare_launch_candidate",
            lambda issue_ref, **kwargs: SimpleNamespace(issue_ref=issue_ref),
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._record_metric",
            lambda *args, **kwargs: None,
        )

        prepared = SimpleNamespace(
            cp_config=load_config(config.critical_paths_path),
            auto_config=None,
            main_workflows={},
            workflow_statuses={"crew": SimpleNamespace(available=True)},
            global_limit=2,
            effective_interval=1,
            dispatchable_repo_prefixes=("crew",),
            board_snapshot=SimpleNamespace(items=(), items_with_status=lambda *a, **k: ()),
            github_memo=SimpleNamespace(),
            admission_summary={},
            timings_ms={},
            github_request_counts={},
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._prepare_cycle",
            lambda *args, **kwargs: prepared,
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._recover_interrupted_sessions",
            lambda *args, **kwargs: [],
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._run_deferred_replay_phase",
            lambda *args, **kwargs: None,
        )
        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._sleep_for_claim_suppression_if_needed",
            lambda *args, **kwargs: False,
        )

        def fake_worker(config, *, target_issue, slot_id, prepared,
                        launch_context=None, dry_run=False, di_kwargs=None):
            return CycleResult(action="claimed", issue_ref=target_issue)

        monkeypatch.setattr(
            "startupai_controller.board_consumer_compat._run_worker_cycle",
            fake_worker,
        )

        def tracking_sleep(seconds):
            nonlocal cycle_count
            sleep_durations.append(seconds)
            cycle_count += 1
            if cycle_count >= 2:
                raise StopIteration("stop after 2 cycles")

        with pytest.raises(StopIteration):
            run_daemon_loop(config, db, sleep_fn=tracking_sleep)

        # First sleep: active (launched crew#84) → 1.0s
        assert sleep_durations[0] == 1.0
        # Second sleep: idle (no candidates) → poll_interval
        assert sleep_durations[1] == config.poll_interval_seconds
