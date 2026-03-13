"""Focused regression tests for post-burn-in controller follow-ups."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

import startupai_controller.automation_compat_ports as automation_compat_ports
import startupai_controller.board_consumer_cli as board_consumer_cli
import startupai_controller.consumer_comment_pr_helpers as comment_pr_helpers
import startupai_controller.consumer_preflight_wiring as preflight_wiring
import startupai_controller.consumer_board_state_helpers as board_state_helpers
import startupai_controller.application.consumer.execution as execution_use_case
import startupai_controller.application.consumer.status as consumer_status
import startupai_controller.consumer_execution_support_helpers as execution_support
import startupai_controller.consumer_session_completion_helpers as completion_helpers
import startupai_controller.runtime.wiring as runtime_wiring
from startupai_controller.board_consumer_cli import _cmd_report_slo
from startupai_controller.consumer_types import (
    ClaimedSessionContext,
    PrCreationOutcome,
    SessionExecutionOutcome,
)
from startupai_controller.domain.models import CycleResult, ReviewQueueDrainSummary, SessionInfo
from tests.test_board_consumer import (
    _make_consumer_config,
    _make_db,
    _make_prepared_cycle_context,
    _make_prepared_launch_context,
)


def _completed_process(
    argv: list[str],
    *,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(argv, returncode, stdout, stderr)


def test_validate_branch_publication_rejects_unpublished_branch() -> None:
    def runner(args, **kwargs):
        if args[3:] == ["branch", "--show-current"]:
            return _completed_process(args, stdout="feat/84-test\n")
        if args[3:] == ["show-ref", "--verify", "refs/heads/feat/84-test"]:
            return _completed_process(args, stdout="abc refs/heads/feat/84-test\n")
        if args[3:] == [
            "ls-remote",
            "--exit-code",
            "--heads",
            "origin",
            "feat/84-test",
        ]:
            return _completed_process(args, returncode=2)
        raise AssertionError(args)

    with pytest.raises(
        execution_support.BranchPublicationError,
        match="not published on origin",
    ) as excinfo:
        execution_support.validate_branch_publication(
            "/tmp/worktree",
            "feat/84-test",
            subprocess_runner=runner,
        )

    assert excinfo.value.reason_code == "branch_not_published"


def test_one_shot_json_reports_idle_without_changing_exit_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FakeConfig:
        def __init__(
            self,
            *,
            db_path: Path,
            automation_config_path: Path,
            critical_paths_path: Path,
            **_kwargs: object,
        ) -> None:
            self.db_path = db_path
            self.automation_config_path = automation_config_path
            self.critical_paths_path = critical_paths_path

    class FakeDb:
        def close(self) -> None:
            return None

    class FakeLogger:
        def info(self, *_args, **_kwargs) -> None:
            return None

    fake_core = type(
        "FakeCore",
        (),
        {
            "DEFAULT_DB_PATH": tmp_path / "consumer.sqlite3",
            "DEFAULT_CONFIG_PATH": tmp_path / "critical-paths.json",
            "DEFAULT_AUTOMATION_CONFIG_PATH": tmp_path / "automation.json",
            "DEFAULT_STATUS_HOST": "127.0.0.1",
            "DEFAULT_STATUS_PORT": 8765,
            "DEFAULT_DRAIN_PATH": tmp_path / "consumer.drain",
            "ConsumerConfig": FakeConfig,
            "ConfigError": RuntimeError,
            "load_config": staticmethod(lambda _path: object()),
            "load_automation_config": staticmethod(lambda _path: None),
            "_apply_automation_runtime": staticmethod(lambda *_args, **_kwargs: None),
            "open_consumer_db": staticmethod(lambda _path: FakeDb()),
            "run_one_cycle": staticmethod(
                lambda *_args, **_kwargs: CycleResult(
                    action="idle",
                    reason="lease-cap",
                )
            ),
            "logger": FakeLogger(),
        },
    )()

    monkeypatch.setattr(board_consumer_cli, "_core", lambda: fake_core)

    exit_code = board_consumer_cli.main(
        ["one-shot", "--dry-run", "--json", "--db-path", str(tmp_path / "db.sqlite3")]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload["action"] == "idle"
    assert payload["reason"] == "lease-cap"
    assert payload["exit_class"] == "idle"


def test_validate_branch_publication_rejects_branch_mismatch() -> None:
    def runner(args, **kwargs):
        if args[3:] == ["branch", "--show-current"]:
            return _completed_process(args, stdout="feat/other-branch\n")
        raise AssertionError(args)

    with pytest.raises(
        execution_support.BranchPublicationError,
        match="worktree is on feat/other-branch",
    ) as excinfo:
        execution_support.validate_branch_publication(
            "/tmp/worktree",
            "feat/84-test",
            subprocess_runner=runner,
        )

    assert excinfo.value.reason_code == "branch_mismatch"


def test_create_pr_for_execution_result_fails_fast_for_unpublished_branch(
    tmp_path: Path,
) -> None:
    config = _make_consumer_config(tmp_path)
    launch_context = _make_prepared_launch_context(tmp_path)
    claimed_context = ClaimedSessionContext("session-123", 3, 1)
    create_pr_called = {"value": False}

    result = completion_helpers.create_pr_for_execution_result(
        config=config,
        launch_context=launch_context,
        claimed_context=claimed_context,
        codex_result=None,
        session_status="success",
        failure_reason=None,
        subprocess_runner=None,
        gh_runner=None,
        has_commits_on_branch=lambda *args, **kwargs: True,
        validate_branch_publication=lambda *args, **kwargs: (_ for _ in ()).throw(
            execution_support.BranchPublicationError(
                reason_code="branch_not_published",
                detail="head branch feat/84-test is not published on origin",
            )
        ),
        create_or_update_pr=lambda *args, **kwargs: create_pr_called.__setitem__(
            "value", True
        )
        or "https://github.com/example/pull/1",
        pr_creation_outcome_factory=PrCreationOutcome,
        logger=type("Logger", (), {"error": lambda self, *_args: None})(),
    )

    assert result.has_commits is True
    assert result.session_status == "failed"
    assert result.failure_reason == "branch_not_published"
    assert create_pr_called["value"] is False


def test_transition_issue_to_in_progress_allows_same_session_ready_race(
    tmp_path: Path,
) -> None:
    del tmp_path
    statuses = {"crew#84": "Ready"}
    transitions: list[tuple[str, str]] = []

    review_state_port = type(
        "ReviewStatePort",
        (),
        {"get_issue_status": lambda self, issue_ref: statuses[issue_ref]},
    )()

    class BoardPort:
        def set_issue_status(self, issue_ref: str, status: str) -> None:
            transitions.append((issue_ref, status))
            statuses[issue_ref] = status

    board_port = BoardPort()

    board_state_helpers.transition_issue_to_in_progress(
        "crew#84",
        config=object(),
        project_owner="StartupAI-site",
        project_number=1,
        build_github_port_bundle=lambda *_args, **_kwargs: None,
        from_statuses={"Review"},
        active_session_id="session-123",
        review_state_port=review_state_port,
        board_port=board_port,
    )

    assert transitions == [("crew#84", "In Progress")]
    assert statuses["crew#84"] == "In Progress"


def test_execute_claimed_session_aborts_cleanly_before_codex_when_draining(
    tmp_path: Path,
) -> None:
    config = _make_consumer_config(tmp_path)
    prepared = _make_prepared_cycle_context(tmp_path, config)
    launch_context = _make_prepared_launch_context(tmp_path)
    claimed_context = ClaimedSessionContext("session-123", 3, 1)
    db = _make_db(tmp_path)
    session_store_updates: list[dict[str, object]] = []

    class SessionStore:
        def update_session(self, _session_id: str, fields: dict[str, object]) -> None:
            session_store_updates.append(fields)

    handle_calls: list[str] = []

    outcome = execution_use_case.execute_claimed_session(
        config=config,
        db=db,
        prepared=prepared,
        deps=execution_use_case.ExecutionDeps(
            assemble_codex_prompt=lambda *_args, **_kwargs: "prompt",
            drain_requested=lambda _path: True,
            run_codex_session=lambda *_args, **_kwargs: pytest.fail(
                "run_codex_session should not run during a pre-execution drain abort"
            ),
            parse_codex_result=lambda *_args, **_kwargs: None,
            session_status_from_codex_result=lambda *_args, **_kwargs: (
                "failed",
                None,
            ),
            create_pr_for_execution_result=lambda **_kwargs: pytest.fail(
                "PR creation should not run during a pre-execution drain abort"
            ),
            handoff_execution_to_review=lambda **_kwargs: pytest.fail(
                "review handoff should not run during a pre-execution drain abort"
            ),
            handle_non_review_execution_outcome=lambda **kwargs: (
                handle_calls.append(kwargs["session_status"]) or ("aborted", None, None)
            ),
            build_session_execution_outcome=SessionExecutionOutcome,
        ),
        launch_context=launch_context,
        claimed_context=claimed_context,
        session_store=SessionStore(),
        gh_runner=None,
        process_runner=None,
        file_reader=None,
        review_state_port=None,
        board_port=object(),
        pr_port=object(),
    )

    assert handle_calls == ["aborted"]
    assert session_store_updates == []
    assert outcome.session_status == "aborted"
    assert outcome.failure_reason == "drain_requested_pre_execution"
    assert outcome.done_reason == "drain_requested_pre_execution"


def test_execute_claimed_session_marks_phase_executing_before_codex(
    tmp_path: Path,
) -> None:
    config = _make_consumer_config(tmp_path)
    prepared = _make_prepared_cycle_context(tmp_path, config)
    launch_context = _make_prepared_launch_context(tmp_path)
    claimed_context = ClaimedSessionContext("session-123", 3, 1)
    db = _make_db(tmp_path)
    session_store_updates: list[dict[str, object]] = []

    class SessionStore:
        def update_session(self, _session_id: str, fields: dict[str, object]) -> None:
            session_store_updates.append(fields)

    def run_codex_session(*_args, **_kwargs) -> int:
        assert session_store_updates == [{"phase": "executing"}]
        return 0

    outcome = execution_use_case.execute_claimed_session(
        config=config,
        db=db,
        prepared=prepared,
        deps=execution_use_case.ExecutionDeps(
            assemble_codex_prompt=lambda *_args, **_kwargs: "prompt",
            drain_requested=lambda _path: False,
            run_codex_session=run_codex_session,
            parse_codex_result=lambda *_args, **_kwargs: None,
            session_status_from_codex_result=lambda *_args, **_kwargs: (
                "failed",
                None,
            ),
            create_pr_for_execution_result=lambda **_kwargs: PrCreationOutcome(
                pr_url=None,
                has_commits=False,
                session_status="failed",
                failure_reason=None,
            ),
            handoff_execution_to_review=lambda **_kwargs: ReviewQueueDrainSummary(),
            handle_non_review_execution_outcome=lambda **_kwargs: (
                "aborted",
                None,
                None,
            ),
            build_session_execution_outcome=SessionExecutionOutcome,
        ),
        launch_context=launch_context,
        claimed_context=claimed_context,
        session_store=SessionStore(),
        gh_runner=None,
        process_runner=None,
        file_reader=None,
        review_state_port=None,
        board_port=object(),
        pr_port=object(),
    )

    assert session_store_updates == [{"phase": "executing"}]
    assert outcome.session_status == "aborted"


def test_worker_status_payload_classifies_drain_wait_for_inflight_execution() -> None:
    worker = SessionInfo(
        id="session-1",
        issue_ref="app#17",
        repo_prefix="app",
        worktree_path="/tmp/worktree",
        branch_name="feat/test",
        executor="codex",
        slot_id=1,
        status="running",
        phase="executing",
        started_at="2026-03-13T12:00:00+00:00",
        completed_at=None,
        outcome_json=None,
        failure_reason=None,
        retry_count=0,
        pr_url=None,
        provenance_id="session-1",
        session_kind="implementation",
        repair_pr_url=None,
        branch_reconcile_state=None,
        branch_reconcile_error=None,
        resolution_kind=None,
        verification_class=None,
        resolution_evidence_json=None,
        resolution_action=None,
        done_reason=None,
    )

    payload = consumer_status._worker_status_payload(
        worker,
        now=datetime(2026, 3, 13, 12, 5, tzinfo=timezone.utc),
        drain_requested=True,
    )

    assert payload["external_execution_started"] is True
    assert payload["drain_wait_class"] == "finishing_inflight_execution"
    assert payload["active_seconds"] == 300


def test_recent_session_payload_surfaces_distinct_drain_abort_reason() -> None:
    session = SessionInfo(
        id="session-2",
        issue_ref="app#18",
        repo_prefix="app",
        worktree_path="/tmp/worktree",
        branch_name="feat/test",
        executor="codex",
        slot_id=1,
        status="aborted",
        phase="running",
        started_at="2026-03-13T12:00:00+00:00",
        completed_at="2026-03-13T12:01:00+00:00",
        outcome_json=None,
        failure_reason="drain_requested_pre_execution",
        retry_count=0,
        pr_url=None,
        provenance_id="session-2",
        session_kind="implementation",
        repair_pr_url=None,
        branch_reconcile_state=None,
        branch_reconcile_error=None,
        resolution_kind=None,
        verification_class=None,
        resolution_evidence_json=None,
        resolution_action=None,
        done_reason="drain_requested_pre_execution",
    )

    payload = consumer_status._recent_session_status_payload(
        session,
        config=_make_consumer_config(Path("/tmp")),
        workflows={},
        now=datetime(2026, 3, 13, 12, 5, tzinfo=timezone.utc),
        drain_requested=True,
        session_retry_state=lambda *_args, **_kwargs: {
            "failure_reason": session.failure_reason,
            "retry_count": session.retry_count,
            "retryable": False,
            "retry_backoff_base_seconds": 0,
            "retry_backoff_max_seconds": 0,
            "retry_delay_seconds": None,
            "next_retry_at": None,
            "retry_remaining_seconds": None,
        },
    )

    assert payload["drain_abort_reason"] == "drain_requested_pre_execution"
    assert payload["drain_wait_class"] == "not_draining"


def test_build_gh_runner_port_is_idempotent_for_existing_port() -> None:
    def fake_runner(args, *, check=True):
        return "ok"

    gh_port = runtime_wiring.build_gh_runner_port(gh_runner=fake_runner)

    assert runtime_wiring.build_gh_runner_port(gh_runner=gh_port) is gh_port


def test_initialize_cycle_runtime_normalizes_existing_gh_runner_port(
    tmp_path: Path,
) -> None:
    config = _make_consumer_config(tmp_path)
    db = _make_db(tmp_path)
    recorded: list[tuple[list[str], bool]] = []

    def fake_runner(args, *, check=True):
        recorded.append((list(args), check))
        return "{}"

    gh_port = runtime_wiring.build_gh_runner_port(gh_runner=fake_runner)

    runtime = preflight_wiring.initialize_cycle_runtime(
        config,
        db,
        gh_runner=gh_port,
    )

    runtime.review_state_port._gh_runner(["api", "/rate_limit"])

    assert recorded == [(["api", "/rate_limit"], True)]


def test_wrap_review_state_port_accepts_typed_comment_exists_method() -> None:
    class ReviewStatePort:
        def comment_exists(self, repo: str, issue_number: int, marker: str) -> bool:
            return (repo, issue_number, marker) == ("StartupAI-site/repo", 17, "marker")

    wrapped = automation_compat_ports.wrap_review_state_port(
        ReviewStatePort(),
        config=object(),
        project_owner="StartupAI-site",
        project_number=1,
        comment_exists_fn=ReviewStatePort().comment_exists,
        gh_runner=lambda *_args, **_kwargs: "",
    )

    assert wrapped.comment_exists("StartupAI-site/repo", 17, "marker") is True


def test_wrap_review_state_port_passes_gh_runner_to_legacy_comment_exists() -> None:
    calls: list[tuple[str, str, int, str, object]] = []

    def legacy_comment_exists(
        owner: str,
        repo: str,
        number: int,
        marker: str,
        *,
        gh_runner,
    ) -> bool:
        calls.append((owner, repo, number, marker, gh_runner))
        return True

    gh_runner = lambda *_args, **_kwargs: ""
    wrapped = automation_compat_ports.wrap_review_state_port(
        object(),
        config=object(),
        project_owner="StartupAI-site",
        project_number=1,
        comment_exists_fn=legacy_comment_exists,
        gh_runner=gh_runner,
    )

    assert wrapped.comment_exists("StartupAI-site/repo", 17, "marker") is True
    assert calls == [("StartupAI-site", "repo", 17, "marker", gh_runner)]


def test_comment_checker_from_review_state_port_adapts_repo_slug_shape() -> None:
    calls: list[tuple[str, int, str]] = []

    class ReviewStatePort:
        def comment_exists(self, repo: str, issue_number: int, marker: str) -> bool:
            calls.append((repo, issue_number, marker))
            return True

    checker = comment_pr_helpers.comment_checker_from_review_state_port(
        ReviewStatePort()
    )

    assert checker("StartupAI-site", "repo", 17, "marker", gh_runner=None) is True
    assert calls == [("StartupAI-site/repo", 17, "marker")]


def test_report_slo_json_includes_transport_metrics(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = _make_consumer_config(tmp_path)
    db = _make_db(tmp_path)
    now = datetime.now(timezone.utc)
    db.record_metric_event(
        "github_transport_observation",
        payload={
            "graphql_requests": 3,
            "rest_requests": 2,
            "retry_attempts": 1,
            "cli_fallbacks": 1,
            "latency_le_250_ms": 4,
            "latency_le_1000_ms": 1,
            "latency_gt_1000_ms": 0,
            "error_counts": {"network": 1},
        },
        now=now,
    )
    db.close()

    exit_code = _cmd_report_slo(config, as_json=True, local_only=True)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["transport_metrics"]["windows"]["1h"]["total_requests"] == 5
    assert payload["transport_metrics"]["windows"]["1h"]["retry_attempts"] == 1
    assert payload["transport_metrics"]["windows"]["1h"]["cli_fallbacks"] == 1
    assert payload["transport_metrics"]["windows"]["1h"]["error_counts"] == {
        "network": 1
    }
