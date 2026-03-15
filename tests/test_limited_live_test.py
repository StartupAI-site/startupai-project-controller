from __future__ import annotations

import json
import signal
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tools.limited_live_test import (
    CommandResult,
    HarnessError,
    LimitedLiveTestConfig,
    LimitedLiveTestHarness,
)

UTC = timezone.utc


class FakeClock:
    def __init__(self) -> None:
        self._monotonic = 0.0
        self._now = datetime(2026, 3, 13, 12, 0, tzinfo=UTC)

    def now_utc(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic

    def sleep(self, seconds: float) -> None:
        self._monotonic += seconds
        self._now += timedelta(seconds=seconds)

    def shift_wall_clock(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)


@dataclass
class FakeProcess:
    clock: FakeClock
    drain_exit_delay: float | None
    sigint_stops: bool = False
    sigterm_stops: bool = True

    def __post_init__(self) -> None:
        self.pid = 4242
        self.exit_at: float | None = None
        self.returncode: int | None = None
        self.signals: list[int] = []

    def poll(self) -> int | None:
        if (
            self.returncode is None
            and self.exit_at is not None
            and self.clock.monotonic() >= self.exit_at
        ):
            self.returncode = 0
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        if self.returncode is None:
            if self.exit_at is not None:
                self.clock.sleep(max(0.0, self.exit_at - self.clock.monotonic()))
                self.returncode = 0
            else:
                self.returncode = 0
        return self.returncode

    def send_signal(self, sig: int) -> None:
        self.signals.append(sig)
        if sig == signal.SIGINT and self.sigint_stops:
            self.returncode = 130
        if sig == signal.SIGTERM and self.sigterm_stops:
            self.returncode = 143

    def terminate(self) -> None:
        self.returncode = 143

    def close(self) -> None:
        return None


class FakeBackend:
    def __init__(
        self,
        *,
        clock: FakeClock,
        process: FakeProcess,
        status_local: list[dict],
        status_full: list[dict],
        report_slo: list[dict],
        tick_payloads: list[dict],
        log_text: str = "",
        systemd_active: bool = False,
        local_processes: list[str] | None = None,
        failures: dict[tuple[str, bool | None], int] | None = None,
        timeouts: dict[tuple[str, bool | None], bool] | None = None,
        delays: dict[tuple[str, bool | None], float] | None = None,
        wall_clock_jumps: dict[tuple[str, bool | None], float] | None = None,
    ) -> None:
        self.clock = clock
        self.process = process
        self.status_local = list(status_local)
        self.status_full = list(status_full)
        self.report_slo = list(report_slo)
        self.tick_payloads = list(tick_payloads)
        self.log_text = log_text
        self._systemd_active = systemd_active
        self._local_processes = local_processes or []
        self.failures = failures or {}
        self.timeouts = timeouts or {}
        self.delays = delays or {}
        self.wall_clock_jumps = dict(wall_clock_jumps or {})
        self.commands: list[list[str]] = []
        self.timeout_requests: list[tuple[list[str], float | None]] = []

    def run(
        self,
        argv: list[str],
        *,
        timeout_seconds: float | None = None,
    ) -> CommandResult:
        self.commands.append(list(argv))
        self.timeout_requests.append((list(argv), timeout_seconds))
        key = self._command_key(argv)
        timed_out = self.timeouts.get(key, False)
        returncode = self.failures.get(key, 0)
        delay_seconds = self.delays.get(key, 0.0)
        wall_clock_jump_seconds = self.wall_clock_jumps.pop(key, 0.0)
        if delay_seconds:
            if timeout_seconds is not None and delay_seconds > timeout_seconds:
                self.clock.sleep(timeout_seconds)
                timed_out = True
            else:
                self.clock.sleep(delay_seconds)
        if wall_clock_jump_seconds:
            self.clock.shift_wall_clock(wall_clock_jump_seconds)
        stdout = ""
        stderr = ""
        if argv[:3] == ["git", "rev-parse", "HEAD"]:
            stdout = "abc123\n"
        elif argv[:3] == ["gh", "auth", "status"]:
            stdout = "logged in\n"
        elif self._is_consumer_command(argv) and "status" in argv:
            payloads = self.status_local if "--local-only" in argv else self.status_full
            stdout = json.dumps(self._next_payload(payloads))
        elif self._is_consumer_command(argv) and "report-slo" in argv:
            stdout = json.dumps(self._next_payload(self.report_slo))
        elif self._is_control_plane_command(argv) and "tick" in argv:
            stdout = json.dumps(self._next_payload(self.tick_payloads))
        elif self._is_consumer_command(argv) and "one-shot" in argv:
            payload = {
                "action": "claimed" if returncode == 0 else "idle",
                "reason": "" if returncode == 0 else "lease-cap",
                "issue_ref": None,
                "session_id": None,
                "pr_url": None,
                "exit_class": "success" if returncode == 0 else "idle",
            }
            if returncode == 4:
                payload["action"] = "error"
                payload["reason"] = "error"
                payload["exit_class"] = "error"
            stdout = json.dumps(payload)
        elif self._is_consumer_command(argv) and "drain" in argv:
            if (
                self.process.drain_exit_delay is not None
                and self.process.exit_at is None
            ):
                self.process.exit_at = (
                    self.clock.monotonic() + self.process.drain_exit_delay
                )
            stdout = '{"drain_requested": true}\n'
        elif self._is_consumer_command(argv) and "recover-interrupted" in argv:
            stdout = json.dumps({"recovered_leases": 0, "issue_refs": []})
        elif self._is_consumer_command(argv) and "reconcile" in argv:
            stdout = "reconcile dry run\n"
        elif argv[:2] == ["ps", "-eo"]:
            stdout = "\n".join(self._local_processes)
        elif argv[:3] == ["systemctl", "--user", "is-active"]:
            stdout = "inactive\n"
        else:
            stdout = "{}\n"
        if timed_out:
            returncode = 124
            stderr = (
                f"command timed out after {timeout_seconds} seconds"
                if timeout_seconds is not None
                else "command timed out"
            )
        if returncode != 0:
            if not stderr:
                stderr = f"simulated failure for {key}\n"
        started_at = self.clock.now_utc().isoformat()
        completed_at = self.clock.now_utc().isoformat()
        return CommandResult(
            argv=list(argv),
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
            started_at=started_at,
            completed_at=completed_at,
            timed_out=timed_out,
            timeout_seconds=timeout_seconds,
        )

    def start_consumer(self, argv: list[str], log_path: Path) -> FakeProcess:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(self.log_text, encoding="utf-8")
        return self.process

    def local_consumer_processes(
        self,
        *,
        timeout_seconds: float | None = None,
    ) -> list[str]:
        self.timeout_requests.append((["ps", "-eo", "pid=,command="], timeout_seconds))
        return list(self._local_processes)

    def systemd_consumer_active(
        self,
        *,
        timeout_seconds: float | None = None,
    ) -> bool:
        self.timeout_requests.append(
            (
                ["systemctl", "--user", "is-active", "startupai-consumer"],
                timeout_seconds,
            )
        )
        return self._systemd_active

    def _next_payload(self, items: list[dict]) -> dict:
        if not items:
            return {}
        if len(items) == 1:
            return items[0]
        return items.pop(0)

    def _command_key(self, argv: list[str]) -> tuple[str, bool | None]:
        if "status" in argv:
            return ("status", "--local-only" in argv)
        if "report-slo" in argv:
            return ("report-slo", "--local-only" in argv)
        if "tick" in argv:
            return ("tick", None)
        if "one-shot" in argv:
            return ("one-shot", None)
        return ("other", None)

    def _is_consumer_command(self, argv: list[str]) -> bool:
        return any(
            part.endswith("startupai_controller.board_consumer") for part in argv
        )

    def _is_control_plane_command(self, argv: list[str]) -> bool:
        return any(
            part.endswith("startupai_controller.board_control_plane") for part in argv
        )


def _status_payload(
    *,
    active_leases: int = 0,
    workers: list[dict] | None = None,
    recent_sessions: list[dict] | None = None,
    degraded: bool = False,
    degraded_reason: str | None = None,
    claim_suppressed_until: str | None = None,
    claim_suppressed_reason: str | None = None,
    last_successful_github_mutation_at: str | None = None,
) -> dict:
    return {
        "active_leases": active_leases,
        "workers": workers or [],
        "recent_sessions": recent_sessions or [],
        "degraded": degraded,
        "degraded_reason": degraded_reason,
        "claim_suppressed_until": claim_suppressed_until,
        "claim_suppressed_reason": claim_suppressed_reason,
        "last_rate_limit_at": None,
        "last_successful_board_sync_at": "2026-03-13T12:00:00+00:00",
        "last_successful_github_mutation_at": last_successful_github_mutation_at,
        "control_plane_health": {"health": "healthy", "reason_code": None},
    }


def _report_payload() -> dict:
    return {"windows": {"1h": {"durable_starts": 1}}}


def _tick_payload() -> dict:
    return {
        "health": "healthy",
        "github_request_counts": {"graphql": 4, "rest": 2},
    }


def _config(tmp_path: Path) -> LimitedLiveTestConfig:
    state_root = tmp_path / "state"
    return LimitedLiveTestConfig(
        state_root=state_root,
        artifact_root=state_root / "test-runs",
        db_path=None,
        duration_seconds=10,
        local_snapshot_seconds=5,
        full_snapshot_seconds=10,
        shutdown_poll_seconds=2,
        drain_timeout_seconds=6,
        post_quiesce_exit_seconds=2,
        command_timeout_seconds=60,
        consumer_interval_seconds=None,
        confirm_single_consumer=True,
        confirmation_note="verified no other host is running the consumer",
    )


def test_limited_live_test_happy_path_creates_artifacts(tmp_path: Path) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[
            _status_payload(),
            _status_payload(
                active_leases=1,
                workers=[{"issue_ref": "crew#84"}],
                recent_sessions=[{"issue_ref": "crew#84"}],
                last_successful_github_mutation_at="2026-03-13T12:05:00+00:00",
            ),
            _status_payload(),
        ],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        log_text="GitHub check completed cleanly\n",
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.meaningful_board_activity is True
    assert summary.shutdown_mode == "natural"
    assert summary.conclusion == "current transport acceptable for live use"


def test_one_shot_idle_json_is_nonfatal_for_harness(tmp_path: Path) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload()],
        status_full=[_status_payload()],
        report_slo=[_report_payload()],
        tick_payloads=[_tick_payload()],
        failures={("one-shot", None): 2},
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    result = backend.run(harness._consumer_one_shot_command())
    payload = json.loads(result.stdout)

    harness._require_one_shot_ok(result, "board_consumer one-shot --dry-run --json")
    assert harness._is_nonfatal_one_shot_idle(result, payload) is True


def test_one_shot_idle_requires_idle_action_for_nonfatal_harness_classification(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload()],
        status_full=[_status_payload()],
        report_slo=[_report_payload()],
        tick_payloads=[_tick_payload()],
        failures={("one-shot", None): 2},
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    result = backend.run(harness._consumer_one_shot_command())
    payload = json.loads(result.stdout)
    payload["action"] = "error"

    assert harness._is_nonfatal_one_shot_idle(result, payload) is False
    with pytest.raises(HarnessError, match="failed with exit code 2"):
        harness._require_one_shot_ok(
            CommandResult(
                argv=result.argv,
                returncode=result.returncode,
                stdout=json.dumps(payload),
                stderr=result.stderr,
                started_at=result.started_at,
                completed_at=result.completed_at,
                timed_out=result.timed_out,
                timeout_seconds=result.timeout_seconds,
            ),
            "board_consumer one-shot --dry-run --json",
        )


def test_limited_live_test_fails_preflight_when_systemd_active(tmp_path: Path) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload()],
        status_full=[_status_payload()],
        report_slo=[_report_payload()],
        tick_payloads=[_tick_payload()],
        systemd_active=True,
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    with pytest.raises(HarnessError, match="systemd"):
        harness.run()


def test_limited_live_test_marks_quiet_run_inconclusive(tmp_path: Path) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    quiet_status = _status_payload()
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[quiet_status, quiet_status, quiet_status],
        status_full=[quiet_status, quiet_status],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.meaningful_board_activity is False
    assert summary.conclusion == "test inconclusive due to insufficient board activity"


def test_limited_live_test_escalates_when_drain_does_not_finish(tmp_path: Path) -> None:
    clock = FakeClock()
    process = FakeProcess(
        clock=clock,
        drain_exit_delay=None,
        sigint_stops=False,
        sigterm_stops=True,
    )
    active_status = _status_payload(
        active_leases=1,
        workers=[{"issue_ref": "crew#84"}],
        recent_sessions=[{"issue_ref": "crew#84"}],
        last_successful_github_mutation_at="2026-03-13T12:05:00+00:00",
    )
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[active_status, active_status, active_status, active_status],
        status_full=[active_status, active_status],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.shutdown_mode == "forced"
    assert summary.forced_shutdown is True
    assert process.signals == [signal.SIGINT, signal.SIGTERM]
    assert "Drain timeout exceeded; sent SIGINT/SIGTERM escalation." in summary.issues


def test_limited_live_test_treats_forced_shutdown_as_acceptable_when_only_waiting_on_inflight_execution(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(
        clock=clock,
        drain_exit_delay=None,
        sigint_stops=False,
        sigterm_stops=True,
    )
    inflight_worker = {
        "issue_ref": "crew#84",
        "status": "running",
        "external_execution_started": True,
        "drain_wait_class": "finishing_inflight_execution",
    }
    active_status = _status_payload(
        active_leases=1,
        workers=[inflight_worker],
        recent_sessions=[inflight_worker],
        last_successful_github_mutation_at="2026-03-13T12:05:00+00:00",
    )
    active_status["local_only"] = True
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[active_status, active_status, active_status, active_status],
        status_full=[
            _status_payload(active_leases=1),
            _status_payload(active_leases=1),
        ],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.shutdown_mode == "forced"
    assert summary.forced_shutdown is True
    assert (
        "Drain timeout exceeded; sent SIGINT/SIGTERM escalation." not in summary.issues
    )
    assert (
        "forced shutdown occurred only after waiting on in-flight external execution"
        in summary.worked
    )
    assert summary.conclusion == "current transport acceptable for live use"
    assert summary.forced_shutdown_blockers == [
        {
            "issue_ref": "crew#84",
            "session_id": None,
            "phase": None,
            "external_execution_started": True,
            "active_seconds": None,
            "shutdown_signal_sent_at": None,
            "last_external_event_before_shutdown_signal_at": None,
            "shutdown_class_at_signal": None,
            "shutdown_class": "finishing_inflight_execution",
            "shutdown_reason": "drain_timeout_during_external_execution",
        }
    ]


def test_limited_live_test_records_snapshot_failure_and_continues(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    active_status = _status_payload(
        active_leases=1,
        workers=[{"issue_ref": "crew#84"}],
        recent_sessions=[{"issue_ref": "crew#84"}],
        last_successful_github_mutation_at="2026-03-13T12:05:00+00:00",
    )
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[active_status, active_status, _status_payload()],
        status_full=[active_status, active_status],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        failures={("tick", None): 1},
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.snapshot_failures
    assert any("tick" in item for item in summary.snapshot_failures)
    assert (harness.run_dir / "summary.json").exists()


def test_limited_live_test_preflight_timeout_is_fatal(tmp_path: Path) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload()],
        status_full=[_status_payload()],
        report_slo=[_report_payload()],
        tick_payloads=[_tick_payload()],
        timeouts={("status", False): True},
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    with pytest.raises(HarnessError, match="timed out"):
        harness.run()


def test_limited_live_test_records_snapshot_timeout_and_continues(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    active_status = _status_payload(
        active_leases=1,
        workers=[{"issue_ref": "crew#84"}],
        recent_sessions=[{"issue_ref": "crew#84"}],
        last_successful_github_mutation_at="2026-03-13T12:05:00+00:00",
    )
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[active_status, active_status, _status_payload()],
        status_full=[active_status, active_status],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        timeouts={("tick", None): True},
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert any("timed out after" in item for item in summary.snapshot_failures)
    assert (harness.run_dir / "summary.json").exists()


def test_limited_live_test_applies_timeout_to_preflight_process_checks(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload(), _status_payload(), _status_payload()],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    harness.run()

    assert (
        ["systemctl", "--user", "is-active", "startupai-consumer"],
        60.0,
    ) in backend.timeout_requests
    assert (["ps", "-eo", "pid=,command="], 60.0) in backend.timeout_requests
    assert (
        [
            "uv",
            "run",
            "python",
            "-m",
            "startupai_controller.board_consumer",
            "one-shot",
            "--dry-run",
            "--json",
        ],
        30.0,
    ) in backend.timeout_requests
    assert (
        [
            "uv",
            "run",
            "python",
            "-m",
            "startupai_controller.board_control_plane",
            "tick",
            "--json",
            "--dry-run",
        ],
        30.0,
    ) in backend.timeout_requests


def test_limited_live_test_omits_baseline_one_shot_snapshot(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload(), _status_payload(), _status_payload()],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    harness.run()

    baseline_dir = harness.run_dir / "artifacts" / "baseline"
    assert not any(
        path.name.startswith("one-shot-dry-run") for path in baseline_dir.iterdir()
    )


def test_limited_live_test_records_preflight_one_shot_timeout_and_continues(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload()],
        status_full=[_status_payload()],
        report_slo=[_report_payload()],
        tick_payloads=[_tick_payload()],
        delays={("one-shot", None): 45.0},
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.conclusion == "test inconclusive due to insufficient board activity"
    assert "preflight one-shot --dry-run timed out after 30.0 seconds" in summary.issues


def test_limited_live_test_surfaces_grouped_workflow_issues_without_changing_conclusion(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    log_text = "\n".join(
        [
            "2026-03-13 18:52:27,264 board-consumer ERROR PR creation skipped: head branch feat/129-example is not published on origin",
            "2026-03-13 18:52:32,537 board-consumer INFO Board reconciliation: ReconciliationResult(moved_ready=('app#129',), moved_in_progress=(), moved_review=(), moved_blocked=())",
            "2026-03-13 19:03:32,752 board-consumer ERROR PR creation skipped: head branch feat/129-example updated detail is not published on origin",
            "2026-03-13 19:03:43,896 board-consumer INFO Worker result [slot=3 issue=app#129]: CycleResult(action='claimed', issue_ref='app#129', session_id='709e167f71a8', reason='failed', pr_url=None)",
        ]
    )
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[
            _status_payload(
                active_leases=1,
                workers=[{"issue_ref": "app#129"}],
                recent_sessions=[{"issue_ref": "app#129"}],
            ),
            _status_payload(
                active_leases=1,
                workers=[{"issue_ref": "app#129"}],
                recent_sessions=[{"issue_ref": "app#129"}],
            ),
            _status_payload(),
        ],
        status_full=[
            _status_payload(
                active_leases=1,
                workers=[{"issue_ref": "app#129"}],
                recent_sessions=[{"issue_ref": "app#129"}],
            ),
            _status_payload(),
        ],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        log_text=log_text,
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.conclusion == "current transport acceptable for live use"
    assert summary.workflow_issues == [
        {
            "kind": "pr_creation_skipped",
            "issue_ref": "unknown",
            "branch_name": "feat/129-example",
            "count": 2,
            "sample": (
                "2026-03-13 18:52:27,264 board-consumer ERROR PR creation skipped: "
                "head branch feat/129-example is not published on origin"
            ),
        }
    ]
    assert any(
        "Workflow issue [pr_creation_skipped unknown branch=feat/129-example x2]"
        in item
        for item in summary.issues
    )


def test_limited_live_test_groups_reclaim_loops_and_worktree_blockers(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    log_text = "\n".join(
        [
            "2026-03-13 21:24:07,653 board-consumer INFO Worker result [slot=3 issue=crew#15]: CycleResult(action='error', issue_ref='crew#15', session_id=None, reason='worktree_in_use:existing worktree is dirty for feat/15-test: /tmp/worktree', pr_url=None)",
            "2026-03-13 21:26:45,049 board-consumer INFO Worker result [slot=2 issue=crew#10]: CycleResult(action='claimed', issue_ref='crew#10', session_id='1', reason='success', pr_url='https://github.com/StartupAI-site/startupai-crew/pull/216')",
            "2026-03-13 21:31:24,813 board-consumer INFO Worker result [slot=2 issue=crew#10]: CycleResult(action='claimed', issue_ref='crew#10', session_id='2', reason='success', pr_url='https://github.com/StartupAI-site/startupai-crew/pull/216')",
            "2026-03-13 21:35:01,380 board-consumer INFO Review queue: ReviewQueueDrainSummary(queued_count=17, due_count=0, seeded=(), removed=('crew#10',), verdict_backfilled=(), rerun=(), auto_merge_enabled=(), requeued=('crew#10',), blocked=(), skipped=(), escalated=(), partial_failure=False, error=None)",
            "2026-03-13 21:35:14,132 board-consumer INFO Worker result [slot=2 issue=crew#10]: CycleResult(action='claimed', issue_ref='crew#10', session_id='3', reason='success', pr_url='https://github.com/StartupAI-site/startupai-crew/pull/216')",
        ]
    )
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload(), _status_payload(), _status_payload()],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        log_text=log_text,
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.workflow_issues == [
        {
            "kind": "review_reclaim_loop",
            "issue_ref": "crew#10",
            "pr_url": "https://github.com/StartupAI-site/startupai-crew/pull/216",
            "count": 1,
            "sample": (
                "2026-03-13 21:31:24,813 board-consumer INFO Worker result [slot=2 issue=crew#10]: "
                "CycleResult(action='claimed', issue_ref='crew#10', session_id='2', "
                "reason='success', pr_url='https://github.com/StartupAI-site/startupai-crew/pull/216')"
            ),
        },
        {
            "kind": "worktree_reuse_blocked",
            "issue_ref": "crew#15",
            "count": 1,
            "sample": (
                "2026-03-13 21:24:07,653 board-consumer INFO Worker result [slot=3 issue=crew#15]: "
                "CycleResult(action='error', issue_ref='crew#15', session_id=None, "
                "reason='worktree_in_use:existing worktree is dirty for feat/15-test: /tmp/worktree', pr_url=None)"
            ),
        },
    ]


def test_limited_live_test_surfaces_single_reclaim_after_review_removal(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    log_text = "\n".join(
        [
            "2026-03-14 09:15:49,496 board-consumer INFO Review queue: ReviewQueueDrainSummary(queued_count=19, due_count=1, seeded=('crew#23',), removed=('crew#10',), verdict_backfilled=(), rerun=(), auto_merge_enabled=(), requeued=(), blocked=(), skipped=(), escalated=(), partial_failure=False, error=None)",
            "2026-03-14 09:19:54,396 board-consumer INFO Worker result [slot=2 issue=crew#10]: CycleResult(action='claimed', issue_ref='crew#10', session_id='d97a7d81521c', reason='success', pr_url='https://github.com/StartupAI-site/startupai-crew/pull/216')",
        ]
    )
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload(), _status_payload(), _status_payload()],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        log_text=log_text,
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.workflow_issues == [
        {
            "kind": "review_reclaimed_after_removal",
            "issue_ref": "crew#10",
            "pr_url": "https://github.com/StartupAI-site/startupai-crew/pull/216",
            "count": 1,
            "sample": (
                "2026-03-14 09:19:54,396 board-consumer INFO Worker result [slot=2 issue=crew#10]: "
                "CycleResult(action='claimed', issue_ref='crew#10', session_id='d97a7d81521c', "
                "reason='success', pr_url='https://github.com/StartupAI-site/startupai-crew/pull/216')"
            ),
        }
    ]


def test_limited_live_test_surfaces_review_summary_parse_failures_from_status_fallback(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    status_local = _status_payload()
    status_local["local_only"] = True
    status_local["review_summary"] = {
        "count": 1,
        "refs": ["app#43"],
        "source": "local-fallback",
        "error": (
            "Invalid issue ref 'StartupAI-site/app.startupai-site#43'. "
            "Expected format <prefix>#123 where prefix is one of app|crew|site."
        ),
    }
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[status_local, status_local, status_local],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.workflow_issues == [
        {
            "kind": "review_summary_parse_error",
            "issue_ref": "global",
            "count": 3,
            "sample": (
                "Invalid issue ref 'StartupAI-site/app.startupai-site#43'. Expected format "
                "<prefix>#123 where prefix is one of app|crew|site. "
                "[StartupAI-site/app.startupai-site#43]"
            ),
        }
    ]


def test_limited_live_test_attributes_multiline_workflow_error_from_forward_block_context(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    log_text = "\n".join(
        [
            "2026-03-15 02:45:00,000 board-consumer INFO Worker result [slot=1 issue=app#51]: CycleResult(action='claimed', issue_ref='app#51', session_id='sess-51', reason='success', pr_url='https://github.com/StartupAI-site/app.startupai-site/pull/240')",
            "2026-03-15 02:46:00,000 board-consumer ERROR codex exec failed (exit 124): command timed out",
            "session id: abc123",
            "Issue: PDF export (#61)",
            "Repository: StartupAI-site/app.startupai-site",
            "Branch: feat/61-pdf-powerpoint-export",
            "workdir: /home/chris/projects/worktrees/app/feat/61-pdf-powerpoint-export",
        ]
    )
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload(), _status_payload(), _status_payload()],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        log_text=log_text,
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert {
        "kind": "workflow_error",
        "issue_ref": "app#61",
        "count": 1,
        "sample": (
            "2026-03-15 02:46:00,000 board-consumer ERROR codex exec failed "
            "(exit 124): command timed out"
        ),
    } in summary.workflow_issues
    assert not any(
        issue["kind"] == "workflow_error" and issue["issue_ref"] == "app#51"
        for issue in summary.workflow_issues
    )


def test_limited_live_test_ignores_removed_then_immediate_completion_then_seed(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    log_text = "\n".join(
        [
            "2026-03-14 10:42:15,101 board-consumer INFO Review queue: ReviewQueueDrainSummary(queued_count=23, due_count=0, seeded=(), removed=('app#36',), verdict_backfilled=(), rerun=(), auto_merge_enabled=(), requeued=(), blocked=(), skipped=(), escalated=(), partial_failure=False, error=None)",
            "2026-03-14 10:42:16,114 board-consumer INFO Worker result [slot=4 issue=app#36]: CycleResult(action='claimed', issue_ref='app#36', session_id='3de57805d798', reason='success', pr_url='https://github.com/StartupAI-site/app.startupai-site/pull/224')",
            "2026-03-14 10:42:31,181 board-consumer INFO Review queue: ReviewQueueDrainSummary(queued_count=24, due_count=1, seeded=('app#36',), removed=(), verdict_backfilled=(), rerun=(), auto_merge_enabled=(), requeued=(), blocked=('StartupAI-site/app.startupai-site#224:missing-copilot-review',), skipped=(), escalated=(), partial_failure=False, error=None)",
        ]
    )
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload(), _status_payload(), _status_payload()],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        log_text=log_text,
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.workflow_issues == []


def test_limited_live_test_recovers_stale_local_state_during_preflight(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[
            _status_payload(active_leases=1, workers=[{"issue_ref": "app#41"}]),
            _status_payload(),
            _status_payload(),
            _status_payload(),
        ],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    harness.run()

    assert any("recover-interrupted" in " ".join(cmd) for cmd in backend.commands)


def test_limited_live_test_requests_drain_before_near_deadline_full_snapshot(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    config = _config(tmp_path)
    config.duration_seconds = 10
    config.full_snapshot_seconds = 9
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload(), _status_payload(), _status_payload()],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        delays={
            ("status", False): 30.0,
            ("report-slo", False): 30.0,
            ("tick", None): 30.0,
        },
    )
    harness = LimitedLiveTestHarness(config, backend, clock)

    summary = harness.run()

    assert summary.actual_drain_requested_at is not None
    assert summary.drain_request_slip_seconds == 0.0
    assert summary.snapshot_failures == []
    assert not any(
        issue["kind"] == "drain_request_late" for issue in summary.workflow_issues
    )


def test_limited_live_test_requests_drain_before_near_deadline_local_snapshot(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    config = _config(tmp_path)
    config.duration_seconds = 10
    config.local_snapshot_seconds = 4
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload(), _status_payload(), _status_payload()],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        delays={
            ("status", True): 30.0,
        },
    )
    harness = LimitedLiveTestHarness(config, backend, clock)

    summary = harness.run()

    assert summary.actual_drain_requested_at is not None
    assert summary.drain_request_slip_seconds == 0.0
    assert summary.snapshot_failures == []
    assert not any(
        issue["kind"] == "drain_request_late" for issue in summary.workflow_issues
    )
    assert (
        harness._consumer_status_command(local_only=True),
        6.0,
    ) in backend.timeout_requests


def test_limited_live_test_persists_drain_request_before_shutdown_wait(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=None)
    config = _config(tmp_path)
    config.duration_seconds = 10
    config.local_snapshot_seconds = 4
    config.full_snapshot_seconds = 900
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[
            _status_payload(),
            _status_payload(),
            _status_payload(),
            _status_payload(),
        ],
        status_full=[_status_payload()],
        report_slo=[_report_payload()],
        tick_payloads=[_tick_payload()],
    )
    observed: dict[str, object] = {}

    class StopAfterDrainRequest(RuntimeError):
        pass

    class InspectHarness(LimitedLiveTestHarness):
        def _drain_and_stop(self, child, *, drain_requested_at_monotonic=None) -> None:
            del child
            run_meta = json.loads((self.run_dir / "run_meta.json").read_text())
            observed["drain_requested_at_monotonic"] = drain_requested_at_monotonic
            observed["scheduled_drain_at"] = run_meta["scheduled_drain_at"]
            observed["actual_drain_requested_at"] = run_meta[
                "actual_drain_requested_at"
            ]
            observed["drain_request_slip_seconds"] = run_meta[
                "drain_request_slip_seconds"
            ]
            raise StopAfterDrainRequest

    harness = InspectHarness(config, backend, clock)

    with pytest.raises(StopAfterDrainRequest):
        harness.run()

    assert observed["drain_requested_at_monotonic"] is not None
    assert observed["scheduled_drain_at"] is not None
    assert observed["actual_drain_requested_at"] is not None
    assert observed["drain_request_slip_seconds"] == 0.0
    assert any("drain" in command for command in backend.commands)


def test_limited_live_test_preempts_near_deadline_full_bundle_command_by_remaining_time(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(clock=clock, drain_exit_delay=1)
    config = _config(tmp_path)
    config.duration_seconds = 45
    config.full_snapshot_seconds = 24
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[_status_payload(), _status_payload(), _status_payload()],
        status_full=[_status_payload(), _status_payload()],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        delays={
            ("status", False): 30.0,
        },
    )
    harness = LimitedLiveTestHarness(config, backend, clock)

    summary = harness.run()

    assert summary.drain_request_slip_seconds == 0.0
    assert summary.snapshot_failures == []
    assert (
        harness._consumer_status_command(local_only=False),
        21.0,
    ) in backend.timeout_requests


def test_limited_live_test_computes_drain_slip_from_monotonic_time_not_wall_clock(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    process = FakeProcess(
        clock=clock,
        drain_exit_delay=None,
        sigint_stops=False,
        sigterm_stops=True,
    )
    inflight_worker = {
        "issue_ref": "crew#84",
        "status": "running",
        "external_execution_started": True,
        "drain_wait_class": "finishing_inflight_execution",
    }
    active_status = _status_payload(
        active_leases=1,
        workers=[inflight_worker],
        recent_sessions=[inflight_worker],
        last_successful_github_mutation_at="2026-03-13T12:05:00+00:00",
    )
    active_status["local_only"] = True
    backend = FakeBackend(
        clock=clock,
        process=process,
        status_local=[active_status, active_status, active_status, active_status],
        status_full=[
            _status_payload(active_leases=1),
            _status_payload(active_leases=1),
        ],
        report_slo=[_report_payload(), _report_payload()],
        tick_payloads=[_tick_payload(), _tick_payload()],
        wall_clock_jumps={("status", True): 10.0},
    )
    harness = LimitedLiveTestHarness(_config(tmp_path), backend, clock)

    summary = harness.run()

    assert summary.drain_request_slip_seconds == 0.0
    assert not any(
        issue["kind"] == "drain_request_late" for issue in summary.workflow_issues
    )
